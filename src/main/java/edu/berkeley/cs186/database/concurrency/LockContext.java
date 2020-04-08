package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;
    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;
    // The name of the resource this LockContext represents.
    protected ResourceName name;
    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;
    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;
    // The number of children that this LockContext has, if it differs from the number of times
    // LockContext#childContext was called with unique parameters: for a table, we do not
    // explicitly create a LockContext for every page (we create them as needed), but
    // the capacity should be the number of pages in the table, so we use this
    // field to override the return value for capacity().
    protected int capacity;

    // You should not modify or use this directly.
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.capacity = -1;
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to NAME from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /* ---------------------------------Start Helper functions-------------------------------------- */

    // check if newLockType can be the parent of its children
    private void checkLockTypeCanBeParent(LockType newParentLockType, TransactionContext transaction) {
        List<Lock> locksCurrHoldByTran = this.lockman.getLocks(transaction);
        for (Lock l : locksCurrHoldByTran) {
            LockContext potentialChildContext = fromResourceName(this.lockman, l.name);
            if (this.isParentOf(potentialChildContext)) { // if potentialChildContext is a children of the current context
                // if after changing the lockType, newLockType cannot be parent
                if (!LockType.canBeParentLock(newParentLockType, l.lockType)) {
                    throw new InvalidLockException("Lock can't be changed due to multigranularity locking constraint.");
                }
            }
        }
    }

    // check UnsupportedOperation
    private void checkUnsupportedOperation() {
        if (this.readonly) {
            throw new UnsupportedOperationException("Context is readonly.");
        }
    }

    // update parent's numChildLock if it has one
    private void updateParentNumChildLock(TransactionContext transaction, boolean shouldAdd) {
        if (this.parent != null) {
            try {
                int currLockNum = this.parent.numChildLocks.get(transaction.getTransNum());
                if (shouldAdd) {
                    this.parent.numChildLocks.put(transaction.getTransNum(), currLockNum + 1);
                } else {
                    this.parent.numChildLocks.put(transaction.getTransNum(), currLockNum - 1);
                }
            } catch (NullPointerException ne) {
                this.parent.numChildLocks.put(transaction.getTransNum(), 1);
            }
        }
    }

    private void releaseAllDescendantSIsLocks(TransactionContext transaction) {
        List<ResourceName> sisRN = sisDescendants(transaction);
        while (sisRN.size() != 0) {
            for (ResourceName trn : sisRN) {
                this.lockman.release(transaction, trn);
                fromResourceName(this.lockman, trn).updateParentNumChildLock(transaction, false);
            }
            sisRN = sisDescendants(transaction);
        }
    }

    private boolean isParentOf(LockContext potentialChildContext) {
        if (potentialChildContext.name.isDescendantOf(this.name)) {
            return potentialChildContext.parent.name.equals(this.name);
        } else {
            return false;
        }
    }

    /* ---------------------------------End Helper functions--------------------------------------- */

    /**
     * Acquire a LOCKTYPE lock, for transaction TRANSACTION.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by TRANSACTION
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
    throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implemented

        // check for UnsupportedOperation
        this.checkUnsupportedOperation();

        // check for invalid
        if (this.parent != null) {
            LockType parentLockType = this.parent.lockman.getLockType(transaction, this.parent.name);
            if (!LockType.canBeParentLock(parentLockType, lockType)) {
                throw new InvalidLockException("Cannot be children.");
            }
        }
        if (this.hasSIXAncestor(transaction) && (lockType == LockType.S || lockType == LockType.IS)) {
            throw new InvalidLockException("SIX redundant with LockType S or IS.");
        }

        // do acquire
        this.lockman.acquire(transaction, this.name, lockType);

        // update parent's numChildLock
        updateParentNumChildLock(transaction, true);
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     * @throws InvalidLockException if the lock cannot be released (because doing so would
     *  violate multigranularity locking constraints)
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
    throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implemented

        // check for UnsupportedOperation
        this.checkUnsupportedOperation();

        // check for InvalidLock
        this.checkLockTypeCanBeParent(LockType.NL, transaction);

        //do release
        this.lockman.release(transaction, this.name);

        // update parent's numChildLock
        updateParentNumChildLock(transaction, false);
    }

    /**
     * Promote TRANSACTION's lock to NEWLOCKTYPE. For promotion to SIX from IS/IX/S, all S,
     * IS, and SIX locks on descendants must be simultaneously released. The helper function sisDescendants
     * may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a NEWLOCKTYPE lock
     * @throws NoLockHeldException if TRANSACTION has no lock
     * @throws InvalidLockException if the requested lock type is not a promotion or promoting
     * would cause the lock manager to enter an invalid state (e.g. IS(parent), X(child)). A promotion
     * from lock type A to lock type B is valid if B is substitutable
     * for A and B is not equal to A, or if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implemented
        LockType oldLockType = this.lockman.getLockType(transaction, this.name);
        boolean sisixToSix = newLockType == LockType.SIX &&
                (oldLockType == LockType.IS || oldLockType == LockType.IX || oldLockType == LockType.S);

        // check UnsupportedOperation
        this.checkUnsupportedOperation();

        // check InvalidLock
        // You should disallow promotion to a SIX lock if an ancestor has SIX, because this would be redundant.
        if (this.hasSIXAncestor(transaction)) {
            if (newLockType == LockType.SIX) {
                throw new InvalidLockException("Ancestor contains a SIX lock, should not promote to SIX lock.");
            }
        }
        if (!(sisixToSix)) {
            this.checkLockTypeCanBeParent(newLockType, transaction); // check if newLockType can be the parent
        }

        // do promote
        this.lockman.promote(transaction, this.name, newLockType);

        // For promotion to SIX from IS/IX/S, simultaneously release all S, IS locks on descendants.
        if (sisixToSix) {
            this.releaseAllDescendantSIsLocks(transaction);
        }
    }

    /**
     * Escalate TRANSACTION's lock from descendants of this context to this level, using either
     * an S or X lock. There should be no descendant locks after this
     * call, and every operation valid on descendants of this context before this call
     * must still be valid. You should only make *one* mutating call to the lock manager,
     * and should only request information about TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *      IX(database) IX(table1) S(table2) S(table1 page3) X(table1 page5)
     * then after table1Context.escalate(transaction) is called, we should have:
     *      IX(database) X(table1) S(table2)
     *
     * You should not make any mutating calls if the locks held by the transaction do not change
     * (such as when you call escalate multiple times in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all relevant contexts, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if TRANSACTION has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implementing

        // check UnsupportedOperation
        this.checkUnsupportedOperation();

        // check if TRANSACTION has no lock at this level
        if (this.lockman.getLockType(transaction, name) == LockType.NL) {
            throw new NoLockHeldException("TRANSACTION has no lock at this level.");
        }



        return;
    }

    /**
     * Gets the type of lock that the transaction has at this level, either implicitly
     * (e.g. explicit S lock at higher level implies S lock at this level) or explicitly.
     * Returns NL if there is no explicit nor implicit lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(proj4_part2): implementing

        return this.lockman.getLockType(transaction, this.name);
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implemented

        return this.hasSIXAncestorHelper(transaction, 0);
    }

    private boolean hasSIXAncestorHelper(TransactionContext transaction, int counter) {
        if (this.lockman.getLockType(transaction, name) == LockType.SIX && counter != 0) {
            return true;
        }
        if (this.parent != null) {
            return this.parent.hasSIXAncestorHelper(transaction, counter+1);
        } else {
            return false;
        }
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or IS and are descendants of current context
     * for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction holds a S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implemented
        if (this.lockman.getLockType(transaction, this.name) == LockType.NL) {
            return new ArrayList<>();
        }

        List<Lock> locksCurrHoldByTran = this.lockman.getLocks(transaction);
        for (Lock l : locksCurrHoldByTran) {
            LockContext potentialChildContext = fromResourceName(this.lockman, l.name);
            if (this.isParentOf(potentialChildContext)) { // if potentialChildContext is a children of the current context
                // get all children's resourceName that contains S/IS lock
                List<ResourceName> childrenResNames = potentialChildContext.sisDescendants(transaction);
                // check if current resource contains a S/IS lock
                if (this.lockman.getLockType(transaction, this.name) == LockType.S
                        || this.lockman.getLockType(transaction, this.name) == LockType.IS) {
                    if (!childrenResNames.contains(this.name)) { // check if the resource name already added.
                        childrenResNames.add(this.name);
                    }
                }
                return childrenResNames;
            }
        }
        return new ArrayList<>();
    }

    /**
     * Get the type of lock that TRANSACTION holds at this level, or NL if no lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(proj4_part2): implemented
        return this.lockman.getLockType(transaction, this.name);
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name NAME (with a readable version READABLE).
     */
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                                           this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name NAME.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
    }

    /**
     * Sets the capacity (number of children).
     */
    public synchronized void capacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Gets the capacity. Defaults to number of child contexts if never explicitly set.
     */
    public synchronized int capacity() {
        return this.capacity < 0 ? this.children.size() : this.capacity;
    }

    /**
     * Gets the saturation (number of locks held on children / number of children) for
     * a single transaction. Saturation is 0 if number of children is 0.
     */
    public double saturation(TransactionContext transaction) {
        if (transaction == null || capacity() == 0) {
            return 0.0;
        }
        return ((double) numChildLocks.getOrDefault(transaction.getTransNum(), 0)) / capacity();
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

