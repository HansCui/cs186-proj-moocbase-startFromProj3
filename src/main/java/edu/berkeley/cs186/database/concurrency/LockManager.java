package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods you should implement!
        // Make sure to use these helper methods to abstract your code and
        // avoid re-implementing every time!

        /**
         * Check if a LOCKTYPE lock is compatible with preexisting locks.
         * Allows conflicts for locks held by transaction id EXCEPT.
         */
        boolean checkCompatible(LockType lockType, long except) {

            for (Lock l : locks) {
                if (!LockType.compatible(l.lockType, lockType)) {
                    if (l.transactionNum == except) {
                        continue;
                    }

                    return false;
                }
            }

            return true;
        }

        /**
         * Gives the transaction the lock LOCK. Assumes that the lock is compatible.
         * Updates lock on resource if the transaction already has a lock.
         */
        void grantOrUpdateLock(Lock lock) {
            long transNum = lock.transactionNum;

            // Dealing with transactionLocks.
            // check if transactionLocks contains the transactionNum in lock
            if (!transactionLocks.containsKey(transNum)) {
                transactionLocks.put(transNum, new ArrayList<>());
                transactionLocks.get(transNum).add(lock);
            } else { // transactionLocks contains the transactionNum in lock
                if (transactionLocks.get(transNum).isEmpty()) { // check if the lock list empty
                    transactionLocks.get(transNum).add(lock);
                } else { // lock list not empty
                    boolean containedLock = false; // variable to check if the lock list contains an updatable old lock
                    // list of locks add to transactionLocks.get(transNum) (Prevent ConcurrentModificationException)
                    List<Lock> locksToAddTemp = new ArrayList<>();

                    for (Lock l : transactionLocks.get(transNum)) {
                        if (l.name == lock.name) {
                            containedLock = true;
                            l.lockType = lock.lockType;
                        }
                        if (!containedLock) { // if the lock list does not contain an updatable old lock
                            locksToAddTemp.add(lock);
                            containedLock = true;
                        }
                    }
                    transactionLocks.get(transNum).addAll(locksToAddTemp);
                }
            }

            // Deal with resourceLock
            boolean resourceContainedLock = false;
            for (Lock lk : this.locks) {
                if (lk.transactionNum == transNum) {
                    resourceContainedLock = true;
                }
            }
            if (!resourceContainedLock) {
                this.locks.add(lock);
            }

        }

        /**
         * Releases the lock LOCK and processes the queue. Assumes it had been granted before.
         */
        void releaseLock(Lock lock) {
            long transNum = lock.transactionNum;

            transactionLocks.get(transNum).remove(lock);
            this.locks.remove(lock);

            processQueue();
        }

        /**
         * Adds a request for LOCK by the transaction to the queue and puts the transaction
         * in a blocked state.
         */
        void addToQueue(LockRequest request, boolean addFront) {
            if (addFront) {
                this.waitingQueue.addFirst(request);
            } else {
                this.waitingQueue.addLast(request);
            }
            request.transaction.prepareBlock();
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted.
         */
        private void processQueue() {

            while (!this.waitingQueue.isEmpty()) {
                long transNum = this.waitingQueue.peek().lock.transactionNum;
                if (checkCompatible(this.waitingQueue.peek().lock.lockType, transNum)) {
                    LockRequest lr = this.waitingQueue.removeFirst();
                    // release locks in lr's releaseLocks
                    for (Lock rls : lr.releasedLocks) {
                        release(lr.transaction, rls.name);
                    }

                    grantOrUpdateLock(lr.lock);
                    lr.transaction.unblock();
                } else {
                    break;
                }
            }
        }

        /**
         * Gets the type of lock TRANSACTION has on this resource.
         */
        LockType getTransactionLockType(long transaction) {

            for (Lock l : this.locks) {
                if (l.transactionNum == transaction) {
                    return l.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // Extra Helper Funcs

//    private void checkDuplicateLockRequest(long transNum, ResourceName name) {
//        // check if there is a Duplicate Lock
//        if (this.transactionLocks.containsKey(transNum)) {
//            for (Lock l : this.transactionLocks.get(transNum)) {
//                if (l.name.equals(name) && l.lockType != LockType.NL) {
//                    throw new DuplicateLockRequestException("Duplicate Lock Exists.");
//                }
//            }
//        }
//    }

    // construct the releaseLock list from a list of resourceNames
    private List<Lock> resourceNamesToLocks(TransactionContext transaction, List<ResourceName> resourceNames) {
        List<Lock> transLocks = this.getLocks(transaction);
        List<Lock> lockList = new ArrayList<>();

        for (ResourceName rsN : resourceNames) {
            for (Lock trL : transLocks) {
                if (trL.name.equals(rsN)) {
                    lockList.add(trL);
                }
            }
        }
        return lockList;
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            long transNum = transaction.getTransNum();
            Lock newLock = new Lock(name, lockType, transNum);

            // check if there is a Duplicate Lock
            if (this.getLockType(transaction, name) != LockType.NL && !releaseLocks.contains(name)) {
                throw new DuplicateLockRequestException("Lock on NAME is held by TRANSACTION and isn't being released");
            }

            // check if there is a noLockHeld
            for (ResourceName rcN : releaseLocks) {
                boolean noSuchLock = true;
                List<Lock> locksOnTrans = getLocks(transaction);
                for (Lock lot : locksOnTrans) {
                    if (lot.name.equals(rcN)) {
                        noSuchLock = false;
                    }
                }
                if (noSuchLock) {
                    throw new NoLockHeldException("No Such Lock Exists.");
                }
            }

            // check if resourceEntry contains the resourceName
            if (!this.resourceEntries.keySet().contains(name)) {
                this.resourceEntries.put(name, getResourceEntry(name));
            }

            // do acquire
            ResourceEntry re = this.resourceEntries.get(name);
            // construct the releaseLock list
            List<Lock> lockListToRelease = resourceNamesToLocks(transaction, releaseLocks);

            if (!re.checkCompatible(lockType, transNum)) {
                LockRequest tempLR = new LockRequest(transaction, newLock, lockListToRelease);
                re.addToQueue(tempLR, true);
                shouldBlock = true;
            } else {
                re.grantOrUpdateLock(newLock);
                // release Locks after acquire
                for (ResourceName rsN : releaseLocks) {
                    if (newLock.name.equals(rsN)) {
                        continue;
                    }
                    this.release(transaction, rsN);
                }
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            long transNum = transaction.getTransNum();
            Lock newLock = new Lock(name, lockType, transNum);

            // check if there is a Duplicate Lock
            if (this.getLockType(transaction, name) != LockType.NL) {
                throw new DuplicateLockRequestException("Lock on NAME is held by TRANSACTION.");
            }

            // check if resourceEntry contains the resourceName
            if (!this.resourceEntries.keySet().contains(name)) {
                this.resourceEntries.put(name, getResourceEntry(name));
            }

            ResourceEntry re = this.resourceEntries.get(name);

            // check lock compatibility
            if (!re.checkCompatible(lockType, transNum) || !re.waitingQueue.isEmpty()) {
                LockRequest tempLr = new LockRequest(transaction, newLock);
                re.addToQueue(tempLr, false);
                shouldBlock = true;
            } else {
                re.grantOrUpdateLock(newLock);
            }
        }
        // block the transaction
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        // You may modify any part of this method.
        synchronized (this) {
            long transNum = transaction.getTransNum();
            ResourceEntry re = this.resourceEntries.get(name);
            Lock lockToRelease = null;

            // check if lock exists; if exists, do release
            if (re == null) {
                throw new NoLockHeldException("No Such Lock exists");
            } else {
                boolean noLockExist = true;
                for (Lock l : re.locks) {
                    if (l.transactionNum == transNum) {
                        noLockExist = false;
                        // get the release lock with the same transNum and resourceName
                        lockToRelease = l;
                    }
                }
                if (noLockExist) {
                    throw new NoLockHeldException("No Such Lock exists");
                }
            }
            re.releaseLock(lockToRelease);
        }
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            long transNum = transaction.getTransNum();
            List<Lock> locksOnTransaction = getLocks(transaction);
            Lock newLock = new Lock(name, newLockType, transNum);
            Lock oldLock = null;

            // check if there is a Duplicate Lock
            if (this.getLockType(transaction, name) == newLockType) {
                throw new DuplicateLockRequestException("TRANSACTION already has a NEWLOCKTYPE lock on NAME.");
            }

            // check if there is a NoLockHeld
            boolean noLockHeld = true;
            for (Lock lot : locksOnTransaction) {
                if (lot.name.equals(name)) {
                    noLockHeld = false;
                    oldLock = lot;
                }
            }
            if (noLockHeld) {
                throw new NoLockHeldException("No SUch Lock Exists.");
            }

            // check if there is a InvalidLock promotion
            if (!LockType.substitutable(newLockType, oldLock.lockType) || oldLock.equals(newLock)) {
                throw new InvalidLockException("Invalid Promotion.");
            }

            ResourceEntry re = this.resourceEntries.get(name);

            if (re == null) {
                throw new NoLockHeldException("No Such Lock exists");
            } else {
                if (!re.checkCompatible(newLockType, transNum)) {
                    LockRequest tempLr = new LockRequest(transaction, newLock);
                    re.addToQueue(tempLr, true);
                    shouldBlock = true;
                } else {
                    re.grantOrUpdateLock(newLock);
                }
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        long transNum = transaction.getTransNum();

        ResourceEntry tempRE = this.resourceEntries.get(name);
        if (tempRE == null) {
            return LockType.NL;
        }
        return tempRE.getTransactionLockType(transNum);

//        return this.resourceEntries.get(name).getTransactionLockType(transNum);

    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
