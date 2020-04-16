package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction == null || lockType == LockType.NL || lockContext.readonly) {
            return;
        }

        // check if auto escalating
        LockContext shouldBeTable = lockContext.parentContext();
        if (shouldBeTable != null) {
            if (shouldBeTable.isTableContext() && shouldBeTable.isAutoEscalatingEnabled()
                    && shouldBeTable.saturation(transaction) >= 0.2 && shouldBeTable.capacity() >= 10) {
                shouldBeTable.escalate(transaction);
            }
        }

        LockType currLockType = lockContext.getExplicitLockType(transaction);
        if(lockType == LockType.S) { // handle S lockType
            if (lockContext.getEffectiveLockType(transaction) == LockType.S
                    || lockContext.getEffectiveLockType(transaction) == LockType.X) {
                return;
            }
            switch (currLockType) {
                case S:
                case X:
                case SIX:
                    return;
                case IS:
//                    ensureAncestorFit(transaction, LockType.IS, lockContext.parentContext());
                    lockContext.escalate(transaction);
                    break;
                case IX:
//                    ensureAncestorFit(transaction, LockType.IX, lockContext.parentContext());
                    lockContext.promote(transaction, LockType.SIX);
                    break;
                case NL:
                    ensureAncestorFit(transaction, LockType.IS, lockContext.parentContext());
                    lockContext.acquire(transaction, lockType);
                    break;
            }
        } else if (lockType == LockType.X) { // handle X lockType
            if (lockContext.getEffectiveLockType(transaction) == LockType.X) {
                return;
            }
            ensureAncestorFit(transaction, LockType.IX, lockContext.parentContext());
            switch (currLockType) {
                case X: return;
                case S:
                    lockContext.promote(transaction, lockType);
                    break;
                case IS:
                    lockContext.escalate(transaction);
                    lockContext.promote(transaction, LockType.X);
                    break;
                case IX:
                case SIX:
                    lockContext.escalate(transaction);
                    break;
                case NL:
                    lockContext.acquire(transaction, lockType);
                    break;
            }
        }

    }


    private static void ensureAncestorFit(TransactionContext transaction, LockType newLockType, LockContext lockContext) {
        if (lockContext == null) {
            return;
        }
        LockContext pCont = lockContext.parentContext();
        switch (newLockType) {
            case IS:
                if (pCont != null) {
                    ensureAncestorFit(transaction, LockType.IS, pCont);
                }
                if (!LockType.substitutable(lockContext.getExplicitLockType(transaction), LockType.IS)) {
                    lockContext.acquire(transaction, LockType.IS);
                }
                break;
            case IX:
            case SIX:
                if (pCont != null) {
                    ensureAncestorFit(transaction, LockType.IX, pCont);
                }
                LockType currOldLock = lockContext.getExplicitLockType(transaction);
                if (currOldLock == LockType.NL) {
                    lockContext.acquire(transaction, LockType.IX);
                } else if (currOldLock == LockType.S) {
                    lockContext.promote(transaction, LockType.SIX);
                } else {
                    if (!LockType.substitutable(currOldLock, LockType.IX)) {
                        lockContext.promote(transaction, LockType.IX);
                    }
                }
                break;
        }
    }

    // Only get the sufficient intent locks but not the actual lock on the current level.
    public static void onlyEnsureSufficientLockHeldForX(LockContext lockContext) {
        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction == null || lockContext.readonly) {
            return;
        }

        ensureAncestorFit(transaction, LockType.IX, lockContext.parentContext());
    }
}
