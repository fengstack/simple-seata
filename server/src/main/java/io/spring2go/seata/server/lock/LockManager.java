package io.spring2go.seata.server.lock;

import io.spring2go.seata.server.session.BranchSession;
import io.spring2go.seata.server.session.GlobalSession;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory Global Row Lock Manager for Seata AT mode.
 */
public class LockManager {
    // Inverted index: rowKey (resourceId^^^tableName^^^pk) -> transactionId
    private final ConcurrentMap<String, Long> lockMap = new ConcurrentHashMap<>();

    /**
     * Parse Seata standard lockKey: tableName:pk1,pk2;tableName2:pk3
     */
    public List<RowLock> parseLockKey(String resourceId, String lockKey) {
        if (lockKey == null || lockKey.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<RowLock> rowLocks = new ArrayList<>();
        String[] tableClauses = lockKey.split(";");
        for (String tableClause : tableClauses) {
            String trimmed = tableClause.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String[] parts = trimmed.split(":");
            String tableName = parts[0].trim();
            if (parts.length > 1) {
                String[] pks = parts[1].split(",");
                for (String pk : pks) {
                    String trimmedPk = pk.trim();
                    if (!trimmedPk.isEmpty()) {
                        rowLocks.add(new RowLock(resourceId, tableName, trimmedPk));
                    }
                }
            }
        }
        return rowLocks;
    }

    /**
     * Acquire lock atomically for branchSession.
     * If any row is already locked by another transaction, returns false.
     */
    public synchronized boolean acquireLock(BranchSession branchSession) {
        if (branchSession == null || branchSession.getLockKey() == null || branchSession.getLockKey().trim().isEmpty()) {
            return true;
        }
        List<RowLock> rowLocks = parseLockKey(branchSession.getResourceId(), branchSession.getLockKey());
        if (rowLocks.isEmpty()) {
            return true;
        }

        long txId = branchSession.getTransactionId();
        // Phase 1: Check conflict
        for (RowLock rowLock : rowLocks) {
            Long holdingTxId = lockMap.get(rowLock.toRowKey());
            if (holdingTxId != null && holdingTxId != txId) {
                return false;
            }
        }

        // Phase 2: Acquire all
        for (RowLock rowLock : rowLocks) {
            lockMap.put(rowLock.toRowKey(), txId);
        }
        return true;
    }

    /**
     * Check if the lockKey is lockable for given txId.
     */
    public synchronized boolean isLockable(Long txId, String resourceId, String lockKey) {
        if (lockKey == null || lockKey.trim().isEmpty()) {
            return true;
        }
        List<RowLock> rowLocks = parseLockKey(resourceId, lockKey);
        for (RowLock rowLock : rowLocks) {
            Long holdingTxId = lockMap.get(rowLock.toRowKey());
            if (holdingTxId != null && !holdingTxId.equals(txId)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Release all locks held by a single branchSession.
     */
    public synchronized void releaseLock(BranchSession branchSession) {
        if (branchSession == null || branchSession.getLockKey() == null) {
            return;
        }
        List<RowLock> rowLocks = parseLockKey(branchSession.getResourceId(), branchSession.getLockKey());
        long txId = branchSession.getTransactionId();
        for (RowLock rowLock : rowLocks) {
            Long holdingTxId = lockMap.get(rowLock.toRowKey());
            if (holdingTxId != null && holdingTxId == txId) {
                lockMap.remove(rowLock.toRowKey());
            }
        }
    }

    /**
     * Release all locks held by all branchSessions under a globalSession.
     */
    public synchronized void releaseLocks(GlobalSession globalSession) {
        if (globalSession == null || globalSession.getBranchSessions() == null) {
            return;
        }
        for (BranchSession branch : globalSession.getBranchSessions()) {
            releaseLock(branch);
        }
    }
}
