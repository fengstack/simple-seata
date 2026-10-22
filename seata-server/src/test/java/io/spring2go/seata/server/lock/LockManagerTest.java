package io.spring2go.seata.server.lock;

import io.spring2go.seata.core.model.BranchType;
import io.spring2go.seata.server.session.BranchSession;
import io.spring2go.seata.server.session.GlobalSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LockManagerTest {

    private LockManager lockManager;

    @Before
    public void setUp() {
        lockManager = new LockManager();
    }

    @Test
    public void testAcquireAndReleaseLock() {
        long txId1 = 1001L;
        String resourceId = "jdbc:mysql://localhost:3306/db_shop";
        String lockKey1 = "t_user:1,2;t_order:1001";

        BranchSession branchSession1 = new BranchSession(txId1, 2001L, resourceId,
                BranchType.AT, lockKey1, "client1");

        // 1. 事务 1 加锁成功
        boolean success = lockManager.acquireLock(branchSession1);
        Assert.assertTrue(success);

        // 事务 1 自身重复检查可加锁
        Assert.assertTrue(lockManager.isLockable(txId1, resourceId, "t_user:1"));
        Assert.assertTrue(lockManager.isLockable(txId1, resourceId, "t_order:1001"));

        // 2. 事务 2 尝试锁定同一行 t_user:2，产生锁冲突
        long txId2 = 1002L;
        String lockKey2 = "t_account:5;t_user:2";
        BranchSession branchSession2 = new BranchSession(txId2, 2002L, resourceId,
                BranchType.AT, lockKey2, "client2");

        boolean conflict = lockManager.acquireLock(branchSession2);
        Assert.assertFalse(conflict);

        // 验证原子回滚：事务 2 未成功，其申请的 t_account:5 必须已被完全回滚，不能泄漏
        long txId3 = 1003L;
        Assert.assertTrue(lockManager.isLockable(txId3, resourceId, "t_account:5"));

        // 3. 事务 1 释放锁
        lockManager.releaseLock(branchSession1);

        // 事务 2 再次尝试加锁成功
        boolean success2 = lockManager.acquireLock(branchSession2);
        Assert.assertTrue(success2);

        // 4. 全局会话批量释放锁
        GlobalSession globalSession = new GlobalSession(txId2, "127.0.0.1:8091:1002", "tx2", 60000);
        globalSession.add(branchSession2);
        lockManager.releaseLocks(globalSession);

        // 所有锁已释放
        Assert.assertTrue(lockManager.isLockable(txId3, resourceId, "t_account:5"));
        Assert.assertTrue(lockManager.isLockable(txId3, resourceId, "t_user:2"));
    }

    @Test
    public void testEmptyAndNullLockKey() {
        BranchSession branch = new BranchSession(1001L, 2001L, "res1", BranchType.AT, null, "client1");
        Assert.assertTrue(lockManager.acquireLock(branch));

        branch.setLockKey("");
        Assert.assertTrue(lockManager.acquireLock(branch));
    }
}
