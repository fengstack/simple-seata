package io.spring2go.seata.server.session;

import io.spring2go.seata.core.model.BranchStatus;
import io.spring2go.seata.core.model.BranchType;
import io.spring2go.seata.core.model.GlobalStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SessionManagerTest {

    private SessionManager sessionManager;

    @Before
    public void setUp() {
        sessionManager = new SessionManager();
    }

    @Test
    public void testGlobalAndBranchSessionLifecycle() {
        long txId = 1001L;
        String xid = "127.0.0.1:8091:" + txId;
        GlobalSession globalSession = new GlobalSession(txId, xid, "testTx", 60000);

        Assert.assertEquals(txId, globalSession.getTransactionId());
        Assert.assertEquals(xid, globalSession.getXid());
        Assert.assertEquals("testTx", globalSession.getTransactionName());
        Assert.assertEquals(GlobalStatus.Begin, globalSession.getStatus());
        Assert.assertTrue(globalSession.isActive());

        // 添加到 SessionManager
        sessionManager.addGlobalSession(globalSession);
        GlobalSession found = sessionManager.findGlobalSession(txId);
        Assert.assertNotNull(found);
        Assert.assertEquals(xid, found.getXid());

        // 添加 BranchSession
        BranchSession branch1 = new BranchSession(txId, 2001L, "jdbc:mysql://127.0.0.1:3306/db1",
                BranchType.AT, "t_user:1", "client1");
        branch1.setStatus(BranchStatus.Registered);

        BranchSession branch2 = new BranchSession(txId, 2002L, "jdbc:mysql://127.0.0.1:3306/db2",
                BranchType.AT, "t_order:100", "client2");
        branch2.setStatus(BranchStatus.Registered);

        globalSession.add(branch1);
        globalSession.add(branch2);

        Assert.assertEquals(2, globalSession.getBranchSessions().size());
        Assert.assertEquals(branch1, globalSession.getBranch(2001L));
        Assert.assertEquals(branch2, globalSession.getBranch(2002L));

        // 状态流转
        globalSession.changeStatus(GlobalStatus.Committing);
        Assert.assertEquals(GlobalStatus.Committing, globalSession.getStatus());

        globalSession.changeStatus(GlobalStatus.Committed);
        Assert.assertEquals(GlobalStatus.Committed, globalSession.getStatus());
        Assert.assertFalse(globalSession.isActive());

        // 移除全局会话
        sessionManager.removeGlobalSession(globalSession);
        Assert.assertNull(sessionManager.findGlobalSession(txId));
    }
}
