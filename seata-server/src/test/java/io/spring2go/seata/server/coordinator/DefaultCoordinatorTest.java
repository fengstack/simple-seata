package io.spring2go.seata.server.coordinator;

import io.spring2go.seata.common.XID;
import io.spring2go.seata.core.exception.TransactionExceptionCode;
import io.spring2go.seata.core.model.BranchType;
import io.spring2go.seata.core.model.GlobalStatus;
import io.spring2go.seata.core.protocol.ResultCode;
import io.spring2go.seata.core.protocol.transaction.*;
import io.spring2go.seata.core.rpc.RpcContext;
import io.spring2go.seata.server.lock.LockManager;
import io.spring2go.seata.server.session.GlobalSession;
import io.spring2go.seata.server.session.SessionManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DefaultCoordinatorTest {

    private DefaultCoordinator coordinator;
    private SessionManager sessionManager;
    private LockManager lockManager;

    @Before
    public void setUp() {
        XID.setIpAddress("127.0.0.1");
        XID.setPort(8091);

        sessionManager = new SessionManager();
        lockManager = new LockManager();
        coordinator = new DefaultCoordinator(sessionManager, lockManager, null);
    }

    @Test
    public void testGlobalBeginAndBranchRegisterFlow() {
        RpcContext rpcContext = new RpcContext();
        rpcContext.setApplicationId("app-order");
        rpcContext.setTransactionServiceGroup("my_test_tx_group");

        // 1. 开启全局事务
        GlobalBeginRequest beginRequest = new GlobalBeginRequest();
        beginRequest.setTransactionName("createOrder");
        beginRequest.setTimeout(60000);

        GlobalBeginResponse beginResponse = coordinator.handle(beginRequest, rpcContext);
        Assert.assertNotNull(beginResponse);
        Assert.assertEquals(ResultCode.Success, beginResponse.getResultCode());

        String xid = beginResponse.getXid();
        Assert.assertNotNull(xid);
        Assert.assertTrue(xid.startsWith("127.0.0.1:8091:"));

        long txId = XID.getTransactionId(xid);
        GlobalSession session = sessionManager.findGlobalSession(txId);
        Assert.assertNotNull(session);
        Assert.assertEquals(GlobalStatus.Begin, session.getStatus());

        // 2. 注册分支事务 1 (申请行锁 t_order:1001)
        BranchRegisterRequest branchRequest1 = new BranchRegisterRequest();
        branchRequest1.setTransactionId(txId);
        branchRequest1.setBranchType(BranchType.AT);
        branchRequest1.setResourceId("jdbc:mysql://127.0.0.1:3306/db_order");
        branchRequest1.setLockKey("t_order:1001");

        BranchRegisterResponse branchResponse1 = coordinator.handle(branchRequest1, rpcContext);
        Assert.assertEquals(ResultCode.Success, branchResponse1.getResultCode());
        Assert.assertTrue(branchResponse1.getBranchId() > 0);

        // 3. 开启另一全局事务，试图竞争同一行锁
        GlobalBeginResponse beginResponse2 = coordinator.handle(beginRequest, rpcContext);
        long txId2 = XID.getTransactionId(beginResponse2.getXid());

        BranchRegisterRequest conflictBranchRequest = new BranchRegisterRequest();
        conflictBranchRequest.setTransactionId(txId2);
        conflictBranchRequest.setBranchType(BranchType.AT);
        conflictBranchRequest.setResourceId("jdbc:mysql://127.0.0.1:3306/db_order");
        conflictBranchRequest.setLockKey("t_order:1001");

        BranchRegisterResponse conflictResponse = coordinator.handle(conflictBranchRequest, rpcContext);
        Assert.assertEquals(ResultCode.Failed, conflictResponse.getResultCode());
        Assert.assertEquals(TransactionExceptionCode.LockKeyConflict, conflictResponse.getTransactionExceptionCode());

        // 4. 提交第一个全局事务，验证释放行锁与移除会话
        GlobalCommitRequest commitRequest = new GlobalCommitRequest();
        commitRequest.setTransactionId(txId);

        GlobalCommitResponse commitResponse = coordinator.handle(commitRequest, rpcContext);
        Assert.assertEquals(ResultCode.Success, commitResponse.getResultCode());
        Assert.assertNull(sessionManager.findGlobalSession(txId));

        // 5. 锁释放后，事务 2 重新注册分支成功
        BranchRegisterResponse retryResponse = coordinator.handle(conflictBranchRequest, rpcContext);
        Assert.assertEquals(ResultCode.Success, retryResponse.getResultCode());

        // 6. 回滚事务 2
        GlobalRollbackRequest rollbackRequest = new GlobalRollbackRequest();
        rollbackRequest.setTransactionId(txId2);
        GlobalRollbackResponse rollbackResponse = coordinator.handle(rollbackRequest, rpcContext);
        Assert.assertEquals(ResultCode.Success, rollbackResponse.getResultCode());
        Assert.assertNull(sessionManager.findGlobalSession(txId2));
    }
}
