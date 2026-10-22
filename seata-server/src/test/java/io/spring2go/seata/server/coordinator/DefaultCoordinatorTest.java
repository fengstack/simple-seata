package io.spring2go.seata.server.coordinator;

import io.spring2go.seata.common.XID;
import io.spring2go.seata.core.exception.TransactionExceptionCode;
import io.spring2go.seata.core.model.BranchStatus;
import io.spring2go.seata.core.model.BranchType;
import io.spring2go.seata.core.model.GlobalStatus;
import io.spring2go.seata.core.protocol.ResultCode;
import io.spring2go.seata.core.protocol.transaction.*;
import io.spring2go.seata.core.rpc.RpcContext;
import io.spring2go.seata.core.rpc.ServerMessageSender;
import io.spring2go.seata.server.lock.LockManager;
import io.spring2go.seata.server.session.GlobalSession;
import io.spring2go.seata.server.session.SessionManager;
import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

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

    /**
     * 场景：RM 在二阶段回滚时不可达。
     * TC 绝不能宣布"回滚成功"——必须保留会话和行锁，之后由 timeoutCheck 重试，
     * 直到 RM 恢复并补偿成功为止。
     */
    @Test
    public void testRollbackFailureKeepsSessionForRetry() {
        FlakySender sender = new FlakySender(); // 第 1 次派发回滚指令直接抛异常，模拟 RM 掉线
        DefaultCoordinator flakyCoordinator = new DefaultCoordinator(sessionManager, lockManager, sender);

        RpcContext rpcContext = new RpcContext();
        rpcContext.setApplicationId("app-order");

        // 开启全局事务并注册一个带行锁的分支
        GlobalBeginRequest beginRequest = new GlobalBeginRequest();
        beginRequest.setTransactionName("orderTx");
        beginRequest.setTimeout(60000);
        long txId = XID.getTransactionId(flakyCoordinator.handle(beginRequest, rpcContext).getXid());

        BranchRegisterRequest branchRequest = new BranchRegisterRequest();
        branchRequest.setTransactionId(txId);
        branchRequest.setBranchType(BranchType.AT);
        branchRequest.setResourceId("db_order");
        branchRequest.setLockKey("t_order:3001");
        flakyCoordinator.handle(branchRequest, rpcContext);

        // 回滚：RM 掉线 → 失败。会话必须保留，锁不能释放
        GlobalRollbackRequest rollbackRequest = new GlobalRollbackRequest();
        rollbackRequest.setTransactionId(txId);
        GlobalRollbackResponse rollbackResponse = flakyCoordinator.handle(rollbackRequest, rpcContext);
        Assert.assertEquals(GlobalStatus.RollbackRetrying, rollbackResponse.getGlobalStatus());
        Assert.assertNotNull(sessionManager.findGlobalSession(txId));
        Assert.assertFalse(lockManager.isLockable(999999L, "db_order", "t_order:3001"));

        // timeoutCheck 重试：RM 还是没恢复 → 依然保留
        flakyCoordinator.timeoutCheck();
        Assert.assertNotNull(sessionManager.findGlobalSession(txId));

        // RM 恢复 → timeoutCheck 重试成功：会话删除、锁释放
        sender.recovered = true;
        flakyCoordinator.timeoutCheck();
        Assert.assertNull(sessionManager.findGlobalSession(txId));
        Assert.assertTrue(lockManager.isLockable(999999L, "db_order", "t_order:3001"));
        Assert.assertEquals(1, sender.rollbackRequests.size());
    }

    /**
     * 场景：TM 开了全局事务后宕机（一直不 commit/rollback）。
     * 超过 timeout 后 timeoutCheck 必须自动回滚并释放行锁，
     * 否则这行数据会被永久锁死。
     */
    @Test
    public void testTimeoutCheckAutoRollbacksOrphanTransaction() throws Exception {
        RecordingSender sender = new RecordingSender();
        DefaultCoordinator timeoutCoordinator = new DefaultCoordinator(sessionManager, lockManager, sender);

        RpcContext rpcContext = new RpcContext();
        rpcContext.setApplicationId("app-order");

        // timeout 设为 100ms，开完事务注册完分支就"消失"（不提交也不回滚）
        GlobalBeginRequest beginRequest = new GlobalBeginRequest();
        beginRequest.setTransactionName("orphanTx");
        beginRequest.setTimeout(100);
        long txId = XID.getTransactionId(timeoutCoordinator.handle(beginRequest, rpcContext).getXid());

        BranchRegisterRequest branchRequest = new BranchRegisterRequest();
        branchRequest.setTransactionId(txId);
        branchRequest.setBranchType(BranchType.AT);
        branchRequest.setResourceId("db_order");
        branchRequest.setLockKey("t_order:4001");
        timeoutCoordinator.handle(branchRequest, rpcContext);

        // 还没超时：timeoutCheck 不应该动它
        timeoutCoordinator.timeoutCheck();
        Assert.assertNotNull(sessionManager.findGlobalSession(txId));

        // 等待超时后再检查：自动回滚，会话删除、锁释放、RM 收到补偿指令
        Thread.sleep(200);
        timeoutCoordinator.timeoutCheck();
        Assert.assertNull(sessionManager.findGlobalSession(txId));
        Assert.assertTrue(lockManager.isLockable(888888L, "db_order", "t_order:4001"));
        Assert.assertEquals(1, sender.rollbackRequests.size());
    }

    /**
     * 可控的假 sender：recovered=false 时模拟 RM 掉线（抛异常），
     * 恢复后返回"回滚成功"。用于验证 TC 的失败重试逻辑。
     */
    private static class FlakySender implements ServerMessageSender {
        volatile boolean recovered = false;
        final List<BranchRollbackRequest> rollbackRequests = new CopyOnWriteArrayList<>();

        @Override
        public void sendRequest(String resourceId, String clientIP, String applicationId, Object request) {
        }

        @Override
        public void sendResponse(long msgId, String dbKey, String clientIp, String clientAppName, Object msg) {
        }

        @Override
        public void sendResponse(long msgId, Channel channel, Object msg) {
        }

        @Override
        public Object sendSynRequest(String dbKey, String clientIp, String clientAppName, Object msg, long timeout) {
            if (!recovered) {
                throw new RuntimeException("simulate RM down");
            }
            if (msg instanceof BranchRollbackRequest) {
                rollbackRequests.add((BranchRollbackRequest) msg);
            }
            BranchRollbackResponse response = new BranchRollbackResponse();
            response.setBranchStatus(BranchStatus.PhaseTwo_Rollbacked);
            return response;
        }

        @Override
        public Object sendSynRequest(String dbKey, String clientIp, String clientAppName, Object msg) throws IOException, TimeoutException {
            return sendSynRequest(dbKey, clientIp, clientAppName, msg, 0);
        }
    }

    /**
     * 记录型假 sender：记下收到的所有 BranchRollbackRequest，并返回"回滚成功"。
     */
    private static class RecordingSender implements ServerMessageSender {
        final List<BranchRollbackRequest> rollbackRequests = new CopyOnWriteArrayList<>();

        @Override
        public void sendRequest(String resourceId, String clientIP, String applicationId, Object request) {
        }

        @Override
        public void sendResponse(long msgId, String dbKey, String clientIp, String clientAppName, Object msg) {
        }

        @Override
        public void sendResponse(long msgId, Channel channel, Object msg) {
        }

        @Override
        public Object sendSynRequest(String dbKey, String clientIp, String clientAppName, Object msg, long timeout)
                throws IOException, TimeoutException {
            if (msg instanceof BranchRollbackRequest) {
                rollbackRequests.add((BranchRollbackRequest) msg);
            }
            BranchRollbackResponse response = new BranchRollbackResponse();
            response.setBranchStatus(BranchStatus.PhaseTwo_Rollbacked);
            return response;
        }

        @Override
        public Object sendSynRequest(String dbKey, String clientIp, String clientAppName, Object msg) throws IOException, TimeoutException {
            return sendSynRequest(dbKey, clientIp, clientAppName, msg, 0);
        }
    }
}
