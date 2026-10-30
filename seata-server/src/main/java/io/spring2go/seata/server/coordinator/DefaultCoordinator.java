package io.spring2go.seata.server.coordinator;

import io.spring2go.seata.common.XID;
import io.spring2go.seata.core.exception.TransactionExceptionCode;
import io.spring2go.seata.core.model.BranchStatus;
import io.spring2go.seata.core.model.GlobalStatus;
import io.spring2go.seata.core.protocol.AbstractMessage;
import io.spring2go.seata.core.protocol.AbstractResultMessage;
import io.spring2go.seata.core.protocol.ResultCode;
import io.spring2go.seata.core.protocol.transaction.*;
import io.spring2go.seata.core.rpc.RpcContext;
import io.spring2go.seata.core.rpc.ServerMessageSender;
import io.spring2go.seata.core.rpc.TransactionMessageHandler;
import io.spring2go.seata.server.lock.LockManager;
import io.spring2go.seata.server.session.BranchSession;
import io.spring2go.seata.server.session.GlobalSession;
import io.spring2go.seata.server.session.SessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default Transaction Coordinator handling TC inbound requests and orchestrating two-phase commit/rollback.
 */
public class DefaultCoordinator implements TCInboundHandler, TransactionMessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCoordinator.class);

    private final SessionManager sessionManager;
    private final LockManager lockManager;
    private final ServerMessageSender serverMessageSender;

    private static final AtomicLong TRANSACTION_ID_GENERATOR = new AtomicLong(1000);
    private static final AtomicLong BRANCH_ID_GENERATOR = new AtomicLong(10000);
    private static final long DEFAULT_GLOBAL_TX_TIMEOUT = 60000;

    public DefaultCoordinator(SessionManager sessionManager, LockManager lockManager, ServerMessageSender serverMessageSender) {
        this.sessionManager = sessionManager != null ? sessionManager : new SessionManager();
        this.lockManager = lockManager != null ? lockManager : new LockManager();
        this.serverMessageSender = serverMessageSender;
    }

    public DefaultCoordinator() {
        this(new SessionManager(), new LockManager(), null);
    }

    @Override
    public GlobalBeginResponse handle(GlobalBeginRequest request, RpcContext rpcContext) {
        GlobalBeginResponse response = new GlobalBeginResponse();
        try {
            long txId = TRANSACTION_ID_GENERATOR.incrementAndGet();
            String xid = XID.generateXID(txId);
            GlobalSession session = new GlobalSession(txId, xid, request.getTransactionName(), request.getTimeout());
            sessionManager.addGlobalSession(session);

            response.setXid(xid);
            response.setResultCode(ResultCode.Success);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Successfully began global transaction [" + xid + "] with name: " + request.getTransactionName());
            }
        } catch (Exception ex) {
            LOGGER.error("Failed to begin global transaction", ex);
            response.setResultCode(ResultCode.Failed);
            response.setMsg(ex.getMessage());
        }
        return response;
    }

    @Override
    public BranchRegisterResponse handle(BranchRegisterRequest request, RpcContext rpcContext) {
        BranchRegisterResponse response = new BranchRegisterResponse();
        try {
            long txId = request.getTransactionId();
            GlobalSession session = sessionManager.findGlobalSession(txId);
            if (session == null) {
                response.setResultCode(ResultCode.Failed);
                response.setMsg("Global transaction [" + txId + "] not found");
                return response;
            }

            long branchId = BRANCH_ID_GENERATOR.incrementAndGet();
            String clientId = (rpcContext != null) ? rpcContext.getApplicationId() : null;
            String clientIp = (rpcContext != null && rpcContext.getChannel() != null) ? rpcContext.getClientIp() : null;
            BranchSession branchSession = new BranchSession(txId, branchId, request.getResourceId(),
                    request.getBranchType(), request.getLockKey(), clientId);
            branchSession.setClientIp(clientIp);

            synchronized (session.getSessionLock()) {
                // 只有还在一阶段（Begin）的事务能注册新分支：二阶段进行中注册的分支
                // 既不会被提交/回滚，它申请到的全局锁也没人释放，行会被永久锁死
                if (session.getStatus() != GlobalStatus.Begin) {
                    response.setResultCode(ResultCode.Failed);
                    response.setMsg("Global transaction [" + txId + "] is not in Begin phase (status="
                            + session.getStatus() + "), reject branch register");
                    return response;
                }

                // 申请全局行锁
                boolean lockAcquired = lockManager.acquireLock(branchSession);
                if (!lockAcquired) {
                    response.setResultCode(ResultCode.Failed);
                    response.setTransactionExceptionCode(TransactionExceptionCode.LockKeyConflict);
                    response.setMsg("Global lock conflict on lockKey [" + request.getLockKey() + "]");
                    return response;
                }

                branchSession.setStatus(BranchStatus.Registered);
                session.add(branchSession);
            }

            response.setTransactionId(txId);
            response.setBranchId(branchId);
            response.setResultCode(ResultCode.Success);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Successfully registered branch [" + branchId + "] for tx [" + txId + "]");
            }
        } catch (Exception ex) {
            LOGGER.error("Failed to register branch for tx [" + request.getTransactionId() + "]", ex);
            response.setResultCode(ResultCode.Failed);
            response.setMsg(ex.getMessage());
        }
        return response;
    }

    @Override
    public BranchReportResponse handle(BranchReportRequest request, RpcContext rpcContext) {
        BranchReportResponse response = new BranchReportResponse();
        try {
            long txId = request.getTransactionId();
            GlobalSession session = sessionManager.findGlobalSession(txId);
            if (session == null) {
                response.setResultCode(ResultCode.Failed);
                response.setMsg("Global transaction [" + txId + "] not found");
                return response;
            }
            BranchSession branch = session.getBranch(request.getBranchId());
            if (branch == null) {
                response.setResultCode(ResultCode.Failed);
                response.setMsg("Branch [" + request.getBranchId() + "] not found in tx [" + txId + "]");
                return response;
            }
            branch.setStatus(request.getStatus());
            response.setResultCode(ResultCode.Success);
        } catch (Exception ex) {
            LOGGER.error("Failed to report branch status for tx [" + request.getTransactionId() + "]", ex);
            response.setResultCode(ResultCode.Failed);
            response.setMsg(ex.getMessage());
        }
        return response;
    }

    @Override
    public GlobalCommitResponse handle(GlobalCommitRequest request, RpcContext rpcContext) {
        GlobalCommitResponse response = new GlobalCommitResponse();
        try {
            long txId = request.getTransactionId();
            GlobalSession session = sessionManager.findGlobalSession(txId);
            if (session == null) {
                // 会话已不存在，说明之前已经提交完成（TM 重试时可能再次到达），直接幂等返回成功
                response.setGlobalStatus(GlobalStatus.Committed);
                response.setResultCode(ResultCode.Success);
                return response;
            }

            completeCommit(session);

            response.setGlobalStatus(session.getStatus());
            response.setResultCode(ResultCode.Success);
            if (session.getStatus() == GlobalStatus.Committed && LOGGER.isInfoEnabled()) {
                LOGGER.info("Successfully committed global transaction [" + txId + "]");
            }
        } catch (Exception ex) {
            LOGGER.error("Failed to commit global transaction [" + request.getTransactionId() + "]", ex);
            response.setResultCode(ResultCode.Failed);
            response.setMsg(ex.getMessage());
        }
        return response;
    }

    @Override
    public GlobalRollbackResponse handle(GlobalRollbackRequest request, RpcContext rpcContext) {
        GlobalRollbackResponse response = new GlobalRollbackResponse();
        try {
            long txId = request.getTransactionId();
            GlobalSession session = sessionManager.findGlobalSession(txId);
            if (session == null) {
                // 会话已不存在，说明之前已经回滚完成（TM 重试时可能再次到达），直接幂等返回成功
                response.setGlobalStatus(GlobalStatus.Rollbacked);
                response.setResultCode(ResultCode.Success);
                return response;
            }

            completeRollback(session);

            response.setGlobalStatus(session.getStatus());
            response.setResultCode(ResultCode.Success);
            if (session.getStatus() == GlobalStatus.Rollbacked && LOGGER.isInfoEnabled()) {
                LOGGER.info("Successfully rollbacked global transaction [" + txId + "]");
            }
        } catch (Exception ex) {
            LOGGER.error("Failed to rollback global transaction [" + request.getTransactionId() + "]", ex);
            response.setResultCode(ResultCode.Failed);
            response.setMsg(ex.getMessage());
        }
        return response;
    }

    /**
     * 二阶段提交：向所有 RM 分支派发 BranchCommitRequest。
     * 只有所有分支都成功才算是完成；任何一个分支失败都不能宣布提交成功，
     * 会话保留在 CommitRetrying 状态，等 timeoutCheck() 线程重试。
     */
    private void completeCommit(GlobalSession session) {
        synchronized (session.getSessionLock()) {
            // 状态机校验：只允许一阶段（Begin）、上次没做完（CommitRetrying）以及
            // 中断在半路的 Committing 进入提交流程。会话已走上回滚链路时
            // （比如超时已被自动回滚），迟到的 commit 必须拒绝——否则同一事务的
            // 分支会一半提交一半回滚，直接破坏数据
            GlobalStatus status = session.getStatus();
            if (status != GlobalStatus.Begin && status != GlobalStatus.Committing
                    && status != GlobalStatus.CommitRetrying) {
                LOGGER.error("Global transaction [" + session.getXid() + "] is in status [" + status
                        + "], reject commit");
                return;
            }
            session.changeStatus(GlobalStatus.Committing);
            boolean allBranchesDone = dispatchBranchCommit(session);

            if (allBranchesDone) {
                // 所有分支提交成功：释放全局锁，删除会话
                lockManager.releaseLocks(session);
                session.changeStatus(GlobalStatus.Committed);
                sessionManager.removeGlobalSession(session);
            } else {
                session.changeStatus(GlobalStatus.CommitRetrying);
                LOGGER.error("Global transaction [" + session.getXid() + "] branch commit failed, will retry later");
            }
        }
    }

    /**
     * 二阶段回滚：按注册的逆序向所有 RM 分支派发 BranchRollbackRequest。
     * 只有所有分支都补偿成功才算是完成；任何一个分支失败都不能宣布回滚成功，
     * 否则数据不一致且再也没人补偿。会话保留在 RollbackRetrying 状态重试。
     */
    private void completeRollback(GlobalSession session) {
        synchronized (session.getSessionLock()) {
            // 与 completeCommit 对称：已走上提交链路的会话拒绝回滚（反之亦然），
            // 防止同一事务的分支一半提交一半回滚
            GlobalStatus status = session.getStatus();
            if (status != GlobalStatus.Begin && status != GlobalStatus.Rollbacking
                    && status != GlobalStatus.RollbackRetrying) {
                LOGGER.error("Global transaction [" + session.getXid() + "] is in status [" + status
                        + "], reject rollback");
                return;
            }
            session.changeStatus(GlobalStatus.Rollbacking);
            boolean allBranchesDone = dispatchBranchRollback(session);

            if (allBranchesDone) {
                lockManager.releaseLocks(session);
                session.changeStatus(GlobalStatus.Rollbacked);
                sessionManager.removeGlobalSession(session);
            } else {
                session.changeStatus(GlobalStatus.RollbackRetrying);
                LOGGER.error("Global transaction [" + session.getXid() + "] branch rollback failed, will retry later");
            }
        }
    }

    private boolean dispatchBranchCommit(GlobalSession session) {
        // 没有消息发送器（比如单元测试里直连 coordinator）时，视为没有分支需要派发
        if (serverMessageSender == null) {
            return true;
        }
        boolean allDone = true;
        for (BranchSession branch : session.getBranchSessions()) {
            if (branch.getStatus() == BranchStatus.PhaseTwo_Committed) {
                continue;
            }
            try {
                BranchCommitRequest branchCommitRequest = new BranchCommitRequest();
                branchCommitRequest.setXid(session.getXid());
                branchCommitRequest.setBranchId(branch.getBranchId());
                branchCommitRequest.setResourceId(branch.getResourceId());
                branchCommitRequest.setBranchType(branch.getBranchType());

                Object result = serverMessageSender.sendSynRequest(branch.getResourceId(),
                        branch.getClientIp(), branch.getClientId(), branchCommitRequest, 3000);

                // RM 返回的分支状态也必须检查：请求送达了，但 RM 可能执行失败
                if (result instanceof AbstractBranchEndResponse
                        && ((AbstractBranchEndResponse) result).getBranchStatus() == BranchStatus.PhaseTwo_Committed) {
                    branch.setStatus(BranchStatus.PhaseTwo_Committed);
                } else {
                    allDone = false;
                }
            } catch (Exception e) {
                allDone = false;
                LOGGER.error("Failed to dispatch BranchCommit to branch [" + branch.getBranchId() + "]", e);
            }
        }
        return allDone;
    }

    private boolean dispatchBranchRollback(GlobalSession session) {
        if (serverMessageSender == null) {
            return true;
        }
        boolean allDone = true;
        List<BranchSession> branches = session.getBranchSessions();
        for (int i = branches.size() - 1; i >= 0; i--) {
            BranchSession branch = branches.get(i);
            if (branch.getStatus() == BranchStatus.PhaseTwo_Rollbacked) {
                continue;
            }
            try {
                BranchRollbackRequest branchRollbackRequest = new BranchRollbackRequest();
                branchRollbackRequest.setXid(session.getXid());
                branchRollbackRequest.setBranchId(branch.getBranchId());
                branchRollbackRequest.setResourceId(branch.getResourceId());
                branchRollbackRequest.setBranchType(branch.getBranchType());

                Object result = serverMessageSender.sendSynRequest(branch.getResourceId(),
                        branch.getClientIp(), branch.getClientId(), branchRollbackRequest, 3000);

                if (result instanceof AbstractBranchEndResponse
                        && ((AbstractBranchEndResponse) result).getBranchStatus() == BranchStatus.PhaseTwo_Rollbacked) {
                    branch.setStatus(BranchStatus.PhaseTwo_Rollbacked);
                } else {
                    allDone = false;
                }
            } catch (Exception e) {
                allDone = false;
                LOGGER.error("Failed to dispatch BranchRollback to branch [" + branch.getBranchId() + "]", e);
            }
        }
        return allDone;
    }

    /**
     * 超时检查，由定时线程周期调用（EmbeddedTC 与独立 Server 进程都会启动）。做两件事：
     * 1. Begin 状态超过 timeout 还没提交/回滚的全局事务（TM 可能已经宕机），
     *    自动驱动回滚，避免会话和行锁永久占用；
     * 2. 二阶段没做完的会话（CommitRetrying / RollbackRetrying）继续重试。
     */
    public void timeoutCheck() {
        for (GlobalSession session : sessionManager.allSessions()) {
            try {
                if (session.getStatus() == GlobalStatus.Begin && isTimeout(session)) {
                    LOGGER.warn("Global transaction [" + session.getXid() + "] timed out, auto rollback");
                    completeRollback(session);
                } else if (session.getStatus() == GlobalStatus.CommitRetrying) {
                    completeCommit(session);
                } else if (session.getStatus() == GlobalStatus.RollbackRetrying) {
                    completeRollback(session);
                }
            } catch (Exception ex) {
                LOGGER.error("Timeout check failed for global transaction [" + session.getXid() + "]", ex);
            }
        }
    }

    private boolean isTimeout(GlobalSession session) {
        long timeout = session.getTimeout() > 0 ? session.getTimeout() : DEFAULT_GLOBAL_TX_TIMEOUT;
        return System.currentTimeMillis() - session.getBeginTime() > timeout;
    }

    @Override
    public GlobalLockQueryResponse handle(GlobalLockQueryRequest checkLock, RpcContext rpcContext) {
        GlobalLockQueryResponse response = new GlobalLockQueryResponse();
        boolean lockable = lockManager.isLockable(checkLock.getTransactionId(), checkLock.getResourceId(), checkLock.getLockKey());
        response.setLockable(lockable);
        response.setResultCode(ResultCode.Success);
        return response;
    }

    @Override
    public GlobalStatusResponse handle(GlobalStatusRequest globalStatus, RpcContext rpcContext) {
        GlobalStatusResponse response = new GlobalStatusResponse();
        GlobalSession session = sessionManager.findGlobalSession(globalStatus.getTransactionId());
        if (session != null) {
            response.setGlobalStatus(session.getStatus());
        } else {
            response.setGlobalStatus(GlobalStatus.Finished);
        }
        response.setResultCode(ResultCode.Success);
        return response;
    }

    @Override
    public AbstractResultMessage onRequest(AbstractMessage request, RpcContext context) {
        if (request instanceof AbstractTransactionRequestToTC) {
            AbstractTransactionRequestToTC tcRequest = (AbstractTransactionRequestToTC) request;
            tcRequest.setTCInboundHandler(this);
            return tcRequest.handle(context);
        }
        return null;
    }

    @Override
    public void onResponse(AbstractResultMessage response, RpcContext context) {
        // Can be used for async responses if needed
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public LockManager getLockManager() {
        return lockManager;
    }
}
