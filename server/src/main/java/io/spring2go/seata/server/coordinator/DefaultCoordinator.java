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
            BranchSession branchSession = new BranchSession(txId, branchId, request.getResourceId(),
                    request.getBranchType(), request.getLockKey(), clientId);

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
            if (session != null) {
                BranchSession branch = session.getBranch(request.getBranchId());
                if (branch != null) {
                    branch.setStatus(request.getStatus());
                }
            }
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
                response.setGlobalStatus(GlobalStatus.Committed);
                response.setResultCode(ResultCode.Success);
                return response;
            }

            session.changeStatus(GlobalStatus.Committing);

            // 二阶段提交调度
            if (serverMessageSender != null) {
                for (BranchSession branch : session.getBranchSessions()) {
                    try {
                        BranchCommitRequest branchCommitRequest = new BranchCommitRequest();
                        branchCommitRequest.setXid(session.getXid());
                        branchCommitRequest.setBranchId(branch.getBranchId());
                        branchCommitRequest.setResourceId(branch.getResourceId());
                        branchCommitRequest.setBranchType(branch.getBranchType());

                        String clientAppName = branch.getClientId();
                        serverMessageSender.sendSynRequest(branch.getResourceId(), null, clientAppName, branchCommitRequest, 3000);
                    } catch (Exception e) {
                        LOGGER.error("Failed to dispatch BranchCommit to branch [" + branch.getBranchId() + "]", e);
                    }
                }
            }

            // 释放全局锁并更新状态
            lockManager.releaseLocks(session);
            session.changeStatus(GlobalStatus.Committed);
            sessionManager.removeGlobalSession(session);

            response.setGlobalStatus(GlobalStatus.Committed);
            response.setResultCode(ResultCode.Success);
            if (LOGGER.isInfoEnabled()) {
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
                response.setGlobalStatus(GlobalStatus.Rollbacked);
                response.setResultCode(ResultCode.Success);
                return response;
            }

            session.changeStatus(GlobalStatus.Rollbacking);

            // 二阶段逆序回滚调度
            if (serverMessageSender != null) {
                List<BranchSession> branches = session.getBranchSessions();
                for (int i = branches.size() - 1; i >= 0; i--) {
                    BranchSession branch = branches.get(i);
                    try {
                        BranchRollbackRequest branchRollbackRequest = new BranchRollbackRequest();
                        branchRollbackRequest.setXid(session.getXid());
                        branchRollbackRequest.setBranchId(branch.getBranchId());
                        branchRollbackRequest.setResourceId(branch.getResourceId());
                        branchRollbackRequest.setBranchType(branch.getBranchType());

                        String clientAppName = branch.getClientId();
                        serverMessageSender.sendSynRequest(branch.getResourceId(), null, clientAppName, branchRollbackRequest, 3000);
                    } catch (Exception e) {
                        LOGGER.error("Failed to dispatch BranchRollback to branch [" + branch.getBranchId() + "]", e);
                    }
                }
            }

            // 释放全局锁并更新状态
            lockManager.releaseLocks(session);
            session.changeStatus(GlobalStatus.Rollbacked);
            sessionManager.removeGlobalSession(session);

            response.setGlobalStatus(GlobalStatus.Rollbacked);
            response.setResultCode(ResultCode.Success);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Successfully rollbacked global transaction [" + txId + "]");
            }
        } catch (Exception ex) {
            LOGGER.error("Failed to rollback global transaction [" + request.getTransactionId() + "]", ex);
            response.setResultCode(ResultCode.Failed);
            response.setMsg(ex.getMessage());
        }
        return response;
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
