package io.spring2go.seata.rm;

import io.spring2go.seata.core.exception.TransactionException;
import io.spring2go.seata.core.model.BranchStatus;
import io.spring2go.seata.core.protocol.AbstractMessage;
import io.spring2go.seata.core.protocol.AbstractResultMessage;
import io.spring2go.seata.core.protocol.ResultCode;
import io.spring2go.seata.core.protocol.transaction.AbstractTransactionRequestToRM;
import io.spring2go.seata.core.protocol.transaction.BranchCommitRequest;
import io.spring2go.seata.core.protocol.transaction.BranchCommitResponse;
import io.spring2go.seata.core.protocol.transaction.BranchRollbackRequest;
import io.spring2go.seata.core.protocol.transaction.BranchRollbackResponse;
import io.spring2go.seata.core.protocol.transaction.RMInboundHandler;
import io.spring2go.seata.core.rpc.RpcContext;
import io.spring2go.seata.core.rpc.TransactionMessageHandler;
import io.spring2go.seata.rm.datasource.DataSourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RM 侧二阶段处理器：接收 TC 下发的分支提交/回滚指令，转交给 DataSourceManager。
 * 提交 = 异步删除 undo_log；回滚 = 用 undo_log 反向补偿。
 */
public class RMHandlerAT implements RMInboundHandler, TransactionMessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(RMHandlerAT.class);

    private final DataSourceManager dataSourceManager = DataSourceManager.get();

    @Override
    public BranchCommitResponse handle(BranchCommitRequest request) {
        BranchCommitResponse response = new BranchCommitResponse();
        try {
            LOGGER.info("AT branch committing: " + request.getXid() + " " + request.getBranchId()
                    + " " + request.getResourceId());
            BranchStatus status = dataSourceManager.branchCommit(
                    request.getXid(), request.getBranchId(), request.getResourceId(), request.getApplicationData());
            response.setBranchStatus(status);
            response.setResultCode(ResultCode.Success);
            LOGGER.info("AT branch commit result: " + status);
        } catch (TransactionException te) {
            response.setResultCode(ResultCode.Failed);
            response.setTransactionExceptionCode(te.getCode());
            response.setMsg("TransactionException[" + te.getMessage() + "]");
        } catch (RuntimeException rex) {
            response.setResultCode(ResultCode.Failed);
            response.setMsg("RuntimeException[" + rex.getMessage() + "]");
        }
        return response;
    }

    @Override
    public BranchRollbackResponse handle(BranchRollbackRequest request) {
        BranchRollbackResponse response = new BranchRollbackResponse();
        try {
            LOGGER.info("AT branch rolling back: " + request.getXid() + " " + request.getBranchId()
                    + " " + request.getResourceId());
            BranchStatus status = dataSourceManager.branchRollback(
                    request.getXid(), request.getBranchId(), request.getResourceId(), request.getApplicationData());
            response.setBranchStatus(status);
            response.setResultCode(ResultCode.Success);
            LOGGER.info("AT branch rollback result: " + status);
        } catch (TransactionException te) {
            response.setResultCode(ResultCode.Failed);
            response.setTransactionExceptionCode(te.getCode());
            response.setMsg("TransactionException[" + te.getMessage() + "]");
        } catch (RuntimeException rex) {
            response.setResultCode(ResultCode.Failed);
            response.setMsg("RuntimeException[" + rex.getMessage() + "]");
        }
        return response;
    }

    @Override
    public AbstractResultMessage onRequest(AbstractMessage request, RpcContext context) {
        if (!(request instanceof AbstractTransactionRequestToRM)) {
            throw new IllegalArgumentException("Unsupported request to RM: " + request);
        }
        AbstractTransactionRequestToRM transactionRequest = (AbstractTransactionRequestToRM) request;
        transactionRequest.setRMInboundMessageHandler(this);
        return transactionRequest.handle(context);
    }

    @Override
    public void onResponse(AbstractResultMessage response, RpcContext context) {
        // RM 只应收到请求，收到响应说明流程错乱
        throw new IllegalStateException("RMHandlerAT should never receive a response: " + response);
    }
}
