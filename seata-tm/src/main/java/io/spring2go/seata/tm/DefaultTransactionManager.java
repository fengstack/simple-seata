package io.spring2go.seata.tm;

import io.spring2go.seata.common.XID;
import io.spring2go.seata.core.exception.TransactionException;
import io.spring2go.seata.core.exception.TransactionExceptionCode;
import io.spring2go.seata.core.model.GlobalStatus;
import io.spring2go.seata.core.model.TransactionManager;
import io.spring2go.seata.core.protocol.ResultCode;
import io.spring2go.seata.core.protocol.transaction.*;
import io.spring2go.seata.core.rpc.netty.TmRpcClient;

import java.util.concurrent.TimeoutException;

public class DefaultTransactionManager implements TransactionManager {

    @Override
    public String begin(String applicationId, String transactionServiceGroup, String name, int timeout) throws TransactionException {
        GlobalBeginRequest request = new GlobalBeginRequest();
        request.setTransactionName(name);
        request.setTimeout(timeout);
        try {
            GlobalBeginResponse response = (GlobalBeginResponse) syncCall(request);
            if (response.getResultCode() == ResultCode.Failed) {
                throw new TransactionException(response.getTransactionExceptionCode(), "Response[" + response.getMsg() + "]");
            }
            return response.getXid();
        } catch (TimeoutException toe) {
            throw new TransactionException(TransactionExceptionCode.IO, "RPC Timeout", toe);
        } catch (RuntimeException rex) {
            throw new TransactionException(TransactionExceptionCode.Unknown, "Runtime", rex);
        }
    }

    @Override
    public GlobalStatus commit(String xid) throws TransactionException {
        long txId = XID.getTransactionId(xid);
        GlobalCommitRequest request = new GlobalCommitRequest();
        request.setTransactionId(txId);
        try {
            GlobalCommitResponse response = (GlobalCommitResponse) syncCall(request);
            if (response.getResultCode() == ResultCode.Failed) {
                throw new TransactionException(response.getTransactionExceptionCode(), "Response[" + response.getMsg() + "]");
            }
            return response.getGlobalStatus();
        } catch (TimeoutException toe) {
            throw new TransactionException(TransactionExceptionCode.IO, "RPC Timeout", toe);
        } catch (RuntimeException rex) {
            throw new TransactionException(TransactionExceptionCode.Unknown, "Runtime", rex);
        }
    }

    @Override
    public GlobalStatus rollback(String xid) throws TransactionException {
        long txId = XID.getTransactionId(xid);
        GlobalRollbackRequest request = new GlobalRollbackRequest();
        request.setTransactionId(txId);
        try {
            GlobalRollbackResponse response = (GlobalRollbackResponse) syncCall(request);
            if (response.getResultCode() == ResultCode.Failed) {
                throw new TransactionException(response.getTransactionExceptionCode(), "Response[" + response.getMsg() + "]");
            }
            return response.getGlobalStatus();
        } catch (TimeoutException toe) {
            throw new TransactionException(TransactionExceptionCode.IO, "RPC Timeout", toe);
        } catch (RuntimeException rex) {
            throw new TransactionException(TransactionExceptionCode.Unknown, "Runtime", rex);
        }
    }

    @Override
    public GlobalStatus getStatus(String xid) throws TransactionException {
        long txId = XID.getTransactionId(xid);
        GlobalStatusRequest request = new GlobalStatusRequest();
        request.setTransactionId(txId);
        try {
            GlobalStatusResponse response = (GlobalStatusResponse) syncCall(request);
            if (response.getResultCode() == ResultCode.Failed) {
                throw new TransactionException(response.getTransactionExceptionCode(), "Response[" + response.getMsg() + "]");
            }
            return response.getGlobalStatus();
        } catch (TimeoutException toe) {
            throw new TransactionException(TransactionExceptionCode.IO, "RPC Timeout", toe);
        } catch (RuntimeException rex) {
            throw new TransactionException(TransactionExceptionCode.Unknown, "Runtime", rex);
        }
    }

    protected Object syncCall(Object request) throws TimeoutException {
        return TmRpcClient.getInstance().sendMsgWithResponse(request);
    }
}
