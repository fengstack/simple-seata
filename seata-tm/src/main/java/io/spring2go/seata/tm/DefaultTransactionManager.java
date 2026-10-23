package io.spring2go.seata.tm;

import io.spring2go.seata.common.XID;
import io.spring2go.seata.core.exception.TransactionException;
import io.spring2go.seata.core.exception.TransactionExceptionCode;
import io.spring2go.seata.core.model.GlobalStatus;
import io.spring2go.seata.core.model.TransactionManager;
import io.spring2go.seata.core.protocol.AbstractMessage;
import io.spring2go.seata.core.protocol.ResultCode;
import io.spring2go.seata.core.protocol.transaction.AbstractTransactionResponse;
import io.spring2go.seata.core.protocol.transaction.GlobalBeginRequest;
import io.spring2go.seata.core.protocol.transaction.GlobalBeginResponse;
import io.spring2go.seata.core.protocol.transaction.GlobalCommitRequest;
import io.spring2go.seata.core.protocol.transaction.GlobalCommitResponse;
import io.spring2go.seata.core.protocol.transaction.GlobalRollbackRequest;
import io.spring2go.seata.core.protocol.transaction.GlobalRollbackResponse;
import io.spring2go.seata.core.protocol.transaction.GlobalStatusRequest;
import io.spring2go.seata.core.protocol.transaction.GlobalStatusResponse;
import io.spring2go.seata.core.rpc.netty.TmRpcClient;

import java.util.concurrent.TimeoutException;

/**
 * TM 的默认实现：把 begin/commit/rollback/status 翻译成 RPC 指令发给 TC。
 */
public class DefaultTransactionManager implements TransactionManager {

    @Override
    public String begin(String applicationId, String transactionServiceGroup, String name, int timeout)
            throws TransactionException {
        GlobalBeginRequest request = new GlobalBeginRequest();
        request.setTransactionName(name);
        request.setTimeout(timeout);
        return call(request, GlobalBeginResponse.class).getXid();
    }

    @Override
    public GlobalStatus commit(String xid) throws TransactionException {
        GlobalCommitRequest request = new GlobalCommitRequest();
        request.setTransactionId(XID.getTransactionId(xid));
        return call(request, GlobalCommitResponse.class).getGlobalStatus();
    }

    @Override
    public GlobalStatus rollback(String xid) throws TransactionException {
        GlobalRollbackRequest request = new GlobalRollbackRequest();
        request.setTransactionId(XID.getTransactionId(xid));
        return call(request, GlobalRollbackResponse.class).getGlobalStatus();
    }

    @Override
    public GlobalStatus getStatus(String xid) throws TransactionException {
        GlobalStatusRequest request = new GlobalStatusRequest();
        request.setTransactionId(XID.getTransactionId(xid));
        return call(request, GlobalStatusResponse.class).getGlobalStatus();
    }

    /** 发送请求并统一处理失败响应 / RPC 超时 / 运行时异常 */
    protected <T extends AbstractTransactionResponse> T call(AbstractMessage request, Class<T> clazz)
            throws TransactionException {
        try {
            AbstractTransactionResponse response = (AbstractTransactionResponse) syncCall(request);
            if (response.getResultCode() == ResultCode.Failed) {
                throw new TransactionException(response.getTransactionExceptionCode(),
                        "Response[" + response.getMsg() + "]");
            }
            return clazz.cast(response);
        } catch (TimeoutException toe) {
            throw new TransactionException(TransactionExceptionCode.IO, "RPC Timeout", toe);
        } catch (RuntimeException rex) {
            throw new TransactionException(TransactionExceptionCode.Unknown, "Runtime", rex);
        }
    }

    /** 同步 RPC：子类（测试 mock）可覆盖 */
    protected Object syncCall(Object request) throws TimeoutException {
        return TmRpcClient.getInstance().sendMsgWithResponse(request);
    }
}
