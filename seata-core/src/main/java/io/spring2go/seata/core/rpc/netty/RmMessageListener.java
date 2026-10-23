package io.spring2go.seata.core.rpc.netty;

import io.spring2go.seata.common.exception.FrameworkErrorCode;
import io.spring2go.seata.core.protocol.ResultCode;
import io.spring2go.seata.core.protocol.transaction.BranchCommitRequest;
import io.spring2go.seata.core.protocol.transaction.BranchCommitResponse;
import io.spring2go.seata.core.protocol.transaction.BranchRollbackRequest;
import io.spring2go.seata.core.protocol.transaction.BranchRollbackResponse;
import io.spring2go.seata.core.rpc.ClientMessageListener;
import io.spring2go.seata.core.rpc.ClientMessageSender;
import io.spring2go.seata.core.rpc.TransactionMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RM 侧消息监听：处理 TC 下发的二阶段分支指令，并把结果回给 TC。
 */
public class RmMessageListener implements ClientMessageListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(RmMessageListener.class);

    private TransactionMessageHandler handler;

    public RmMessageListener(TransactionMessageHandler handler) {
        this.handler = handler;
    }

    public void setHandler(TransactionMessageHandler handler) {
        this.handler = handler;
    }

    @Override
    public void onMessage(long msgId, String serverAddress, Object msg, ClientMessageSender sender) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("onMessage:" + msg);
        }
        if (msg instanceof BranchCommitRequest) {
            handleBranchCommit(msgId, serverAddress, (BranchCommitRequest) msg, sender);
        } else if (msg instanceof BranchRollbackRequest) {
            handleBranchRollback(msgId, serverAddress, (BranchRollbackRequest) msg, sender);
        }
    }

    private void handleBranchRollback(long msgId, String serverAddress,
                                      BranchRollbackRequest branchRollbackRequest, ClientMessageSender sender) {
        BranchRollbackResponse resultMessage =
                (BranchRollbackResponse) handler.onRequest(branchRollbackRequest, null);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("branch rollback result:" + resultMessage);
        }
        try {
            sender.sendResponse(msgId, serverAddress, resultMessage);
        } catch (Throwable throwable) {
            LOGGER.error("", "send response error", throwable);
        }
    }

    private void handleBranchCommit(long msgId, String serverAddress,
                                    BranchCommitRequest branchCommitRequest, ClientMessageSender sender) {
        BranchCommitResponse resultMessage;
        try {
            resultMessage = (BranchCommitResponse) handler.onRequest(branchCommitRequest, null);
        } catch (Exception e) {
            // handler 正常不会走到这里（内部已兜底），真到了说明流程错乱，也要回一个失败响应而不是中断
            LOGGER.error(FrameworkErrorCode.NetOnMessage.errCode, e.getMessage(), e);
            resultMessage = new BranchCommitResponse();
            resultMessage.setResultCode(ResultCode.Failed);
            resultMessage.setMsg(e.getMessage());
        }
        try {
            sender.sendResponse(msgId, serverAddress, resultMessage);
        } catch (Throwable throwable) {
            LOGGER.error("", "send response error", throwable);
        }
    }
}
