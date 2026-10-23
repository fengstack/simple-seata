package io.spring2go.seata.core.rpc;

import io.netty.channel.ChannelHandlerContext;
import io.spring2go.seata.common.util.NetUtil;
import io.spring2go.seata.core.protocol.AbstractMessage;
import io.spring2go.seata.core.protocol.AbstractResultMessage;
import io.spring2go.seata.core.protocol.HeartbeatMessage;
import io.spring2go.seata.core.protocol.MergeResultMessage;
import io.spring2go.seata.core.protocol.MergedWrapMessage;
import io.spring2go.seata.core.protocol.RegisterRMRequest;
import io.spring2go.seata.core.protocol.RegisterRMResponse;
import io.spring2go.seata.core.protocol.RegisterTMRequest;
import io.spring2go.seata.core.protocol.RegisterTMResponse;
import io.spring2go.seata.core.rpc.netty.RegisterCheckAuthHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TC 侧消息分发：注册握手、心跳应答、事务消息（含合并帧的拆包逐条处理）。
 */
public class DefaultServerMessageListenerImpl implements ServerMessageListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultServerMessageListenerImpl.class);

    private ServerMessageSender serverMessageSender;
    private final TransactionMessageHandler transactionMessageHandler;

    public DefaultServerMessageListenerImpl(TransactionMessageHandler transactionMessageHandler) {
        this.transactionMessageHandler = transactionMessageHandler;
    }

    @Override
    public void onTrxMessage(long msgId, ChannelHandlerContext ctx, Object message, ServerMessageSender sender) {
        RpcContext rpcContext = ChannelManager.getContextFromIdentified(ctx.channel());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("server received:" + message + ",clientIp:" + NetUtil.toIpAddress(ctx.channel().remoteAddress())
                    + ",vgroup:" + (rpcContext == null ? null : rpcContext.getTransactionServiceGroup()));
        }
        if (!(message instanceof AbstractMessage)) {
            return;
        }
        if (message instanceof MergedWrapMessage) {
            // 客户端把多条请求合并成一帧发来：逐条交给协调器处理，再把结果合并成一帧回去
            AbstractResultMessage[] results = new AbstractResultMessage[((MergedWrapMessage) message).msgs.size()];
            for (int i = 0; i < results.length; i++) {
                final AbstractMessage subMessage = ((MergedWrapMessage) message).msgs.get(i);
                results[i] = transactionMessageHandler.onRequest(subMessage, rpcContext);
            }
            MergeResultMessage resultMessage = new MergeResultMessage();
            resultMessage.setMsgs(results);
            sender.sendResponse(msgId, ctx.channel(), resultMessage);
        } else if (message instanceof AbstractResultMessage) {
            // RM 对二阶段指令的响应
            transactionMessageHandler.onResponse((AbstractResultMessage) message, rpcContext);
        }
    }

    @Override
    public void onRegRmMessage(long msgId, ChannelHandlerContext ctx, RegisterRMRequest message,
                               ServerMessageSender sender, RegisterCheckAuthHandler checkAuthHandler) {
        boolean isSuccess = false;
        try {
            if (null == checkAuthHandler || checkAuthHandler.regResourceManagerChecKAuth(message)) {
                ChannelManager.registerRMChannel(message, ctx.channel());
                isSuccess = true;
            }
        } catch (Exception exx) {
            LOGGER.error(exx.getMessage());
        }
        sender.sendResponse(msgId, ctx.channel(), new RegisterRMResponse(isSuccess));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("rm register {}, message:{}, channel:{}", isSuccess ? "success" : "failed", message, ctx.channel());
        }
    }

    @Override
    public void onRegTmMessage(long msgId, ChannelHandlerContext ctx, RegisterTMRequest message,
                               ServerMessageSender sender, RegisterCheckAuthHandler checkAuthHandler) {
        boolean isSuccess = false;
        try {
            if (null == checkAuthHandler || checkAuthHandler.regTransactionManagerCheckAuth(message)) {
                ChannelManager.registerTMChannel(message, ctx.channel());
                isSuccess = true;
            }
        } catch (Exception exx) {
            LOGGER.error(exx.getMessage());
        }
        sender.sendResponse(msgId, ctx.channel(), new RegisterTMResponse(isSuccess));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("tm register {}, message:{}, channel:{}", isSuccess ? "success" : "failed", message, ctx.channel());
        }
    }

    @Override
    public void onCheckMessage(long msgId, ChannelHandlerContext ctx, ServerMessageSender sender) {
        try {
            sender.sendResponse(msgId, ctx.channel(), HeartbeatMessage.PONG);
        } catch (Throwable throwable) {
            LOGGER.error("", "send response error", throwable);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received PING from {}", ctx.channel().remoteAddress());
        }
    }

    public ServerMessageSender getServerMessageSender() {
        return serverMessageSender;
    }

    public void setServerMessageSender(ServerMessageSender serverMessageSender) {
        this.serverMessageSender = serverMessageSender;
    }
}
