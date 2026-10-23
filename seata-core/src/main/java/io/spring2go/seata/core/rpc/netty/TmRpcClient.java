package io.spring2go.seata.core.rpc.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.util.concurrent.EventExecutorGroup;
import io.spring2go.seata.common.XID;
import io.spring2go.seata.common.exception.FrameworkException;
import io.spring2go.seata.common.thread.NamedThreadFactory;
import io.spring2go.seata.core.context.RootContext;
import io.spring2go.seata.core.protocol.AbstractMessage;
import io.spring2go.seata.core.protocol.RegisterTMRequest;
import io.spring2go.seata.core.protocol.RegisterTMResponse;
import io.spring2go.seata.core.protocol.ResultCode;
import io.spring2go.seata.core.protocol.transaction.GlobalBeginResponse;
import io.spring2go.seata.core.rpc.netty.NettyPoolKey.TransactionRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.spring2go.seata.common.exception.FrameworkErrorCode.NoAvailableService;

/**
 * TM 客户端：与 TC 保持长连接，收发全局事务的 begin/commit/rollback/status 指令。
 */
@Sharable
public class TmRpcClient extends AbstractRpcRemotingClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(TmRpcClient.class);
    private static volatile TmRpcClient instance;
    private static final long KEEP_ALIVE_TIME = Integer.MAX_VALUE;
    private static final int MAX_QUEUE_SIZE = 2000;

    private String applicationId;
    private String transactionServiceGroup;

    private TmRpcClient(NettyClientConfig nettyClientConfig,
                        EventExecutorGroup eventExecutorGroup,
                        ThreadPoolExecutor messageExecutor) {
        super(nettyClientConfig, eventExecutorGroup, messageExecutor);
    }

    public static TmRpcClient getInstance(String applicationId, String transactionServiceGroup) {
        TmRpcClient tmRpcClient = getInstance();
        tmRpcClient.setApplicationId(applicationId);
        tmRpcClient.setTransactionServiceGroup(transactionServiceGroup);
        return tmRpcClient;
    }

    public static TmRpcClient getInstance() {
        if (null == instance) {
            synchronized (TmRpcClient.class) {
                if (null == instance) {
                    NettyClientConfig nettyClientConfig = new NettyClientConfig();
                    final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                            nettyClientConfig.getClientWorkerThreads(), nettyClientConfig.getClientWorkerThreads(),
                            KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                            new LinkedBlockingQueue<Runnable>(MAX_QUEUE_SIZE),
                            new NamedThreadFactory(nettyClientConfig.getTmDispatchThreadPrefix(),
                                    nettyClientConfig.getClientWorkerThreads()),
                            new ThreadPoolExecutor.CallerRunsPolicy());
                    instance = new TmRpcClient(nettyClientConfig, null, threadPoolExecutor);
                }
            }
        }
        return instance;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getTransactionServiceGroup() {
        return transactionServiceGroup;
    }

    public void setTransactionServiceGroup(String transactionServiceGroup) {
        this.transactionServiceGroup = transactionServiceGroup;
    }

    @Override
    protected AbstractMessage buildRegisterMessage(String serverAddress) {
        return new RegisterTMRequest(applicationId, transactionServiceGroup);
    }

    @Override
    protected TransactionRole getTransactionRole() {
        return TransactionRole.TMROLE;
    }

    /**
     * 发送指令并同步等待响应。寻址规则：XID 里带着 TC 地址就直连它
     * （同一全局事务的所有指令都发往同一个 TC）；还没有 XID（比如 begin）就取分组里的第一个节点。
     */
    @Override
    public Object sendMsgWithResponse(Object msg, long timeout) throws TimeoutException {
        String svrAddr = XID.getServerAddress(RootContext.getXID());
        String validAddress = svrAddr != null ? svrAddr : loadBalance();
        Channel acquireChannel = connect(validAddress);
        Object result = super.sendAsyncRequestWithResponse(validAddress, acquireChannel, msg, timeout);
        if (result instanceof GlobalBeginResponse
                && ((GlobalBeginResponse) result).getResultCode() == ResultCode.Failed) {
            LOGGER.error("begin response error, release channel:" + acquireChannel);
            releaseChannel(acquireChannel, validAddress);
        }
        return result;
    }

    private String loadBalance() {
        String[] addresses = serviceManager.lookup(transactionServiceGroup);
        if (addresses == null || addresses.length == 0) {
            throw new FrameworkException(NoAvailableService);
        }
        // 教学实现只有单节点
        return addresses[0];
    }

    @Override
    public void onRegisterMsgSuccess(String serverAddress, Channel channel, Object response,
                                     AbstractMessage requestMessage) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("register TM success, server version:" + ((RegisterTMResponse) response).getVersion()
                    + ", channel:" + channel);
        }
        channels.put(serverAddress, channel);
    }

    @Override
    public void onRegisterMsgFail(String serverAddress, Channel channel, Object response,
                                  AbstractMessage requestMessage) {
        if (response instanceof RegisterTMResponse && LOGGER.isInfoEnabled()) {
            LOGGER.info("register TM failed, server version:" + ((RegisterTMResponse) response).getVersion());
        }
        throw new FrameworkException("register TM failed.");
    }
}
