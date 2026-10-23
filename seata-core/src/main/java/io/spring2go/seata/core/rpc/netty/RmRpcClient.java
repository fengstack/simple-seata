package io.spring2go.seata.core.rpc.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.util.concurrent.EventExecutorGroup;
import io.spring2go.seata.common.XID;
import io.spring2go.seata.common.exception.FrameworkErrorCode;
import io.spring2go.seata.common.exception.FrameworkException;
import io.spring2go.seata.common.thread.NamedThreadFactory;
import io.spring2go.seata.core.context.RootContext;
import io.spring2go.seata.core.model.Resource;
import io.spring2go.seata.core.model.ResourceManager;
import io.spring2go.seata.core.protocol.AbstractMessage;
import io.spring2go.seata.core.protocol.RegisterRMRequest;
import io.spring2go.seata.core.protocol.RegisterRMResponse;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import io.spring2go.seata.core.rpc.netty.NettyPoolKey.TransactionRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.spring2go.seata.common.Constants.DBKEYS_SPLIT_CHAR;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * RM 客户端：向 TC 注册自己管理的数据源资源（resourceId），
 * 并接收 TC 在二阶段下发的 BranchCommit/BranchRollback 指令。
 */
@Sharable
public class RmRpcClient extends AbstractRpcRemotingClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(RmRpcClient.class);

    private static volatile RmRpcClient instance;
    private static final long KEEP_ALIVE_TIME = Integer.MAX_VALUE;
    private static final int MAX_QUEUE_SIZE = 20000;

    private ResourceManager resourceManager;
    private String applicationId;
    private String transactionServiceGroup;
    private String customerKeys;

    private RmRpcClient(NettyClientConfig nettyClientConfig, EventExecutorGroup eventExecutorGroup,
                        ThreadPoolExecutor messageExecutor) {
        super(nettyClientConfig, eventExecutorGroup, messageExecutor);
    }

    public void setResourceManager(ResourceManager resourceManager) {
        this.resourceManager = resourceManager;
    }

    public static RmRpcClient getInstance(String applicationId, String transactionServiceGroup) {
        RmRpcClient rmRpcClient = getInstance();
        rmRpcClient.setApplicationId(applicationId);
        rmRpcClient.setTransactionServiceGroup(transactionServiceGroup);
        return rmRpcClient;
    }

    public static RmRpcClient getInstance() {
        if (null == instance) {
            synchronized (RmRpcClient.class) {
                if (null == instance) {
                    NettyClientConfig nettyClientConfig = new NettyClientConfig();
                    final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                            nettyClientConfig.getClientWorkerThreads(), nettyClientConfig.getClientWorkerThreads(),
                            KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                            new LinkedBlockingQueue<Runnable>(MAX_QUEUE_SIZE),
                            new NamedThreadFactory(nettyClientConfig.getRmDispatchThreadPrefix(),
                                    nettyClientConfig.getClientWorkerThreads()),
                            new ThreadPoolExecutor.CallerRunsPolicy());
                    instance = new RmRpcClient(nettyClientConfig, null, threadPoolExecutor);
                }
            }
        }
        return instance;
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
        RegisterRMRequest message = new RegisterRMRequest(applicationId, transactionServiceGroup);
        message.setResourceIds(customerKeys == null ? getMergedResourceKeys(resourceManager) : customerKeys);
        return message;
    }

    @Override
    protected NettyPoolKey getPoolKey(String serverAddress) {
        NettyPoolKey poolKey = super.getPoolKey(serverAddress);
        // 复用已创建的注册报文，每次借用前刷新资源列表（可能有新数据源注册进来）
        ((RegisterRMRequest) poolKey.getMessage()).setResourceIds(
                customerKeys == null ? getMergedResourceKeys(resourceManager) : customerKeys);
        return poolKey;
    }

    @Override
    protected TransactionRole getTransactionRole() {
        return TransactionRole.RMROLE;
    }

    /**
     * RM 的指令都服务于当前全局事务，所以直接按 XID 里记录的 TC 地址发送。
     */
    @Override
    public Object sendMsgWithResponse(Object msg, long timeout) throws TimeoutException {
        String serverAddress = RootContext.inGlobalTransaction() ? XID.getServerAddress(RootContext.getXID()) : null;
        Channel channel = serverAddress == null ? null : connect(serverAddress);
        return super.sendAsyncRequestWithResponse(serverAddress, channel, msg, timeout);
    }

    /** 新数据源初始化时调用：向所有已连接的 TC 补发一次资源注册 */
    public void registerResource(String resourceGroupId, String resourceId) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("register AT resourceId:" + resourceId);
        }
        synchronized (channels) {
            for (Map.Entry<String, Channel> entry : channels.entrySet()) {
                sendRegisterMessage(entry.getKey(), entry.getValue(), resourceId);
            }
        }
    }

    private void sendRegisterMessage(String serverAddress, Channel channel, String dbKey) {
        RegisterRMRequest message = new RegisterRMRequest(applicationId, transactionServiceGroup);
        message.setResourceIds(dbKey);
        try {
            super.sendAsyncRequestWithoutResponse(null, channel, message);
        } catch (FrameworkException e) {
            if (e.getErrcode() == FrameworkErrorCode.ChannelIsNotWritable && serverAddress != null) {
                releaseChannel(channel, serverAddress);
            } else {
                LOGGER.error("", "register resource failed", e);
            }
        } catch (TimeoutException e) {
            LOGGER.error(e.getMessage());
        }
    }

    @Override
    public void onRegisterMsgSuccess(String serverAddress, Channel channel, Object response,
                                     AbstractMessage requestMessage) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("register RM success, server version:" + ((RegisterRMResponse) response).getVersion()
                    + ", channel:" + channel);
        }
        channels.put(serverAddress, channel);
    }

    @Override
    public void onRegisterMsgFail(String serverAddress, Channel channel, Object response,
                                  AbstractMessage requestMessage) {
        if (response instanceof RegisterRMResponse && LOGGER.isInfoEnabled()) {
            LOGGER.info("register RM failed, server version:" + ((RegisterRMResponse) response).getVersion());
        }
        throw new FrameworkException("register RM failed.");
    }

    /** 汇总 ResourceManager 当前管理的全部 resourceId，用逗号拼接 */
    public String getMergedResourceKeys(ResourceManager resourceManager) {
        Map<String, Resource> managedResources = resourceManager.getManagedResources();
        Set<String> resourceIds = managedResources.keySet();
        if (!resourceIds.isEmpty()) {
            StringBuffer sb = new StringBuffer();
            boolean first = true;
            for (String resourceId : resourceIds) {
                if (first) {
                    first = false;
                } else {
                    sb.append(DBKEYS_SPLIT_CHAR);
                }
                sb.append(resourceId);
            }
            return sb.toString();
        }
        return null;
    }
}
