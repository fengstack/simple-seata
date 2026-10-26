package io.spring2go.seata.core.rpc.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.spring2go.seata.common.exception.FrameworkErrorCode;
import io.spring2go.seata.common.exception.FrameworkException;
import io.spring2go.seata.common.thread.NamedThreadFactory;
import io.spring2go.seata.common.util.NetUtil;
import io.spring2go.seata.core.protocol.AbstractMessage;
import io.spring2go.seata.core.protocol.HeartbeatMessage;
import io.spring2go.seata.core.protocol.MessageFuture;
import io.spring2go.seata.core.protocol.MergedWrapMessage;
import io.spring2go.seata.core.protocol.MergeResultMessage;
import io.spring2go.seata.core.protocol.RpcMessage;
import io.spring2go.seata.core.rpc.ClientMessageListener;
import io.spring2go.seata.core.rpc.ClientMessageSender;
import io.spring2go.seata.core.rpc.RemotingService;
import io.spring2go.seata.core.service.ServiceManager;
import io.spring2go.seata.core.service.ServiceManagerStaticConfigImpl;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import io.spring2go.seata.core.rpc.netty.NettyPoolKey.TransactionRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TM/RM 客户端公共基类：连接管理（建连、重连、归还连接池）、心跳、
 * 请求-响应收发与"多条请求合并成一帧发送"。
 * 子类只决定自己的注册报文（TM 注册应用身份，RM 额外注册资源列表）与寻址方式。
 */
public abstract class AbstractRpcRemotingClient extends AbstractRpcRemoting
        implements RemotingService, RegisterMsgListener, ClientMessageSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRpcRemotingClient.class);

    private static final int MAX_MERGE_SEND_THREAD = 1;
    /** 攒批窗口（毫秒）：等这么久就把 basket 里的请求合并发出 */
    private static final int MAX_MERGE_SEND_MILLS = 1;
    private static final long KEEP_ALIVE_TIME = Integer.MAX_VALUE;
    /** 定时重连/健康检查间隔（秒） */
    private static final int SCHEDULE_INTERVAL_MILLS = 5;
    private static final String MERGE_THREAD_PREFIX = "rpcMergeMessageSend";
    private static final String THREAD_PREFIX_SPLIT_CHAR = "_";

    protected final NettyClientConfig nettyClientConfig;
    protected ServiceManager serviceManager;
    protected ClientMessageListener clientMessageListener;

    /** 连接池：key 里带角色注册报文，建连时先完成注册握手才归还可用连接 */
    protected GenericKeyedObjectPool<NettyPoolKey, Channel> nettyClientKeyPool;
    /** serverAddress -> 池化 key（内含注册报文） */
    protected final ConcurrentMap<String, NettyPoolKey> poolKeyMap = new ConcurrentHashMap<String, NettyPoolKey>();
    /** serverAddress -> 当前可用连接 */
    protected final ConcurrentMap<String, Channel> channels = new ConcurrentHashMap<String, Channel>();
    /** 每个 serverAddress 一把建连锁，避免并发重复建连 */
    private final ConcurrentMap<String, Object> channelLocks = new ConcurrentHashMap<String, Object>();

    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    private EventExecutorGroup defaultEventExecutorGroup;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    public AbstractRpcRemotingClient(final NettyClientConfig nettyClientConfig,
                                     final EventExecutorGroup eventExecutorGroup,
                                     final ThreadPoolExecutor messageExecutor) {
        super(messageExecutor);
        NettyClientConfig clientConfig = nettyClientConfig;
        if (clientConfig == null) {
            clientConfig = new NettyClientConfig();
        }
        this.nettyClientConfig = clientConfig;
        int selectorThreadSize = clientConfig.getClientSelectorThreadSize();
        this.eventLoopGroupWorker = new NioEventLoopGroup(selectorThreadSize,
                new NamedThreadFactory(getThreadPrefix(clientConfig.getClientSelectorThreadPrefix()),
                        selectorThreadSize));
        this.defaultEventExecutorGroup = eventExecutorGroup;
    }

    @Override
    public void init() {
        if (initialized.compareAndSet(false, true)) {
            nettyClientKeyPool = new GenericKeyedObjectPool<NettyPoolKey, Channel>(new NettyPoolableFactory(this));
            nettyClientKeyPool.setConfig(getNettyPoolConfig());
            serviceManager = new ServiceManagerStaticConfigImpl();
            super.init();

            // 周期性重连：TC 重启或网络抖动后，客户端能自动恢复连接
            timerExecutor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    reconnect();
                }
            }, SCHEDULE_INTERVAL_MILLS, SCHEDULE_INTERVAL_MILLS, TimeUnit.SECONDS);

            // 合并发送线程：把同一地址的多条请求攒成一帧（MergedWrapMessage）发出
            ExecutorService mergeSendExecutorService = new ThreadPoolExecutor(MAX_MERGE_SEND_THREAD,
                    MAX_MERGE_SEND_THREAD, KEEP_ALIVE_TIME, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new NamedThreadFactory(getThreadPrefix(MERGE_THREAD_PREFIX), MAX_MERGE_SEND_THREAD));
            mergeSendExecutorService.submit(new MergedSendRunnable());
        }
    }

    private void reconnect() {
        for (String serverAddress : serviceManager.lookup(getTransactionServiceGroup())) {
            try {
                connect(serverAddress);
            } catch (Exception e) {
                LOGGER.error(FrameworkErrorCode.NetConnect.errCode,
                        "can not connect to " + serverAddress + " cause:" + e.getMessage(), e);
            }
        }
    }

    @Override
    public void start() {
        if (this.defaultEventExecutorGroup == null) {
            this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(nettyClientConfig.getClientWorkerThreads(),
                    new NamedThreadFactory(getThreadPrefix(nettyClientConfig.getClientWorkerThreadPrefix()),
                            nettyClientConfig.getClientWorkerThreads()));
        }
        this.bootstrap.group(this.eventLoopGroupWorker).channel(nettyClientConfig.getClientChannelClazz())
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
                .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize())
                .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        // 客户端流水线：空闲检测（触发心跳）+ 0xdada 协议编解码 + 业务处理器
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new IdleStateHandler(nettyClientConfig.getChannelMaxReadIdleSeconds(),
                                nettyClientConfig.getChannelMaxWriteIdleSeconds(),
                                nettyClientConfig.getChannelMaxAllIdleSeconds()));
                        pipeline.addLast(new MessageCodecHandler());
                        if (null != channelHandlers) {
                            addChannelPipelineLast(ch, channelHandlers);
                        }
                    }
                });
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(this.getClass().getSimpleName() + " has started");
        }
    }

    /**
     * 直连指定地址（不走连接池），用于注册握手的原始通道创建。
     */
    protected Channel getNewChannel(InetSocketAddress address) {
        Channel channel = null;
        ChannelFuture f = this.bootstrap.connect(address);
        try {
            f.await(this.nettyClientConfig.getConnectTimeoutMillis(), TimeUnit.MILLISECONDS);
            if (f.isCancelled()) {
                throw new FrameworkException("connect cancelled, can not connect to seata-server.");
            } else if (!f.isSuccess()) {
                throw new FrameworkException("connect failed, can not connect to seata-server.");
            } else {
                channel = f.channel();
            }
        } catch (Exception e) {
            throw new FrameworkException(e, "can not connect to seata-server.");
        }
        return channel;
    }

    @Override
    public void shutdown() {
        try {
            this.eventLoopGroupWorker.shutdownGracefully();
            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
            super.destroy();
        } catch (Exception exx) {
            LOGGER.error("shutdown error:" + exx.getMessage());
        }
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof RpcMessage) {
            RpcMessage rpcMessage = (RpcMessage) msg;
            if (rpcMessage.getBody() == HeartbeatMessage.PONG) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("received PONG from " + ctx.channel().remoteAddress());
                }
                return;
            }
            if (rpcMessage.getBody() instanceof MergeResultMessage) {
                // 合并响应：把一帧里的多个结果按消息 id 逐个唤醒对应的等待方
                MergeResultMessage results = (MergeResultMessage) rpcMessage.getBody();
                MergedWrapMessage mergeMessage = (MergedWrapMessage) mergeMsgMap.remove(rpcMessage.getId());
                int num = mergeMessage.msgs.size();
                for (int i = 0; i < num; i++) {
                    long msgId = mergeMessage.msgIds.get(i);
                    MessageFuture future = futures.remove(msgId);
                    if (future == null) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("msg:" + msgId + " is not found in futures.");
                        }
                    } else {
                        future.setResultMessage(results.getMsgs()[i]);
                    }
                }
                return;
            }
        }
        super.channelRead(ctx, msg);
    }

    public ClientMessageListener getClientMessageListener() {
        return clientMessageListener;
    }

    public void setClientMessageListener(ClientMessageListener clientMessageListener) {
        this.clientMessageListener = clientMessageListener;
    }

    @Override
    public void dispatch(long msgId, ChannelHandlerContext ctx, Object msg) {
        if (clientMessageListener != null) {
            String remoteAddress = NetUtil.toStringAddress(ctx.channel().remoteAddress());
            clientMessageListener.onMessage(msgId, remoteAddress, msg, this);
        }
    }

    protected String getThreadPrefix(String threadPrefix) {
        return threadPrefix + THREAD_PREFIX_SPLIT_CHAR + getTransactionRole().name();
    }

    // ---------- 连接管理（TM/RM 共用） ----------

    /**
     * 取一条到 TC 的可用连接：优先复用缓存里活跃的，否则在锁内新建（借出前完成注册握手）。
     */
    protected Channel connect(String serverAddress) {
        Channel channelToServer = channels.get(serverAddress);
        if (null != channelToServer) {
            channelToServer = getExistAliveChannel(channelToServer, serverAddress);
            if (null != channelToServer) {
                return channelToServer;
            }
        }
        channelLocks.putIfAbsent(serverAddress, new Object());
        Object connectLock = channelLocks.get(serverAddress);
        synchronized (connectLock) {
            channelToServer = doConnect(serverAddress);
            channels.put(serverAddress, channelToServer);
            return channelToServer;
        }
    }

    private Channel doConnect(String serverAddress) {
        Channel channelToServer = channels.get(serverAddress);
        if (null != channelToServer && channelToServer.isActive()) {
            return channelToServer;
        }
        try {
            channelToServer = nettyClientKeyPool.borrowObject(getPoolKey(serverAddress));
        } catch (Exception exx) {
            LOGGER.error("get channel from pool error.", exx);
            throw new FrameworkException("can not connect to " + serverAddress + ", err:" + exx.getMessage());
        }
        return channelToServer;
    }

    /**
     * 返回该地址的池化 key（含角色注册报文），并登记到 poolKeyMap 供空闲失效时反查。
     */
    protected NettyPoolKey getPoolKey(String serverAddress) {
        NettyPoolKey poolKey = poolKeyMap.get(serverAddress);
        if (poolKey == null) {
            poolKey = new NettyPoolKey(getTransactionRole(), serverAddress, buildRegisterMessage(serverAddress));
            poolKeyMap.putIfAbsent(serverAddress, poolKey);
            poolKey = poolKeyMap.get(serverAddress);
        }
        return poolKey;
    }

    /** 建连时携带的注册报文：TM 注册应用身份，RM 注册应用身份 + 资源列表 */
    protected abstract AbstractMessage buildRegisterMessage(String serverAddress);

    /** 连接不活跃时最多等待重连这么多轮，仍不活跃则释放 */
    private Channel getExistAliveChannel(Channel channel, String serverAddress) {
        if (channel.isActive()) {
            return channel;
        }
        int i = 0;
        for (; i < NettyClientConfig.getMaxCheckAliveRetry(); i++) {
            try {
                Thread.sleep(NettyClientConfig.getCheckAliveInternal());
            } catch (InterruptedException exx) {
                LOGGER.error(exx.getMessage());
            }
            channel = channels.get(serverAddress);
            if (null == channel || channel.isActive()) {
                return channel;
            }
        }
        LOGGER.warn("channel " + channel + " is not active after long wait, close it.");
        releaseChannel(channel, serverAddress);
        return null;
    }

    /** 空闲/异常时归还连接：缓存里还是这条就销毁，否则还给连接池 */
    public void releaseChannel(Channel channel, String serverAddress) {
        if (null == channel || null == serverAddress) {
            return;
        }
        try {
            Object connectLock = channelLocks.get(serverAddress);
            if (connectLock == null) {
                return;
            }
            synchronized (connectLock) {
                Channel ch = channels.get(serverAddress);
                if (ch == null) {
                    nettyClientKeyPool.returnObject(poolKeyMap.get(serverAddress), channel);
                    return;
                }
                if (ch.compareTo(channel) == 0) {
                    destroyChannel(serverAddress, channel);
                } else {
                    nettyClientKeyPool.returnObject(poolKeyMap.get(serverAddress), channel);
                }
            }
        } catch (Exception exx) {
            LOGGER.error(exx.getMessage());
        }
    }

    @Override
    public void destroyChannel(String serverAddress, Channel channel) {
        if (null == channel) {
            return;
        }
        try {
            if (channel.equals(channels.get(serverAddress))) {
                channels.remove(serverAddress);
            }
            nettyClientKeyPool.returnObject(poolKeyMap.get(serverAddress), channel);
        } catch (Exception exx) {
            LOGGER.error("return channel to pool error:" + exx.getMessage());
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (messageExecutor.isShutdown()) {
            return;
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("channel inactive:" + ctx.channel());
        }
        releaseChannel(ctx.channel(), NetUtil.toStringAddress(ctx.channel().remoteAddress()));
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOGGER.error(FrameworkErrorCode.ExceptionCaught.errCode,
                NetUtil.toStringAddress(ctx.channel().remoteAddress()) + " connect exception. " + cause.getMessage(),
                cause);
        releaseChannel(ctx.channel(), getAddressFromChannel(ctx.channel()));
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent idleStateEvent = (IdleStateEvent) evt;
            if (idleStateEvent.state() == IdleState.READER_IDLE) {
                // 长时间收不到 TC 响应：连接大概率已死，失效重建
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("channel " + ctx.channel() + " read idle.");
                }
                try {
                    nettyClientKeyPool.invalidateObject(poolKeyMap.get(ctx.channel().remoteAddress()), ctx.channel());
                } catch (Exception exx) {
                    LOGGER.error(exx.getMessage());
                } finally {
                    releaseChannel(ctx.channel(), getAddressFromContext(ctx));
                }
            }
            if (idleStateEvent == IdleStateEvent.WRITER_IDLE_STATE_EVENT) {
                // 长时间没有写出：发一个 PING 保活
                try {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("will send ping msg, channel" + ctx.channel());
                    }
                    sendRequest(ctx.channel(), HeartbeatMessage.PING);
                } catch (Throwable throwable) {
                    LOGGER.error("", "send request error", throwable);
                }
            }
        }
    }

    @Override
    public Object sendMsgWithResponse(String serverAddress, Object msg, long timeout) throws TimeoutException {
        return sendAsyncRequestWithResponse(serverAddress, connect(serverAddress), msg, timeout);
    }

    @Override
    public Object sendMsgWithResponse(Object msg) throws TimeoutException {
        return sendMsgWithResponse(msg, NettyClientConfig.getRpcRequestTimeout());
    }

    @Override
    public void sendResponse(long msgId, String serverAddress, Object msg) {
        super.sendResponse(msgId, connect(serverAddress), msg);
    }

    protected GenericKeyedObjectPool.Config getNettyPoolConfig() {
        GenericKeyedObjectPool.Config poolConfig = new GenericKeyedObjectPool.Config();
        poolConfig.maxActive = nettyClientConfig.getMaxPoolActive();
        poolConfig.minIdle = nettyClientConfig.getMinPoolIdle();
        poolConfig.maxWait = nettyClientConfig.getMaxAcquireConnMills();
        poolConfig.testOnBorrow = nettyClientConfig.isPoolTestBorrow();
        poolConfig.testOnReturn = nettyClientConfig.isPoolTestReturn();
        poolConfig.lifo = nettyClientConfig.isPoolFifo();
        return poolConfig;
    }

    protected abstract TransactionRole getTransactionRole();

    /** 本客户端所属的事务分组（用于从配置里查 TC 地址） */
    protected abstract String getTransactionServiceGroup();

    /**
     * 合并发送线程：把 basketMap 里按地址攒的请求打包成 MergedWrapMessage 一次发出，
     * 减少系统调用与网络开销。
     */
    public class MergedSendRunnable implements Runnable {

        @Override
        public void run() {
            while (true) {
                synchronized (mergeLock) {
                    try {
                        mergeLock.wait(MAX_MERGE_SEND_MILLS);
                    } catch (InterruptedException e) {
                    }
                }
                isSending = true;
                for (String address : basketMap.keySet()) {
                    BlockingQueue<RpcMessage> basket = basketMap.get(address);
                    if (basket.isEmpty()) {
                        continue;
                    }

                    MergedWrapMessage mergeMessage = new MergedWrapMessage();
                    while (!basket.isEmpty()) {
                        RpcMessage msg = basket.poll();
                        mergeMessage.msgs.add((AbstractMessage) msg.getBody());
                        mergeMessage.msgIds.add(msg.getId());
                    }
                    if (mergeMessage.msgIds.size() > 1 && LOGGER.isDebugEnabled()) {
                        LOGGER.debug("merge msg size:" + mergeMessage.msgIds.size());
                    }
                    // connect 也必须包在 try 里：TC 不可达时它同样抛 FrameworkException，
                    // 而这是唯一的合并发送线程，一旦异常逃出 while(true) 就再也没人发送了
                    Channel sendChannel = null;
                    try {
                        sendChannel = connect(address);
                        sendRequest(sendChannel, mergeMessage);
                    } catch (FrameworkException e) {
                        if (e.getErrcode() == FrameworkErrorCode.ChannelIsNotWritable
                                && address != null && sendChannel != null) {
                            destroyChannel(address, sendChannel);
                        }
                        LOGGER.error("client merge call failed, address:" + address, e);
                    }
                }
                isSending = false;
            }
        }
    }
}
