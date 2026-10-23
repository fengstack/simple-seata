package io.spring2go.seata.core.rpc.netty;

import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.spring2go.seata.core.rpc.netty.NettyPoolKey.TransactionRole;

/**
 * Netty 客户端（TM/RM 共用）参数。
 */
public class NettyClientConfig extends NettyBaseConfig {

    private int connectTimeoutMillis = 10000;
    private int clientSocketSndBufSize = 153600;
    private int clientSocketRcvBufSize = 153600;
    private int clientWorkerThreads = WORKER_THREAD_SIZE;
    private final Class<? extends Channel> clientChannelClazz = NioSocketChannel.class;

    private static final int RPC_REQUEST_TIMEOUT = 30 * 1000;
    private static final int MAX_NOT_WRITEABLE_RETRY = 2000;
    private static final int MAX_CHECK_ALIVE_RETRY = 300;
    private static final int CHECK_ALIVE_INTERNAL = 10;
    private static final String SOCKET_ADDRESS_START_CHAR = "/";
    private static final int DEFAULT_SELECTOR_THREAD_SIZE = 1;
    private static final long MAX_ACQUIRE_CONN_MILLS = 60 * 1000L;
    private static final String DEFAULT_SELECTOR_THREAD_PREFIX = "NettyClientSelector";
    private static final String DEFAULT_WORKER_THREAD_PREFIX = "NettyClientWorkerThread";
    private static final String RPC_DISPATCH_THREAD_PREFIX = "rpcDispatch";

    // GenericKeyedObjectPool 连接池参数
    private static final int DEFAULT_MAX_POOL_ACTIVE = 1;
    private static final int DEFAULT_MIN_POOL_IDLE = 0;
    private static final boolean DEFAULT_POOL_TEST_BORROW = true;
    private static final boolean DEFAULT_POOL_TEST_RETURN = true;
    private static final boolean DEFAULT_POOL_FIFO = true;

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public int getClientSocketSndBufSize() {
        return clientSocketSndBufSize;
    }

    public int getClientSocketRcvBufSize() {
        return clientSocketRcvBufSize;
    }

    public int getChannelMaxWriteIdleSeconds() {
        return MAX_WRITE_IDLE_SECONDS;
    }

    public int getChannelMaxReadIdleSeconds() {
        return MAX_READ_IDLE_SECONDS;
    }

    public int getChannelMaxAllIdleSeconds() {
        return MAX_ALL_IDLE_SECONDS;
    }

    public int getClientWorkerThreads() {
        return clientWorkerThreads;
    }

    public Class<? extends Channel> getClientChannelClazz() {
        return clientChannelClazz;
    }

    public static int getRpcRequestTimeout() {
        return RPC_REQUEST_TIMEOUT;
    }

    public static int getMaxNotWriteableRetry() {
        return MAX_NOT_WRITEABLE_RETRY;
    }

    public static int getMaxCheckAliveRetry() {
        return MAX_CHECK_ALIVE_RETRY;
    }

    public static int getCheckAliveInternal() {
        return CHECK_ALIVE_INTERNAL;
    }

    public static String getSocketAddressStartChar() {
        return SOCKET_ADDRESS_START_CHAR;
    }

    public int getClientSelectorThreadSize() {
        return DEFAULT_SELECTOR_THREAD_SIZE;
    }

    public long getMaxAcquireConnMills() {
        return MAX_ACQUIRE_CONN_MILLS;
    }

    public String getClientSelectorThreadPrefix() {
        return DEFAULT_SELECTOR_THREAD_PREFIX;
    }

    public String getClientWorkerThreadPrefix() {
        return DEFAULT_WORKER_THREAD_PREFIX;
    }

    public int getMaxPoolActive() {
        return DEFAULT_MAX_POOL_ACTIVE;
    }

    public int getMinPoolIdle() {
        return DEFAULT_MIN_POOL_IDLE;
    }

    public boolean isPoolTestBorrow() {
        return DEFAULT_POOL_TEST_BORROW;
    }

    public boolean isPoolTestReturn() {
        return DEFAULT_POOL_TEST_RETURN;
    }

    public boolean isPoolFifo() {
        return DEFAULT_POOL_FIFO;
    }

    public String getTmDispatchThreadPrefix() {
        return RPC_DISPATCH_THREAD_PREFIX + "_" + TransactionRole.TMROLE.name();
    }

    public String getRmDispatchThreadPrefix() {
        return RPC_DISPATCH_THREAD_PREFIX + "_" + TransactionRole.RMROLE.name();
    }
}
