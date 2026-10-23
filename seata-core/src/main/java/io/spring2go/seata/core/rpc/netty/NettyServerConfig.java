package io.spring2go.seata.core.rpc.netty;

import io.netty.channel.ServerChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * Netty 服务端（TC）参数。
 */
public class NettyServerConfig extends NettyBaseConfig {

    private int serverSelectorThreads = WORKER_THREAD_SIZE;
    private int serverSocketSendBufSize = 153600;
    private int serverSocketResvBufSize = 153600;
    private int serverWorkerThreads = WORKER_THREAD_SIZE;
    private int soBackLogSize = 1024;
    private int writeBufferHighWaterMark = 67108864;
    private int writeBufferLowWaterMark = 1048576;

    private static final int DEFAULT_LISTEN_PORT = 8091;
    private static final int RPC_REQUEST_TIMEOUT = 30 * 1000;
    private static final String DEFAULT_BOSS_THREAD_PREFIX = "NettyBoss";
    private static final String NIO_WORKER_THREAD_PREFIX = "NettyServerNIOWorker";
    private static final String DEFAULT_EXECUTOR_THREAD_PREFIX = "NettyServerBizHandler";
    private static final int DEFAULT_BOSS_THREAD_SIZE = 1;

    /** The Server channel clazz. */
    public final Class<? extends ServerChannel> SERVER_CHANNEL_CLAZZ = NioServerSocketChannel.class;

    public int getServerSelectorThreads() {
        return serverSelectorThreads;
    }

    public int getServerSocketSendBufSize() {
        return serverSocketSendBufSize;
    }

    public int getServerSocketResvBufSize() {
        return serverSocketResvBufSize;
    }

    public int getServerWorkerThreads() {
        return serverWorkerThreads;
    }

    public int getSoBackLogSize() {
        return soBackLogSize;
    }

    public int getWriteBufferHighWaterMark() {
        return writeBufferHighWaterMark;
    }

    public int getWriteBufferLowWaterMark() {
        return writeBufferLowWaterMark;
    }

    public int getDefaultListenPort() {
        return DEFAULT_LISTEN_PORT;
    }

    public int getChannelMaxReadIdleSeconds() {
        return MAX_READ_IDLE_SECONDS;
    }

    public static int getRpcRequestTimeout() {
        return RPC_REQUEST_TIMEOUT;
    }

    public String getBossThreadPrefix() {
        return DEFAULT_BOSS_THREAD_PREFIX;
    }

    public String getWorkerThreadPrefix() {
        return NIO_WORKER_THREAD_PREFIX;
    }

    public String getExecutorThreadPrefix() {
        return DEFAULT_EXECUTOR_THREAD_PREFIX;
    }

    public int getBossThreadSize() {
        return DEFAULT_BOSS_THREAD_SIZE;
    }
}
