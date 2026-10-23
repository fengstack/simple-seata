package io.spring2go.seata.core.rpc.netty;

import io.netty.util.NettyRuntime;

/**
 * Netty 传输层公共参数。
 * 教学实现固定使用 NIO（跨平台、无额外依赖），不做 epoll/kqueue/UDS 等原生传输选择。
 */
public class NettyBaseConfig {

    /** IO 与业务线程数 */
    protected static final int WORKER_THREAD_SIZE = NettyRuntime.availableProcessors() * 2;

    /**
     * 空闲检测秒数。0 表示不启用——心跳 PING/PONG 的编解码与收发链路保留在教学代码里，
     * 但默认不触发空闲事件。
     */
    protected static final int MAX_WRITE_IDLE_SECONDS = 0;
    protected static final int MAX_READ_IDLE_SECONDS = MAX_WRITE_IDLE_SECONDS * 3;
    protected static final int MAX_ALL_IDLE_SECONDS = 0;
}
