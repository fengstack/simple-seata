package io.spring2go.seata.core.rpc.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.spring2go.seata.common.thread.NamedThreadFactory;
import io.spring2go.seata.core.rpc.RemotingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * TC 侧 Netty 接收容器：绑定端口、装配"空闲检测 + 协议编解码 + 业务分发"流水线。
 */
public abstract class AbstractRpcRemotingServer extends AbstractRpcRemoting implements RemotingService {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRpcRemotingServer.class);
    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup eventLoopGroupWorker;
    private final EventLoopGroup eventLoopGroupBoss;
    private final NettyServerConfig nettyServerConfig;
    private DefaultEventExecutorGroup defaultEventExecutorGroup;
    private int listenPort;

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getListenPort() {
        return listenPort;
    }

    public AbstractRpcRemotingServer(final NettyServerConfig nettyServerConfig, final ThreadPoolExecutor messageExecutor,
                                     final ChannelHandler... handlers) {
        super(messageExecutor);
        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerConfig = nettyServerConfig;
        this.eventLoopGroupBoss = new NioEventLoopGroup(nettyServerConfig.getBossThreadSize(),
                new NamedThreadFactory(nettyServerConfig.getBossThreadPrefix(), nettyServerConfig.getBossThreadSize()));
        this.eventLoopGroupWorker = new NioEventLoopGroup(nettyServerConfig.getServerWorkerThreads(),
                new NamedThreadFactory(nettyServerConfig.getWorkerThreadPrefix(),
                        nettyServerConfig.getServerWorkerThreads()));
        if (null != handlers && handlers.length > 0) {
            channelHandlers = handlers;
        }
    }

    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                nettyServerConfig.getServerWorkerThreads(),
                new NamedThreadFactory(nettyServerConfig.getExecutorThreadPrefix(),
                        nettyServerConfig.getServerWorkerThreads()));
        if (listenPort == 0) {
            listenPort = nettyServerConfig.getDefaultListenPort();
        }
        this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupWorker)
                .channel(nettyServerConfig.SERVER_CHANNEL_CLAZZ)
                .option(ChannelOption.SO_BACKLOG, nettyServerConfig.getSoBackLogSize())
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSendBufSize())
                .childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketResvBufSize())
                .childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK,
                        nettyServerConfig.getWriteBufferHighWaterMark())
                .childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK,
                        nettyServerConfig.getWriteBufferLowWaterMark())
                .localAddress(new InetSocketAddress(listenPort))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new IdleStateHandler(nettyServerConfig.getChannelMaxReadIdleSeconds(), 0, 0))
                                .addLast(new MessageCodecHandler());
                        if (null != channelHandlers) {
                            addChannelPipelineLast(ch, channelHandlers);
                        }
                    }
                });

        try {
            LOGGER.info("Server starting on port " + listenPort + " ...");
            ChannelFuture future = this.serverBootstrap.bind(listenPort).sync();
            LOGGER.info("Server started ... ");
            // 阻塞直到服务端 Channel 关闭（独立进程模式下让 main 线程驻留）
            future.channel().closeFuture().sync();
        } catch (InterruptedException exx) {
            throw new RuntimeException(exx);
        }
    }

    @Override
    public void shutdown() {
        try {
            this.eventLoopGroupBoss.shutdownGracefully();
            this.eventLoopGroupWorker.shutdownGracefully();
            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception exx) {
            LOGGER.error(exx.getMessage());
        }
    }

    @Override
    public void destroyChannel(String serverAddress, Channel channel) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("will destroy channel:" + channel + ",address:" + serverAddress);
        }
        channel.disconnect();
        channel.close();
    }
}
