package io.spring2go.seata.server;

import io.spring2go.seata.common.XID;
import io.spring2go.seata.common.thread.NamedThreadFactory;
import io.spring2go.seata.core.rpc.netty.RpcServer;
import io.spring2go.seata.server.coordinator.DefaultCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Embedded TC Server helper for in-process testing and examples.
 */
public class EmbeddedTC {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedTC.class);
    private static final EmbeddedTC INSTANCE = new EmbeddedTC();

    private final AtomicBoolean started = new AtomicBoolean(false);
    private RpcServer rpcServer;
    private ThreadPoolExecutor workingThreads;
    private DefaultCoordinator coordinator;
    private int port;

    private EmbeddedTC() {
    }

    public static EmbeddedTC getInstance() {
        return INSTANCE;
    }

    public synchronized void start(int port) {
        if (started.get()) {
            LOGGER.info("EmbeddedTC already started on port " + this.port);
            return;
        }

        this.port = port;
        XID.setIpAddress("127.0.0.1");
        XID.setPort(port);

        this.workingThreads = new ThreadPoolExecutor(
                5, 20, 500, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(512),
                new NamedThreadFactory("EmbeddedTCHandlerThread", 5),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        this.rpcServer = new RpcServer(this.workingThreads);
        this.rpcServer.setListenPort(port);

        this.coordinator = new DefaultCoordinator(null, null, this.rpcServer);
        this.rpcServer.setHandler(this.coordinator);

        Thread serverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    rpcServer.init();
                } catch (Throwable t) {
                    if (started.get()) {
                        LOGGER.error("EmbeddedTC server encountered error", t);
                    }
                }
            }
        }, "embedded-tc-server");
        serverThread.setDaemon(true);
        serverThread.start();

        // 轮询等待端口就绪（超时 3 秒）
        long deadline = System.currentTimeMillis() + 3000;
        boolean connected = false;
        while (System.currentTimeMillis() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", port), 200);
                connected = true;
                break;
            } catch (Exception ignored) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        if (!connected) {
            stop();
            throw new RuntimeException("EmbeddedTC failed to start on port " + port + " within timeout");
        }

        started.set(true);
        LOGGER.info("EmbeddedTC successfully started and listening on port " + port);
    }

    public synchronized void stop() {
        if (!started.get()) {
            return;
        }

        LOGGER.info("Stopping EmbeddedTC on port " + port + " ...");
        started.set(false);

        if (rpcServer != null) {
            try {
                rpcServer.shutdown();
            } catch (Exception e) {
                LOGGER.error("Error shutting down rpcServer", e);
            }
        }

        if (workingThreads != null) {
            try {
                workingThreads.shutdownNow();
            } catch (Exception e) {
                LOGGER.error("Error shutting down workingThreads", e);
            }
        }

        // 允许 socket 完全释放
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        LOGGER.info("EmbeddedTC stopped.");
    }

    public boolean isStarted() {
        return started.get();
    }

    public DefaultCoordinator getCoordinator() {
        return coordinator;
    }

    public int getPort() {
        return port;
    }
}
