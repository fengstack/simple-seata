package io.spring2go.seata.server;

import io.spring2go.seata.common.XID;
import io.spring2go.seata.common.thread.NamedThreadFactory;
import io.spring2go.seata.core.rpc.netty.RpcServer;
import io.spring2go.seata.server.coordinator.DefaultCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Embedded TC Server helper for in-process testing and examples.
 */
public class EmbeddedTC {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedTC.class);
    private static final EmbeddedTC INSTANCE = new EmbeddedTC();

    private final AtomicBoolean started = new AtomicBoolean(false);
    private RpcServer rpcServer;
    private ThreadPoolExecutor workingThreads;
    private ScheduledExecutorService timeoutChecker;
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

        // 预检：启动前先确认端口没人用。后面的"就绪探测"是往端口发连接，
        // 如果端口本来就被别人占着，连接也会成功，会把启动误判成成功
        Socket probe = new Socket();
        try {
            probe.connect(new InetSocketAddress("127.0.0.1", port), 200);
            throw new RuntimeException("EmbeddedTC failed to start: port " + port + " already in use");
        } catch (java.io.IOException portIsFree) {
            // 连接失败（拒绝或超时）= 端口空闲，可以启动
        } finally {
            try {
                probe.close();
            } catch (java.io.IOException ignore) {
            }
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

        // 记录 rpcServer.init() 的失败原因，否则根因（如端口被占用）会被吞掉
        final AtomicReference<Throwable> initError = new AtomicReference<Throwable>();
        Thread serverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    rpcServer.init();
                } catch (Throwable t) {
                    initError.set(t);
                    LOGGER.error("EmbeddedTC server failed to init", t);
                }
            }
        }, "embedded-tc-server");
        serverThread.setDaemon(true);
        serverThread.start();

        // 轮询等待端口就绪（超时 3 秒；一旦发现 init 失败立即退出，不等满超时）
        long deadline = System.currentTimeMillis() + 3000;
        boolean connected = false;
        while (System.currentTimeMillis() < deadline) {
            if (initError.get() != null) {
                break;
            }
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
            // 启动失败也必须清理已经创建的资源（线程池、rpcServer），否则会泄漏
            stop();
            throw new RuntimeException("EmbeddedTC failed to start on port " + port
                    + " (port already in use?)", initError.get());
        }

        // 超时检查线程：每秒扫描一次会话。
        // 1) 超时还没提交/回滚的全局事务自动回滚（TM 可能已经宕机）；
        // 2) 二阶段没做完的会话（CommitRetrying/RollbackRetrying）继续重试。
        this.timeoutChecker = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("TCTimeoutChecker", 1, true));
        this.timeoutChecker.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    coordinator.timeoutCheck();
                } catch (Throwable t) {
                    LOGGER.warn("Timeout check error", t);
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);

        started.set(true);
        LOGGER.info("EmbeddedTC successfully started and listening on port " + port);
    }

    public synchronized void stop() {
        // 不再依赖 started 标志做前置判断：start() 中途失败时也要能进来清理
        LOGGER.info("Stopping EmbeddedTC on port " + port + " ...");
        started.set(false);

        if (timeoutChecker != null) {
            timeoutChecker.shutdownNow();
            timeoutChecker = null;
        }

        if (rpcServer != null) {
            try {
                rpcServer.shutdown();
            } catch (Exception e) {
                LOGGER.error("Error shutting down rpcServer", e);
            }
            rpcServer = null;
        }

        if (workingThreads != null) {
            try {
                workingThreads.shutdownNow();
            } catch (Exception e) {
                LOGGER.error("Error shutting down workingThreads", e);
            }
            workingThreads = null;
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
