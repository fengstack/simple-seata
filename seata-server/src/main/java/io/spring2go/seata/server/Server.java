package io.spring2go.seata.server;

import io.spring2go.seata.common.XID;
import io.spring2go.seata.common.thread.NamedThreadFactory;
import io.spring2go.seata.core.rpc.netty.RpcServer;
import io.spring2go.seata.server.coordinator.DefaultCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Server entry point for standalone TC process.
 */
public class Server {
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);
    private static final int DEFAULT_PORT = 8091;

    public static void main(String[] args) {
        int port = DEFAULT_PORT;
        if (args != null && args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid port argument: " + args[0] + ", using default: " + DEFAULT_PORT);
            }
        }

        XID.setIpAddress("127.0.0.1");
        XID.setPort(port);

        ThreadPoolExecutor workingThreads = new ThreadPoolExecutor(
                10, 50, 500, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1024),
                new NamedThreadFactory("ServerHandlerThread", 10),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        RpcServer rpcServer = new RpcServer(workingThreads);
        rpcServer.setListenPort(port);

        DefaultCoordinator coordinator = new DefaultCoordinator(null, null, rpcServer);
        rpcServer.setHandler(coordinator);

        // 超时检查线程（与 EmbeddedTC 相同的兜底逻辑），每秒扫描一次会话：
        // 1) 超时还没提交/回滚的全局事务自动回滚（TM 可能已经宕机）；
        // 2) 二阶段没做完的会话（CommitRetrying/RollbackRetrying）继续重试。
        // 独立 TC 进程绝不能省这一步，否则超时事务的会话和行锁会永久占用
        ScheduledExecutorService timeoutChecker = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("TCTimeoutChecker", 1, true));
        timeoutChecker.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    coordinator.timeoutCheck();
                } catch (Throwable t) {
                    LOGGER.warn("Timeout check error", t);
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);

        LOGGER.info("Starting Seata In-Memory TC Server on port " + port + " ...");
        rpcServer.init(); // Blocks until server channel closes
    }
}
