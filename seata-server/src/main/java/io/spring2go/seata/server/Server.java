package io.spring2go.seata.server;

import io.spring2go.seata.common.XID;
import io.spring2go.seata.common.thread.NamedThreadFactory;
import io.spring2go.seata.core.rpc.netty.RpcServer;
import io.spring2go.seata.server.coordinator.DefaultCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
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

        LOGGER.info("Starting Seata In-Memory TC Server on port " + port + " ...");
        rpcServer.init(); // Blocks until server channel closes
    }
}
