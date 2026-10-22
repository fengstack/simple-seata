package io.spring2go.seata.server;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.Socket;

public class EmbeddedTCTest {

    @Test
    public void testEmbeddedTCStartAndStop() throws Exception {
        EmbeddedTC embeddedTC = EmbeddedTC.getInstance();
        int port = 8091;

        // 1. 启动内嵌服务端
        embeddedTC.start(port);
        Assert.assertTrue(embeddedTC.isStarted());
        Assert.assertNotNull(embeddedTC.getCoordinator());

        // 2. 验证 Netty 端口成功监听，TCP Socket 可正常连接
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 2000);
            Assert.assertTrue(socket.isConnected());
        }

        // 3. 停止内嵌服务端
        embeddedTC.stop();
        Assert.assertFalse(embeddedTC.isStarted());
    }
}
