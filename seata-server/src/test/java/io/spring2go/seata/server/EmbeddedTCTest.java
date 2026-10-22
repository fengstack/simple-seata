package io.spring2go.seata.server;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
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

    /**
     * 场景：端口已被占用时启动 EmbeddedTC。
     * 要求：直接报"端口被占用"错误（而不是干等 3 秒后报一个含糊的超时），
     * 失败后状态干净，之后换一个端口还能正常启动。
     */
    @Test
    public void testStartFailsCleanlyOnOccupiedPort() throws Exception {
        EmbeddedTC embeddedTC = EmbeddedTC.getInstance();

        try (ServerSocket occupied = new ServerSocket(8093)) { // 先占住端口
            try {
                embeddedTC.start(8093);
                Assert.fail("Expected start failure on occupied port");
            } catch (RuntimeException e) {
                Assert.assertTrue("Error should mention port in use but was: " + e.getMessage(),
                        e.getMessage().contains("already in use"));
            }
        }

        // 启动失败后状态必须干净：未启动，且换端口可以正常启动
        Assert.assertFalse(embeddedTC.isStarted());
        embeddedTC.start(8094);
        Assert.assertTrue(embeddedTC.isStarted());
        embeddedTC.stop();
    }
}
