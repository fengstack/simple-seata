package io.spring2go.seata.core.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.spring2go.seata.core.model.BranchType;
import io.spring2go.seata.core.protocol.transaction.BranchCommitRequest;
import io.spring2go.seata.core.protocol.transaction.BranchRegisterRequest;
import io.spring2go.seata.core.protocol.transaction.BranchRegisterResponse;
import org.junit.Assert;
import org.junit.Test;

/**
 * 协议消息的 encode/decode 回归测试：encode 之后再 decode，字段必须一字不差地还原。
 *
 * 这里覆盖的都是历史上真实出过 bug 的点：
 * 1. AbstractBranchEndRequest 的 applicationData 长度 encode 用 int(4字节)、
 *    decode 却按 short(2字节) 读——宽度不一致会让字节流错位（运行日志里
 *    "magicIndex is not zero offset" 的告警就是它的表现）。
 * 2. 合并帧（MergedWrapMessage/MergeResultMessage）曾按"条数*1024"固定分配
 *    缓冲，任一子消息超过 1KB 就 BufferOverflowException。
 * 3. 半包：TCP 分段到达时 decode 必须返回 false 等待剩余字节，
 *    绝不能抛 IndexOutOfBoundsException（那会杀掉整条连接）。
 */
public class ProtocolCodecRoundTripTest {

    private BranchCommitRequest buildBranchCommitRequest() {
        BranchCommitRequest request = new BranchCommitRequest();
        request.setXid("127.0.0.1:8091:123456789");
        request.setBranchId(10001L);
        request.setBranchType(BranchType.AT);
        request.setResourceId("jdbc:h2:mem:seata_account;MODE=MySQL");
        // 超过 512 字节，同时覆盖 encode 里重新分配缓冲区的分支
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 600; i++) {
            sb.append('a');
        }
        request.setApplicationData(sb.toString());
        return request;
    }

    /**
     * 场景 1：BranchCommitRequest 完整往返。
     * applicationData 非空时，长度字段的读写宽度必须一致（int 对 int）。
     */
    @Test
    public void testBranchCommitRequestRoundTrip() {
        BranchCommitRequest origin = buildBranchCommitRequest();

        BranchCommitRequest decoded = new BranchCommitRequest();
        Assert.assertTrue(decoded.decode(Unpooled.wrappedBuffer(origin.encode())));

        Assert.assertEquals(origin.getXid(), decoded.getXid());
        Assert.assertEquals(origin.getBranchId(), decoded.getBranchId());
        Assert.assertEquals(origin.getBranchType(), decoded.getBranchType());
        Assert.assertEquals(origin.getResourceId(), decoded.getResourceId());
        Assert.assertEquals(origin.getApplicationData(), decoded.getApplicationData());
    }

    /**
     * 场景 2：消息体在任意位置被 TCP 拆成两半。
     * 前半段 decode 必须返回 false 且不抛异常；补齐后必须能完整还原。
     */
    @Test
    public void testBranchCommitRequestHalfPacket() {
        byte[] full = buildBranchCommitRequest().encode();

        for (int split = 0; split < full.length; split++) {
            BranchCommitRequest decoded = new BranchCommitRequest();
            ByteBuf buf = Unpooled.buffer();
            buf.writeBytes(full, 0, split);

            boolean completeAtFirstTry = decoded.decode(buf);
            if (!completeAtFirstTry) {
                // 模拟 MessageCodecHandler 的行为：decode 返回 false 时回退读指针，等剩余字节
                buf.readerIndex(0);
                buf.writeBytes(full, split, full.length - split);
                Assert.assertTrue("split at " + split + ": 补齐后必须解码成功", decoded.decode(buf));
            }
            Assert.assertEquals("split at " + split, "127.0.0.1:8091:123456789", decoded.getXid());
            Assert.assertEquals("split at " + split, 10001L, decoded.getBranchId());
            Assert.assertNotNull("split at " + split, decoded.getApplicationData());
        }
    }

    private RegisterRMRequest buildRegisterRMRequest() {
        RegisterRMRequest request = new RegisterRMRequest("demo-app", "default_tx_group");
        request.setExtraData("extra");
        request.setResourceIds("jdbc:h2:mem:seata_storage;jdbc:h2:mem:seata_account");
        return request;
    }

    /**
     * 场景 3：RM 注册报文的往返与半包安全。
     * 它有 4 个 short 长度字段 + 1 个 int 长度字段，历史上半包时直接抛越界异常。
     */
    @Test
    public void testRegisterRMRequestRoundTripAndHalfPacket() {
        RegisterRMRequest origin = buildRegisterRMRequest();
        byte[] full = origin.encode();

        // 完整往返
        RegisterRMRequest decoded = new RegisterRMRequest();
        Assert.assertTrue(decoded.decode(Unpooled.wrappedBuffer(full)));
        Assert.assertEquals(origin.getApplicationId(), decoded.getApplicationId());
        Assert.assertEquals(origin.getTransactionServiceGroup(), decoded.getTransactionServiceGroup());
        Assert.assertEquals(origin.getExtraData(), decoded.getExtraData());
        Assert.assertEquals(origin.getResourceIds(), decoded.getResourceIds());

        // 任意位置半包：返回 false 等剩余字节，不抛异常
        for (int split = 0; split < full.length; split++) {
            RegisterRMRequest partial = new RegisterRMRequest();
            ByteBuf buf = Unpooled.buffer();
            buf.writeBytes(full, 0, split);
            Assert.assertFalse("split at " + split + ": 不完整的报文必须返回 false",
                    partial.decode(buf));
        }
    }

    /**
     * 场景 4：合并请求帧携带超过 1KB 的子消息。
     * 固定"条数*1024"缓冲会在这里 BufferOverflowException，缓冲必须按实际编码长度分配。
     */
    @Test
    public void testMergedWrapMessageWithLargeBody() {
        StringBuilder bigLockKey = new StringBuilder();
        for (int i = 0; i < 3000; i++) {
            bigLockKey.append('k');
        }

        BranchRegisterRequest big = new BranchRegisterRequest();
        big.setTransactionId(123456789L);
        big.setBranchType(BranchType.AT);
        big.setResourceId("jdbc:h2:mem:seata_storage");
        big.setLockKey(bigLockKey.toString());

        BranchRegisterRequest small = new BranchRegisterRequest();
        small.setTransactionId(987654321L);
        small.setBranchType(BranchType.AT);
        small.setResourceId("jdbc:h2:mem:seata_account");
        small.setLockKey("account:1");

        MergedWrapMessage merged = new MergedWrapMessage();
        merged.msgs.add(big);
        merged.msgs.add(small);

        MergedWrapMessage decoded = new MergedWrapMessage();
        Assert.assertTrue(decoded.decode(Unpooled.wrappedBuffer(merged.encode())));

        Assert.assertEquals(2, decoded.msgs.size());
        BranchRegisterRequest decodedBig = (BranchRegisterRequest) decoded.msgs.get(0);
        BranchRegisterRequest decodedSmall = (BranchRegisterRequest) decoded.msgs.get(1);
        Assert.assertEquals(big.getTransactionId(), decodedBig.getTransactionId());
        Assert.assertEquals(big.getLockKey(), decodedBig.getLockKey());
        Assert.assertEquals(small.getTransactionId(), decodedSmall.getTransactionId());
        Assert.assertEquals(small.getLockKey(), decodedSmall.getLockKey());
    }

    /**
     * 场景 5：合并响应帧的往返（与合并请求帧共用同一套按实际长度分配缓冲的编码逻辑）。
     */
    @Test
    public void testMergeResultMessageRoundTrip() {
        BranchRegisterResponse first = new BranchRegisterResponse();
        first.setResultCode(ResultCode.Success);
        first.setTransactionId(123456789L);
        first.setBranchId(10001L);

        BranchRegisterResponse second = new BranchRegisterResponse();
        second.setResultCode(ResultCode.Success);
        second.setTransactionId(987654321L);
        second.setBranchId(10002L);

        MergeResultMessage merged = new MergeResultMessage();
        merged.setMsgs(new AbstractResultMessage[] {first, second});

        MergeResultMessage decoded = new MergeResultMessage();
        Assert.assertTrue(decoded.decode(Unpooled.wrappedBuffer(merged.encode())));

        Assert.assertEquals(2, decoded.getMsgs().length);
        BranchRegisterResponse decodedFirst = (BranchRegisterResponse) decoded.getMsgs()[0];
        BranchRegisterResponse decodedSecond = (BranchRegisterResponse) decoded.getMsgs()[1];
        Assert.assertEquals(first.getBranchId(), decodedFirst.getBranchId());
        Assert.assertEquals(second.getBranchId(), decodedSecond.getBranchId());
    }
}
