package io.spring2go.seata.core.rpc.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.spring2go.seata.core.protocol.HeartbeatMessage;
import io.spring2go.seata.core.protocol.RpcMessage;
import org.junit.Assert;
import org.junit.Test;

/**
 * 验证 0xdada 协议解码器对"半包"的处理：
 * magic 只有半个字节到达时不能把字节丢掉（否则整个字节流错位，后面的消息全部解不出来）。
 */
public class MessageCodecHandlerTest {

    /**
     * 构造一个最简单的完整报文：心跳帧。
     * 结构：magic(2) + flag(2) + type(2) + msgId(8) = 14 字节定长头，无消息体。
     */
    private ByteBuf heartbeatFrame(short flag, long msgId) {
        ByteBuf buf = Unpooled.buffer(14);
        buf.writeShort(0xdada);
        buf.writeShort(flag);
        buf.writeShort(0);
        buf.writeLong(msgId);
        return buf;
    }

    private static final short FLAG_REQUEST_HEARTBEAT = (short) (0x80 | 0x20);

    /**
     * 场景 1：TCP 先送达了 magic 的前 1 个字节（0xda）。
     * 解码器必须保留它等下一个字节，不能当成"找不到 magic"丢掉。
     */
    @Test
    public void testHalfMagicIsNotDiscarded() {
        EmbeddedChannel channel = new EmbeddedChannel(new MessageCodecHandler());

        // 先只喂 magic 的第一个字节
        ByteBuf firstByte = Unpooled.buffer();
        firstByte.writeByte(0xda);
        channel.writeInbound(firstByte);
        Assert.assertNull("半截 magic 不应该解出消息", channel.readInbound());

        // 再喂剩下的 13 个字节，应当能解出完整心跳
        ByteBuf rest = heartbeatFrame(FLAG_REQUEST_HEARTBEAT, 1L);
        rest.readByte(); // 去掉 magic 第一个字节（已经在上面喂过了）
        channel.writeInbound(rest);

        RpcMessage message = (RpcMessage) channel.readInbound();
        Assert.assertNotNull("拆成两半送达的帧必须能正常解码", message);
        Assert.assertEquals(1L, message.getId());
        Assert.assertTrue(message.isHeartbeat());
        Assert.assertEquals(HeartbeatMessage.PING, message.getBody());
    }

    /**
     * 场景 2：报文头（14 字节）没到齐。
     * 解码器应该安静地等更多数据，正常解出后续消息。
     */
    @Test
    public void testPartialHeaderWaitsForMoreBytes() {
        EmbeddedChannel channel = new EmbeddedChannel(new MessageCodecHandler());

        ByteBuf frame = heartbeatFrame(FLAG_REQUEST_HEARTBEAT, 2L);
        ByteBuf head = frame.readBytes(10); // 只到前 10 个字节（不足 14）
        channel.writeInbound(head);
        Assert.assertNull("不完整的报文头不应该解出消息", channel.readInbound());

        ByteBuf tail = frame.readBytes(frame.readableBytes());
        channel.writeInbound(tail);

        RpcMessage message = (RpcMessage) channel.readInbound();
        Assert.assertNotNull(message);
        Assert.assertEquals(2L, message.getId());
    }

    /**
     * 场景 3：字节流里混进了脏数据。
     * 丢弃脏数据时要保留最后 1 个字节（它可能是下一个 magic 的前半段），
     * 否则紧跟其后的合法帧会从中间开始解，永远解不出来。
     */
    @Test
    public void testDirtyBytesKeepTrailingByteForResync() {
        EmbeddedChannel channel = new EmbeddedChannel(new MessageCodecHandler());

        // 3 个字节的脏数据
        ByteBuf dirty = Unpooled.buffer();
        dirty.writeByte(0x01);
        dirty.writeByte(0x02);
        dirty.writeByte(0x03);
        channel.writeInbound(dirty);
        Assert.assertNull(channel.readInbound());

        // 紧跟着一个完整的合法帧
        channel.writeInbound(heartbeatFrame(FLAG_REQUEST_HEARTBEAT, 3L));

        RpcMessage message = (RpcMessage) channel.readInbound();
        Assert.assertNotNull("脏数据之后的合法帧必须还能解出来", message);
        Assert.assertEquals(3L, message.getId());
    }
}
