package io.spring2go.seata.core.rpc.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import io.spring2go.seata.core.protocol.AbstractMessage;
import io.spring2go.seata.core.protocol.HeartbeatMessage;
import io.spring2go.seata.core.protocol.MessageCodec;
import io.spring2go.seata.core.protocol.RpcMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * 0xdada 二进制协议编解码器。帧结构（14 字节定长头 + 变长体）：
 *
 *   magic(2) | flag(2) | type/length(2) | msgId(8) | body(...)
 *
 * flag 里的 FLAG_FESCAR_CODEC 区分"自定义编码的消息体"（全部业务消息）
 * 与心跳帧（无消息体）。处理 TCP 半包/粘包：头没到齐、体没到齐都先攒着。
 */
public class MessageCodecHandler extends ByteToMessageCodec<RpcMessage> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageCodecHandler.class);
    private static final short MAGIC = (short) 0xdada;
    private static final int HEAD_LENGHT = 14;
    private static final int FLAG_REQUEST = 0x80;
    private static final int FLAG_ASYNC = 0x40;
    private static final int FLAG_HEARTBEAT = 0x20;
    private static final int FLAG_FESCAR_CODEC = 0x10;
    private static final int MAGIC_HALF = -38; // (byte)0xda
    private static final int NOT_FOUND_INDEX = -1;

    @Override
    protected void encode(ChannelHandlerContext ctx, RpcMessage msg, ByteBuf out) throws Exception {
        byte[] head = encodeHeader(msg);
        out.writeBytes(head);
        if (msg.getBody() instanceof MessageCodec) {
            // 消息体自带二进制编码
            out.writeBytes(((MessageCodec) msg.getBody()).encode());
        } else if (!(msg.getBody() instanceof HeartbeatMessage)) {
            throw new IllegalArgumentException(
                    "Unsupported message body (must be MessageCodec or HeartbeatMessage): " + msg.getBody());
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Send:" + msg.getBody());
        }
    }

    /** 14 字节定长头：心跳帧没有消息体，其余帧先写头、体由调用方追加 */
    private byte[] encodeHeader(RpcMessage msg) {
        MessageCodec msgCodec = msg.getBody() instanceof MessageCodec ? (MessageCodec) msg.getBody() : null;
        ByteBuffer byteBuffer = ByteBuffer.allocate(HEAD_LENGHT);
        byteBuffer.putShort(MAGIC);
        int flag = (msg.isAsync() ? FLAG_ASYNC : 0)
                | (msg.isHeartbeat() ? FLAG_HEARTBEAT : 0)
                | (msg.isRequest() ? FLAG_REQUEST : 0)
                | (msgCodec != null ? FLAG_FESCAR_CODEC : 0);
        byteBuffer.putShort((short) flag);
        if (msgCodec != null) {
            byteBuffer.putShort(msgCodec.getTypeCode()); // type
        } else {
            byteBuffer.putShort((short) 0); // 心跳帧无消息体
        }
        byteBuffer.putLong(msg.getId());
        byteBuffer.flip();
        byte[] content = new byte[byteBuffer.limit()];
        byteBuffer.get(content);
        return content;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        int begin = in.readerIndex();
        int magicIndex = getMagicIndex(in);
        if (magicIndex == NOT_FOUND_INDEX) {
            // 半包：可能 magic 两个字节只到了一半。最多只剩 2 个字节时直接等下一次数据，
            // 千万不能把字节丢掉，否则整个字节流就错位了
            if (in.readableBytes() <= 2) {
                return;
            }
            // 真正的脏数据才丢弃，并且保留最后 1 个字节——它可能是下一个 magic 的前半段
            LOGGER.error("codec decode not found magic offset, discard dirty bytes");
            in.skipBytes(in.readableBytes() - 1);
            return;
        }
        if (magicIndex != 0 && LOGGER.isInfoEnabled()) {
            LOGGER.info("please notice magicIndex is not zero offset!!!");
        }
        in.skipBytes(magicIndex - in.readerIndex());
        if (in.readableBytes() < HEAD_LENGHT) {
            // 半包：报文头（14 字节定长）还没到齐，等下一次数据，这是正常现象
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("decode wait more bytes for header");
            }
            return;
        }
        byte[] buffer = new byte[HEAD_LENGHT];
        in.readBytes(buffer);
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        short magic = byteBuffer.getShort();
        if (magic != MAGIC) {
            LOGGER.error("decode error, will close channel:" + ctx.channel());
            ctx.channel().close();
            return;
        }

        int flag = byteBuffer.getShort();
        boolean isHeartbeat = (FLAG_HEARTBEAT & flag) > 0;
        boolean isRequest = (FLAG_REQUEST & flag) > 0;
        boolean isFescarCodec = (FLAG_FESCAR_CODEC & flag) > 0;
        short typeCode = byteBuffer.getShort();
        long msgId = byteBuffer.getLong();

        if (isHeartbeat) {
            RpcMessage rpcMessage = new RpcMessage();
            rpcMessage.setId(msgId);
            rpcMessage.setAsync(true);
            rpcMessage.setHeartbeat(true);
            rpcMessage.setRequest(isRequest);
            rpcMessage.setBody(isRequest ? HeartbeatMessage.PING : HeartbeatMessage.PONG);
            out.add(rpcMessage);
            return;
        }

        if (!isFescarCodec) {
            // 除了心跳帧，本协议只传输自定义编码的消息体
            LOGGER.error("decode error (not a codec frame), will close channel:" + ctx.channel());
            ctx.channel().close();
            return;
        }

        RpcMessage rpcMessage = new RpcMessage();
        rpcMessage.setId(msgId);
        rpcMessage.setAsync((FLAG_ASYNC & flag) > 0);
        rpcMessage.setHeartbeat(false);
        rpcMessage.setRequest(isRequest);

        MessageCodec msgCodec = AbstractMessage.getMsgInstanceByCode(typeCode);
        if (msgCodec == null) {
            LOGGER.error("unknown typeCode " + typeCode + ", will close channel:" + ctx.channel());
            ctx.channel().close();
            return;
        }
        if (!msgCodec.decode(in)) {
            // 消息体没到齐（半包）：回退读指针，等下一次数据
            in.readerIndex(begin);
            return;
        }
        rpcMessage.setBody(msgCodec);
        out.add(rpcMessage);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Receive:" + rpcMessage.getBody() + ",messageId:" + msgId);
        }
    }

    /** 在字节流里向前搜索 magic（0xdada）的位置，找不到返回 -1 */
    private static int getMagicIndex(ByteBuf in) {
        int readIndex = in.readerIndex();
        while (readIndex < in.writerIndex() - 1) {
            if (in.getByte(readIndex) == MAGIC_HALF && in.getByte(readIndex + 1) == MAGIC_HALF) {
                return readIndex;
            }
            ++readIndex;
        }
        return NOT_FOUND_INDEX;
    }
}
