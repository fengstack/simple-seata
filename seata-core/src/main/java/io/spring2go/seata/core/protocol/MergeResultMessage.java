package io.spring2go.seata.core.protocol;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Created by william on May, 2020
 */
public class MergeResultMessage extends AbstractMessage implements MergeMessage{
    private static final long serialVersionUID = -7719219648774528552L;
    public AbstractResultMessage[] msgs;
    private static final Logger LOGGER = LoggerFactory.getLogger(MergeResultMessage.class);

    public AbstractResultMessage[] getMsgs() {
        return msgs;
    }

    public void setMsgs(AbstractResultMessage[] msgs) {
        this.msgs = msgs;
    }

    @Override
    public short getTypeCode() {
        return TYPE_SEATA_MERGE_RESULT;
    }

    @Override
    public byte[] encode() {
        // 先让每个子消息完成编码，再按实际长度之和分配缓冲：
        // 固定按"条数*1024"分配时，任一子消息超过 1KB 就会 BufferOverflowException
        byte[][] encoded = new byte[msgs.length][];
        int bufferSize = 2; // msgNum
        for (int i = 0; i < msgs.length; i++) {
            encoded[i] = msgs[i].encode();
            bufferSize += 2 + encoded[i].length; // typeCode + body
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
        byteBuffer.putShort((short)msgs.length);
        for (int i = 0; i < msgs.length; i++) {
            byteBuffer.putShort(msgs[i].getTypeCode());
            byteBuffer.put(encoded[i]);
        }

        byteBuffer.flip();
        int length = byteBuffer.limit();
        byte[] content = new byte[length + 4];
        intToBytes(length, content, 0);
        byteBuffer.get(content, 4, length);
        if (msgs.length > 20) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("msg in one seata merge packet:" + msgs.length
                        + ",buffer size:" + content.length);
            }
        }
        return content;
    }

    @Override
    public boolean decode(ByteBuf in) {
        int i = in.readableBytes();
        if (i < 4) { return false; }

        i -= 4;
        int length = in.readInt();
        if (i < length) { return false; }
        byte[] buffer = new byte[length];
        in.readBytes(buffer);
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        decode(byteBuffer);
        return true;
    }

    public void decode(ByteBuffer byteBuffer) {
        short msgNum = byteBuffer.getShort();
        msgs = new AbstractResultMessage[msgNum];
        for (int idx = 0; idx < msgNum; idx++) {
            short typeCode = byteBuffer.getShort();
            MergedMessage message = getMergeResponseInstanceByCode(typeCode);
            message.decode(byteBuffer);
            msgs[idx] = (AbstractResultMessage)message;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("MergeResultMessage ");
        for (AbstractMessage msg : msgs) { sb.append(msg.toString()).append("\n"); }
        return sb.toString();
    }
}
