package io.spring2go.seata.core.protocol;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by william on May, 2020
 */
public class MergedWrapMessage extends AbstractMessage implements Serializable, MergeMessage {
    private static final long serialVersionUID = -5758802337446717090L;
    public List<AbstractMessage> msgs = new ArrayList<AbstractMessage>();
    public List<Long> msgIds = new ArrayList<Long>();
    private static final Logger LOGGER = LoggerFactory.getLogger(MergedWrapMessage.class);

    @Override
    public short getTypeCode() {
        return TYPE_SEATA_MERGE;
    }

    @Override
    public byte[] encode() {
        // 先让每个子消息完成编码，再按实际长度之和分配缓冲：
        // 固定按"条数*1024"分配时，任一子消息超过 1KB 就会 BufferOverflowException
        byte[][] encoded = new byte[msgs.size()][];
        int bufferSize = 2; // msgNum
        for (int i = 0; i < msgs.size(); i++) {
            encoded[i] = msgs.get(i).encode();
            bufferSize += 2 + encoded[i].length; // typeCode + body
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
        byteBuffer.putShort((short) msgs.size());
        for (int i = 0; i < msgs.size(); i++) {
            byteBuffer.putShort(msgs.get(i).getTypeCode());
            byteBuffer.put(encoded[i]);
        }

        byteBuffer.flip();
        int length = byteBuffer.limit();
        byte[] content = new byte[length + 4];
        intToBytes(length, content, 0);
        byteBuffer.get(content, 4, length);
        if (msgs.size() > 20) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("msg in one packet:" + msgs.size() + ",buffer size:" + content.length);
            }
        }
        return content;
    }

    @Override
    public boolean decode(ByteBuf in) {
        int i = in.readableBytes();
        if (i < 4) {
            return false;
        }

        i -= 4;
        int length = in.readInt();
        if (i < length) {
            return false;
        }

        byte[] buffer = new byte[length];
        in.readBytes(buffer);
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        doDecode(byteBuffer);
        return true;
    }

    private void doDecode(ByteBuffer byteBuffer) {
        short msgNum = byteBuffer.getShort();
        for (int idx = 0; idx < msgNum; idx++) {
            short typeCode = byteBuffer.getShort();
            MergedMessage message = getMergeRequestInstanceByCode(typeCode);
            message.decode(byteBuffer);
            msgs.add((AbstractMessage) message);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("FescarMergeMessage ");
        for (AbstractMessage msg : msgs) {
            sb.append(msg.toString()).append("\n");
        }
        return sb.toString();
    }
}
