package io.spring2go.seata.core.protocol;

import io.netty.buffer.ByteBuf;

import java.io.Serializable;

/**
 * Created by william on May, 2020
 */
public class RegisterRMRequest extends AbstractIdentifyRequest implements Serializable {
    private static final long serialVersionUID = 7539732523682335742L;

    private String resourceIds;

    public RegisterRMRequest() {
        this(null, null);
    }

    public RegisterRMRequest(String applicationId, String transactionServiceGroup) {
        super(applicationId, transactionServiceGroup);
    }

    public String getResourceIds() {
        return resourceIds;
    }

    public void setResourceIds(String resourceIds) {
        this.resourceIds = resourceIds;
    }
    @Override
    public short getTypeCode() {
        return TYPE_REG_RM;
    }

    @Override
    protected void doEncode() {
        super.doEncode();
        if (this.resourceIds != null) {
            byte[] bs = resourceIds.getBytes(UTF8);
            byteBuffer.putInt(bs.length);
            if (bs.length > 0) {
                byteBuffer.put(bs);
            }
        } else {
            byteBuffer.putInt(0);
        }
    }

    @Override
    public boolean decode(ByteBuf in) {
        // 每个字段读取前先确认字节到齐；返回 false 时编解码器会回退读指针，
        // 等剩余半包到达后对同一段字节重新解码。漏掉任何一个检查，
        // 半包时 readBytes 会抛 IndexOutOfBoundsException 直接杀掉整条连接
        if (in.readableBytes() < 2) {
            return false;
        }
        short len = in.readShort();
        if (len > 0) {
            if (in.readableBytes() < len) {
                return false;
            }
            byte[] bs = new byte[len];
            in.readBytes(bs);
            this.setVersion(new String(bs, UTF8));
        }

        if (in.readableBytes() < 2) {
            return false;
        }
        len = in.readShort();
        if (len > 0) {
            if (in.readableBytes() < len) {
                return false;
            }
            byte[] bs = new byte[len];
            in.readBytes(bs);
            this.setApplicationId(new String(bs, UTF8));
        }

        if (in.readableBytes() < 2) {
            return false;
        }
        len = in.readShort();
        if (len > 0) {
            if (in.readableBytes() < len) {
                return false;
            }
            byte[] bs = new byte[len];
            in.readBytes(bs);
            this.setTransactionServiceGroup(new String(bs, UTF8));
        }

        if (in.readableBytes() < 2) {
            return false;
        }
        len = in.readShort();
        if (len > 0) {
            if (in.readableBytes() < len) {
                return false;
            }
            byte[] bs = new byte[len];
            in.readBytes(bs);
            this.setExtraData(new String(bs, UTF8));
        }

        // resourceIds 的长度是 int（与 encode 的 putInt 对齐）
        if (in.readableBytes() < 4) {
            return false;
        }
        int iLen = in.readInt();
        if (iLen > 0) {
            if (in.readableBytes() < iLen) {
                return false;
            }
            byte[] bs = new byte[iLen];
            in.readBytes(bs);
            this.setResourceIds(new String(bs, UTF8));
        }
        return true;
    }

    @Override
    public String toString() {
        return "RegisterRMRequest{" +
                "resourceIds='" + resourceIds + '\'' +
                ", applicationId='" + applicationId + '\'' +
                ", transactionServiceGroup='" + transactionServiceGroup + '\'' +
                '}';
    }
}
