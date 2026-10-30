package io.spring2go.seata.core.protocol.transaction;

import io.netty.buffer.ByteBuf;
import io.spring2go.seata.core.model.BranchType;

import java.nio.ByteBuffer;

/**
 * Created by william on May, 2020
 */
public abstract class AbstractBranchEndRequest extends AbstractTransactionRequestToRM {
    private static final long serialVersionUID = 5083828939317068713L;

    protected String xid;

    protected long branchId;

    protected BranchType branchType = BranchType.AT;

    protected String resourceId;

    protected String applicationData;

    public String getXid() {
        return xid;
    }

    public void setXid(String xid) {
        this.xid = xid;
    }

    public long getBranchId() {
        return branchId;
    }

    public void setBranchId(long branchId) {
        this.branchId = branchId;
    }

    public BranchType getBranchType() {
        return branchType;
    }

    public void setBranchType(BranchType branchType) {
        this.branchType = branchType;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public String getApplicationData() {
        return applicationData;
    }

    public void setApplicationData(String applicationData) {
        this.applicationData = applicationData;
    }

    @Override
    public byte[] encode() {
        byte[] applicationDataBytes = null;
        int bufferSize = 1024;
        if (this.applicationData != null) {
            applicationDataBytes = applicationData.getBytes(UTF8);
            if (applicationDataBytes.length > 512) {
                bufferSize = applicationDataBytes.length + 1024;
            }
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);

        // 1. xid
        if (this.xid != null) {
            byte[] bs = xid.getBytes(UTF8);
            byteBuffer.putShort((short) bs.length);
            if (bs.length > 0) {
                byteBuffer.put(bs);
            }
        } else {
            byteBuffer.putShort((short) 0);
        }
        // 2. Branch Id
        byteBuffer.putLong(this.branchId);
        // 3. Branch Type
        byteBuffer.put((byte) this.branchType.ordinal());
        // 4. Resource Id
        if (this.resourceId != null) {
            byte[] bs = resourceId.getBytes(UTF8);
            byteBuffer.putShort((short) bs.length);
            if (bs.length > 0) {
                byteBuffer.put(bs);
            }
        } else {
            byteBuffer.putShort((short) 0);
        }

        // 5. Application Data
        if (this.applicationData != null) {
            byteBuffer.putInt(applicationDataBytes.length);
            if (applicationDataBytes.length > 0) {
                byteBuffer.put(applicationDataBytes);
            }
        } else {
            byteBuffer.putInt(0);
        }

        byteBuffer.flip();
        byte[] content = new byte[byteBuffer.limit()];
        byteBuffer.get(content);
        return content;
    }

    @Override
    public boolean decode(ByteBuf in) {
        // 每个字段读取前先确认字节到齐；返回 false 时编解码器会回退读指针，
        // 等剩余半包到达后对同一段字节重新解码（所以下面可以放心先读后判断）
        if (in.readableBytes() < 2) {
            return false;
        }
        // 1. xid
        int xidLen = in.readShort();
        if (xidLen > 0) {
            if (in.readableBytes() < xidLen) {
                return false;
            }
            byte[] bs = new byte[xidLen];
            in.readBytes(bs);
            setXid(new String(bs, UTF8));
        }
        // 2-3. branchId(8) + branchType(1)
        if (in.readableBytes() < 8 + 1) {
            return false;
        }
        this.branchId = in.readLong();
        this.branchType = BranchType.get(in.readByte());

        // 4. Resource Id
        if (in.readableBytes() < 2) {
            return false;
        }
        int resourceIdLen = in.readShort();
        if (resourceIdLen > 0) {
            if (in.readableBytes() < resourceIdLen) {
                return false;
            }
            byte[] bs = new byte[resourceIdLen];
            in.readBytes(bs);
            setResourceId(new String(bs, UTF8));
        }

        // 5. Application Data（长度是 int/4 字节，与 encode 的 putInt 对齐，不是 short）
        if (in.readableBytes() < 4) {
            return false;
        }
        int applicationDataLen = in.readInt();
        if (applicationDataLen > 0) {
            if (in.readableBytes() < applicationDataLen) {
                return false;
            }
            byte[] bs = new byte[applicationDataLen];
            in.readBytes(bs);
            setApplicationData(new String(bs, UTF8));
        }

        return true;
    }
}
