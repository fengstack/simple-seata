package io.spring2go.seata.core.rpc;

import io.netty.channel.Channel;
import io.spring2go.seata.common.Constants;
import io.spring2go.seata.core.rpc.netty.NettyPoolKey.TransactionRole;

import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;

/**
 * 一条已完成注册握手的客户端连接的上下文：谁（角色/应用）、属于哪个事务分组、
 * 持有哪些数据源资源（仅 RM）。由 {@link ChannelManager} 登记，随连接生命周期管理。
 */
public class RpcContext {

    private TransactionRole clientRole;
    private String version;
    private String applicationId;
    private String transactionServiceGroup;
    private Channel channel;
    private Set<String> resourceIds;

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public TransactionRole getClientRole() {
        return clientRole;
    }

    public void setClientRole(TransactionRole clientRole) {
        this.clientRole = clientRole;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getTransactionServiceGroup() {
        return transactionServiceGroup;
    }

    public void setTransactionServiceGroup(String transactionServiceGroup) {
        this.transactionServiceGroup = transactionServiceGroup;
    }

    public Set<String> getResourceIds() {
        return resourceIds;
    }

    public void addResources(Set<String> resourceIds) {
        if (resourceIds == null) {
            return;
        }
        if (this.resourceIds == null) {
            this.resourceIds = new HashSet<String>();
        }
        this.resourceIds.addAll(resourceIds);
    }

    /** 从连接远端地址解析客户端 IP（端口前的部分） */
    public String getClientIp() {
        if (channel == null) {
            return null;
        }
        SocketAddress socketAddress = channel.remoteAddress();
        String address = socketAddress.toString();
        if (address.startsWith(Constants.ENDPOINT_BEGIN_CHAR)) {
            address = address.substring(Constants.ENDPOINT_BEGIN_CHAR.length());
        }
        int splitIndex = address.lastIndexOf(Constants.IP_PORT_SPLIT_CHAR);
        return splitIndex < 0 ? address : address.substring(0, splitIndex);
    }

    @Override
    public String toString() {
        return "vgroup:" + transactionServiceGroup + ",appname:" + applicationId
                + ",channel:" + channel + ",version:" + version + ",type:" + clientRole;
    }
}
