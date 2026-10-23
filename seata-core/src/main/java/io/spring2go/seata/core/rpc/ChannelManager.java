package io.spring2go.seata.core.rpc;

import io.netty.channel.Channel;
import io.spring2go.seata.common.Constants;
import io.spring2go.seata.core.protocol.RegisterRMRequest;
import io.spring2go.seata.core.protocol.RegisterTMRequest;
import io.spring2go.seata.core.rpc.netty.NettyPoolKey.TransactionRole;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TC 侧连接注册表，维护两级索引：
 * <ul>
 *   <li>{@link #IDENTIFIED_CHANNELS}：已完成注册握手的连接（TM/RM 通用），连接断开时从这里摘除；</li>
 *   <li>{@link #RM_CHANNELS}：resourceId -> 持有该资源的 RM 连接集合。
 *       二阶段派发分支指令时，TC 按 resourceId 从这里找回通道。</li>
 * </ul>
 */
public class ChannelManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelManager.class);

    private static final ConcurrentMap<Channel, RpcContext> IDENTIFIED_CHANNELS
            = new ConcurrentHashMap<Channel, RpcContext>();

    private static final ConcurrentMap<String, ConcurrentMap<Channel, RpcContext>> RM_CHANNELS
            = new ConcurrentHashMap<String, ConcurrentMap<Channel, RpcContext>>();

    public static boolean isRegistered(Channel channel) {
        return IDENTIFIED_CHANNELS.containsKey(channel);
    }

    public static RpcContext getContextFromIdentified(Channel channel) {
        return IDENTIFIED_CHANNELS.get(channel);
    }

    /** TM 连接注册：记录身份与分组即可，TM 不持有数据源资源 */
    public static void registerTMChannel(RegisterTMRequest request, Channel channel) {
        RpcContext rpcContext = new RpcContext();
        rpcContext.setClientRole(TransactionRole.TMROLE);
        rpcContext.setVersion(request.getVersion());
        rpcContext.setApplicationId(request.getApplicationId());
        rpcContext.setTransactionServiceGroup(request.getTransactionServiceGroup());
        rpcContext.setChannel(channel);
        IDENTIFIED_CHANNELS.put(channel, rpcContext);
    }

    /** RM 连接注册：除身份外，把连接登记到它声明的每一个 resourceId 下 */
    public static void registerRMChannel(RegisterRMRequest request, Channel channel) {
        RpcContext rpcContext = IDENTIFIED_CHANNELS.get(channel);
        if (rpcContext == null) {
            rpcContext = new RpcContext();
            rpcContext.setClientRole(TransactionRole.RMROLE);
            rpcContext.setVersion(request.getVersion());
            rpcContext.setApplicationId(request.getApplicationId());
            rpcContext.setTransactionServiceGroup(request.getTransactionServiceGroup());
            rpcContext.setChannel(channel);
            IDENTIFIED_CHANNELS.put(channel, rpcContext);
        }
        Set<String> resourceIds = dbKeyToSet(request.getResourceIds());
        if (resourceIds == null || resourceIds.isEmpty()) {
            return;
        }
        rpcContext.addResources(resourceIds);
        for (String resourceId : resourceIds) {
            ConcurrentMap<Channel, RpcContext> channelMap = RM_CHANNELS.get(resourceId);
            if (channelMap == null) {
                RM_CHANNELS.putIfAbsent(resourceId, new ConcurrentHashMap<Channel, RpcContext>());
                channelMap = RM_CHANNELS.get(resourceId);
            }
            channelMap.put(channel, rpcContext);
        }
    }

    /** 连接断开/失效：从两级索引里移除 */
    public static void releaseRpcContext(Channel channel) {
        RpcContext rpcContext = IDENTIFIED_CHANNELS.remove(channel);
        if (rpcContext == null) {
            return;
        }
        if (rpcContext.getClientRole() == TransactionRole.RMROLE && rpcContext.getResourceIds() != null) {
            for (String resourceId : rpcContext.getResourceIds()) {
                ConcurrentMap<Channel, RpcContext> channelMap = RM_CHANNELS.get(resourceId);
                if (channelMap != null) {
                    channelMap.remove(channel);
                    if (channelMap.isEmpty()) {
                        RM_CHANNELS.remove(resourceId, channelMap);
                    }
                }
            }
        }
    }

    /** 按 resourceId 找一条活跃的 RM 通道（找不到说明 RM 掉线，交由上层重试） */
    public static Channel getChannel(String resourceId, String clientIp, String clientAppName) {
        ConcurrentMap<Channel, RpcContext> channelMap = RM_CHANNELS.get(resourceId);
        if (channelMap == null) {
            return null;
        }
        for (Map.Entry<Channel, RpcContext> entry : channelMap.entrySet()) {
            Channel channel = entry.getKey();
            if (channel.isActive()) {
                return channel;
            }
            channelMap.remove(channel);
        }
        return null;
    }

    /** 响应回写：请求从哪条连接来，就往哪条连接回 */
    public static Channel getSameClientChannel(Channel channel) {
        if (channel.isActive()) {
            return channel;
        }
        LOGGER.error("channel is not active, cannot send response:" + channel);
        return null;
    }

    private static Set<String> dbKeyToSet(String dbkey) {
        if (StringUtils.isEmpty(dbkey)) {
            return null;
        }
        Set<String> set = new java.util.HashSet<String>();
        for (String s : dbkey.split(Constants.DBKEYS_SPLIT_CHAR)) {
            set.add(s);
        }
        return set;
    }
}
