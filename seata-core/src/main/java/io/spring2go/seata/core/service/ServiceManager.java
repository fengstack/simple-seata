package io.spring2go.seata.core.service;

/**
 * 按事务分组查找 TC 集群地址。教学实现直接读本地配置
 * （service.vgroup_mapping.<group> -> rgroup, service.<rgroup>.grouplist），
 * 不接入注册中心。
 */
public interface ServiceManager {

    /**
     * Lookup server addresses for the given txServiceGroup
     * @param txServiceGroup transaction service group (service name)
     * @return available addresses
     */
    String[] lookup(String txServiceGroup);
}
