package io.spring2go.seata.core.service;

import io.spring2go.seata.common.exception.FrameworkException;
import io.spring2go.seata.common.util.StringUtils;
import io.spring2go.seata.config.Configuration;
import io.spring2go.seata.config.ConfigurationFactory;

import static io.spring2go.seata.common.exception.FrameworkErrorCode.InvalidConfiguration;
import static io.spring2go.seata.core.service.ConfigurationKeys.SERVER_NODE_SPLIT_CHAR;

/**
 * 从 application.conf 读取分组 -> TC 地址列表的映射，结果只查一次。
 */
public class ServiceManagerStaticConfigImpl implements ServiceManager {
    private final Configuration configuration = ConfigurationFactory.getInstance();

    private volatile String[] serverAddresses;

    @Override
    public String[] lookup(String txServiceGroup) {
        if (serverAddresses != null) {
            return serverAddresses;
        }
        synchronized (this) {
            if (serverAddresses != null) {
                return serverAddresses;
            }
            // vgroup_mapping.my_test_tx_group = "default" -> default.grouplist = "127.0.0.1:8091"
            String rGroup = configuration.getConfig(ConfigurationKeys.SERVICE_GROUP_MAPPING_PREFIX + txServiceGroup);
            if (rGroup == null) {
                throw new FrameworkException(InvalidConfiguration);
            }
            String serverListConfig = configuration.getConfig(
                    ConfigurationKeys.SERVICE_PREFIX + rGroup + ConfigurationKeys.GROUPLIST_POSTFIX);
            if (StringUtils.isEmpty(serverListConfig)) {
                throw new FrameworkException(InvalidConfiguration);
            }
            serverAddresses = serverListConfig.split(SERVER_NODE_SPLIT_CHAR);
            return serverAddresses;
        }
    }
}
