package io.spring2go.seata.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * 基于 classpath 下 application.conf（Typesafe Config/HOCON 格式）的本地配置实现。
 * 直接同步读取：配置文件不可变，无需异步线程池与超时机制。
 */
public class FileConfiguration implements Configuration {

    private static final Config CONFIG = ConfigFactory.load();

    @Override
    public String getConfig(String dataId) {
        return CONFIG.hasPath(dataId) ? CONFIG.getString(dataId) : null;
    }

    @Override
    public String getConfig(String dataId, String defaultValue) {
        return CONFIG.hasPath(dataId) ? CONFIG.getString(dataId) : defaultValue;
    }

    @Override
    public int getInt(String dataId, int defaultValue) {
        return CONFIG.hasPath(dataId) ? CONFIG.getInt(dataId) : defaultValue;
    }
}
