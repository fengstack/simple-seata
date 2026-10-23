package io.spring2go.seata.config;

/**
 * 只读配置接口。教学实现只从本地 application.conf 读取，
 * 不做动态写入和变更监听（那需要真实的配置中心，与本项目的目标无关）。
 */
public interface Configuration {

    String getConfig(String dataId);

    String getConfig(String dataId, String defaultValue);

    int getInt(String dataId, int defaultValue);
}
