package io.spring2go.seata.rm;

import io.spring2go.seata.core.rpc.netty.RmMessageListener;
import io.spring2go.seata.core.rpc.netty.RmRpcClient;
import io.spring2go.seata.rm.datasource.AsyncWorker;
import io.spring2go.seata.rm.datasource.DataSourceManager;

/**
 * AT 模式 RM 客户端入口：装配 RPC 通道、二阶段异步提交器与指令处理器。
 */
public class RMClientAT {
    public static void init(String applicationId, String transactionServiceGroup) {
        RmRpcClient rmRpcClient = RmRpcClient.getInstance(applicationId, transactionServiceGroup);
        AsyncWorker asyncWorker = new AsyncWorker();
        asyncWorker.init();
        DataSourceManager.get().setAsyncWorker(asyncWorker);
        rmRpcClient.setResourceManager(DataSourceManager.get());
        rmRpcClient.setClientMessageListener(new RmMessageListener(new RMHandlerAT()));
        rmRpcClient.init();
    }
}
