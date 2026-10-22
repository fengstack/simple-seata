package io.spring2go.seata.tm;

import io.spring2go.seata.core.rpc.netty.TmRpcClient;

public class TMClient {
    public static void init(String applicationId, String transactionServiceGroup) {
        TmRpcClient tmRpcClient = TmRpcClient.getInstance(applicationId, transactionServiceGroup);
        tmRpcClient.init();
    }
}
