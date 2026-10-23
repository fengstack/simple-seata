package io.spring2go.seata.rm.datasource;

import io.spring2go.seata.common.XID;
import io.spring2go.seata.common.exception.NotSupportYetException;
import io.spring2go.seata.common.exception.ShouldNeverHappenException;
import io.spring2go.seata.core.exception.TransactionException;
import io.spring2go.seata.core.exception.TransactionExceptionCode;
import io.spring2go.seata.core.model.BranchStatus;
import io.spring2go.seata.core.model.BranchType;
import io.spring2go.seata.core.model.Resource;
import io.spring2go.seata.core.model.ResourceManager;
import io.spring2go.seata.core.protocol.AbstractMessage;
import io.spring2go.seata.core.protocol.ResultCode;
import io.spring2go.seata.core.protocol.transaction.AbstractTransactionResponse;
import io.spring2go.seata.core.protocol.transaction.BranchRegisterRequest;
import io.spring2go.seata.core.protocol.transaction.BranchRegisterResponse;
import io.spring2go.seata.core.protocol.transaction.BranchReportRequest;
import io.spring2go.seata.core.protocol.transaction.BranchReportResponse;
import io.spring2go.seata.core.protocol.transaction.GlobalLockQueryRequest;
import io.spring2go.seata.core.protocol.transaction.GlobalLockQueryResponse;
import io.spring2go.seata.core.rpc.netty.RmRpcClient;
import io.spring2go.seata.rm.datasource.undo.UndoLogManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * RM 核心：管理所有 DataSourceProxy 资源，实现一阶段（分支注册/上报/锁查询）
 * 与二阶段（提交=异步删 undo_log，回滚=undo 反向补偿）。
 */
public class DataSourceManager implements ResourceManager {

    /** 二阶段提交执行器（删 undo_log），由 RMClientAT.init 注入 */
    private AsyncWorker asyncWorker;

    private final Map<String, Resource> dataSourceCache = new ConcurrentHashMap<String, Resource>();

    public void setAsyncWorker(AsyncWorker asyncWorker) {
        this.asyncWorker = asyncWorker;
    }

    private static class SingletonHolder {
        private static DataSourceManager INSTANCE = new DataSourceManager();
    }

    public static DataSourceManager get() {
        return SingletonHolder.INSTANCE;
    }

    public static void set(DataSourceManager mock) {
        SingletonHolder.INSTANCE = mock;
    }

    protected DataSourceManager() {
    }

    // ---- 资源管理 ----

    @Override
    public void registerResource(Resource resource) {
        DataSourceProxy dataSourceProxy = (DataSourceProxy) resource;
        dataSourceCache.put(dataSourceProxy.getResourceId(), dataSourceProxy);
        // 向所有已连接的 TC 声明本资源，TC 才知道二阶段指令该发回给谁
        RmRpcClient.getInstance().registerResource(dataSourceProxy.getResourceGroupId(), dataSourceProxy.getResourceId());
    }

    @Override
    public void unregisterResource(Resource resource) {
        throw new NotSupportYetException("unregister a resource");
    }

    public DataSourceProxy get(String resourceId) {
        return (DataSourceProxy) dataSourceCache.get(resourceId);
    }

    @Override
    public Map<String, Resource> getManagedResources() {
        return dataSourceCache;
    }

    // ---- 一阶段：RM -> TC ----

    @Override
    public Long branchRegister(BranchType branchType, String resourceId, String clientId, String xid, String lockKeys)
            throws TransactionException {
        BranchRegisterRequest request = new BranchRegisterRequest();
        request.setTransactionId(XID.getTransactionId(xid));
        request.setLockKey(lockKeys);
        request.setResourceId(resourceId);
        BranchRegisterResponse response = callTC(request, TransactionExceptionCode.BranchRegisterFailed,
                BranchRegisterResponse.class);
        return response.getBranchId();
    }

    @Override
    public void branchReport(String xid, long branchId, BranchStatus status, String applicationData)
            throws TransactionException {
        BranchReportRequest request = new BranchReportRequest();
        request.setTransactionId(XID.getTransactionId(xid));
        request.setBranchId(branchId);
        request.setStatus(status);
        request.setApplicationData(applicationData);
        callTC(request, TransactionExceptionCode.BranchReportFailed, BranchReportResponse.class);
    }

    @Override
    public boolean lockQuery(BranchType branchType, String resourceId, String xid, String lockKeys)
            throws TransactionException {
        GlobalLockQueryRequest request = new GlobalLockQueryRequest();
        request.setTransactionId(XID.getTransactionId(xid));
        request.setLockKey(lockKeys);
        request.setResourceId(resourceId);
        GlobalLockQueryResponse response = callTC(request, TransactionExceptionCode.LockableCheckFailed,
                GlobalLockQueryResponse.class);
        return response.isLockable();
    }

    /** 发送请求到 TC 并统一处理失败响应 / RPC 超时 / 运行时异常 */
    private <T extends AbstractTransactionResponse> T callTC(AbstractMessage request,
                                                             TransactionExceptionCode runtimeCode,
                                                             Class<T> clazz) throws TransactionException {
        try {
            AbstractTransactionResponse response =
                    (AbstractTransactionResponse) RmRpcClient.getInstance().sendMsgWithResponse(request);
            if (response.getResultCode() == ResultCode.Failed) {
                throw new TransactionException(response.getTransactionExceptionCode(),
                        "Response[" + response.getMsg() + "]");
            }
            return clazz.cast(response);
        } catch (TimeoutException toe) {
            throw new TransactionException(TransactionExceptionCode.IO, "RPC Timeout", toe);
        } catch (RuntimeException rex) {
            throw new TransactionException(runtimeCode, "Runtime", rex);
        }
    }

    // ---- 二阶段：TC -> RM ----

    @Override
    public BranchStatus branchCommit(String xid, long branchId, String resourceId, String applicationData)
            throws TransactionException {
        // 一阶段数据已落库，提交 = 异步删 undo_log
        return asyncWorker.branchCommit(xid, branchId, resourceId);
    }

    @Override
    public BranchStatus branchRollback(String xid, long branchId, String resourceId, String applicationData)
            throws TransactionException {
        DataSourceProxy dataSourceProxy = get(resourceId);
        if (dataSourceProxy == null) {
            throw new ShouldNeverHappenException("resource not found: " + resourceId);
        }
        try {
            UndoLogManager.undo(dataSourceProxy, xid, branchId);
        } catch (TransactionException te) {
            if (te.getCode() == TransactionExceptionCode.BranchRollbackFailed_Unretriable) {
                return BranchStatus.PhaseTwo_RollbackFailed_Unretriable;
            } else {
                return BranchStatus.PhaseTwo_RollbackFailed_Retriable;
            }
        }
        return BranchStatus.PhaseTwo_Rollbacked;
    }
}
