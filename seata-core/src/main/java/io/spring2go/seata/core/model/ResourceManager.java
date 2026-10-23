package io.spring2go.seata.core.model;

import io.spring2go.seata.core.exception.TransactionException;

import java.util.Map;

/**
 * Resource Manager：RM 管理数据库资源并参与全局事务的两个方向。
 * <ul>
 *   <li>一阶段（RM -> TC）：分支注册（同时申请全局行锁）、状态上报、锁查询；</li>
 *   <li>二阶段（TC -> RM）：分支提交 / 分支回滚（反向补偿）。</li>
 * </ul>
 */
public interface ResourceManager {

    // ---- 资源管理 ----

    void registerResource(Resource resource);

    void unregisterResource(Resource resource);

    Map<String, Resource> getManagedResources();

    // ---- 一阶段：RM -> TC ----

    Long branchRegister(BranchType branchType, String resourceId, String clientId, String xid, String lockKeys)
            throws TransactionException;

    void branchReport(String xid, long branchId, BranchStatus status, String applicationData)
            throws TransactionException;

    boolean lockQuery(BranchType branchType, String resourceId, String xid, String lockKeys)
            throws TransactionException;

    // ---- 二阶段：TC -> RM ----

    BranchStatus branchCommit(String xid, long branchId, String resourceId, String applicationData)
            throws TransactionException;

    BranchStatus branchRollback(String xid, long branchId, String resourceId, String applicationData)
            throws TransactionException;
}
