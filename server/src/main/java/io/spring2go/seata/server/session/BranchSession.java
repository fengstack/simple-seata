package io.spring2go.seata.server.session;

import io.spring2go.seata.core.model.BranchStatus;
import io.spring2go.seata.core.model.BranchType;

/**
 * Branch transaction session representation on TC.
 */
public class BranchSession {
    private long transactionId;
    private long branchId;
    private String resourceId;
    private BranchType branchType = BranchType.AT;
    private BranchStatus status = BranchStatus.Unknown;
    private String clientId;
    private String applicationData;
    private String lockKey;

    public BranchSession() {
    }

    public BranchSession(long transactionId, long branchId, String resourceId,
                         BranchType branchType, String lockKey, String clientId) {
        this.transactionId = transactionId;
        this.branchId = branchId;
        this.resourceId = resourceId;
        this.branchType = branchType;
        this.lockKey = lockKey;
        this.clientId = clientId;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public long getBranchId() {
        return branchId;
    }

    public void setBranchId(long branchId) {
        this.branchId = branchId;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public BranchType getBranchType() {
        return branchType;
    }

    public void setBranchType(BranchType branchType) {
        this.branchType = branchType;
    }

    public BranchStatus getStatus() {
        return status;
    }

    public void setStatus(BranchStatus status) {
        this.status = status;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getApplicationData() {
        return applicationData;
    }

    public void setApplicationData(String applicationData) {
        this.applicationData = applicationData;
    }

    public String getLockKey() {
        return lockKey;
    }

    public void setLockKey(String lockKey) {
        this.lockKey = lockKey;
    }
}
