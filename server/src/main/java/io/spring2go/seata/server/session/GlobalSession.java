package io.spring2go.seata.server.session;

import io.spring2go.seata.core.model.GlobalStatus;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Global transaction session representation on TC.
 */
public class GlobalSession {
    private long transactionId;
    private String xid;
    private String transactionName;
    private int timeout;
    private long beginTime;
    private GlobalStatus status = GlobalStatus.Begin;
    private final List<BranchSession> branchSessions = new CopyOnWriteArrayList<>();

    public GlobalSession() {
        this.beginTime = System.currentTimeMillis();
    }

    public GlobalSession(long transactionId, String xid, String transactionName, int timeout) {
        this.transactionId = transactionId;
        this.xid = xid;
        this.transactionName = transactionName;
        this.timeout = timeout;
        this.beginTime = System.currentTimeMillis();
        this.status = GlobalStatus.Begin;
    }

    public void add(BranchSession branchSession) {
        this.branchSessions.add(branchSession);
    }

    public boolean remove(BranchSession branchSession) {
        return this.branchSessions.remove(branchSession);
    }

    public BranchSession getBranch(long branchId) {
        for (BranchSession branch : branchSessions) {
            if (branch.getBranchId() == branchId) {
                return branch;
            }
        }
        return null;
    }

    public boolean isActive() {
        return status == GlobalStatus.Begin || status == GlobalStatus.Committing || status == GlobalStatus.Rollbacking;
    }

    public void changeStatus(GlobalStatus status) {
        this.status = status;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public String getXid() {
        return xid;
    }

    public void setXid(String xid) {
        this.xid = xid;
    }

    public String getTransactionName() {
        return transactionName;
    }

    public void setTransactionName(String transactionName) {
        this.transactionName = transactionName;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public long getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(long beginTime) {
        this.beginTime = beginTime;
    }

    public GlobalStatus getStatus() {
        return status;
    }

    public void setStatus(GlobalStatus status) {
        this.status = status;
    }

    public List<BranchSession> getBranchSessions() {
        return Collections.unmodifiableList(branchSessions);
    }
}
