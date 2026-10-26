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
    /**
     * 二阶段状态。timeoutCheck 定时线程和 RPC 工作线程都会读写它，必须 volatile
     * 保证跨线程可见（原子性由 sessionLock 保证，见下方）。
     */
    private volatile GlobalStatus status = GlobalStatus.Begin;
    private final List<BranchSession> branchSessions = new CopyOnWriteArrayList<>();
    /** 每会话一把锁：串行化同一事务的二阶段驱动（提交/回滚/超时自动回滚互斥） */
    private final Object sessionLock = new Object();

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

    public Object getSessionLock() {
        return sessionLock;
    }

    public void setStatus(GlobalStatus status) {
        this.status = status;
    }

    public List<BranchSession> getBranchSessions() {
        return Collections.unmodifiableList(branchSessions);
    }
}
