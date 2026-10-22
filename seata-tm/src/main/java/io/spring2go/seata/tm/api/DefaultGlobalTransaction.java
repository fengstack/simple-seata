package io.spring2go.seata.tm.api;

import io.spring2go.seata.core.context.RootContext;
import io.spring2go.seata.core.exception.TransactionException;
import io.spring2go.seata.core.model.GlobalStatus;
import io.spring2go.seata.core.model.TransactionManager;
import io.spring2go.seata.tm.DefaultTransactionManager;

public class DefaultGlobalTransaction implements GlobalTransaction {
    private static final int DEFAULT_GLOBAL_TX_TIMEOUT = 60000;
    private static final String DEFAULT_GLOBAL_TX_NAME = "default";

    private final TransactionManager transactionManager;
    private String xid;
    private GlobalStatus status = GlobalStatus.UnKnown;

    public DefaultGlobalTransaction() {
        this(new DefaultTransactionManager());
    }

    public DefaultGlobalTransaction(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    @Override
    public void begin() throws TransactionException {
        begin(DEFAULT_GLOBAL_TX_TIMEOUT);
    }

    @Override
    public void begin(int timeout) throws TransactionException {
        begin(timeout, DEFAULT_GLOBAL_TX_NAME);
    }

    @Override
    public void begin(int timeout, String name) throws TransactionException {
        if (xid != null) {
            throw new IllegalStateException("Global transaction already began: " + xid);
        }
        xid = transactionManager.begin("default", "default_tx_group", name, timeout);
        status = GlobalStatus.Begin;
        RootContext.bind(xid);
    }

    @Override
    public void commit() throws TransactionException {
        if (xid == null) {
            return;
        }
        try {
            status = transactionManager.commit(xid);
        } finally {
            RootContext.unbind();
            xid = null;
        }
    }

    @Override
    public void rollback() throws TransactionException {
        if (xid == null) {
            return;
        }
        try {
            status = transactionManager.rollback(xid);
        } finally {
            RootContext.unbind();
            xid = null;
        }
    }

    @Override
    public GlobalStatus getStatus() throws TransactionException {
        if (xid == null) {
            return status;
        }
        return transactionManager.getStatus(xid);
    }

    @Override
    public String getXid() {
        return xid;
    }
}
