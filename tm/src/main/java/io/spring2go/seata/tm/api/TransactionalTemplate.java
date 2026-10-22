package io.spring2go.seata.tm.api;

import io.spring2go.seata.core.exception.TransactionException;
import io.spring2go.seata.core.model.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalTemplate {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalTemplate.class);

    private final TransactionManager transactionManager;

    public TransactionalTemplate() {
        this(null);
    }

    public TransactionalTemplate(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public <T> T execute(TransactionalExecutor<T> business) throws Throwable {
        return execute(business, 60000, "default");
    }

    public <T> T execute(TransactionalExecutor<T> business, int timeout, String name) throws Throwable {
        GlobalTransaction tx = transactionManager == null
                ? new DefaultGlobalTransaction()
                : new DefaultGlobalTransaction(transactionManager);

        tx.begin(timeout, name);
        T rs;
        try {
            rs = business.execute();
        } catch (Throwable th) {
            try {
                tx.rollback();
            } catch (TransactionException te) {
                LOGGER.error("Failed to rollback global transaction [" + tx.getXid() + "]", te);
            }
            throw th;
        }
        tx.commit();
        return rs;
    }
}
