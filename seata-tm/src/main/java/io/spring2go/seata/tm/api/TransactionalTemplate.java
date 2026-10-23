package io.spring2go.seata.tm.api;

import io.spring2go.seata.core.exception.TransactionException;
import io.spring2go.seata.core.model.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 编程式全局事务模板：begin -> 业务 -> commit，业务抛异常则 rollback 后原样抛出。
 * 这是教学项目里 TM 用法的主入口（生产版 Seata 在这里挂 @GlobalTransactional 注解拦截）。
 */
public class TransactionalTemplate {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalTemplate.class);

    private final TransactionManager transactionManager;

    public TransactionalTemplate() {
        this.transactionManager = null;
    }

    /** 测试/定制时注入自定义 TransactionManager（默认走 TmRpcClient） */
    public TransactionalTemplate(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public <T> T execute(TransactionalExecutor<T> business) throws Throwable {
        return execute(business, 60000, "default");
    }

    public <T> T execute(TransactionalExecutor<T> business, int timeout, String name) throws Throwable {
        DefaultGlobalTransaction tx = transactionManager == null
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
