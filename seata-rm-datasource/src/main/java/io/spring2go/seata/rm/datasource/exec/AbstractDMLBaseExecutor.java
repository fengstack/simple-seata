package io.spring2go.seata.rm.datasource.exec;

import io.spring2go.seata.rm.datasource.AbstractConnectionProxy;
import io.spring2go.seata.rm.datasource.ConnectionProxy;
import io.spring2go.seata.rm.datasource.StatementProxy;
import io.spring2go.seata.rm.datasource.sql.SQLRecognizer;
import io.spring2go.seata.rm.datasource.sql.struct.TableRecords;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by william on May, 2020
 */
public abstract class AbstractDMLBaseExecutor<T, S extends Statement> extends BaseTransactionalExecutor<T, S> {
    public AbstractDMLBaseExecutor(StatementProxy<S> statementProxy, StatementCallback<T, S> statementCallback, SQLRecognizer sqlRecognizer) {
        super(statementProxy, statementCallback, sqlRecognizer);
    }

    @Override
    public T doExecute(Object... args) throws Throwable {
        AbstractConnectionProxy connectionProxy = statementProxy.getConnectionProxy();
        if (connectionProxy.getAutoCommit()) {
            return executeAutoCommitTrue(args);
        } else {
            return executeAutoCommitFalse(args);
        }
    }

    protected T executeAutoCommitFalse(Object[] args) throws Throwable {
        TableRecords beforeImage = beforeImage();
        T result = statementCallback.execute(statementProxy.getTargetStatement(), args);
        TableRecords afterImage = afterImage(beforeImage);
        statementProxy.getConnectionProxy().prepareUndoLog(sqlRecognizer.getSQLType(), sqlRecognizer.getTableName(), beforeImage, afterImage);
        return result;
    }

    protected T executeAutoCommitTrue(Object[] args) throws Throwable {
        T result = null;
        ConnectionProxy connectionProxy = statementProxy.getConnectionProxy();
        LockRetryController lockRetryController = new LockRetryController();
        boolean committed = false;
        try {
            connectionProxy.setAutoCommit(false);
            while (true) {
                try {
                    result = executeAutoCommitFalse(args);
                    connectionProxy.commit();
                    committed = true;
                    break;
                } catch (LockConflictException lockConflict) {
                    // 关键：重试前必须先回滚本地事务（rollbackForRetry 会保留 XID）。
                    // 否则业务 SQL 会在同一个未提交的本地事务里再执行一遍
                    // （比如"扣 5 件库存"变成扣 10 件），undo_log 也会重复。
                    connectionProxy.rollbackForRetry();
                    lockRetryController.sleep(lockConflict);
                }
            }

        } finally {
            // 注意这里直接操作 targetConnection：物理回滚不会发任何 RPC，也不会抛出新异常，
            // 保证不掩盖 catch/while 里抛出的原始异常
            if (!committed) {
                connectionProxy.getTargetConnection().rollback();
            }
            connectionProxy.getTargetConnection().setAutoCommit(true);
        }
        return result;
    }

    protected abstract TableRecords beforeImage() throws SQLException;

    protected abstract TableRecords afterImage(TableRecords beforeImage) throws SQLException;
}
