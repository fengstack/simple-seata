package io.spring2go.seata.examples;

import io.spring2go.seata.core.context.RootContext;
import io.spring2go.seata.examples.config.DataSourceProvider;
import io.spring2go.seata.rm.datasource.ConnectionProxy;
import io.spring2go.seata.rm.datasource.DataSourceProxy;
import io.spring2go.seata.rm.datasource.StatementProxy;
import io.spring2go.seata.rm.datasource.exec.ExecuteTemplate;
import io.spring2go.seata.rm.datasource.exec.StatementCallback;
import io.spring2go.seata.rm.datasource.sql.SQLRecognizer;
import io.spring2go.seata.rm.datasource.sql.SQLVisitorFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * 回归测试：ExecuteTemplate 绝不能吞掉执行器抛出的非 SQL 异常。
 *
 * 历史缺陷：catch 块里 `new SQLException(ex)` 只创建不抛出，导致 SQL 解析/镜像构建等
 * 环节的运行时异常被静默吞掉、executeUpdate 返回 0，业务方以为执行成功。
 * 修复后任何非 SQLException 都必须包装成 SQLException 原样上抛。
 */
public class ExecuteTemplateExceptionTest {

    @After
    public void tearDown() {
        RootContext.unbind();
    }

    @Test
    public void nonSqlExceptionMustBeThrownNotSwallowed() throws Exception {
        DataSourceProxy storageDb = DataSourceProvider.getStorageDb();
        DataSourceProvider.resetTestData();

        // 进入全局事务上下文，让 ExecuteTemplate 走执行器路径（否则直接透传）
        RootContext.bind("127.0.0.1:8091:999999");

        ConnectionProxy conn = (ConnectionProxy) storageDb.getConnection();
        StatementProxy<Statement> st = (StatementProxy<Statement>) conn.createStatement();

        SQLRecognizer recognizer = SQLVisitorFactory.get(
                "UPDATE storage_tbl SET count = count - 1 WHERE commodity_code = 'C001'", "mysql");
        Assert.assertNotNull("recognizer should parse the update sql", recognizer);

        try {
            ExecuteTemplate.execute(recognizer, st, new StatementCallback<Integer, Statement>() {
                @Override
                public Integer execute(Statement statement, Object... args) throws SQLException {
                    // 模拟镜像构建/业务执行环节抛出的运行时异常
                    throw new IllegalStateException("boom-from-executor");
                }
            });
            Assert.fail("exception from executor must propagate, not be swallowed");
        } catch (SQLException e) {
            Assert.assertNotNull("original exception must be attached as cause", e.getCause());
            Assert.assertEquals("boom-from-executor", e.getCause().getMessage());
        }
    }
}
