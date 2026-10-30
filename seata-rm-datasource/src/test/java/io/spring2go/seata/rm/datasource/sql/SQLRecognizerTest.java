package io.spring2go.seata.rm.datasource.sql;

import com.alibaba.druid.util.JdbcConstants;
import io.spring2go.seata.rm.datasource.sql.druid.MySQLDeleteRecognizer;
import io.spring2go.seata.rm.datasource.sql.druid.MySQLInsertRecognizer;
import io.spring2go.seata.rm.datasource.sql.druid.MySQLUpdateRecognizer;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SQLRecognizerTest {

    @Test
    public void testMySQLUpdateRecognizer() {
        String sql = "UPDATE t_user SET age = ?, name = ? WHERE id = ?";
        SQLRecognizer recognizer = SQLVisitorFactory.get(sql, JdbcConstants.MYSQL);

        Assert.assertNotNull(recognizer);
        Assert.assertEquals(SQLType.UPDATE, recognizer.getSQLType());
        Assert.assertTrue(recognizer instanceof MySQLUpdateRecognizer);
        Assert.assertEquals("t_user", recognizer.getTableName());

        SQLUpdateRecognizer updateRecognizer = (SQLUpdateRecognizer) recognizer;
        List<String> updateColumns = updateRecognizer.getUpdateColumns();
        Assert.assertEquals(2, updateColumns.size());
        Assert.assertEquals("age", updateColumns.get(0));
        Assert.assertEquals("name", updateColumns.get(1));

        String whereCondition = updateRecognizer.getWhereCondition();
        Assert.assertTrue(whereCondition.contains("id = ?"));
    }

    @Test
    public void testMySQLDeleteRecognizer() {
        String sql = "DELETE FROM t_order WHERE id = ? AND status = ?";
        SQLRecognizer recognizer = SQLVisitorFactory.get(sql, JdbcConstants.MYSQL);

        Assert.assertNotNull(recognizer);
        Assert.assertEquals(SQLType.DELETE, recognizer.getSQLType());
        Assert.assertTrue(recognizer instanceof MySQLDeleteRecognizer);
        Assert.assertEquals("t_order", recognizer.getTableName());

        SQLDeleteRecognizer deleteRecognizer = (SQLDeleteRecognizer) recognizer;
        String whereCondition = deleteRecognizer.getWhereCondition();
        Assert.assertTrue(whereCondition.contains("id = ?"));
        Assert.assertTrue(whereCondition.contains("status = ?"));
    }

    @Test
    public void testMySQLInsertRecognizer() {
        String sql = "INSERT INTO t_account (id, user_id, balance) VALUES (?, ?, ?)";
        SQLRecognizer recognizer = SQLVisitorFactory.get(sql, JdbcConstants.MYSQL);

        Assert.assertNotNull(recognizer);
        Assert.assertEquals(SQLType.INSERT, recognizer.getSQLType());
        Assert.assertTrue(recognizer instanceof MySQLInsertRecognizer);
        Assert.assertEquals("t_account", recognizer.getTableName());

        SQLInsertRecognizer insertRecognizer = (SQLInsertRecognizer) recognizer;
        List<String> insertColumns = insertRecognizer.getInsertColumns();
        Assert.assertEquals(3, insertColumns.size());
        Assert.assertEquals("ID", insertColumns.get(0));
        Assert.assertEquals("USER_ID", insertColumns.get(1));
        Assert.assertEquals("BALANCE", insertColumns.get(2));
    }

    @Test
    public void testWhereConditionWithInAndBetween() {
        // 测试非 SQLBinaryOpExpr 的 WHERE 条件（如 IN 列表、BETWEEN）不会抛 ClassCastException
        String updateInSql = "UPDATE t_order SET status = 'CANCELLED' WHERE id IN (?, ?)";
        SQLUpdateRecognizer updateInRecognizer = (SQLUpdateRecognizer) SQLVisitorFactory.get(updateInSql, JdbcConstants.MYSQL);
        Assert.assertTrue(updateInRecognizer.getWhereCondition().contains("IN"));

        String deleteBetweenSql = "DELETE FROM t_order WHERE id BETWEEN ? AND ?";
        SQLDeleteRecognizer deleteBetweenRecognizer = (SQLDeleteRecognizer) SQLVisitorFactory.get(deleteBetweenSql, JdbcConstants.MYSQL);
        Assert.assertTrue(deleteBetweenRecognizer.getWhereCondition().contains("BETWEEN"));

        String selectInSql = "SELECT id, status FROM t_order WHERE id IN (1, 2) FOR UPDATE";
        SQLSelectRecognizer selectInRecognizer = (SQLSelectRecognizer) SQLVisitorFactory.get(selectInSql, JdbcConstants.MYSQL);
        Assert.assertTrue(selectInRecognizer.getWhereCondition().contains("IN"));
    }

    @Test
    public void testPreparedStatementReuseOverwritesParameter() {
        String sql = "UPDATE t_user SET age = ? WHERE id = ?";
        final SQLUpdateRecognizer recognizer = (SQLUpdateRecognizer) SQLVisitorFactory.get(sql, JdbcConstants.MYSQL);

        // 模拟同一个 PreparedStatementProxy 连续两次 setInt(2, val) 而不调 clearParameters()
        final java.util.ArrayList<Object>[] params = new java.util.ArrayList[2];
        params[0] = new java.util.ArrayList<>();
        params[1] = new java.util.ArrayList<>();

        io.spring2go.seata.rm.datasource.ParametersHolder holder = new io.spring2go.seata.rm.datasource.ParametersHolder() {
            @Override
            public java.util.ArrayList<Object>[] getParameters() {
                return params;
            }
        };

        // 第一次绑定 id = 100，第二次重绑 id = 200（真实 PreparedStatementProxy.setParamByIndex 应覆盖旧值）
        params[0].add(18);
        params[1].add(100L);
        // 进一步直接验证 AbstractPreparedStatementProxy.setParamByIndex 的行为
        java.lang.reflect.InvocationHandler noopHandler = new java.lang.reflect.InvocationHandler() {
            @Override
            public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) {
                if ("getParameterMetaData".equals(method.getName())) {
                    return java.lang.reflect.Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class[]{java.sql.ParameterMetaData.class},
                            new java.lang.reflect.InvocationHandler() {
                                @Override
                                public Object invoke(Object p, java.lang.reflect.Method m, Object[] a) {
                                    if ("getParameterCount".equals(m.getName())) {
                                        return 2;
                                    }
                                    return null;
                                }
                            });
                }
                return null;
            }
        };
        java.sql.PreparedStatement rawPs = (java.sql.PreparedStatement) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[]{java.sql.PreparedStatement.class},
                noopHandler);
        try {
            io.spring2go.seata.rm.datasource.PreparedStatementProxy proxy =
                    new io.spring2go.seata.rm.datasource.PreparedStatementProxy(null, rawPs, sql);
            proxy.setInt(1, 18);
            proxy.setLong(2, 100L);
            // 第二次直接复用该 PreparedStatement，重新设置参数
            proxy.setInt(1, 20);
            proxy.setLong(2, 200L);

            java.util.ArrayList<Object> paramAppender = new java.util.ArrayList<>();
            recognizer.getWhereCondition(proxy, paramAppender);
            Assert.assertEquals(1, paramAppender.size());
            Assert.assertEquals("复用 PreparedStatement 重新 set 参数后，镜像查询必须取到最新值 200L",
                    200L, paramAppender.get(0));
        } catch (java.sql.SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
