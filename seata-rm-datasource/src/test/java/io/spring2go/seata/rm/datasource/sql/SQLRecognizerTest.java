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
}
