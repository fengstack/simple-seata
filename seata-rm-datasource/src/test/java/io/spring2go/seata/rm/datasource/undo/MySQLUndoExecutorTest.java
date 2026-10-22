package io.spring2go.seata.rm.datasource.undo;

import com.alibaba.druid.util.JdbcConstants;
import io.spring2go.seata.rm.datasource.sql.SQLType;
import io.spring2go.seata.rm.datasource.sql.struct.Field;
import io.spring2go.seata.rm.datasource.sql.struct.KeyType;
import io.spring2go.seata.rm.datasource.sql.struct.Row;
import io.spring2go.seata.rm.datasource.sql.struct.TableRecords;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Types;

public class MySQLUndoExecutorTest {

    @Test
    public void testBuildUndoSQLForUpdate() {
        SQLUndoLog sqlUndoLog = new SQLUndoLog();
        sqlUndoLog.setTableName("t_user");
        sqlUndoLog.setSqlType(SQLType.UPDATE);

        // 前镜像：原始旧数据 (id=1, name='Alice', age=18)
        TableRecords beforeImage = new TableRecords();
        beforeImage.setTableName("t_user");
        Row row = new Row();

        Field pkField = new Field("id", Types.BIGINT, 1L);
        pkField.setKeyType(KeyType.PrimaryKey);
        row.add(pkField);

        Field nameField = new Field("name", Types.VARCHAR, "Alice");
        row.add(nameField);

        Field ageField = new Field("age", Types.INTEGER, 18);
        row.add(ageField);

        beforeImage.getRows().add(row);
        sqlUndoLog.setBeforeImage(beforeImage);

        AbstractUndoExecutor executor = UndoExecutorFactory.getUndoExecutor(JdbcConstants.MYSQL, sqlUndoLog);
        Assert.assertNotNull(executor);

        String undoSQL = executor.buildUndoSQL();
        Assert.assertEquals("UPDATE t_user SET name = ?, age = ? WHERE id = ?", undoSQL);
    }

    @Test
    public void testBuildUndoSQLForDelete() {
        SQLUndoLog sqlUndoLog = new SQLUndoLog();
        sqlUndoLog.setTableName("t_order");
        sqlUndoLog.setSqlType(SQLType.DELETE);

        // 被删除记录的前镜像 (id=1001, status='PAID')
        TableRecords beforeImage = new TableRecords();
        beforeImage.setTableName("t_order");
        Row row = new Row();

        Field pkField = new Field("id", Types.BIGINT, 1001L);
        pkField.setKeyType(KeyType.PrimaryKey);
        row.add(pkField);

        Field statusField = new Field("status", Types.VARCHAR, "PAID");
        row.add(statusField);

        beforeImage.getRows().add(row);
        sqlUndoLog.setBeforeImage(beforeImage);

        AbstractUndoExecutor executor = UndoExecutorFactory.getUndoExecutor(JdbcConstants.MYSQL, sqlUndoLog);
        Assert.assertNotNull(executor);

        // 删除操作的反向补偿是 INSERT
        String undoSQL = executor.buildUndoSQL();
        Assert.assertEquals("INSERT INTO t_order(status, id) VALUES (?, ?)", undoSQL);
    }

    @Test
    public void testBuildUndoSQLForInsert() {
        SQLUndoLog sqlUndoLog = new SQLUndoLog();
        sqlUndoLog.setTableName("t_account");
        sqlUndoLog.setSqlType(SQLType.INSERT);

        // 新增记录的后镜像 (id=500, balance=100.0)
        TableRecords afterImage = new TableRecords();
        afterImage.setTableName("t_account");
        Row row = new Row();

        Field pkField = new Field("id", Types.BIGINT, 500L);
        pkField.setKeyType(KeyType.PrimaryKey);
        row.add(pkField);

        Field balanceField = new Field("balance", Types.DOUBLE, 100.0);
        row.add(balanceField);

        afterImage.getRows().add(row);
        sqlUndoLog.setAfterImage(afterImage);

        AbstractUndoExecutor executor = UndoExecutorFactory.getUndoExecutor(JdbcConstants.MYSQL, sqlUndoLog);
        Assert.assertNotNull(executor);

        // 插入操作的反向补偿是 DELETE
        String undoSQL = executor.buildUndoSQL().trim();
        Assert.assertEquals("DELETE FROM t_account WHERE id = ?", undoSQL);
    }
}
