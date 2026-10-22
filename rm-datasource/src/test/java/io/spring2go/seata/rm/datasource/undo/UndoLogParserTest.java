package io.spring2go.seata.rm.datasource.undo;

import io.spring2go.seata.rm.datasource.sql.SQLType;
import io.spring2go.seata.rm.datasource.sql.struct.Field;
import io.spring2go.seata.rm.datasource.sql.struct.KeyType;
import io.spring2go.seata.rm.datasource.sql.struct.Row;
import io.spring2go.seata.rm.datasource.sql.struct.TableRecords;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class UndoLogParserTest {

    @Test
    public void testJSONBasedUndoLogParserEncodeAndDecode() {
        UndoLogParser parser = UndoLogParserFactory.getInstance();
        Assert.assertNotNull(parser);
        Assert.assertTrue(parser instanceof JSONBasedUndoLogParser);

        // 1. 构建分支回滚日志 (BranchUndoLog)
        BranchUndoLog branchUndoLog = new BranchUndoLog();
        branchUndoLog.setXid("127.0.0.1:8091:10001");
        branchUndoLog.setBranchId(20001L);

        List<SQLUndoLog> sqlUndoLogs = new ArrayList<>();
        SQLUndoLog sqlUndoLog = new SQLUndoLog();
        sqlUndoLog.setTableName("t_user");
        sqlUndoLog.setSqlType(SQLType.UPDATE);

        // 构建前镜像 (Before Image: age = 18)
        TableRecords beforeImage = new TableRecords();
        beforeImage.setTableName("t_user");
        Row beforeRow = new Row();

        Field pkField = new Field("id", Types.BIGINT, 1L);
        pkField.setKeyType(KeyType.PrimaryKey);
        beforeRow.add(pkField);

        Field ageFieldBefore = new Field("age", Types.INTEGER, 18);
        beforeRow.add(ageFieldBefore);

        beforeImage.getRows().add(beforeRow);
        sqlUndoLog.setBeforeImage(beforeImage);

        // 构建后镜像 (After Image: age = 20)
        TableRecords afterImage = new TableRecords();
        afterImage.setTableName("t_user");
        Row afterRow = new Row();

        Field pkFieldAfter = new Field("id", Types.BIGINT, 1L);
        pkFieldAfter.setKeyType(KeyType.PrimaryKey);
        afterRow.add(pkFieldAfter);

        Field ageFieldAfter = new Field("age", Types.INTEGER, 20);
        afterRow.add(ageFieldAfter);

        afterImage.getRows().add(afterRow);
        sqlUndoLog.setAfterImage(afterImage);

        sqlUndoLogs.add(sqlUndoLog);
        branchUndoLog.setSqlUndoLogs(sqlUndoLogs);

        // 2. 序列化为 JSON
        String encodedJson = parser.encode(branchUndoLog);
        Assert.assertNotNull(encodedJson);
        Assert.assertTrue(encodedJson.contains("127.0.0.1:8091:10001"));
        Assert.assertTrue(encodedJson.contains("t_user"));

        // 3. 反序列化
        BranchUndoLog decoded = parser.decode(encodedJson);
        Assert.assertNotNull(decoded);
        Assert.assertEquals("127.0.0.1:8091:10001", decoded.getXid());
        Assert.assertEquals(20001L, decoded.getBranchId());
        Assert.assertEquals(1, decoded.getSqlUndoLogs().size());

        SQLUndoLog decodedSqlUndoLog = decoded.getSqlUndoLogs().get(0);
        Assert.assertEquals("t_user", decodedSqlUndoLog.getTableName());
        Assert.assertEquals(SQLType.UPDATE, decodedSqlUndoLog.getSqlType());

        // 验证前镜像行数据
        Assert.assertEquals(1, decodedSqlUndoLog.getBeforeImage().getRows().size());
        Row decodedBeforeRow = decodedSqlUndoLog.getBeforeImage().getRows().get(0);
        Assert.assertEquals(2, decodedBeforeRow.getFields().size());
        Assert.assertEquals("id", decodedBeforeRow.getFields().get(0).getName());
        Assert.assertEquals(1L, ((Number) decodedBeforeRow.getFields().get(0).getValue()).longValue());
        Assert.assertEquals(18, ((Number) decodedBeforeRow.getFields().get(1).getValue()).intValue());

        // 验证后镜像行数据
        Assert.assertEquals(1, decodedSqlUndoLog.getAfterImage().getRows().size());
        Row decodedAfterRow = decodedSqlUndoLog.getAfterImage().getRows().get(0);
        Assert.assertEquals(20, ((Number) decodedAfterRow.getFields().get(1).getValue()).intValue());
    }
}
