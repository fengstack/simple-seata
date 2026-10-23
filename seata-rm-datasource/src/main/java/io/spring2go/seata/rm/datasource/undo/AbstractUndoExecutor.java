package io.spring2go.seata.rm.datasource.undo;

import io.spring2go.seata.rm.datasource.sql.struct.Field;
import io.spring2go.seata.rm.datasource.sql.struct.KeyType;
import io.spring2go.seata.rm.datasource.sql.struct.Row;
import io.spring2go.seata.rm.datasource.sql.struct.TableRecords;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Objects;

/**
 * Created by william on May, 2020
 */
public abstract class AbstractUndoExecutor {
    protected SQLUndoLog sqlUndoLog;

    protected abstract String buildUndoSQL();

    public AbstractUndoExecutor(SQLUndoLog sqlUndoLog) {
        this.sqlUndoLog = sqlUndoLog;
    }

    public void executeOn(Connection conn) throws SQLException {
        dataValidation(conn);

        String undoSQL = buildUndoSQL();
        PreparedStatement undoPST = null;
        try {
            undoPST = conn.prepareStatement(undoSQL);

            TableRecords undoRows = getUndoRows();

            for (Row undoRow : undoRows.getRows()) {
                ArrayList<Field> undoValues = new ArrayList<Field>();
                Field pkValue = null;
                for (Field field : undoRow.getFields()) {
                    if (field.getKeyType() == KeyType.PrimaryKey) {
                        pkValue = field;
                    } else {
                        undoValues.add(field);
                    }
                }

                undoPrepare(undoPST, undoValues, pkValue);

                undoPST.executeUpdate();
            }
        } finally {
            if (undoPST != null) {
                undoPST.close();
            }
        }
    }

    protected void undoPrepare(PreparedStatement undoPST, ArrayList<Field> undoValues, Field pkValue) throws SQLException {
        int undoIndex = 0;
        for (Field undoValue : undoValues) {
            undoIndex++;
            undoPST.setObject(undoIndex, undoValue.getValue(), undoValue.getType());
        }
        // PK is at last one.
        // INSERT INTO a (x, y, z, pk) VALUES (?, ?, ?, ?)
        // UPDATE a SET x=?, y=?, z=? WHERE pk = ?
        // DELETE FROM a WHERE pk = ?
        undoIndex++;
        undoPST.setObject(undoIndex, pkValue.getValue(), pkValue.getType());
    }

    protected abstract TableRecords getUndoRows();

    /**
     * 防脏写校验（二阶段回滚前的安全检查）。
     *
     * 一阶段提交后，这行数据的"应然状态"就冻结成了后镜像（afterImage）。
     * 回滚前先查一遍当前数据库里的真实数据：
     *   - 和后镜像一致 → 没有人动过，可以放心补偿；
     *   - 不一致 → 有人在全局事务之外改了这行（绕过了全局锁），这就是"脏写"。
     *     此时盲目补偿会用旧数据覆盖别人的修改，所以必须拒绝补偿并报错，
     *     交给 TC 的 RollbackRetrying 状态反复重试，等数据被人工修复后再补偿。
     */
    protected void dataValidation(Connection conn) throws SQLException {
        TableRecords afterImage = sqlUndoLog.getAfterImage();

        if (afterImage != null && afterImage.size() > 0) {
            // 一阶段结束时这些行存在：现在必须仍然存在，且每个字段都和后镜像一致
            for (Row expectedRow : afterImage.getRows()) {
                Row currentRow = queryCurrentRow(conn, expectedRow);
                if (currentRow == null) {
                    throw new SQLException("Dirty write detected: row was deleted after phase one, table="
                            + sqlUndoLog.getTableName() + ", pk=" + pkValueOf(expectedRow));
                }
                if (!rowsEqual(expectedRow, currentRow)) {
                    throw new SQLException("Dirty write detected: row was modified after phase one, table="
                            + sqlUndoLog.getTableName() + ", pk=" + pkValueOf(expectedRow)
                            + ", expect(after image)=" + expectedRow + ", actual=" + currentRow);
                }
            }
        } else if (sqlUndoLog.getBeforeImage() != null && sqlUndoLog.getBeforeImage().size() > 0) {
            // 后镜像为空 = 一阶段把这些行删掉了：这些行现在不应该"复活"
            for (Row deletedRow : sqlUndoLog.getBeforeImage().getRows()) {
                if (queryCurrentRow(conn, deletedRow) != null) {
                    throw new SQLException("Dirty write detected: deleted row reappeared, table="
                            + sqlUndoLog.getTableName() + ", pk=" + pkValueOf(deletedRow));
                }
            }
        }
    }

    /** 按主键查询这行数据当前的真实值，行不存在返回 null */
    private Row queryCurrentRow(Connection conn, Row row) throws SQLException {
        Field pk = null;
        StringBuilder columns = new StringBuilder();
        for (Field field : row.getFields()) {
            if (field.getKeyType() == KeyType.PrimaryKey) {
                pk = field;
            }
            if (columns.length() > 0) {
                columns.append(", ");
            }
            columns.append(field.getName());
        }

        String sql = "SELECT " + columns + " FROM " + sqlUndoLog.getTableName() + " WHERE " + pk.getName() + " = ?";
        PreparedStatement pst = null;
        ResultSet rs = null;
        try {
            pst = conn.prepareStatement(sql);
            pst.setObject(1, pk.getValue(), pk.getType());
            rs = pst.executeQuery();
            if (!rs.next()) {
                return null;
            }
            Row current = new Row();
            for (Field field : row.getFields()) {
                Field copy = new Field(field.getName(), field.getType(), rs.getObject(field.getName()));
                copy.setKeyType(field.getKeyType());
                current.add(copy);
            }
            return current;
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (pst != null) {
                pst.close();
            }
        }
    }

    /** 逐字段比较两行数据是否一致 */
    private boolean rowsEqual(Row expected, Row actual) {
        for (Field expectedField : expected.getFields()) {
            for (Field actualField : actual.getFields()) {
                if (expectedField.getName().equals(actualField.getName())) {
                    if (!Objects.equals(expectedField.getValue(), actualField.getValue())) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private String pkValueOf(Row row) {
        for (Field field : row.getFields()) {
            if (field.getKeyType() == KeyType.PrimaryKey) {
                return String.valueOf(field.getValue());
            }
        }
        return "?";
    }
}
