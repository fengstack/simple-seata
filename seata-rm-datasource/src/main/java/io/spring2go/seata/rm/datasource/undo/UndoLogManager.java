package io.spring2go.seata.rm.datasource.undo;

import com.alibaba.druid.util.JdbcConstants;
import io.spring2go.seata.common.exception.NotSupportYetException;
import io.spring2go.seata.common.util.BlobUtils;
import io.spring2go.seata.core.exception.TransactionException;
import io.spring2go.seata.rm.datasource.ConnectionContext;
import io.spring2go.seata.rm.datasource.ConnectionProxy;
import io.spring2go.seata.rm.datasource.DataSourceProxy;
import io.spring2go.seata.rm.datasource.sql.struct.TableMeta;
import io.spring2go.seata.rm.datasource.sql.struct.TableMetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

import static io.spring2go.seata.core.exception.TransactionExceptionCode.BranchRollbackFailed_Retriable;

/**
 * Created by william on May, 2020
 */
public class UndoLogManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(UndoLogManager.class);

    private enum State {
        /** 正常一阶段写入的回滚日志 */
        Normal(0),
        /** 防悬挂状态：二阶段回滚先于一阶段提交到达时写入，阻止迟到的一阶段本地事务提交 */
        GlobalFinished(1);

        private final int value;

        State(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    private static String UNDO_LOG_TABLE_NAME = "undo_log";

    private static String INSERT_UNDO_LOG_SQL = "INSERT INTO " + UNDO_LOG_TABLE_NAME + "\n" +
            "\t(branch_id, xid, rollback_info, log_status, log_created, log_modified)\n" +
            "VALUES (?, ?, ?, ?, now(), now())";
    private static String DELETE_UNDO_LOG_SQL = "DELETE FROM " + UNDO_LOG_TABLE_NAME + "\n" +
            "\tWHERE branch_id = ? AND xid = ?";

    private static String SELECT_UNDO_LOG_SQL = "SELECT * FROM " + UNDO_LOG_TABLE_NAME + " WHERE branch_id = ? AND xid = ? FOR UPDATE";

    private UndoLogManager() {

    }

    public static void flushUndoLogs(ConnectionProxy cp) throws SQLException {
        assertDbSupport(cp.getDbType());

        ConnectionContext connectionContext = cp.getContext();
        String xid = connectionContext.getXid();
        long branchID = connectionContext.getBranchId();

        BranchUndoLog branchUndoLog = new BranchUndoLog();
        branchUndoLog.setXid(xid);
        branchUndoLog.setBranchId(branchID);
        branchUndoLog.setSqlUndoLogs(connectionContext.getUndoItems());

        String undoLogContent = UndoLogParserFactory.getInstance().encode(branchUndoLog);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Flushing UNDO LOG: " + undoLogContent);
        }

        insertUndoLog(xid, branchID, undoLogContent, State.Normal, cp.getTargetConnection());
    }

    private static void insertUndoLog(String xid, long branchId, String undoLogContent,
                                      State state, Connection conn) throws SQLException {
        PreparedStatement pst = null;
        try {
            pst = conn.prepareStatement(INSERT_UNDO_LOG_SQL);
            pst.setLong(1, branchId);
            pst.setString(2, xid);
            pst.setBlob(3, BlobUtils.string2blob(undoLogContent));
            pst.setInt(4, state.getValue());
            pst.executeUpdate();
        } finally {
            if (pst != null) {
                pst.close();
            }
        }
    }

    private static void assertDbSupport(String dbType) {
        if (!JdbcConstants.MYSQL.equals(dbType)) {
            throw new NotSupportYetException("DbType[" + dbType + "] is not support yet!");
        }
    }

    public static void undo(DataSourceProxy dataSourceProxy, String xid, long branchId) throws TransactionException {
        assertDbSupport(dataSourceProxy.getTargetDataSource().getDbType());

        Connection conn = null;
        ResultSet rs = null;
        PreparedStatement selectPST = null;
        try {
            conn = dataSourceProxy.getPlainConnection();

            // The entire undo process should run in a local transaction.
            conn.setAutoCommit(false);

            // Find UNDO LOG
            selectPST = conn.prepareStatement(SELECT_UNDO_LOG_SQL);
            selectPST.setLong(1, branchId);
            selectPST.setString(2, xid);
            rs = selectPST.executeQuery();

            boolean exists = false;
            while (rs.next()) {
                exists = true;
                int state = rs.getInt("log_status");
                if (state != State.Normal.getValue()) {
                    // 已经是 GlobalFinished 状态，说明之前已经做过空回滚防悬挂处理，直接幂等返回
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("xid {} branch {} undo_log status is GlobalFinished, ignore rollback", xid, branchId);
                    }
                    return;
                }

                Blob b = rs.getBlob("rollback_info");
                String rollbackInfo = BlobUtils.blob2string(b);
                BranchUndoLog branchUndoLog = UndoLogParserFactory.getInstance().decode(rollbackInfo);

                // 补偿必须按执行顺序的逆序进行：同一行被本事务多次修改时，
                // 只有先补偿"最后一次"修改，数据才会回到上一次修改完成时的样子（后镜像），
                // 防脏写校验才不会误报。正序补偿会在第一次补偿时就撞上"数据不一致"
                List<SQLUndoLog> sqlUndoLogs = branchUndoLog.getSqlUndoLogs();
                for (int i = sqlUndoLogs.size() - 1; i >= 0; i--) {
                    SQLUndoLog sqlUndoLog = sqlUndoLogs.get(i);
                    TableMeta tableMeta = TableMetaCache.getTableMeta(dataSourceProxy, sqlUndoLog.getTableName());
                    sqlUndoLog.setTableMeta(tableMeta);
                    AbstractUndoExecutor undoExecutor = UndoExecutorFactory.getUndoExecutor(dataSourceProxy.getDbType(), sqlUndoLog);
                    undoExecutor.executeOn(conn);
                }

            }

            if (exists) {
                deleteUndoLog(xid, branchId, conn);
            } else {
                // 空回滚与防悬挂：查不到 undo_log，说明一阶段本地事务还没提交（或已回滚）。
                // 插入一条 log_status = 1 (GlobalFinished) 的屏障记录，利用 (xid, branch_id) 唯一索引
                // 阻止可能迟到的一阶段本地事务提交（防悬挂）。
                insertUndoLog(xid, branchId, "", State.GlobalFinished, conn);
            }

            conn.commit();

        } catch (Throwable e) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackEx) {
                    LOGGER.warn("Failed to close JDBC resource while undo ... ", rollbackEx);
                }
            }
            throw new TransactionException(BranchRollbackFailed_Retriable, String.format("%s/%s", branchId, xid), e);

        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (selectPST != null) {
                    selectPST.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException closeEx) {
                LOGGER.warn("Failed to close JDBC resource while undo ... ", closeEx);
            }
        }

    }

    public static void deleteUndoLog(String xid, long branchId, Connection conn) throws SQLException {
        PreparedStatement deletePST = conn.prepareStatement(DELETE_UNDO_LOG_SQL);
        deletePST.setLong(1, branchId);
        deletePST.setString(2, xid);
        deletePST.executeUpdate();
    }
}
