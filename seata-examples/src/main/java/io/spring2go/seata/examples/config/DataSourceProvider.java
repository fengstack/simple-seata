package io.spring2go.seata.examples.config;

import com.alibaba.druid.pool.DruidDataSource;
import io.spring2go.seata.rm.datasource.DataSourceProxy;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 演示用的数据源提供者：两个完全独立的 H2 内存库，模拟两个微服务各自的数据库。
 *
 *   库存库 (storageDb) -> storage_tbl  + 自己的 undo_log
 *   账户库 (accountDb) -> account_tbl  + 自己的 undo_log
 *
 * 这样一次全局事务会注册两个分支事务、在两个库里各写一份 undo_log，
 * 这才是 AT 模式"分布式"的意义所在。
 */
public class DataSourceProvider {
    private static volatile DataSourceProxy storageDb;
    private static volatile DataSourceProxy accountDb;
    private static DruidDataSource storageRaw;
    private static DruidDataSource accountRaw;

    /** 库存库（业务表 storage_tbl，自带 undo_log） */
    public static synchronized DataSourceProxy getStorageDb() {
        if (storageDb == null) {
            storageRaw = createRawDataSource("jdbc:h2:mem:seata_storage;MODE=MySQL;DB_CLOSE_DELAY=-1");
            executeSqlScript(storageRaw, "storage-schema.sql");
            storageDb = new DataSourceProxy(storageRaw);
        }
        return storageDb;
    }

    /** 账户库（业务表 account_tbl，自带 undo_log） */
    public static synchronized DataSourceProxy getAccountDb() {
        if (accountDb == null) {
            accountRaw = createRawDataSource("jdbc:h2:mem:seata_account;MODE=MySQL;DB_CLOSE_DELAY=-1");
            executeSqlScript(accountRaw, "account-schema.sql");
            accountDb = new DataSourceProxy(accountRaw);
        }
        return accountDb;
    }

    private static DruidDataSource createRawDataSource(String jdbcUrl) {
        DruidDataSource ds = new DruidDataSource();
        ds.setUrl(jdbcUrl);
        ds.setUsername("sa");
        ds.setPassword("");
        ds.setDriverClassName("org.h2.Driver");
        ds.setDbType("mysql");
        ds.setValidationQuery("SELECT 1");
        ds.setInitialSize(2);
        ds.setMaxActive(10);
        try {
            ds.init();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize DataSource for " + jdbcUrl, e);
        }
        return ds;
    }

    public static void executeSqlScript(DruidDataSource ds, String scriptName) {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             InputStream in = DataSourceProvider.class.getClassLoader().getResourceAsStream(scriptName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("--") || line.startsWith("#")) {
                    continue;
                }
                sb.append(" ").append(line);
                if (line.endsWith(";")) {
                    stmt.execute(sb.toString().trim());
                    sb.setLength(0);
                }
            }
            if (sb.length() > 0 && !sb.toString().trim().isEmpty()) {
                stmt.execute(sb.toString().trim());
            }
        } catch (Exception e) {
            throw new RuntimeException("Error executing script: " + scriptName, e);
        }
    }

    /** 重置两个库的测试数据（不经过 Seata 代理，纯粹造数） */
    public static void resetTestData() {
        executeRawUpdate(storageRaw(), "DELETE FROM undo_log");
        executeRawUpdate(storageRaw(), "DELETE FROM storage_tbl");
        executeRawUpdate(storageRaw(), "INSERT INTO storage_tbl (id, commodity_code, count) VALUES (1, 'C001', 100)");
        executeRawUpdate(accountRaw(), "DELETE FROM undo_log");
        executeRawUpdate(accountRaw(), "DELETE FROM account_tbl");
        executeRawUpdate(accountRaw(), "INSERT INTO account_tbl (id, user_id, money) VALUES (1, 'U001', 1000)");
    }

    /**
     * 演示"脏写"用：绕过 Seata 代理，用原始连接直接改库存数据，
     * 模拟另一个不懂全局事务的应用改了同一行。
     */
    public static void dirtyWriteStorageCount(int count) {
        executeRawUpdate(storageRaw(), "UPDATE storage_tbl SET count = " + count + " WHERE id = 1");
    }

    /** 查某个库当前 undo_log 的条数（观察一阶段/二阶段行为用） */
    public static int getUndoLogCount(DataSourceProxy db) {
        DruidDataSource raw = rawOf(db);
        try (Connection conn = raw.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count(*) FROM undo_log")) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to count undo_log", e);
        }
        return 0;
    }

    /**
     * 把某个库 undo_log 里的 rollback_info（前后镜像 JSON）原样打出来——
     * 学习 AT 模式最直观的方式：亲眼看到"反向补偿需要的数据"长什么样。
     */
    public static List<String> dumpUndoLogs(DataSourceProxy db) {
        DruidDataSource raw = rawOf(db);
        List<String> contents = new ArrayList<>();
        try (Connection conn = raw.getConnection();
             PreparedStatement pst = conn.prepareStatement(
                     "SELECT xid, branch_id, rollback_info FROM undo_log ORDER BY id");
             ResultSet rs = pst.executeQuery()) {
            while (rs.next()) {
                java.sql.Blob blob = rs.getBlob("rollback_info");
                byte[] bytes = blob.getBytes(1, (int) blob.length());
                contents.add("xid=" + rs.getString("xid") + ", branchId=" + rs.getLong("branch_id")
                        + "\n  " + new String(bytes, "UTF-8"));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to dump undo_log", e);
        }
        return contents;
    }

    private static DruidDataSource rawOf(DataSourceProxy db) {
        if (storageDb != null && storageDb == db) {
            return storageRaw;
        }
        if (accountDb != null && accountDb == db) {
            return accountRaw;
        }
        throw new IllegalArgumentException("Unknown DataSourceProxy: " + (db == null ? null : db.getResourceId()));
    }

    private static DruidDataSource storageRaw() {
        if (storageRaw == null) {
            getStorageDb();
        }
        return storageRaw;
    }

    private static DruidDataSource accountRaw() {
        if (accountRaw == null) {
            getAccountDb();
        }
        return accountRaw;
    }

    private static void executeRawUpdate(DruidDataSource ds, String sql) {
        if (ds == null) {
            throw new IllegalStateException("DataSource not initialized yet");
        }
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute: " + sql, e);
        }
    }
}
