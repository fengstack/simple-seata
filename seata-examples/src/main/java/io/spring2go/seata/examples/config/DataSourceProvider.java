package io.spring2go.seata.examples.config;

import com.alibaba.druid.pool.DruidDataSource;
import io.spring2go.seata.rm.datasource.DataSourceProxy;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Statement;

public class DataSourceProvider {
    private static volatile DataSourceProxy dataSourceProxy;
    private static DruidDataSource rawDataSource;

    public static synchronized DataSourceProxy getDataSourceProxy() {
        if (dataSourceProxy == null) {
            init("jdbc:h2:mem:seata_demo;MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
        }
        return dataSourceProxy;
    }

    public static synchronized void init(String jdbcUrl, String username, String password) {
        if (rawDataSource != null) {
            rawDataSource.close();
        }
        rawDataSource = new DruidDataSource();
        rawDataSource.setUrl(jdbcUrl);
        rawDataSource.setUsername(username);
        rawDataSource.setPassword(password);
        rawDataSource.setDriverClassName("org.h2.Driver");
        rawDataSource.setDbType("mysql");
        rawDataSource.setValidationQuery("SELECT 1");
        rawDataSource.setInitialSize(2);
        rawDataSource.setMaxActive(10);

        try {
            rawDataSource.init();
            executeSqlScript(rawDataSource, "schema.sql");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize DataSource", e);
        }

        dataSourceProxy = new DataSourceProxy(rawDataSource);
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

    public static void resetTestData() {
        if (rawDataSource == null) {
            getDataSourceProxy();
        }
        try (Connection conn = rawDataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM storage_tbl");
            stmt.execute("DELETE FROM account_tbl");
            stmt.execute("DELETE FROM undo_log");
            stmt.execute("INSERT INTO storage_tbl (id, commodity_code, count) VALUES (1, 'C001', 100)");
            stmt.execute("INSERT INTO account_tbl (id, user_id, money) VALUES (1, 'U001', 1000)");
        } catch (Exception e) {
            throw new RuntimeException("Failed to reset test data", e);
        }
    }

    public static int getUndoLogCount() {
        if (rawDataSource == null) {
            getDataSourceProxy();
        }
        try (Connection conn = rawDataSource.getConnection();
             Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery("SELECT count(*) FROM undo_log")) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to count undo_log", e);
        }
        return 0;
    }
}
