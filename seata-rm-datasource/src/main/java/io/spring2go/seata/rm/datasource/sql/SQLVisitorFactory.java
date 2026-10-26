package io.spring2go.seata.rm.datasource.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.util.JdbcConstants;
import io.spring2go.seata.rm.datasource.sql.druid.MySQLDeleteRecognizer;
import io.spring2go.seata.rm.datasource.sql.druid.MySQLInsertRecognizer;
import io.spring2go.seata.rm.datasource.sql.druid.MySQLSelectForUpdateRecognizer;
import io.spring2go.seata.rm.datasource.sql.druid.MySQLUpdateRecognizer;

import java.util.List;

/**
 * Created by william on May, 2020
 */
public class SQLVisitorFactory {
    public static SQLRecognizer get(String sql, String dbType) {
        List<SQLStatement> asts = SQLUtils.parseStatements(sql, dbType);
        if (asts == null || asts.size() != 1) {
            throw new UnsupportedOperationException("xxx");
        }
        SQLRecognizer recognizer = null;
        SQLStatement ast = asts.get(0);
        if (JdbcConstants.MYSQL.equalsIgnoreCase(dbType)) {
            if (ast instanceof SQLInsertStatement) {
                recognizer = new MySQLInsertRecognizer(sql, ast);
            } else if (ast instanceof SQLUpdateStatement) {
                recognizer = new MySQLUpdateRecognizer(sql, ast);
            } else if (ast instanceof SQLDeleteStatement) {
                recognizer = new MySQLDeleteRecognizer(sql, ast);
            } else if (ast instanceof SQLSelectStatement) {
                // 只有 SELECT ... FOR UPDATE 才需要拦截（探测并申请全局锁）；
                // 普通 SELECT 返回 null，由调用方直接走原生执行——
                // AT 模式的一阶段里普通读不申请全局锁，加锁反而会造成无谓的冲突和死锁
                SQLSelectQuery query = ((SQLSelectStatement) ast).getSelect().getQuery();
                if (query instanceof SQLSelectQueryBlock && ((SQLSelectQueryBlock) query).isForUpdate()) {
                    recognizer = new MySQLSelectForUpdateRecognizer(sql, ast);
                }
            }
        } else {
            throw new UnsupportedOperationException("Just support MySQL by now!");
        }
        return recognizer;
    }

    public static void main(String[] args) throws Throwable {
        String sql = "update material_inventory SET version = ? WHERE is_deleted = 0 and id = ? and version = ?";

        for (int i=0; i<100; i++) {
            long begin = System.currentTimeMillis();
            SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
            long end = System.currentTimeMillis();
            System.out.println(String.format("SQL:[%s] parser too slow, cost [%d] ms.", sql, (end - begin)));
        }

    }
}
