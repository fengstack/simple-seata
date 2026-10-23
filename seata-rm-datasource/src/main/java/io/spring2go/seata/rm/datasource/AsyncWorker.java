package io.spring2go.seata.rm.datasource;

import io.spring2go.seata.common.thread.NamedThreadFactory;
import io.spring2go.seata.config.ConfigurationFactory;
import io.spring2go.seata.core.model.BranchStatus;
import io.spring2go.seata.core.service.ConfigurationKeys;
import io.spring2go.seata.rm.datasource.undo.UndoLogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 二阶段提交的异步执行器：一阶段业务数据早已落库，"提交"只剩删 undo_log 这点打扫工作，
 * 所以 RM 收到 BranchCommit 指令后只是把 (xid, branchId) 塞进内存队列立即应答成功，
 * 后台线程每秒按数据源分组批量 DELETE undo_log。这就是"AT 二阶段提交几乎零开销"的来源。
 */
public class AsyncWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncWorker.class);

    private static class Phase2Context {
        final String xid;
        final long branchId;
        final String resourceId;

        Phase2Context(String xid, long branchId, String resourceId) {
            this.xid = xid;
            this.branchId = branchId;
            this.resourceId = resourceId;
        }
    }

    private static final List<Phase2Context> ASYNC_COMMIT_BUFFER =
            Collections.synchronizedList(new ArrayList<Phase2Context>());

    private static final int ASYNC_COMMIT_BUFFER_LIMIT = ConfigurationFactory.getInstance().getInt(
            ConfigurationKeys.CLIENT_ASYNC_COMMIT_BUFFER_LIMIT, 10000);

    private ScheduledExecutorService timerExecutor;

    /** 分支提交：入队即返回成功，真正的 undo_log 删除由后台线程批量完成 */
    public BranchStatus branchCommit(String xid, long branchId, String resourceId) {
        if (ASYNC_COMMIT_BUFFER.size() < ASYNC_COMMIT_BUFFER_LIMIT) {
            ASYNC_COMMIT_BUFFER.add(new Phase2Context(xid, branchId, resourceId));
        } else {
            LOGGER.warn("Async commit buffer is FULL. Rejected branch [" + branchId + "/" + xid
                    + "] will be handled by housekeeping later.");
        }
        return BranchStatus.PhaseTwo_Committed;
    }

    /** 启动每秒一次的批量清理线程 */
    public synchronized void init() {
        LOGGER.info("Async commit buffer limit: " + ASYNC_COMMIT_BUFFER_LIMIT);
        timerExecutor = new ScheduledThreadPoolExecutor(1,
                new NamedThreadFactory("AsyncWorker", 1, true));
        timerExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    doBranchCommits();
                } catch (Throwable e) {
                    LOGGER.info("Failed at async committing ... " + e.getMessage());
                }
            }
        }, 10, 1000, TimeUnit.MILLISECONDS);
    }

    /** 取出队列里全部待提交分支，按数据源分组，每组开一个连接批量删 undo_log */
    private void doBranchCommits() {
        if (ASYNC_COMMIT_BUFFER.size() == 0) {
            return;
        }
        Map<String, List<Phase2Context>> mappedContexts = new HashMap<String, List<Phase2Context>>();
        synchronized (ASYNC_COMMIT_BUFFER) {
            Iterator<Phase2Context> iterator = ASYNC_COMMIT_BUFFER.iterator();
            while (iterator.hasNext()) {
                Phase2Context commitContext = iterator.next();
                List<Phase2Context> contextsGroupedByResourceId = mappedContexts.get(commitContext.resourceId);
                if (contextsGroupedByResourceId == null) {
                    contextsGroupedByResourceId = new ArrayList<Phase2Context>();
                    mappedContexts.put(commitContext.resourceId, contextsGroupedByResourceId);
                }
                contextsGroupedByResourceId.add(commitContext);
                iterator.remove();
            }
        }

        for (Map.Entry<String, List<Phase2Context>> entry : mappedContexts.entrySet()) {
            Connection conn = null;
            try {
                try {
                    DataSourceProxy dataSourceProxy = DataSourceManager.get().get(entry.getKey());
                    conn = dataSourceProxy.getPlainConnection();
                } catch (SQLException sqle) {
                    LOGGER.warn("Failed to get connection for async committing on " + entry.getKey(), sqle);
                    continue;
                }
                for (Phase2Context commitContext : entry.getValue()) {
                    try {
                        UndoLogManager.deleteUndoLog(commitContext.xid, commitContext.branchId, conn);
                    } catch (Exception ex) {
                        LOGGER.warn("Failed to delete undo log [" + commitContext.branchId + "/"
                                + commitContext.xid + "]", ex);
                    }
                }
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException closeEx) {
                        LOGGER.warn("Failed to close JDBC resource while deleting undo_log ", closeEx);
                    }
                }
            }
        }
    }
}
