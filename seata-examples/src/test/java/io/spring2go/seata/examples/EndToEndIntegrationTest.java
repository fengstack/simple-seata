package io.spring2go.seata.examples;

import io.spring2go.seata.core.context.RootContext;
import io.spring2go.seata.examples.config.DataSourceProvider;
import io.spring2go.seata.examples.service.AccountService;
import io.spring2go.seata.examples.service.BusinessService;
import io.spring2go.seata.examples.service.StorageService;
import io.spring2go.seata.rm.RMClientAT;
import io.spring2go.seata.rm.datasource.DataSourceProxy;
import io.spring2go.seata.rm.datasource.exec.LockWaitTimeoutException;
import io.spring2go.seata.server.EmbeddedTC;
import io.spring2go.seata.tm.TMClient;
import io.spring2go.seata.tm.api.DefaultGlobalTransaction;
import org.junit.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class EndToEndIntegrationTest {

    private static StorageService storageService;
    private static AccountService accountService;
    private static BusinessService businessService;
    private static DataSourceProxy storageDb;
    private static DataSourceProxy accountDb;

    @BeforeClass
    public static void setUpClass() throws Exception {
        // 1. 启动内嵌协调器 TC
        EmbeddedTC.getInstance().start(8091);

        // 2. 初始化 TM 与 RM 客户端
        TMClient.init("demo-app", "my_test_tx_group");
        RMClientAT.init("demo-app", "my_test_tx_group");

        // 3. 初始化两个独立数据库（库存库 + 账户库）与业务服务
        storageDb = DataSourceProvider.getStorageDb();
        accountDb = DataSourceProvider.getAccountDb();
        storageService = new StorageService(storageDb);
        accountService = new AccountService(accountDb);
        businessService = new BusinessService(storageService, accountService);
    }

    @AfterClass
    public static void tearDownClass() {
        EmbeddedTC.getInstance().stop();
    }

    @Before
    public void setUp() {
        RootContext.unbind();
        DataSourceProvider.resetTestData();
    }

    @After
    public void tearDown() {
        RootContext.unbind();
    }

    @Test
    public void testTwoPhaseCommitSuccess() throws Throwable {
        Assert.assertEquals(100, storageService.getCount("C001"));
        Assert.assertEquals(1000, accountService.getBalance("U001"));

        // 执行全局事务：库存库扣 10 件（分支1），账户库扣 100 元（分支2）
        businessService.purchase("U001", "C001", 10, false);

        // 验证两个库的业务数据都正确扣减
        Assert.assertEquals(90, storageService.getCount("C001"));
        Assert.assertEquals(900, accountService.getBalance("U001"));

        // 验证全局事务上下文已清理
        Assert.assertFalse(RootContext.inGlobalTransaction());

        // 一阶段结束时两个库各写了一份 undo_log（两个分支 = 两条日志）
        // AsyncWorker 每秒批量清理一次，等它跑完
        int retry = 0;
        while ((DataSourceProvider.getUndoLogCount(storageDb) > 0
                || DataSourceProvider.getUndoLogCount(accountDb) > 0) && retry++ < 30) {
            Thread.sleep(100);
        }
        Assert.assertEquals("storage undo_log should be asynchronously deleted after global commit",
                0, DataSourceProvider.getUndoLogCount(storageDb));
        Assert.assertEquals("account undo_log should be asynchronously deleted after global commit",
                0, DataSourceProvider.getUndoLogCount(accountDb));
    }

    @Test
    public void testTwoPhaseRollbackSuccess() throws Throwable {
        Assert.assertEquals(100, storageService.getCount("C001"));
        Assert.assertEquals(1000, accountService.getBalance("U001"));

        // 执行全局事务并模拟下游异常
        try {
            businessService.purchase("U001", "C001", 10, true);
            Assert.fail("Expected business simulated exception");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Business simulated exception"));
        }

        // 验证两个库的数据都已反向补偿恢复为原值
        Assert.assertEquals(100, storageService.getCount("C001"));
        Assert.assertEquals(1000, accountService.getBalance("U001"));

        // 验证全局事务上下文已解绑
        Assert.assertFalse(RootContext.inGlobalTransaction());

        // 回滚补偿后，undo_log 在补偿事务中即时删除
        Assert.assertEquals(0, DataSourceProvider.getUndoLogCount(storageDb));
        Assert.assertEquals(0, DataSourceProvider.getUndoLogCount(accountDb));
    }

    @Test
    public void testGlobalLockConflict() throws Exception {
        DefaultGlobalTransaction tx1 = new DefaultGlobalTransaction();
        tx1.begin();

        // 事务 1 扣减并本地提交，向 TC 注册并持有 storage_tbl:1 全局行锁
        storageService.deduct("C001", 10);

        // 事务 2 在并发线程中尝试修改相同行
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> future = executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    DefaultGlobalTransaction tx2 = new DefaultGlobalTransaction();
                    tx2.begin();
                    try {
                        storageService.deduct("C001", 5);
                        return true;
                    } finally {
                        tx2.rollback();
                    }
                }
            });

            try {
                future.get();
                Assert.fail("Expected LockWaitTimeoutException due to global row lock collision");
            } catch (ExecutionException ee) {
                // 锁重试耗尽后抛出的是 LockWaitTimeoutException。
                // tx2 的本地事务在每轮重试前都被回滚，绝不会有"业务 SQL 执行两遍"的脏数据
                Throwable cause = ee.getCause();
                Assert.assertTrue("Cause must be LockWaitTimeoutException but was " + cause,
                        cause instanceof LockWaitTimeoutException);
            }
        } finally {
            tx1.rollback();
            executor.shutdown();
        }

        // 两个事务最终都回滚：库存必须恢复原值（验证 tx2 重试期间没有把扣减重复执行并提交）
        Assert.assertEquals(100, storageService.getCount("C001"));
    }

    /**
     * 进阶场景：脏写检测（AT 模式的安全底线）。
     *
     * 一阶段提交后、二阶段回滚前，如果有人绕过全局事务直接改了这行数据，
     * 回滚时的"后镜像校验"会发现数据对不上，拒绝盲目补偿（否则会用旧值覆盖别人的修改）。
     * 分支回滚失败后 TC 把全局事务留在 RollbackRetrying 状态反复重试，
     * 直到数据被人工修复，补偿才会真正执行。
     */
    @Test
    public void testDirtyWriteDetectionAndRecovery() throws Exception {
        DefaultGlobalTransaction tx = new DefaultGlobalTransaction();
        tx.begin();

        // 一阶段：扣减库存 100 -> 90，本地事务提交，undo_log 写入，全局行锁被持有
        storageService.deduct("C001", 10);
        Assert.assertEquals(90, storageService.getCount("C001"));
        Assert.assertEquals(1, DataSourceProvider.getUndoLogCount(storageDb));

        // 模拟脏写：绕过 Seata 直接改这行数据（另一个不懂全局事务的应用干的）
        DataSourceProvider.dirtyWriteStorageCount(55);
        Assert.assertEquals(55, storageService.getCount("C001"));

        // 二阶段回滚：后镜像里 count=90，但当前是 55 → 防脏写校验失败，拒绝补偿
        tx.rollback();

        // 数据保持脏值 55（没有被旧值盲目覆盖），undo_log 也保留着等待重试
        Assert.assertEquals(55, storageService.getCount("C001"));
        Assert.assertEquals(1, DataSourceProvider.getUndoLogCount(storageDb));

        // 人工修复：把数据改回"一阶段提交后的应然状态"（后镜像 count=90）
        DataSourceProvider.dirtyWriteStorageCount(90);

        // TC 的超时检查线程（每秒一次）会自动重试 RollbackRetrying 的会话，
        // 校验通过后补偿执行，库存恢复为扣减前的 100
        long deadline = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < deadline) {
            if (storageService.getCount("C001") == 100
                    && DataSourceProvider.getUndoLogCount(storageDb) == 0) {
                break;
            }
            Thread.sleep(200);
        }
        Assert.assertEquals("auto retry after manual repair should restore the original value",
                100, storageService.getCount("C001"));
        Assert.assertEquals("undo_log should be deleted after successful retry",
                0, DataSourceProvider.getUndoLogCount(storageDb));
    }

    /**
     * 核心防御场景：空回滚与防悬挂（Anti-Suspension）。
     *
     * 当分支事务已经注册，但本地一阶段事务尚未提交（或已回滚）时，TC 下发了 BranchRollback：
     * 1. RM 查不到 status=0 的 undo_log（空回滚），此时不能什么都不做就返回，
     *    必须插入一条 log_status=1 (GlobalFinished) 的防悬挂标记记录；
     * 2. 如果迟到的一阶段本地事务随后尝试 flushUndoLogs 并 commit，
     *    就会被 undo_log 表上的 (xid, branch_id) 唯一约束拦截而失败回滚，绝不允许悬挂数据落库。
     */
    @Test
    public void testAntiSuspensionPreventsLatePhaseOneCommit() throws Exception {
        String fakeXid = "127.0.0.1:8091:999999";
        long fakeBranchId = 888888L;

        // 1. 模拟二阶段回滚先于一阶段本地提交到达（此时库里还没有该分支的 undo_log）
        io.spring2go.seata.rm.datasource.undo.UndoLogManager.undo(storageDb, fakeXid, fakeBranchId);

        // 验证库里插入了一条防悬挂屏障记录（log_status = 1）
        Assert.assertEquals("Empty rollback must insert a defense undo_log record to prevent suspension",
                1, DataSourceProvider.getUndoLogCount(storageDb));

        // 2. 模拟迟到的一阶段试图往同一个 (xid, branch_id) 刷入正常 undo_log：必须因唯一约束冲突抛出 SQLException
        io.spring2go.seata.rm.datasource.ConnectionProxy connProxy = storageDb.getConnection();
        connProxy.setAutoCommit(false);
        connProxy.bind(fakeXid);
        java.lang.reflect.Method setBranchId = io.spring2go.seata.rm.datasource.ConnectionContext.class
                .getDeclaredMethod("setBranchId", Long.class);
        setBranchId.setAccessible(true);
        setBranchId.invoke(connProxy.getContext(), fakeBranchId);
        try {
            io.spring2go.seata.rm.datasource.undo.UndoLogManager.flushUndoLogs(connProxy);
            Assert.fail("Late phase-1 flushUndoLogs must be rejected by unique index on (xid, branch_id)");
        } catch (java.sql.SQLException expected) {
            // 唯一索引冲突拦截成功！回滚迟到的本地事务
            connProxy.getTargetConnection().rollback();
        } finally {
            connProxy.close();
        }
    }
}
