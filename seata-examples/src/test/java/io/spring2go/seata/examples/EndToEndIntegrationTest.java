package io.spring2go.seata.examples;

import io.spring2go.seata.core.context.RootContext;
import io.spring2go.seata.examples.config.DataSourceProvider;
import io.spring2go.seata.examples.service.AccountService;
import io.spring2go.seata.examples.service.BusinessService;
import io.spring2go.seata.examples.service.StorageService;
import io.spring2go.seata.rm.RMClientAT;
import io.spring2go.seata.rm.datasource.DataSourceProxy;
import io.spring2go.seata.rm.datasource.exec.LockConflictException;
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

    @BeforeClass
    public static void setUpClass() throws Exception {
        // 1. 启动内嵌协调器 TC
        EmbeddedTC.getInstance().start(8091);

        // 2. 初始化 TM 与 RM 客户端
        TMClient.init("demo-app", "my_test_tx_group");
        RMClientAT.init("demo-app", "my_test_tx_group");

        // 3. 初始化数据源与业务服务
        DataSourceProxy proxy = DataSourceProvider.getDataSourceProxy();
        storageService = new StorageService(proxy);
        accountService = new AccountService(proxy);
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

        // 执行全局事务：扣减库存 10 件，扣除账户 100 元
        businessService.purchase("U001", "C001", 10, false);

        // 验证业务数据正确扣减
        Assert.assertEquals(90, storageService.getCount("C001"));
        Assert.assertEquals(900, accountService.getBalance("U001"));

        // 验证全局事务上下文已清理
        Assert.assertFalse(RootContext.inGlobalTransaction());

        // 异步等待 AsyncWorker 执行二阶段提交删除 undo_log（每秒执行一次批处理）
        int retry = 0;
        while (DataSourceProvider.getUndoLogCount() > 0 && retry++ < 30) {
            Thread.sleep(100);
        }
        Assert.assertEquals("UndoLog should be asynchronously deleted after global commit", 0, DataSourceProvider.getUndoLogCount());
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

        // 验证数据已反向补偿恢复为原值
        Assert.assertEquals(100, storageService.getCount("C001"));
        Assert.assertEquals(1000, accountService.getBalance("U001"));

        // 验证全局事务上下文已解绑
        Assert.assertFalse(RootContext.inGlobalTransaction());

        // 回滚补偿后，undo_log 在当前事务中即时删除
        Assert.assertEquals(0, DataSourceProvider.getUndoLogCount());
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
                Assert.fail("Expected LockConflictException due to global row lock collision");
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause();
                while (cause.getCause() != null && !(cause instanceof LockConflictException)) {
                    cause = cause.getCause();
                }
                Assert.assertTrue("Cause must be LockConflictException but was " + cause,
                        cause instanceof LockConflictException);
            }
        } finally {
            tx1.rollback();
            executor.shutdown();
        }
    }
}
