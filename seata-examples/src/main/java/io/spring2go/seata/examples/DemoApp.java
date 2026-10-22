package io.spring2go.seata.examples;

import io.spring2go.seata.examples.config.DataSourceProvider;
import io.spring2go.seata.examples.service.AccountService;
import io.spring2go.seata.examples.service.BusinessService;
import io.spring2go.seata.examples.service.StorageService;
import io.spring2go.seata.rm.RMClientAT;
import io.spring2go.seata.rm.datasource.DataSourceProxy;
import io.spring2go.seata.server.EmbeddedTC;
import io.spring2go.seata.tm.TMClient;
import io.spring2go.seata.tm.api.DefaultGlobalTransaction;

/**
 * AT 模式可观察演示：每一步都把内部数据（undo_log 前后镜像、两个库的状态）打出来，
 * 让你"亲眼看到" AT 模式在每个阶段做了什么。
 *
 * 运行方式：mvn -pl seata-examples -am exec:java -Dexec.mainClass=io.spring2go.seata.examples.DemoApp
 * 配套讲解：docs/learning/at-mode-guide.md
 */
public class DemoApp {

    private static void banner(String title) {
        System.out.println();
        System.out.println("==================================================================");
        System.out.println("  " + title);
        System.out.println("==================================================================");
    }

    private static void dumpUndoLogs(String dbName, DataSourceProxy db) {
        System.out.println("  [" + dbName + "] undo_log 内容（前后镜像 = 反向补偿的依据）:");
        for (String content : DataSourceProvider.dumpUndoLogs(db)) {
            System.out.println("    " + content);
        }
    }

    public static void main(String[] args) throws Throwable {
        banner("第 0 步：启动 TC（事务协调器）—— 全局事务的'大脑'");
        EmbeddedTC embeddedTC = EmbeddedTC.getInstance();
        embeddedTC.start(8091);

        banner("第 1 步：初始化 TM / RM 客户端 和 两个独立的数据库");
        TMClient.init("demo-app", "my_test_tx_group");
        RMClientAT.init("demo-app", "my_test_tx_group");

        DataSourceProxy storageDb = DataSourceProvider.getStorageDb();
        DataSourceProxy accountDb = DataSourceProvider.getAccountDb();
        DataSourceProvider.resetTestData();

        StorageService storageService = new StorageService(storageDb);
        AccountService accountService = new AccountService(accountDb);
        BusinessService businessService = new BusinessService(storageService, accountService);

        System.out.println("  库存库 " + storageDb.getResourceId());
        System.out.println("  账户库 " + accountDb.getResourceId());
        System.out.println("  初始状态: 库存 C001 = " + storageService.getCount("C001")
                + ", 账户 U001 = " + accountService.getBalance("U001"));

        // ================= 场景一：全局事务成功提交 =================
        banner("场景一：下单成功 —— 两个分支（扣库存 + 扣账户）两阶段提交");
        System.out.println("  >>> 一阶段：TM 开启全局事务，两个库各自：");
        System.out.println("      记录前镜像 -> 执行业务 SQL -> 记录后镜像 -> 向 TC 注册分支申请全局行锁");
        System.out.println("      -> undo_log 与业务数据在同一个本地事务里原子提交");
        businessService.purchase("U001", "C001", 10, false);

        System.out.println("  >>> 一阶段结束后的数据状态（两个库的本地事务都已提交）:");
        System.out.println("      库存 C001 = " + storageService.getCount("C001")
                + " (100-10), 账户 U001 = " + accountService.getBalance("U001") + " (1000-100)");
        dumpUndoLogs("库存库", storageDb);
        dumpUndoLogs("账户库", accountDb);

        System.out.println("  >>> 二阶段提交：TC 通知两个分支提交，RM 只是异步批量删除 undo_log（几乎零开销）");
        System.out.println("      等待 AsyncWorker 每秒一次的批量清理...");
        long deadline = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < deadline
                && (DataSourceProvider.getUndoLogCount(storageDb) > 0
                || DataSourceProvider.getUndoLogCount(accountDb) > 0)) {
            Thread.sleep(300);
        }
        System.out.println("  >>> 二阶段完成: undo_log 已清理, 库存 undo_log = "
                + DataSourceProvider.getUndoLogCount(storageDb) + ", 账户 undo_log = "
                + DataSourceProvider.getUndoLogCount(accountDb));

        // ================= 场景二：业务异常，全局回滚反向补偿 =================
        banner("场景二：下单失败 —— 业务异常触发全局回滚，用 undo_log 反向补偿");
        DataSourceProvider.resetTestData();
        System.out.println("  重置数据: 库存 C001 = " + storageService.getCount("C001")
                + ", 账户 U001 = " + accountService.getBalance("U001"));
        System.out.println("  >>> 模拟：扣库存成功、扣账户成功后，下游订单服务抛出异常");

        try {
            businessService.purchase("U001", "C001", 10, true);
        } catch (Throwable ex) {
            System.out.println("  >>> 捕获业务异常: " + ex.getMessage());
        }

        System.out.println("  >>> 二阶段回滚：TC 按分支注册的逆序下发回滚指令，");
        System.out.println("      RM 校验后镜像（防脏写）后，用前镜像构造反向 SQL 把数据还原:");
        System.out.println("      库存 C001 = " + storageService.getCount("C001")
                + " (已恢复100), 账户 U001 = " + accountService.getBalance("U001") + " (已恢复1000)");
        System.out.println("      undo_log: 库存 = " + DataSourceProvider.getUndoLogCount(storageDb)
                + ", 账户 = " + DataSourceProvider.getUndoLogCount(accountDb) + " (补偿成功即删除)");

        // ================= 场景三（进阶）：脏写检测 =================
        banner("场景三（进阶）：脏写检测 —— 有人绕过全局事务改了数据，AT 拒绝盲目补偿");
        DataSourceProvider.resetTestData();
        System.out.println("  重置数据: 库存 C001 = " + storageService.getCount("C001"));

        DefaultGlobalTransaction tx = new DefaultGlobalTransaction();
        tx.begin();
        storageService.deduct("C001", 10); // 一阶段：100 -> 90，本地已提交，undo_log 已写入
        System.out.println("  >>> 一阶段完成: 库存 C001 = " + storageService.getCount("C001")
                + "（本地事务已提交，全局行锁仍被持有，等待二阶段）");

        DataSourceProvider.dirtyWriteStorageCount(55);
        System.out.println("  >>> 模拟脏写: 另一个应用绕过 Seata 直接把库存改成 "
                + storageService.getCount("C001"));

        System.out.println("  >>> 二阶段回滚: RM 发现当前数据(55) != 后镜像(90)，判定为脏写，拒绝补偿");
        tx.rollback();

        System.out.println("      库存 C001 = " + storageService.getCount("C001")
                + "（保持脏值，没有用旧值盲目覆盖）, undo_log = "
                + DataSourceProvider.getUndoLogCount(storageDb) + "（保留，TC 留在 RollbackRetrying 重试）");

        System.out.println("  >>> 人工修复: 把数据改回后镜像的应然状态 90");
        DataSourceProvider.dirtyWriteStorageCount(90);

        deadline = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < deadline && storageService.getCount("C001") != 100) {
            Thread.sleep(300);
        }
        System.out.println("  >>> TC 超时检查线程自动重试成功: 库存 C001 = " + storageService.getCount("C001")
                + " (补偿完成恢复100), undo_log = " + DataSourceProvider.getUndoLogCount(storageDb));

        banner("演示结束。想读懂每一步背后的代码？看 docs/learning/at-mode-guide.md");
        embeddedTC.stop();
        System.exit(0);
    }
}
