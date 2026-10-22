package io.spring2go.seata.examples;

import io.spring2go.seata.examples.config.DataSourceProvider;
import io.spring2go.seata.examples.service.AccountService;
import io.spring2go.seata.examples.service.BusinessService;
import io.spring2go.seata.examples.service.StorageService;
import io.spring2go.seata.rm.RMClientAT;
import io.spring2go.seata.rm.datasource.DataSourceProxy;
import io.spring2go.seata.server.EmbeddedTC;
import io.spring2go.seata.tm.TMClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DemoApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(DemoApp.class);

    public static void main(String[] args) throws Throwable {
        LOGGER.info("==================================================================");
        LOGGER.info("        Simple-Seata End-to-End Demo Application Starting         ");
        LOGGER.info("==================================================================");

        // 1. 启动内嵌事务协调器 TC
        int tcPort = 8091;
        EmbeddedTC embeddedTC = EmbeddedTC.getInstance();
        embeddedTC.start(tcPort);
        LOGGER.info("[DemoApp] Embedded TC Server started on port: {}", tcPort);

        // 2. 初始化 TM 和 RM 客户端
        String applicationId = "simple-seata-demo";
        String txGroup = "my_test_tx_group";
        TMClient.init(applicationId, txGroup);
        RMClientAT.init(applicationId, txGroup);
        LOGGER.info("[DemoApp] TM and RM clients initialized with txGroup: {}", txGroup);

        // 3. 初始化内存数据源与业务服务
        DataSourceProxy dataSourceProxy = DataSourceProvider.getDataSourceProxy();
        DataSourceProvider.resetTestData();
        StorageService storageService = new StorageService(dataSourceProxy);
        AccountService accountService = new AccountService(dataSourceProxy);
        BusinessService businessService = new BusinessService(storageService, accountService);

        LOGGER.info("[DemoApp] Initial State: Storage count={}, Account balance={}",
                storageService.getCount("C001"), accountService.getBalance("U001"));

        // ================= 场景一：正常全局事务提交 =================
        LOGGER.info("------------------------------------------------------------------");
        LOGGER.info(">>> Scenario 1: Successful Global Transaction (Two-Phase Commit)");
        LOGGER.info("------------------------------------------------------------------");
        businessService.purchase("U001", "C001", 10, false);
        LOGGER.info("[DemoApp] After Commit: Storage count={}, Account balance={}",
                storageService.getCount("C001"), accountService.getBalance("U001"));

        // 等待异步清理 undo_log
        Thread.sleep(1200);
        LOGGER.info("[DemoApp] Remaining undo_log count in DB: {}", DataSourceProvider.getUndoLogCount());

        // ================= 场景二：异常全局事务回滚与反向补偿 =================
        LOGGER.info("------------------------------------------------------------------");
        LOGGER.info(">>> Scenario 2: Rollback Global Transaction (Two-Phase Compensation)");
        LOGGER.info("------------------------------------------------------------------");
        try {
            businessService.purchase("U001", "C001", 10, true);
        } catch (Exception ex) {
            LOGGER.info("[DemoApp] Caught simulated business exception: {}", ex.getMessage());
        }

        LOGGER.info("[DemoApp] After Rollback: Storage count={}, Account balance={} (Restored!)",
                storageService.getCount("C001"), accountService.getBalance("U001"));
        LOGGER.info("[DemoApp] Remaining undo_log count in DB: {}", DataSourceProvider.getUndoLogCount());

        LOGGER.info("==================================================================");
        LOGGER.info("        Simple-Seata End-to-End Demo Completed Successfully!      ");
        LOGGER.info("==================================================================");

        embeddedTC.stop();
        System.exit(0);
    }
}
