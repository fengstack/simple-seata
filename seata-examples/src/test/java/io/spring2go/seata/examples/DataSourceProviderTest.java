package io.spring2go.seata.examples;

import io.spring2go.seata.examples.config.DataSourceProvider;
import io.spring2go.seata.examples.service.AccountService;
import io.spring2go.seata.examples.service.StorageService;
import io.spring2go.seata.rm.datasource.DataSourceProxy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataSourceProviderTest {

    @Before
    public void setup() {
        DataSourceProvider.resetTestData();
    }

    @Test
    public void testTwoIndependentDatabases() throws Exception {
        DataSourceProxy storageDb = DataSourceProvider.getStorageDb();
        DataSourceProxy accountDb = DataSourceProvider.getAccountDb();

        // 两个库是相互独立的资源（resourceId 不同，各自向 RM 注册）
        Assert.assertNotEquals(storageDb.getResourceId(), accountDb.getResourceId());

        StorageService storageService = new StorageService(storageDb);
        AccountService accountService = new AccountService(accountDb);

        Assert.assertEquals(100, storageService.getCount("C001"));
        Assert.assertEquals(1000, accountService.getBalance("U001"));

        // 不在全局事务里时，代理退化为普通 JDBC 操作
        storageService.deduct("C001", 10);
        accountService.debit("U001", 50);

        Assert.assertEquals(90, storageService.getCount("C001"));
        Assert.assertEquals(950, accountService.getBalance("U001"));

        // 没有全局事务就没有 undo_log
        Assert.assertEquals(0, DataSourceProvider.getUndoLogCount(storageDb));
        Assert.assertEquals(0, DataSourceProvider.getUndoLogCount(accountDb));
    }
}
