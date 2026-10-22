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
    public void testDataSourceInitAndServices() throws Exception {
        DataSourceProxy proxy = DataSourceProvider.getDataSourceProxy();
        Assert.assertNotNull(proxy);

        StorageService storageService = new StorageService(proxy);
        AccountService accountService = new AccountService(proxy);

        Assert.assertEquals(100, storageService.getCount("C001"));
        Assert.assertEquals(1000, accountService.getBalance("U001"));

        storageService.deduct("C001", 10);
        accountService.debit("U001", 50);

        Assert.assertEquals(90, storageService.getCount("C001"));
        Assert.assertEquals(950, accountService.getBalance("U001"));
    }
}
