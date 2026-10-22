package io.spring2go.seata.examples.service;

import io.spring2go.seata.core.context.RootContext;
import io.spring2go.seata.tm.api.TransactionalTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BusinessService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessService.class);

    private final StorageService storageService;
    private final AccountService accountService;
    private final TransactionalTemplate transactionalTemplate;

    public BusinessService(StorageService storageService, AccountService accountService) {
        this.storageService = storageService;
        this.accountService = accountService;
        this.transactionalTemplate = new TransactionalTemplate();
    }

    public void purchase(String userId, String commodityCode, int orderCount, boolean shouldFail) throws Throwable {
        LOGGER.info("[purchase] Starting purchase for user={}, commodity={}, count={}, shouldFail={}",
                userId, commodityCode, orderCount, shouldFail);

        transactionalTemplate.execute(() -> {
            String xid = RootContext.getXID();
            LOGGER.info("[purchase] Global transaction began with XID={}", xid);

            // Step 1: 扣减库存
            storageService.deduct(commodityCode, orderCount);
            LOGGER.info("[purchase] Deducted storage by {}", orderCount);

            // Step 2: 扣减账户余额 (单价 10 元)
            int orderAmount = orderCount * 10;
            accountService.debit(userId, orderAmount);
            LOGGER.info("[purchase] Debited account by {}", orderAmount);

            if (shouldFail) {
                LOGGER.warn("[purchase] Simulating downstream business failure! Throwing exception...");
                throw new RuntimeException("Business simulated exception: Insufficient funds or downstream service error!");
            }

            return null;
        });

        LOGGER.info("[purchase] Purchase completed successfully.");
    }
}
