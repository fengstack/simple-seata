package io.spring2go.seata.tm.api;

import io.spring2go.seata.core.context.RootContext;
import io.spring2go.seata.core.exception.TransactionException;
import io.spring2go.seata.core.model.GlobalStatus;
import io.spring2go.seata.core.model.TransactionManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TransactionalTemplateTest {

    static class DummyTransactionManager implements TransactionManager {
        boolean beginCalled = false;
        boolean commitCalled = false;
        boolean rollbackCalled = false;
        String xid = "127.0.0.1:8091:2001";

        @Override
        public String begin(String applicationId, String transactionServiceGroup, String name, int timeout) throws TransactionException {
            beginCalled = true;
            return xid;
        }

        @Override
        public GlobalStatus commit(String xid) throws TransactionException {
            commitCalled = true;
            return GlobalStatus.Committed;
        }

        @Override
        public GlobalStatus rollback(String xid) throws TransactionException {
            rollbackCalled = true;
            return GlobalStatus.Rollbacked;
        }

        @Override
        public GlobalStatus getStatus(String xid) throws TransactionException {
            return GlobalStatus.Begin;
        }
    }

    @Before
    @After
    public void cleanup() {
        RootContext.unbind();
    }

    @Test
    public void testSuccessfulExecution() throws Throwable {
        DummyTransactionManager tm = new DummyTransactionManager();
        TransactionalTemplate template = new TransactionalTemplate(tm);

        String result = template.execute(() -> {
            Assert.assertTrue(RootContext.inGlobalTransaction());
            Assert.assertEquals(tm.xid, RootContext.getXID());
            return "SUCCESS";
        });

        Assert.assertEquals("SUCCESS", result);
        Assert.assertTrue(tm.beginCalled);
        Assert.assertTrue(tm.commitCalled);
        Assert.assertFalse(tm.rollbackCalled);
        Assert.assertFalse(RootContext.inGlobalTransaction());
    }

    @Test
    public void testExceptionExecutionTriggersRollback() throws Throwable {
        DummyTransactionManager tm = new DummyTransactionManager();
        TransactionalTemplate template = new TransactionalTemplate(tm);

        try {
            template.execute(() -> {
                Assert.assertTrue(RootContext.inGlobalTransaction());
                throw new IllegalStateException("Business Failure");
            });
            Assert.fail("Expected exception to be re-thrown");
        } catch (IllegalStateException e) {
            Assert.assertEquals("Business Failure", e.getMessage());
        }

        Assert.assertTrue(tm.beginCalled);
        Assert.assertFalse(tm.commitCalled);
        Assert.assertTrue(tm.rollbackCalled);
        Assert.assertFalse(RootContext.inGlobalTransaction());
    }
}
