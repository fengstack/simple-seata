package io.spring2go.seata.tm;

import io.spring2go.seata.common.XID;
import io.spring2go.seata.core.exception.TransactionException;
import io.spring2go.seata.core.exception.TransactionExceptionCode;
import io.spring2go.seata.core.model.GlobalStatus;
import io.spring2go.seata.core.protocol.ResultCode;
import io.spring2go.seata.core.protocol.transaction.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

public class DefaultTransactionManagerTest {

    static class MockTransactionManager extends DefaultTransactionManager {
        Object responseToReturn;
        Exception exceptionToThrow;
        Object lastRequest;

        @Override
        protected Object syncCall(Object request) throws TimeoutException {
            this.lastRequest = request;
            if (exceptionToThrow instanceof TimeoutException) {
                throw (TimeoutException) exceptionToThrow;
            }
            if (exceptionToThrow instanceof RuntimeException) {
                throw (RuntimeException) exceptionToThrow;
            }
            return responseToReturn;
        }
    }

    @Test
    public void testBeginSuccess() throws Exception {
        MockTransactionManager tm = new MockTransactionManager();
        GlobalBeginResponse resp = new GlobalBeginResponse();
        resp.setResultCode(ResultCode.Success);
        resp.setXid("127.0.0.1:8091:1001");
        tm.responseToReturn = resp;

        String xid = tm.begin("testApp", "testGroup", "tx-test", 60000);
        Assert.assertEquals("127.0.0.1:8091:1001", xid);
        Assert.assertTrue(tm.lastRequest instanceof GlobalBeginRequest);
        GlobalBeginRequest req = (GlobalBeginRequest) tm.lastRequest;
        Assert.assertEquals("tx-test", req.getTransactionName());
        Assert.assertEquals(60000, req.getTimeout());
    }

    @Test(expected = TransactionException.class)
    public void testBeginFailedResponse() throws Exception {
        MockTransactionManager tm = new MockTransactionManager();
        GlobalBeginResponse resp = new GlobalBeginResponse();
        resp.setResultCode(ResultCode.Failed);
        resp.setMsg("begin failed");
        resp.setTransactionExceptionCode(TransactionExceptionCode.Unknown);
        tm.responseToReturn = resp;

        tm.begin("testApp", "testGroup", "tx-test", 60000);
    }

    @Test(expected = TransactionException.class)
    public void testBeginTimeout() throws Exception {
        MockTransactionManager tm = new MockTransactionManager();
        tm.exceptionToThrow = new TimeoutException("timeout");
        tm.begin("testApp", "testGroup", "tx-test", 60000);
    }

    @Test
    public void testCommitSuccess() throws Exception {
        MockTransactionManager tm = new MockTransactionManager();
        GlobalCommitResponse resp = new GlobalCommitResponse();
        resp.setResultCode(ResultCode.Success);
        resp.setGlobalStatus(GlobalStatus.Committed);
        tm.responseToReturn = resp;

        GlobalStatus status = tm.commit("127.0.0.1:8091:1001");
        Assert.assertEquals(GlobalStatus.Committed, status);
        Assert.assertTrue(tm.lastRequest instanceof GlobalCommitRequest);
        GlobalCommitRequest req = (GlobalCommitRequest) tm.lastRequest;
        Assert.assertEquals(1001L, req.getTransactionId());
    }

    @Test
    public void testRollbackSuccess() throws Exception {
        MockTransactionManager tm = new MockTransactionManager();
        GlobalRollbackResponse resp = new GlobalRollbackResponse();
        resp.setResultCode(ResultCode.Success);
        resp.setGlobalStatus(GlobalStatus.Rollbacked);
        tm.responseToReturn = resp;

        GlobalStatus status = tm.rollback("127.0.0.1:8091:1001");
        Assert.assertEquals(GlobalStatus.Rollbacked, status);
        Assert.assertTrue(tm.lastRequest instanceof GlobalRollbackRequest);
        GlobalRollbackRequest req = (GlobalRollbackRequest) tm.lastRequest;
        Assert.assertEquals(1001L, req.getTransactionId());
    }

    @Test
    public void testGetStatusSuccess() throws Exception {
        MockTransactionManager tm = new MockTransactionManager();
        GlobalStatusResponse resp = new GlobalStatusResponse();
        resp.setResultCode(ResultCode.Success);
        resp.setGlobalStatus(GlobalStatus.CommitRetrying);
        tm.responseToReturn = resp;

        GlobalStatus status = tm.getStatus("127.0.0.1:8091:1001");
        Assert.assertEquals(GlobalStatus.CommitRetrying, status);
        Assert.assertTrue(tm.lastRequest instanceof GlobalStatusRequest);
        GlobalStatusRequest req = (GlobalStatusRequest) tm.lastRequest;
        Assert.assertEquals(1001L, req.getTransactionId());
    }
}
