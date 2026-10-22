package io.spring2go.seata.tm.api;

import io.spring2go.seata.core.exception.TransactionException;
import io.spring2go.seata.core.model.GlobalStatus;

public interface GlobalTransaction {

    void begin() throws TransactionException;

    void begin(int timeout) throws TransactionException;

    void begin(int timeout, String name) throws TransactionException;

    void commit() throws TransactionException;

    void rollback() throws TransactionException;

    GlobalStatus getStatus() throws TransactionException;

    String getXid();
}
