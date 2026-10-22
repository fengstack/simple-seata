package io.spring2go.seata.tm.api;

@FunctionalInterface
public interface TransactionalExecutor<T> {
    T execute() throws Throwable;
}
