package io.spring2go.seata.core.protocol.transaction;

/**
 * Created by william on May, 2020
 */
public abstract class AbstractTransactionRequestToTC extends AbstractTransactionRequest {
    protected TCInboundHandler handler;

    public void setTCInboundHandler(TCInboundHandler handler) {
        this.handler = handler;
    }
}
