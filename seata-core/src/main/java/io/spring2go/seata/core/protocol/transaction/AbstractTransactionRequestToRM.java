package io.spring2go.seata.core.protocol.transaction;

/**
 * Created by william on May, 2020
 */
public abstract class AbstractTransactionRequestToRM extends AbstractTransactionRequest {
    protected RMInboundHandler handler;

    public void setRMInboundMessageHandler(RMInboundHandler handler) {
        this.handler = handler;
    }
}
