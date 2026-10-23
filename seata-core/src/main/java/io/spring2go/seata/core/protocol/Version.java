package io.spring2go.seata.core.protocol;

/**
 * 协议版本号，随注册报文携带（客户端与服务端同版本，仅作演示）。
 */
public class Version {
    public static final String CURRENT = "0.1.0";

    private Version() {
    }
}
