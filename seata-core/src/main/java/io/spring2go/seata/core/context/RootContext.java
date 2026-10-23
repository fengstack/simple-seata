package io.spring2go.seata.core.context;

import io.spring2go.seata.common.exception.ShouldNeverHappenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 全局事务上下文：把 XID 绑定到当前线程，让同一线程上的所有 RM 操作
 * 都知道自己属于哪个全局事务。TM 开启事务时 bind，结束时 unbind。
 */
public class RootContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(RootContext.class);

    private static final ThreadLocal<String> XID_HOLDER = new ThreadLocal<String>();

    public static String getXID() {
        return XID_HOLDER.get();
    }

    public static void bind(String xid) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("bind " + xid);
        }
        XID_HOLDER.set(xid);
    }

    public static String unbind() {
        String xid = XID_HOLDER.get();
        XID_HOLDER.remove();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("unbind " + xid);
        }
        return xid;
    }

    public static boolean inGlobalTransaction() {
        return XID_HOLDER.get() != null;
    }

    public static void assertNotInGlobalTransaction() {
        if (inGlobalTransaction()) {
            throw new ShouldNeverHappenException();
        }
    }
}
