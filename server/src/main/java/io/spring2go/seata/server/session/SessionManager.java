package io.spring2go.seata.server.session;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory global session manager.
 */
public class SessionManager {
    private final ConcurrentMap<Long, GlobalSession> sessionMap = new ConcurrentHashMap<>();

    public void addGlobalSession(GlobalSession session) {
        sessionMap.put(session.getTransactionId(), session);
    }

    public GlobalSession findGlobalSession(Long transactionId) {
        if (transactionId == null) {
            return null;
        }
        return sessionMap.get(transactionId);
    }

    public void removeGlobalSession(GlobalSession session) {
        if (session != null) {
            sessionMap.remove(session.getTransactionId());
        }
    }

    public void removeGlobalSession(Long transactionId) {
        if (transactionId != null) {
            sessionMap.remove(transactionId);
        }
    }

    public Collection<GlobalSession> allSessions() {
        return sessionMap.values();
    }
}
