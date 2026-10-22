package io.spring2go.seata.server.lock;

import java.util.Objects;

/**
 * Represents a granular row lock (resourceId, tableName, pk).
 */
public class RowLock {
    private final String resourceId;
    private final String tableName;
    private final String pk;

    public RowLock(String resourceId, String tableName, String pk) {
        this.resourceId = resourceId;
        this.tableName = tableName;
        this.pk = pk;
    }

    public String toRowKey() {
        return resourceId + "^^^" + tableName + "^^^" + pk;
    }

    public String getResourceId() {
        return resourceId;
    }

    public String getTableName() {
        return tableName;
    }

    public String getPk() {
        return pk;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowLock rowLock = (RowLock) o;
        return Objects.equals(resourceId, rowLock.resourceId) &&
                Objects.equals(tableName, rowLock.tableName) &&
                Objects.equals(pk, rowLock.pk);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceId, tableName, pk);
    }

    @Override
    public String toString() {
        return toRowKey();
    }
}
