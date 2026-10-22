package io.spring2go.seata.examples.service;

import io.spring2go.seata.rm.datasource.DataSourceProxy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class StorageService {
    private final DataSourceProxy dataSourceProxy;

    public StorageService(DataSourceProxy dataSourceProxy) {
        this.dataSourceProxy = dataSourceProxy;
    }

    public void deduct(String commodityCode, int count) throws SQLException {
        String sql = "UPDATE storage_tbl SET count = count - ? WHERE commodity_code = ?";
        try (Connection conn = dataSourceProxy.getConnection();
             PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setInt(1, count);
            pst.setString(2, commodityCode);
            pst.executeUpdate();
        }
    }

    public int getCount(String commodityCode) throws SQLException {
        String sql = "SELECT count FROM storage_tbl WHERE commodity_code = ?";
        try (Connection conn = dataSourceProxy.getConnection();
             PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setString(1, commodityCode);
            try (ResultSet rs = pst.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return -1;
    }
}
