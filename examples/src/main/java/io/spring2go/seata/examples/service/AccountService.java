package io.spring2go.seata.examples.service;

import io.spring2go.seata.rm.datasource.DataSourceProxy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AccountService {
    private final DataSourceProxy dataSourceProxy;

    public AccountService(DataSourceProxy dataSourceProxy) {
        this.dataSourceProxy = dataSourceProxy;
    }

    public void debit(String userId, int money) throws SQLException {
        String sql = "UPDATE account_tbl SET money = money - ? WHERE user_id = ?";
        try (Connection conn = dataSourceProxy.getConnection();
             PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setInt(1, money);
            pst.setString(2, userId);
            pst.executeUpdate();
        }
    }

    public int getBalance(String userId) throws SQLException {
        String sql = "SELECT money FROM account_tbl WHERE user_id = ?";
        try (Connection conn = dataSourceProxy.getConnection();
             PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setString(1, userId);
            try (ResultSet rs = pst.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return -1;
    }
}
