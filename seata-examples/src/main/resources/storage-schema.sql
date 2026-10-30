-- 库存库（模拟独立的库存微服务的数据库）
CREATE TABLE IF NOT EXISTS storage_tbl (
    id INT NOT NULL AUTO_INCREMENT,
    commodity_code VARCHAR(32) NOT NULL,
    count INT NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
);

-- AT 模式回滚日志表：每个参与全局事务的库都要有这张表
CREATE TABLE IF NOT EXISTS undo_log (
    id BIGINT NOT NULL AUTO_INCREMENT,
    branch_id BIGINT NOT NULL,
    xid VARCHAR(100) NOT NULL,
    rollback_info BLOB NOT NULL,
    log_status INT NOT NULL,
    log_created DATETIME NOT NULL,
    log_modified DATETIME NOT NULL,
    ext VARCHAR(100) DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY ux_undo_log (xid, branch_id)
);
