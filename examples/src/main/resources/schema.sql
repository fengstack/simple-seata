CREATE TABLE IF NOT EXISTS storage_tbl (
    id INT NOT NULL AUTO_INCREMENT,
    commodity_code VARCHAR(32) NOT NULL,
    count INT NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS account_tbl (
    id INT NOT NULL AUTO_INCREMENT,
    user_id VARCHAR(32) NOT NULL,
    money INT NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
);

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
    KEY idx_unionkey (xid, branch_id)
);
