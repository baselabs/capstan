-- Source-server init: the watched table + the replication account.
-- Throwaway credentials for a disposable local example — not secrets.

CREATE TABLE IF NOT EXISTS orders (
  id         INT          NOT NULL PRIMARY KEY,
  note       VARCHAR(128) NOT NULL,
  updated_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE USER IF NOT EXISTS 'capstan_reader'@'%'
  IDENTIFIED WITH caching_sha2_password BY 'capstan_reader_pw';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'capstan_reader'@'%';
FLUSH PRIVILEGES;
