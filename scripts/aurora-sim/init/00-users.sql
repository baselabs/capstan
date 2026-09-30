-- capstan aurora-sim — the account seed, identical on every node (idempotent).
-- capstan_sha2 mirrors scripts/mysql-init (the pipeline account); `repl` is the
-- replication account the harness uses to wire the GTID replica chain. Throwaway
-- local-only credentials, never a real secret.
CREATE USER IF NOT EXISTS 'capstan_sha2'@'%' IDENTIFIED WITH caching_sha2_password BY 'capstan_sha2_pw';
ALTER USER 'capstan_sha2'@'%' IDENTIFIED WITH caching_sha2_password BY 'capstan_sha2_pw';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT, LOCK TABLES ON *.* TO 'capstan_sha2'@'%';
GRANT XA_RECOVER_ADMIN ON *.* TO 'capstan_sha2'@'%';

CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED WITH caching_sha2_password BY 'repl_pw';
ALTER USER 'repl'@'%' IDENTIFIED WITH caching_sha2_password BY 'repl_pw';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';

CREATE USER IF NOT EXISTS 'admin_sim'@'%' IDENTIFIED WITH caching_sha2_password BY 'admin_sim_pw';
ALTER USER 'admin_sim'@'%' IDENTIFIED WITH caching_sha2_password BY 'admin_sim_pw';
GRANT SYSTEM_VARIABLES_ADMIN, REPLICATION_SLAVE_ADMIN, SELECT ON *.* TO 'admin_sim'@'%';
