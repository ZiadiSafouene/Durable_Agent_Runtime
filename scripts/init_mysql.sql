CREATE DATABASE IF NOT EXISTS durable_agent CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS durable_agent_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'durable'@'%' IDENTIFIED BY 'durable';
GRANT ALL PRIVILEGES ON durable_agent.* TO 'durable'@'%';
GRANT ALL PRIVILEGES ON durable_agent_test.* TO 'durable'@'%';
FLUSH PRIVILEGES;
