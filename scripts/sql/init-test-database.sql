RESET MASTER;

SET GLOBAL max_connections=1000;
SET GLOBAL gtid_strict_mode=ON;

CREATE USER 'maxuser'@'127.0.0.1' IDENTIFIED BY 'maxpwd';
CREATE USER 'maxuser'@'%' IDENTIFIED BY 'maxpwd';
GRANT ALL ON *.* TO 'maxuser'@'127.0.0.1' WITH GRANT OPTION;
GRANT ALL ON *.* TO 'maxuser'@'%' WITH GRANT OPTION;

CREATE DATABASE test;

USE test;

CREATE TABLE tests (
  id int PRIMARY KEY
);

INSERT INTO tests (id)
VALUES
  (1),
  (2),
  (3);
