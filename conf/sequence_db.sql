-- All operation executed in the specified data node such as 'db1'
--
-- MYCAT_SEQUENCE
DROP TABLE IF EXISTS MYCAT_SEQUENCE;
-- name: sequence name
-- current_value: current value
-- increment: increment step, MyCat fetches one time for how many sequence value

CREATE TABLE MYCAT_SEQUENCE (
  name VARCHAR(64) NOT NULL,
  current_value BIGINT NOT NULL,
  increment INT NOT NULL DEFAULT 100,
  PRIMARY KEY(name)
)ENGINE=InnoDB;

-- Init some sequences
INSERT INTO MYCAT_SEQUENCE(name, current_value, increment)
VALUES ('COMPANY', 10000, 100);
INSERT INTO MYCAT_SEQUENCE(name, current_value, increment)
VALUES ('GLOBAL', 1, 50);
-- Here only for test
-- INSERT INTO MYCAT_SEQUENCE(name, current_value, increment) VALUES ('CUSTOMER', 1, 0);

-- Related function
-- Fetch current sequence value(current value, increment)
DROP FUNCTION IF EXISTS mycat_seq_currval;
DELIMITER ;;
CREATE FUNCTION `mycat_seq_currval`(seq_name VARCHAR(64))
RETURNS VARCHAR(64) charset utf8
DETERMINISTIC
BEGIN
DECLARE retval VARCHAR(64);
SET retval = "-999999999,null";
SELECT CONCAT(CAST(current_value AS CHAR),",",CAST(increment AS CHAR)) INTO retval
FROM MYCAT_SEQUENCE WHERE NAME = seq_name;
RETURN retval;
END
;;
DELIMITER ;

-- Setting sequence value
DROP FUNCTION IF EXISTS mycat_seq_setval;
DELIMITER ;;
CREATE FUNCTION mycat_seq_setval(seq_name VARCHAR(64), value BIGINT)
RETURNS varchar(64) CHARSET utf8
DETERMINISTIC
BEGIN
UPDATE MYCAT_SEQUENCE
SET current_value = value
WHERE name = seq_name;
RETURN mycat_seq_currval(seq_name);
END ;;
DELIMITER ;

-- Fetch next sequence value
DROP FUNCTION IF EXISTS `mycat_seq_nextval`;
DELIMITER ;;
CREATE FUNCTION `mycat_seq_nextval`(seq_name VARCHAR(64))
RETURNS VARCHAR(64)CHARSET utf8
DETERMINISTIC
BEGIN
UPDATE MYCAT_SEQUENCE SET current_value = current_value + increment
WHERE NAME = seq_name;
RETURN mycat_seq_currval(seq_name);
END;;
DELIMITER ;
