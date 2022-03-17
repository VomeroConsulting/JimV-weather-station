-- creates clean database for weerstation
DROP USER IF EXISTS 'frontend'@'localhost';
DROP USER IF EXISTS 'backend'@'localhost';
DROP USER IF EXISTS 'pi'@'localhost';
DROP DATABASE IF EXISTS `weather_data`;
CREATE DATABASE `weather_data`;
USE `weather_data`;
CREATE TABLE `sensors` (
`id` BIGINT NOT NULL AUTO_INCREMENT,
`time` TIMESTAMP NOT NULL DEFAULT current_timestamp,
`ws_ave` FLOAT(4.1) NULL,
`ws_max` FLOAT(4.1) NULL,
`w_dir` CHAR(3) NULL,
`humid` FLOAT(4.1) NULL,
`press` FLOAT(4.1) NULL,
`temp` FLOAT(4.1) NULL,
`therm` FLOAT(4.1) NULL,
`rain` FLOAT(4.1) NULL,
 PRIMARY KEY (`id`)
);
CREATE USER 'frontend'@'localhost' IDENTIFIED BY '123456';
CREATE USER 'backend'@'localhost' IDENTIFIED BY '123456';
CREATE USER 'pi'@'localhost' IDENTIFIED BY 'raspberry';
GRANT SELECT ON weather_data.sensors TO 'frontend'@'localhost';
GRANT INSERT ON weather_data.sensors TO 'backend'@'localhost';
GRANT SELECT, INSERT ON weather_data.* TO 'pi'@'localhost';
FLUSH PRIVILEGES;
