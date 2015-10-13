SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

CREATE SCHEMA IF NOT EXISTS `powertac_analysis` ;
CREATE SCHEMA IF NOT EXISTS `powertac_analysis` ;
USE `powertac_analysis` ;

-- -----------------------------------------------------
-- Table `broker`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `broker` ;

CREATE TABLE IF NOT EXISTS `broker` (
  `idbroker` INT NOT NULL,
  `name` VARCHAR(45) NULL,
  PRIMARY KEY (`idbroker`),
  UNIQUE INDEX `idbroker_UNIQUE` (`idbroker` ASC))
ENGINE = InnoDB
COMMENT = 'A broker in a tournament.';


-- -----------------------------------------------------
-- Table `game`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `game` ;

CREATE TABLE IF NOT EXISTS `game` (
  `idgame` INT NOT NULL,
  `size` INT NULL COMMENT 'Number of brokers',
  `length` INT NULL COMMENT 'Number of timeslots',
  PRIMARY KEY (`idgame`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `broker_game`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `broker_game` ;

CREATE TABLE IF NOT EXISTS `broker_game` (
  `broker_idbroker` INT NOT NULL,
  `game_idgame` INT NOT NULL,
  `broker_id_game` INT NULL COMMENT 'Broker instance id in this game\n',
  PRIMARY KEY (`broker_idbroker`, `game_idgame`),
  INDEX `fk_broker_game_broker_idx` (`broker_idbroker` ASC),
  INDEX `fk_broker_game_game1_idx` (`game_idgame` ASC),
  CONSTRAINT `fk_broker_game_broker`
    FOREIGN KEY (`broker_idbroker`)
    REFERENCES `broker` (`idbroker`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_broker_game_game1`
    FOREIGN KEY (`game_idgame`)
    REFERENCES `game` (`idgame`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `market_position`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `market_position` ;

CREATE TABLE IF NOT EXISTS `market_position` (
  `broker_game_broker_idbroker` INT NOT NULL,
  `broker_game_game_idgame` INT NOT NULL,
  `timeslot` INT NOT NULL,
  `value` DOUBLE NULL,
  PRIMARY KEY (`broker_game_broker_idbroker`, `broker_game_game_idgame`, `timeslot`),
  CONSTRAINT `fk_market_position_broker_game1`
    FOREIGN KEY (`broker_game_broker_idbroker` , `broker_game_game_idgame`)
    REFERENCES `broker_game` (`broker_idbroker` , `game_idgame`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `cash_position`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `cash_position` ;

CREATE TABLE IF NOT EXISTS `cash_position` (
  `broker_game_broker_idbroker` INT NOT NULL,
  `broker_game_game_idgame` INT NOT NULL,
  `timeslot` INT NOT NULL,
  `balance` DOUBLE NULL,
  PRIMARY KEY (`broker_game_broker_idbroker`, `broker_game_game_idgame`, `timeslot`),
  CONSTRAINT `fk_cash_position_broker_game1`
    FOREIGN KEY (`broker_game_broker_idbroker` , `broker_game_game_idgame`)
    REFERENCES `broker_game` (`broker_idbroker` , `game_idgame`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

USE `powertac_analysis` ;

SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
