-- -----------------------------------------------------
-- Database creation and selection.
-- -----------------------------------------------------
CREATE DATABASE IF NOT EXISTS recruitment_dw;
USE recruitment_dw;


-- Drop staging tables first
DROP TABLE IF EXISTS fact_application_stg;
DROP TABLE IF EXISTS dim_experience_stg;
DROP TABLE IF EXISTS dim_candidate_stg;
DROP TABLE IF EXISTS dim_seniority_stg;
DROP TABLE IF EXISTS dim_date_stg;
DROP TABLE IF EXISTS dim_country_stg;
DROP TABLE IF EXISTS dim_technology_stg;

-- -----------------------------------------------------
-- Drop final tables
-- -----------------------------------------------------
DROP TABLE IF EXISTS fact_application;
DROP TABLE IF EXISTS dim_experience;
DROP TABLE IF EXISTS dim_candidate;
DROP TABLE IF EXISTS dim_seniority;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_country;
DROP TABLE IF EXISTS dim_technology;

-- -----------------------------------------------------
-- Table `experience` (Dimension)
-- -----------------------------------------------------
-- Stores years of experience range.
CREATE TABLE IF NOT EXISTS dim_experience 
(
    experience_key INT PRIMARY KEY,
    experience_range VARCHAR (50) NOT NULL
);

-- -----------------------------------------------------
-- Table `candidate` (Dimension)
-- -----------------------------------------------------
-- Stores candidate information.
CREATE TABLE dim_candidate 
(
    candidate_key INT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL
);


-- -----------------------------------------------------
-- Table `seniority` (Dimension)
-- -----------------------------------------------------
-- Seniority level. (Junior, Mid level, Senior, etc.).
CREATE TABLE dim_seniority 
(
    seniority_key INT PRIMARY KEY,
    seniority_level VARCHAR(50) NOT NULL
) ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `Country` (Dimension)
-- -----------------------------------------------------
-- Countries.
CREATE TABLE dim_country 
(
    country_key INT PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL
) ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `Technology` (Dimension)
-- -----------------------------------------------------
-- Different technologies.
CREATE TABLE dim_technology 
(
    technology_key INT PRIMARY KEY,
    technology_name VARCHAR(100) NOT NULL ) 
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `Date` (Dimension)
-- -----------------------------------------------------
-- Date.
CREATE TABLE dim_date 
(
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL ) 
ENGINE = InnoDB;


-- =========================
-- FACT TABLE
-- =========================
-- The central fact table storing quantitative transactional data.
-- It links to all dimensions via Foreign Keys.

CREATE TABLE fact_application
(
    application_key INT PRIMARY KEY,

    -- Foreign Keys
    experience_key INT NOT NULL,
    candidate_key INT NOT NULL,
    seniority_key INT NOT NULL,
    date_key INT NOT NULL,
    country_key INT NOT NULL,
    technology_key INT NOT NULL,

    -- Metrics
    code_challenge_score INT NOT NULL,
    technical_interview_score INT NOT NULL,

    -- Indicator
    hired_flag TINYINT NOT NULL,

    -- Foreign Key Constraints
    CONSTRAINT fk_experience
        FOREIGN KEY (experience_key) REFERENCES dim_experience(experience_key),

    CONSTRAINT fk_candidate
        FOREIGN KEY (candidate_key) REFERENCES dim_candidate(candidate_key),
 
    CONSTRAINT fk_seniority
        FOREIGN KEY (seniority_key) REFERENCES dim_seniority(seniority_key),

    CONSTRAINT fk_date
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key),

    CONSTRAINT fk_country
        FOREIGN KEY (country_key) REFERENCES dim_country(country_key),

    CONSTRAINT fk_technology
        FOREIGN KEY (technology_key) REFERENCES dim_technology(technology_key)
) ENGINE = InnoDB;

-- =====================================================
-- STAGING DIMENSION TABLES  (*_stg)
-- Mirror of each final dimension table but with NO
-- foreign key constraints and NO primary key enforcement.
-- This allows load.py to TRUNCATE and reload them freely
-- in any order without MySQL raising constraint errors.
-- Data is validated and promoted to final tables by
-- load_tables.sql only after a successful full load.
-- =====================================================

-- -----------------------------------------------------
-- dim_experience_stg (Staging)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_experience_stg
(
    experience_key   INT         NOT NULL,
    experience_range VARCHAR(50) NOT NULL
) ENGINE = InnoDB;

-- -----------------------------------------------------
-- dim_candidate_stg (Staging)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_candidate_stg
(
    candidate_key INT          NOT NULL,
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    email         VARCHAR(100) NOT NULL
) ENGINE = InnoDB;

-- -----------------------------------------------------
-- dim_seniority_stg (Staging)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_seniority_stg
(
    seniority_key   INT         NOT NULL,
    seniority_level VARCHAR(50) NOT NULL
) ENGINE = InnoDB;

-- -----------------------------------------------------
-- dim_country_stg (Staging)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_country_stg
(
    country_key  INT          NOT NULL,
    country_name VARCHAR(100) NOT NULL
) ENGINE = InnoDB;

-- -----------------------------------------------------
-- dim_technology_stg (Staging)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_technology_stg
(
    technology_key  INT          NOT NULL,
    technology_name VARCHAR(100) NOT NULL
) ENGINE = InnoDB;

-- -----------------------------------------------------
-- dim_date_stg (Staging)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date_stg
(
    date_key   INT         NOT NULL,
    full_date  DATE        NOT NULL,
    day        INT         NOT NULL,
    month      INT         NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter    INT         NOT NULL,
    year       INT         NOT NULL
) ENGINE = InnoDB;


-- =====================================================
-- STAGING FACT TABLE  (fact_application_stg)
-- Mirror of fact_application with NO FK constraints.
-- Receives data from load.py first. Only promoted to
-- fact_application by load_tables.sql after all staging
-- dimension tables have been successfully loaded.
-- =====================================================
CREATE TABLE IF NOT EXISTS fact_application_stg
(
    application_key           INT     NOT NULL,
    experience_key            INT     NOT NULL,
    candidate_key             INT     NOT NULL,
    seniority_key             INT     NOT NULL,
    date_key                  INT     NOT NULL,
    country_key               INT     NOT NULL,
    technology_key            INT     NOT NULL,
    code_challenge_score      INT     NOT NULL,
    technical_interview_score INT     NOT NULL,
    hired_flag                TINYINT NOT NULL
) ENGINE = InnoDB;

-- =====================================================
-- END OF SCHEMA CREATION
-- Total tables created: 14
--   Final    : dim_experience, dim_candidate, dim_seniority,
--              dim_country, dim_technology, dim_date,
--              fact_application                          (7)
--   Staging  : dim_experience_stg, dim_candidate_stg,
--              dim_seniority_stg, dim_country_stg,
--              dim_technology_stg, dim_date_stg,
--              fact_application_stg                      (7)
-- =====================================================