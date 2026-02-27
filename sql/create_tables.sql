-- -----------------------------------------------------
-- Database creation and selection.
-- -----------------------------------------------------
CREATE DATABASE IF NOT EXISTS recruitment_dw;
USE recruitment_dw;

-- -----------------------------------------------------
-- Drop tables to avoid clashes
-- -----------------------------------------------------
DROP TABLE IF EXISTS fact_application;
DROP TABLE IF EXISTS dim_experience;
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

    CONSTRAINT fk_seniority
        FOREIGN KEY (seniority_key) REFERENCES dim_seniority(seniority_key),

    CONSTRAINT fk_date
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key),

    CONSTRAINT fk_country
        FOREIGN KEY (country_key) REFERENCES dim_country(country_key),

    CONSTRAINT fk_technology
        FOREIGN KEY (technology_key) REFERENCES dim_technology(technology_key)
) ENGINE = InnoDB;