-- =====================================================
-- LOAD FROM STAGING TO FINAL TABLES
-- Full Reload Strategy
-- =====================================================
-- Disable FK checks to allow truncation safely
SET FOREIGN_KEY_CHECKS = 0;


-- -----------------------------------------------------
-- 1. Truncate Final Tables
-- -----------------------------------------------------
TRUNCATE TABLE fact_application;

TRUNCATE TABLE dim_experience;
TRUNCATE TABLE dim_candidate;
TRUNCATE TABLE dim_seniority;
TRUNCATE TABLE dim_country;
TRUNCATE TABLE dim_technology;
TRUNCATE TABLE dim_date;


SET FOREIGN_KEY_CHECKS = 1;


-- -----------------------------------------------------
-- 2. Insert Dimensions from Staging
-- -----------------------------------------------------

INSERT INTO dim_experience (experience_key, experience_range)
SELECT experience_key, experience_range
FROM dim_experience_stg;

INSERT INTO dim_candidate (candidate_key, first_name, last_name, email)
SELECT candidate_key, first_name, last_name, email
FROM dim_candidate_stg;

INSERT INTO dim_seniority (seniority_key, seniority_level)
SELECT seniority_key, seniority_level
FROM dim_seniority_stg;

INSERT INTO dim_country (country_key, country_name)
SELECT country_key, country_name
FROM dim_country_stg;

INSERT INTO dim_technology (technology_key, technology_name)
SELECT technology_key, technology_name
FROM dim_technology_stg;

INSERT INTO dim_date (
    date_key,
    full_date,
    day,
    month,
    month_name,
    quarter,
    year
)
SELECT
    date_key,
    full_date,
    day,
    month,
    month_name,
    quarter,
    year
FROM dim_date_stg;


-- -----------------------------------------------------
-- 3. Insert Fact Table
-- -----------------------------------------------------

INSERT INTO fact_application (
    application_key,
    experience_key,
    candidate_key,
    seniority_key,
    country_key,
    technology_key,
    date_key,
    code_challenge_score,
    technical_interview_score,
    hired_flag
)
SELECT
    application_key,
    experience_key,
    candidate_key,
    seniority_key,
    country_key,
    technology_key,
    date_key,
    code_challenge_score,
    technical_interview_score,
    hired_flag
FROM fact_application_stg;


-- =====================================================
-- END LOAD
-- =====================================================