SELECT 
    t.technology_name,
    COUNT(*) AS total_hires
FROM fact_application f
JOIN dim_technology t 
    ON f.technology_key = t.technology_key
WHERE f.hired_flag = 1
GROUP BY t.technology_name
ORDER BY total_hires DESC;

SELECT 
    d.year,
    COUNT(*) AS total_hires
FROM fact_application f
JOIN dim_date d 
    ON f.date_key = d.date_key
WHERE f.hired_flag = 1
GROUP BY d.year
ORDER BY d.year;

SELECT 
    s.seniority_level,
    COUNT(*) AS total_hires
FROM fact_application f
JOIN dim_seniority s 
    ON f.seniority_key = s.seniority_key
WHERE f.hired_flag = 1
GROUP BY s.seniority_level
ORDER BY total_hires DESC;

SELECT 
    d.year,
    c.country_name,
    COUNT(*) AS total_hires
FROM fact_application f
JOIN dim_country c 
    ON f.country_key = c.country_key
JOIN dim_date d
    ON f.date_key = d.date_key
WHERE f.hired_flag = 1
  AND c.country_name IN ('usa','brazil','colombia','ecuador')
GROUP BY d.year, c.country_name
ORDER BY d.year, c.country_name;

SELECT 
    ROUND(
        SUM(CASE WHEN hired_flag = 1 THEN 1 ELSE 0 END) 
        * 100.0 / COUNT(*), 2
    ) AS hire_rate_percentage
FROM fact_application;

SELECT 
    ROUND(AVG(code_challenge_score),2) AS avg_code_score,
    ROUND(AVG(technical_interview_score),2) AS avg_interview_score
FROM fact_application
WHERE hired_flag = 1;

