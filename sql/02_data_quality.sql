-- PART 1: DATA QUALITY CHECKS (computed in every datasets as separated entities)
-- ==========================================
-- 1️Check for missing values
SELECT 
    'marketing_spend' AS table_name,
    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS missing_dates,
    SUM(CASE WHEN spend IS NULL THEN 1 ELSE 0 END) AS missing_spend
FROM marketing_spend
UNION ALL
SELECT 
    'revenue',
    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END)
FROM revenue
UNION ALL
SELECT 
    'external_factors',
    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN factor_value IS NULL THEN 1 ELSE 0 END)
FROM external_factors;

-- ==========================================
-- 2️Identify date gaps in the data (31.12.2022 is not in the dataset as 2022 is not an year considered)
SELECT date,
       DATE(date, '-1 day') AS previous_date
FROM marketing_spend
WHERE DATE(date, '-1 day') NOT IN (SELECT date FROM marketing_spend)
ORDER BY date;

-- ==========================================
-- 3️ Find outliers in spend and revenue
/* Simple outlier detection for revenue (|z| > 3)
   - SQLite has no STDDEV, so we compute sigma via sqrt(avg(x*x) - avg(x)^2)
*/
WITH stats AS (
  SELECT
    AVG(revenue)                                    AS mu,
    SQRT(AVG(revenue * revenue) - AVG(revenue)*AVG(revenue)) AS sigma
  FROM revenue
)
SELECT r.*
FROM revenue r
CROSS JOIN stats s
WHERE s.sigma IS NOT NULL
  AND ABS(r.revenue - s.mu) > 3.0 * s.sigma
ORDER BY ABS(r.revenue - s.mu) DESC;

-- spend
WITH ms AS (
  SELECT
    date,
    (paid_search_spend + paid_social_spend + display_spend +
     email_spend + affiliate_spend + tv_spend) AS spend
  FROM marketing_spend
),
stats AS (
  SELECT
    AVG(spend)                                    AS mu,
    SQRT(AVG(spend*spend) - AVG(spend)*AVG(spend)) AS sigma
  FROM ms
)
SELECT m.*
FROM ms m
CROSS JOIN stats s
WHERE s.sigma IS NOT NULL
  AND ABS(m.spend - s.mu) > 3.0 * s.sigma
ORDER BY ABS(m.spend - s.mu) DESC;

-- ==========================================
-- 4 Check duplicates (note: in the ext_factors file to the same date, 
--two different answers are provided is_holiday or not --> this is most probably an issue so only one row should be taken): 

-- Duplicates for date in marketing_spend
SELECT 
    date,
    COUNT(*) AS num_occurrences
FROM marketing_spend
GROUP BY date
HAVING COUNT(*) > 1;

-- Duplicates for date in revenue
SELECT 
    date,
    COUNT(*) AS num_occurrences
FROM revenue
GROUP BY date
HAVING COUNT(*) > 1;

-- Duplicates for date in external_factors
SELECT 
    date,
    COUNT(*) AS num_occurrences
FROM external_factors
GROUP BY date
HAVING COUNT(*) > 1;

-- NOTES:
-- Two missing dates detected: 2023-10-29, 2024-10-27 --> These correspond to daylight saving time transitions.
-- The gaps are left explicit (no interpolation) to preserve data integrity.
-- 26.03.2023 and 31.03.2024 are duplicated in every datasets (--> again, these correspond to daylight saving time transitions.) 
--> let's keep the first row only (value are closed but dicotomic variables can't be averaged of course)
-- final check: every datasets shows 730 rows: 1 date is missing, 1 date is duplicated --> final datasets should have 728 rows
