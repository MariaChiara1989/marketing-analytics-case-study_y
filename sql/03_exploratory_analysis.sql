/* ===========================================================
   EXPLORATORY ANALYSIS 
   ===========================================================

   Notes:
     - Duplicate dates are handled by keeping only the first row per date.

/* ===========================================================
   1. MARKETING CHANNEL SUMMARY
   -----------------------------------------------------------
     - Compute total, average, min, and max spend per channel.
   =========================================================== */

WITH ms_dedup AS (
    SELECT
        date,
        paid_search_spend,
        paid_social_spend,
        display_spend,
        email_spend,
        affiliate_spend,
        tv_spend,
        ROW_NUMBER() OVER (
            PARTITION BY date
            ORDER BY date
        ) AS rn
    FROM marketing_spend
),
-- Convert wide table to long format (one row per date and channel)
ms_long AS (
    SELECT date, 'paid_search' AS channel, paid_search_spend AS spend FROM ms_dedup WHERE rn = 1
    UNION ALL
    SELECT date, 'paid_social' AS channel, paid_social_spend AS spend FROM ms_dedup WHERE rn = 1
    UNION ALL
    SELECT date, 'display' AS channel, display_spend AS spend FROM ms_dedup WHERE rn = 1
    UNION ALL
    SELECT date, 'email' AS channel, email_spend AS spend FROM ms_dedup WHERE rn = 1
    UNION ALL
    SELECT date, 'affiliate' AS channel, affiliate_spend AS spend FROM ms_dedup WHERE rn = 1
    UNION ALL
    SELECT date, 'tv' AS channel, tv_spend AS spend FROM ms_dedup WHERE rn = 1
)
-- Aggregate spend metrics by channel
SELECT
    channel,
    COUNT(*) AS days_count,
    SUM(spend) AS total_spend,
    AVG(spend) AS avg_spend_per_day,
    MIN(spend) AS min_spend,
    MAX(spend) AS max_spend
FROM ms_long
GROUP BY channel
ORDER BY total_spend DESC;
/* ===========================================================
   2. REVENUE SUMMARY
   =========================================================== */
WITH rev_dedup AS (
    SELECT
        date,
        revenue,
        transactions,
        new_customers,
        ROW_NUMBER() OVER (
            PARTITION BY date
            ORDER BY date
        ) AS rn
    FROM revenue
)
-- Aggregate metrics across deduplicated dates
SELECT
    COUNT(*) AS days_count,
    SUM(revenue) AS total_revenue,
    AVG(revenue) AS avg_revenue_per_day,
    MIN(revenue) AS min_revenue,
    MAX(revenue) AS max_revenue,
    SUM(transactions) AS total_transactions,
    AVG(transactions) AS avg_transactions_per_day,
    SUM(new_customers) AS total_new_customers,
    AVG(new_customers) AS avg_new_customers_per_day
FROM rev_dedup
WHERE rn = 1;

/* ===========================================
   MONTHLY TOTALS OF SPEND AND REVENUE 
   - Dedup rule: keep MIN(ROWID) per date
   - Join revenue with total marketing spend (sum of channels)
   =========================================== */
WITH
-- Deduplicate each table by date (keep first physical row)
ms_dedup AS (
  SELECT *
  FROM marketing_spend
  WHERE ROWID IN (
    SELECT MIN(ROWID) FROM marketing_spend GROUP BY date
  )
),
rev_dedup AS (
  SELECT *
  FROM revenue
  WHERE ROWID IN (
    SELECT MIN(ROWID) FROM revenue GROUP BY date
  )
),
-- Precompute total daily spend
spend_total AS (
  SELECT
    date,
    (paid_search_spend + paid_social_spend + display_spend +
     email_spend + affiliate_spend + tv_spend) AS total_spend
  FROM ms_dedup
)
SELECT
  strftime('%Y-%m', r.date)           AS year_month,
  ROUND(SUM(s.total_spend), 2)        AS total_spend,
  ROUND(SUM(r.revenue), 2)            AS total_revenue
FROM rev_dedup r
JOIN spend_total s ON s.date = r.date
GROUP BY 1
ORDER BY 1;

/* ===========================================
DAY-OF-WEEK PATTERNS
   =========================================== */
WITH
ms_dedup AS (
  SELECT *
  FROM marketing_spend
  WHERE ROWID IN (
    SELECT MIN(ROWID) FROM marketing_spend GROUP BY date
  )
),
rev_dedup AS (
  SELECT *
  FROM revenue
  WHERE ROWID IN (
    SELECT MIN(ROWID) FROM revenue GROUP BY date
  )
),
spend_total AS (
  SELECT
    date,
    (paid_search_spend + paid_social_spend + display_spend +
     email_spend + affiliate_spend + tv_spend) AS total_spend
  FROM ms_dedup
)
SELECT
  CASE strftime('%w', r.date)
    WHEN '0' THEN 'Sun' WHEN '1' THEN 'Mon' WHEN '2' THEN 'Tue'
    WHEN '3' THEN 'Wed' WHEN '4' THEN 'Thu' WHEN '5' THEN 'Fri'
    WHEN '6' THEN 'Sat' END                    AS dow,
  ROUND(AVG(s.total_spend), 2)                 AS avg_tot_spend,
  ROUND(AVG(r.revenue), 2)                     AS avg_tot_revenue
FROM rev_dedup r
JOIN spend_total s ON s.date = r.date
GROUP BY strftime('%w', r.date)
ORDER BY CASE dow
  WHEN 'Mon' THEN 1 WHEN 'Tue' THEN 2 WHEN 'Wed' THEN 3 WHEN 'Thu' THEN 4
  WHEN 'Fri' THEN 5 WHEN 'Sat' THEN 6 WHEN 'Sun' THEN 7 END;

/* ===========================================
SEASONALITY
   - Averages by calendar month across years
   - Includes external_factors.seasonality_index --> >1 "strong" period, <1 "weak" month
   =========================================== */
WITH
ms_dedup AS (
  SELECT *
  FROM marketing_spend
  WHERE ROWID IN (
    SELECT MIN(ROWID) FROM marketing_spend GROUP BY date
  )
),
rev_dedup AS (
  SELECT *
  FROM revenue
  WHERE ROWID IN (
    SELECT MIN(ROWID) FROM revenue GROUP BY date
  )
),
ef_dedup AS (
  SELECT *
  FROM external_factors
  WHERE ROWID IN (
    SELECT MIN(ROWID) FROM external_factors GROUP BY date
  )
),
spend_total AS (
  SELECT
    date,
    (paid_search_spend + paid_social_spend + display_spend +
     email_spend + affiliate_spend + tv_spend) AS total_spend
  FROM ms_dedup
)
SELECT
  CAST(strftime('%m', r.date) AS INTEGER) AS month_num,
  ROUND(AVG(s.total_spend), 2)            AS avg_tot_spend,
  ROUND(AVG(r.revenue), 2)                AS avg_tot_revenue,
  ROUND(AVG(ef.seasonality_index), 3)     AS avg_seasonality_index
FROM rev_dedup r
JOIN spend_total s   ON s.date = r.date
JOIN ef_dedup   ef   ON ef.date = r.date
GROUP BY CAST(strftime('%m', r.date) AS INTEGER)
ORDER BY month_num;
