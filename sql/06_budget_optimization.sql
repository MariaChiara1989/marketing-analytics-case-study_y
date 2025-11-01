-- ============================================================
-- Budget Optimization
-- Includes:
-- 4.1 Spend consistency / variability
-- 4.2 Efficiency curves (ROAS by spend decile)
-- 4.3 Budget reallocation with expected impact (30‑day ROAS model)
-- ============================================================
-- Spend consistency & variability by channel: Which channels have the most consistent spend?
--Which channels show the most variability?
----------------------------------------------------------------
-- ===============================================================
-- Goal:
--   Identify patterns in revenue by answering to:
--     1. Which channels have the most consistent spend?
--     2. Which channels show the most variability?
--
-- Metrics:
--   - avg_spend:       avg daily spend for each channel
--   - variance --> values obscill around mean
--   - cv:    coefficiente di variazione (CV) = stddev / avg * 100 --> normalized standard dev
--
-- Interpretazione business:
--   - Low CV => high stability,
--     budget "always on" and planned with continuity.
--   - High CV => budget variable
--
-- In this query:
--   - first row: more consistent.
--   - Last row: high variability.
--
--Tech note:
--   SQLite no STDDEV() built-in, so:
--       stddev(x) = sqrt( avg(x^2) - (avg(x))^2 )
-- ===============================================================
WITH
marketing_spend_clean AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
    FROM marketing_spend
),
dedup AS (
    SELECT * FROM marketing_spend_clean WHERE rn = 1
),
spend_unpivot AS (
    SELECT date, 'paid_search' AS channel, paid_search_spend AS spend FROM dedup
    UNION ALL SELECT date,'paid_social',paid_social_spend FROM dedup
    UNION ALL SELECT date,'display',display_spend FROM dedup
    UNION ALL SELECT date,'email',email_spend FROM dedup
    UNION ALL SELECT date,'affiliate',affiliate_spend FROM dedup
    UNION ALL SELECT date,'tv',tv_spend FROM dedup
),
stats AS (
    SELECT
      channel,
      --COUNT(*) AS days,
      AVG(spend) AS mean_spend,
      AVG(spend*spend) AS mean_spend_sq,
      MIN(spend) AS min_spend,
      MAX(spend) AS max_spend
    FROM spend_unpivot
    GROUP BY channel
)
SELECT
  channel,
  --days,
  mean_spend,
  CASE WHEN (mean_spend_sq - mean_spend*mean_spend) < 0 THEN 0
       ELSE sqrt(mean_spend_sq - mean_spend*mean_spend) END AS stddev_spend,
  CASE WHEN mean_spend > 0 THEN
       (CASE WHEN (mean_spend_sq - mean_spend*mean_spend) < 0 THEN 0
             ELSE sqrt(mean_spend_sq - mean_spend*mean_spend) END) / mean_spend END AS cv_spend,
  min_spend,
  max_spend,
  (max_spend - min_spend) AS range_spend
FROM stats
ORDER BY cv_spend ASC;
----------------------------------------------------------------
-- Efficiency curves — ROAS by spend decile (same‑day attribution)
----------------------------------------------------------------
WITH
marketing_spend_clean AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn FROM marketing_spend
),
revenue_clean AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn FROM revenue
),
ms AS (SELECT * FROM marketing_spend_clean WHERE rn=1),
rv AS (SELECT * FROM revenue_clean WHERE rn=1),
spend_unpivot AS (
  SELECT date,'paid_search' AS channel,paid_search_spend AS spend FROM ms
  UNION ALL SELECT date,'paid_social',paid_social_spend FROM ms
  UNION ALL SELECT date,'display',display_spend FROM ms
  UNION ALL SELECT date,'email',email_spend FROM ms
  UNION ALL SELECT date,'affiliate',affiliate_spend FROM ms
  UNION ALL SELECT date,'tv',tv_spend FROM ms
),
tot AS (
  SELECT date, SUM(spend) AS total_spend FROM spend_unpivot GROUP BY date
),
attr AS (
  SELECT s.date,s.channel,s.spend,
         CASE WHEN t.total_spend>0 THEN r.revenue*(s.spend*1.0/t.total_spend) ELSE 0 END AS attr_rev
  FROM spend_unpivot s
  JOIN tot t USING(date)
  JOIN rv r USING(date)
),
ranked AS (
  SELECT channel,date,spend,attr_rev,
         ROW_NUMBER() OVER (PARTITION BY channel ORDER BY spend) AS rn,
         COUNT(*) OVER (PARTITION BY channel) AS n
  FROM attr
),
deciles AS (
  SELECT channel,date,spend,attr_rev,
         CAST(((rn-1)*10.0)/n AS INT) + 1 AS decile
  FROM ranked
)
SELECT channel, decile,
       AVG(spend) AS avg_spend,
       AVG(attr_rev) AS avg_attr_rev,
       AVG(attr_rev)/AVG(spend) AS avg_roas
FROM deciles
GROUP BY channel, decile
ORDER BY channel, decile;

----------------------------------------------------------------
-- Budget reallocation + expected revenue impact (30‑day ROAS)
----------------------------------------------------------------
--This query performs a budget reallocation simulation using 30-day rolling ROAS. It:
--Calculates each channel’s total spend, attributed revenue, and 30-day ROAS
--Identifies the top 2 most efficient channels and the bottom 2 least efficient channels
--Reallocates 10% of budget away from the low-ROAS channels to the high-ROAS ones (even split)
--Estimates the expected incremental revenue impact assuming ROAS stays constant at the margin
WITH
-- 0) DEDUP: tieni la prima occorrenza per data in ms e rev
ms_dedup AS (
  SELECT *
  FROM (
    SELECT m.*,
           ROW_NUMBER() OVER (PARTITION BY m.date ORDER BY ROWID) AS rn
    FROM marketing_spend m
  )
  WHERE rn = 1
),
rev_dedup AS (
  SELECT *
  FROM (
    SELECT r.*,
           ROW_NUMBER() OVER (PARTITION BY r.date ORDER BY ROWID) AS rn
    FROM revenue r
  )
  WHERE rn = 1
),
-- 1) UNPIVOT: da colonne canali a righe (date, channel, spend)
ms_long AS (
  SELECT date, 'paid_search_spend'  AS channel, paid_search_spend  AS spend FROM ms_dedup
  UNION ALL
  SELECT date, 'paid_social_spend'  AS channel, paid_social_spend  AS spend FROM ms_dedup
  UNION ALL
  SELECT date, 'display_spend'      AS channel, display_spend      AS spend FROM ms_dedup
  UNION ALL
  SELECT date, 'email_spend'        AS channel, email_spend        AS spend FROM ms_dedup
  UNION ALL
  SELECT date, 'affiliate_spend'    AS channel, affiliate_spend    AS spend FROM ms_dedup
  UNION ALL
  SELECT date, 'tv_spend'           AS channel, tv_spend           AS spend FROM ms_dedup
),
-- 2) Spesa giornaliera per canale (serve in caso di duplicazioni residue o NaN→0)
channel_daily AS (
  SELECT date, channel, COALESCE(SUM(spend),0.0) AS spend
  FROM ms_long
  GROUP BY date, channel
),
-- 3) Totale spesa giornaliero
total_daily AS (
  SELECT date, SUM(spend) AS total_spend
  FROM channel_daily
  GROUP BY date
),
-- 4) Rolling 30d per canale e totale (incluso il giorno corrente)
channel_roll30 AS (
  SELECT
    date,
    channel,
    spend,
    SUM(spend) OVER (
      PARTITION BY channel
      ORDER BY date
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS roll30_spend_channel
  FROM channel_daily
),
total_roll30 AS (
  SELECT
    date,
    SUM(total_spend) OVER (
      ORDER BY date
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS roll30_spend_total
  FROM total_daily
),
-- 5) Share-of-spend e revenue attribuita giornaliera
share_and_attr AS (
  SELECT
    c.date,
    c.channel,
    c.spend,
    c.roll30_spend_channel,
    t.roll30_spend_total,
    CASE WHEN t.roll30_spend_total > 0
         THEN c.roll30_spend_channel * 1.0 / t.roll30_spend_total
         ELSE 0.0 END AS spend_share_30d,
    COALESCE(rv.revenue, 0.0) AS revenue,
    COALESCE(rv.revenue, 0.0) *
    CASE WHEN t.roll30_spend_total > 0
         THEN c.roll30_spend_channel * 1.0 / t.roll30_spend_total
         ELSE 0.0 END AS attributed_revenue_30d
  FROM channel_roll30 c
  JOIN total_roll30   t  USING (date)
  LEFT JOIN rev_dedup rv USING (date)
),
-- 6) Metriche aggregate per canale
channel_summary AS (
  SELECT
    channel,
    SUM(spend)                  AS total_spend,
    SUM(attributed_revenue_30d) AS attr_rev_30d,
    CASE WHEN SUM(spend) > 0
         THEN SUM(attributed_revenue_30d) * 1.0 / SUM(spend)
         ELSE 0.0 END          AS roas_30d
  FROM share_and_attr
  GROUP BY channel
),
-- 7) Ranking e scelta recipient/donor
ranked AS (
  SELECT
    channel, total_spend, attr_rev_30d, roas_30d,
    RANK() OVER (ORDER BY roas_30d DESC) AS rk_desc,
    RANK() OVER (ORDER BY roas_30d ASC)  AS rk_asc
  FROM channel_summary
),
recipients AS (
  SELECT channel, total_spend, roas_30d
  FROM ranked
  WHERE rk_desc <= 2          -- top 2 per efficienza
),
donors AS (
  SELECT channel, total_spend, roas_30d
  FROM ranked
  WHERE rk_asc  <= 2          -- bottom 2 per efficienza
),
-- 8) Riallocazione: 10% dai donor, ripartito equamente sui recipient
donor_out AS (
  SELECT channel, 0.10 * total_spend AS delta_out
  FROM donors
),
pool AS (
  SELECT SUM(delta_out) AS total_pool FROM donor_out
),
recipient_in AS (
  SELECT r.channel,
         (SELECT total_pool FROM pool) * 1.0 / (SELECT COUNT(*) FROM recipients) AS delta_in
  FROM recipients r
),
-- 9) Nuove allocazioni e impatto atteso (ROAS marginale ≈ costante)
new_alloc AS (
  SELECT
    s.channel,
    s.total_spend,
    s.roas_30d,
    (COALESCE(ri.delta_in,0) - COALESCE(do.delta_out,0))               AS delta_spend,
    s.total_spend + (COALESCE(ri.delta_in,0) - COALESCE(do.delta_out,0)) AS new_total_spend
  FROM channel_summary s
  LEFT JOIN recipient_in ri ON s.channel = ri.channel
  LEFT JOIN donor_out    do ON s.channel = do.channel
),
impact AS (
  SELECT
    channel,
    total_spend,
    roas_30d,
    new_total_spend,
    (total_spend     * roas_30d) AS expected_revenue_before,
    (new_total_spend * roas_30d) AS expected_revenue_after,
    ((new_total_spend - total_spend) * roas_30d) AS incremental_revenue
  FROM new_alloc
)
-- 10) RISULTATO FINALE
SELECT
  channel,
  ROUND(total_spend, 2)            AS total_spend,
  ROUND(roas_30d, 6)               AS roas_30d,
  ROUND(new_total_spend, 2)        AS new_total_spend,
  ROUND(expected_revenue_before,2) AS exp_rev_before,
  ROUND(expected_revenue_after,2)  AS exp_rev_after,
  ROUND(incremental_revenue,2)     AS incr_rev
FROM impact
ORDER BY roas_30d DESC;