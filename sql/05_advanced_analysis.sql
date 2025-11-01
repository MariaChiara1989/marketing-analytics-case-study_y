-- ============================================================
-- ADVANCED ANALYSIS
-- ============================================================
-- Includes:
--  • 3.1 correlation between each channel's spend and revenue
--  • 3.2 External factor impact (holiday, promo, weekend) + seasonality
--  • 3.3 Incrementality proxy (bottom 10% baseline), marginal returns (quartiles),
--        cohort efficiency (rolling 30-day ROAS by quarter)
-- ============================================================
-- Correlation between each channel's spend and total revenue
-- Goal: measure Pearson correlation r(spend_channel_d, revenue_d) by channel.
-- ============================================================
WITH
marketing_spend_clean AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
        FROM marketing_spend
    ) WHERE rn = 1
),
revenue_clean AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
        FROM revenue
    ) WHERE rn = 1
),
spend_unpivot AS (
    SELECT date,'paid_search' AS channel,paid_search_spend AS spend FROM marketing_spend_clean
    UNION ALL SELECT date,'paid_social',paid_social_spend FROM marketing_spend_clean
    UNION ALL SELECT date,'display',display_spend FROM marketing_spend_clean
    UNION ALL SELECT date,'email',email_spend FROM marketing_spend_clean
    UNION ALL SELECT date,'affiliate',affiliate_spend FROM marketing_spend_clean
    UNION ALL SELECT date,'tv',tv_spend FROM marketing_spend_clean
),
paired AS (
    SELECT s.channel, s.spend AS x, r.revenue AS y
    FROM spend_unpivot s
    JOIN revenue_clean r USING (date)
),
stats AS (
    SELECT
        channel, COUNT(*) AS n,
        SUM(x) AS sum_x, SUM(y) AS sum_y,
        SUM(x*y) AS sum_xy, SUM(x*x) AS sum_x2, SUM(y*y) AS sum_y2
    FROM paired
    GROUP BY channel
)
SELECT
    channel,
    CASE
        WHEN (n*sum_x2 - sum_x*sum_x)=0 OR (n*sum_y2 - sum_y*sum_y)=0 THEN NULL
        ELSE (n*sum_xy - sum_x*sum_y)*1.0 /
             (sqrt(n*sum_x2 - sum_x*sum_x)*sqrt(n*sum_y2 - sum_y*sum_y))
    END AS pearson_corr_spend_vs_revenue
FROM stats
ORDER BY channel;
-- ============================================================
-- Revenue lift: holiday / promotion / weekend
-- Goal: compare average revenue on treatment (dycotomic = 1) vs control (dycotomic = 0) days for each factor.
-- ============================================================
WITH
revenue_clean AS (
  SELECT *
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
    FROM revenue
  ) WHERE rn=1
),
external_factors_clean AS (
  SELECT *
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
    FROM external_factors
  ) WHERE rn=1
),
jr AS (
  SELECT e.date, r.revenue, e.is_holiday, e.promotion_active, e.is_weekend
  FROM external_factors_clean e
  JOIN revenue_clean r USING (date)
),
lifts AS (
  SELECT 'holiday' AS factor,
         AVG(CASE WHEN is_holiday=1 THEN revenue END) AS avg_treated,
         AVG(CASE WHEN is_holiday=0 THEN revenue END) AS avg_control
  FROM jr
  UNION ALL
  SELECT 'promotion',
         AVG(CASE WHEN promotion_active=1 THEN revenue END),
         AVG(CASE WHEN promotion_active=0 THEN revenue END)
  FROM jr
  UNION ALL
  SELECT 'weekend',
         AVG(CASE WHEN is_weekend=1 THEN revenue END),
         AVG(CASE WHEN is_weekend=0 THEN revenue END)
  FROM jr
)
SELECT
  factor,
  avg_control,
  avg_treated,
  (avg_treated - avg_control) AS abs_lift,
  CASE WHEN avg_control>0 THEN (avg_treated-avg_control)*100/avg_control END AS pct_lift
FROM lifts
ORDER BY factor;

-- ============================================================
-- SEASONALITY
--Query 1 measures how strongly revenue moves with seasonality; Query 2 shows how much revenue changes across seasonality levels. 
--Together, they confirm (and size) the seasonality effect for forecasting and budget planning.
-- ============================================================
-- Seasonality correlation
-- Goal: check alignment of revenue with seasonality_index (Pearson r).
-- Output: a single number that tells you the strength and direction of the linear relationship.
-- ============================================================
WITH
revenue_clean AS (
  SELECT *
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
    FROM revenue
  ) WHERE rn=1
),
external_factors_clean AS (
  SELECT *
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
    FROM external_factors
  ) WHERE rn=1
),
jr AS (
  SELECT e.date, r.revenue, e.seasonality_index
  FROM external_factors_clean e
  JOIN revenue_clean r USING (date)
),
stats AS (
  SELECT
    COUNT(*) AS n,
    SUM(seasonality_index) AS sum_x,
    SUM(revenue) AS sum_y,
    SUM(seasonality_index*revenue) AS sum_xy,
    SUM(seasonality_index*seasonality_index) AS sum_x2,
    SUM(revenue*revenue) AS sum_y2
  FROM jr
)
SELECT
  CASE WHEN (n*sum_x2 - sum_x*sum_x)=0 OR (n*sum_y2 - sum_y*sum_y)=0 THEN NULL
       ELSE (n*sum_xy - sum_x*sum_y)*1.0 /
            (sqrt(n*sum_x2 - sum_x*sum_x) * sqrt(n*sum_y2 - sum_y*sum_y))
  END AS pearson_corr_revenue_vs_seasonality_index
FROM stats;

-- Seasonality effect on revenue
--For each bucket, reports avg revenue, count of days, min/max.
--Output: a descriptive, non-parametric view of how revenue changes across seasonality regimes.
--
--  Bucket definition (seasonality_index):
--    'low_seasonality'  : seasonality_index < 0.9
--    'mid_seasonality'  : 0.9 <= seasonality_index < 1.1
--    'high_seasonality' : seasonality_index >= 1.1
--
--  Final Output:
--    - bucket: seasonality
--    - avg_revenue: avg revenue for that slice
--    - num_days: observed days number 
--    - min_revenue / max_revenue: max and min in that bucket
--
--  Lettura:
--    Se avg_revenue increases from low -> mid -> high, seasonality is a positive driver for revenues.
WITH
rev AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
        FROM revenue
    )
    WHERE rn = 1
),
-- 2. Dedup external_factors (ef)
ef AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
        FROM external_factors
    )
    WHERE rn = 1
),
-- 3. Join revenue + external_factors
rev_ef AS (
    SELECT
        rev.date,
        rev.revenue,
        ef.seasonality_index
    FROM rev
    JOIN ef USING(date)
),
-- 4. Bucket di stagionalità basato su seasonality_index
rev_seasonality_buckets AS (
    SELECT
        CASE
            WHEN seasonality_index < 0.9 THEN 'low_seasonality'
            WHEN seasonality_index < 1.1 THEN 'mid_seasonality'
            ELSE 'high_seasonality'
        END AS seasonality_bucket,
        revenue
    FROM rev_ef
)
-- 5. Statistics per bucket
SELECT
    seasonality_bucket                           AS bucket,
    AVG(revenue)                                 AS avg_revenue,
    COUNT(*)                                     AS num_days,
    MIN(revenue)                                 AS min_revenue,
    MAX(revenue)                                 AS max_revenue
FROM rev_seasonality_buckets
GROUP BY seasonality_bucket
ORDER BY
    CASE bucket
        WHEN 'low_seasonality'  THEN 1
        WHEN 'mid_seasonality'  THEN 2
        WHEN 'high_seasonality' THEN 3
        ELSE 4
    END;

-- ============================================================
-- Incrementality proxy (When a channel spends more 
-- (vs when it spends little or nothing), how much more revenue do we see?)
-- using bottom 10% spend as baseline 
-- Reason: dataset has no true zero-spend days per channel.
-- Steps: proportional same-day attribution → rank spend by channel → bottom10 vs rest.
-- ============================================================
WITH
marketing_spend_clean AS (
  SELECT *
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
    FROM marketing_spend
  ) WHERE rn=1
),
revenue_clean AS (
  SELECT *
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
    FROM revenue
  ) WHERE rn=1
),
spend_unpivot AS (
  SELECT date,'paid_search' AS channel,paid_search_spend AS spend FROM marketing_spend_clean
  UNION ALL SELECT date,'paid_social',paid_social_spend FROM marketing_spend_clean
  UNION ALL SELECT date,'display',display_spend FROM marketing_spend_clean
  UNION ALL SELECT date,'email',email_spend FROM marketing_spend_clean
  UNION ALL SELECT date,'affiliate',affiliate_spend FROM marketing_spend_clean
  UNION ALL SELECT date,'tv',tv_spend FROM marketing_spend_clean
),
daily_tot AS (
  SELECT date,SUM(spend) AS total_spend
  FROM spend_unpivot
  GROUP BY date
),
same_day_attr AS (
  SELECT s.date,s.channel,s.spend,
         CASE WHEN d.total_spend>0 THEN r.revenue*(s.spend*1.0/d.total_spend) ELSE 0 END AS attr_revenue
  FROM spend_unpivot s
  JOIN daily_tot d USING(date)
  JOIN revenue_clean r USING(date)
),
ranked AS (
  SELECT channel, spend, attr_revenue,
         ROW_NUMBER() OVER (PARTITION BY channel ORDER BY spend) AS rn,
         COUNT(*) OVER (PARTITION BY channel) AS n
  FROM same_day_attr
),
flagged AS (
  SELECT channel, spend, attr_revenue,
         CASE WHEN rn <= n*0.10 THEN 1 ELSE 0 END AS is_bottom10
  FROM ranked
)
SELECT
  channel,
  AVG(CASE WHEN is_bottom10=1 THEN attr_revenue END) AS baseline_attr_rev,
  AVG(CASE WHEN is_bottom10=0 THEN attr_revenue END) AS higher_spend_attr_rev,
  AVG(CASE WHEN is_bottom10=0 THEN attr_revenue END) -
  AVG(CASE WHEN is_bottom10=1 THEN attr_revenue END) AS abs_lift,
  CASE WHEN AVG(CASE WHEN is_bottom10=1 THEN attr_revenue END)>0 THEN
       (AVG(CASE WHEN is_bottom10=0 THEN attr_revenue END) -
        AVG(CASE WHEN is_bottom10=1 THEN attr_revenue END))*100 /
        AVG(CASE WHEN is_bottom10=1 THEN attr_revenue END)
  END AS pct_lift
FROM flagged
GROUP BY channel
ORDER BY channel;

-- ============================================================
-- Marginal returns by spend quartile (per channel)
-- As spend increases, do returns improve or degrade? What does marginal ROAS look like?
-- If I spend more today, do I get more today?
-- Does efficiency hold as we scale spend?
-- Goal: detect diminishing returns via avg ROAS across spend quartiles.
-- ============================================================
WITH
marketing_spend_clean AS (
  SELECT *
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
    FROM marketing_spend
  ) WHERE rn=1
),
revenue_clean AS (
  SELECT *
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
    FROM revenue
  ) WHERE rn=1
),
spend_unpivot AS (
  SELECT date,'paid_search' AS channel,paid_search_spend AS spend FROM marketing_spend_clean
  UNION ALL SELECT date,'paid_social',paid_social_spend FROM marketing_spend_clean
  UNION ALL SELECT date,'display',display_spend FROM marketing_spend_clean
  UNION ALL SELECT date,'email',email_spend FROM marketing_spend_clean
  UNION ALL SELECT date,'affiliate',affiliate_spend FROM marketing_spend_clean
  UNION ALL SELECT date,'tv',tv_spend FROM marketing_spend_clean
),
daily_tot AS (
  SELECT date,SUM(spend) AS total_spend
  FROM spend_unpivot
  GROUP BY date
),
same_day_attr AS (
  SELECT s.date,s.channel,s.spend,
         CASE WHEN d.total_spend>0 THEN r.revenue*(s.spend*1.0/d.total_spend) ELSE 0 END AS attr_revenue
  FROM spend_unpivot s
  JOIN daily_tot d USING(date)
  JOIN revenue_clean r USING(date)
),
ranked AS (
  SELECT channel, date, spend, attr_revenue,
         ROW_NUMBER() OVER (PARTITION BY channel ORDER BY spend) AS rn,
         COUNT(*) OVER (PARTITION BY channel) AS n
  FROM same_day_attr
),
with_quartile AS (
  SELECT channel, date, spend, attr_revenue, rn, n,
         CAST(((rn-1)*4.0)/n AS INT) + 1 AS quartile  -- values 1..4
  FROM ranked
)
SELECT
  channel,
  quartile,
  AVG(spend) AS avg_spend,
  AVG(attr_revenue) AS avg_attr_revenue,
  CASE WHEN AVG(spend)>0 THEN AVG(attr_revenue)/AVG(spend) END AS avg_roas
FROM with_quartile
GROUP BY channel, quartile
ORDER BY channel, quartile;

-- ============================================================
-- Cohort efficiency: rolling 30-day ROAS by quarter (per channel)
-- Track ROAS / CPA / incremental return for different time cohorts to see whether 
-- the efficiency of marketing investment improves, deteriorates, or stays stable across periods.
-- A cohort here = a period of spend (monthly / quarterly cohorts based on spend date).
-- is the system getting less efficient year over year?
-- Goal: track efficiency trend over time at quarterly grain.
-- ============================================================
WITH
marketing_spend_clean AS (
  SELECT *
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
    FROM marketing_spend
  ) WHERE rn=1
),
revenue_clean AS (
  SELECT *
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
    FROM revenue
  ) WHERE rn=1
),
spend_unpivot AS (
  SELECT date,'paid_search' AS channel,paid_search_spend AS spend FROM marketing_spend_clean
  UNION ALL SELECT date,'paid_social',paid_social_spend FROM marketing_spend_clean
  UNION ALL SELECT date,'display',display_spend FROM marketing_spend_clean
  UNION ALL SELECT date,'email',email_spend FROM marketing_spend_clean
  UNION ALL SELECT date,'affiliate',affiliate_spend FROM marketing_spend_clean
  UNION ALL SELECT date,'tv',tv_spend FROM marketing_spend_clean
),
roll_30 AS (
  SELECT
    date, channel, spend,
    SUM(spend) OVER (PARTITION BY channel ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_spend_30
  FROM spend_unpivot
),
daily_roll_tot AS (
  SELECT date, SUM(rolling_spend_30) AS tot_roll_spend_30
  FROM roll_30
  GROUP BY date
),
attr_30 AS (
  SELECT
    r30.date, r30.channel, r30.spend,
    CASE WHEN d.tot_roll_spend_30>0 THEN rc.revenue*(r30.rolling_spend_30*1.0/d.tot_roll_spend_30) ELSE 0 END AS attr_rev_30
  FROM roll_30 r30
  JOIN daily_roll_tot d USING(date)
  JOIN revenue_clean rc USING(date)
),
by_quarter AS (
  SELECT
    channel,
    strftime('%Y', date) AS y,
    'Q' || ((CAST(strftime('%m', date) AS INT)+2)/3) AS q,
    SUM(spend) AS total_spend,
    SUM(attr_rev_30) AS total_attr_rev_30
  FROM attr_30
  GROUP BY channel, y, q
)
SELECT
  channel,
  (y || '-' || q) AS period_quarter,
  total_spend,
  total_attr_rev_30,
  CASE WHEN total_spend>0 THEN total_attr_rev_30*1.0/total_spend END AS roas_30
FROM by_quarter
ORDER BY period_quarter, channel;
