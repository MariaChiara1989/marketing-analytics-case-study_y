-- PART 2: CHANNEL PERFORMANCE ANALYSIS
-- ==================================================================
-- 2.1 Total spend and total revenue by channel for the entire period (same-day attribution by channel)
-- ===================================================================
--This query cleans the data by removing duplicate dates, converts the spend table from wide to long format (one row per channel per day), 
--and then allocates each day’s revenue to channels according to their share of daily marketing spend.
--Meaning that if a channel represents 40% of total marketing spend on a given day, it receives 40% of that day’s revenue. 
--Finally, the query aggregates the spend and attributed revenue across the full period 
--to calculate each channel’s total spend and allocated revenue.
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
    SELECT date, 'paid_search' AS channel, paid_search_spend AS spend FROM marketing_spend_clean
    UNION ALL SELECT date,'paid_social',paid_social_spend FROM marketing_spend_clean
    UNION ALL SELECT date,'display',display_spend FROM marketing_spend_clean
    UNION ALL SELECT date,'email',email_spend FROM marketing_spend_clean
    UNION ALL SELECT date,'affiliate',affiliate_spend FROM marketing_spend_clean
    UNION ALL SELECT date,'tv',tv_spend FROM marketing_spend_clean
),
daily_total_spend AS (
    SELECT date, SUM(spend) AS total_spend
    FROM spend_unpivot GROUP BY date
),
same_day_attribution AS (
    SELECT
        s.date, s.channel, s.spend, r.revenue,
        CASE WHEN d.total_spend>0 THEN r.revenue*(s.spend*1.0/d.total_spend) ELSE 0 END AS attributed_revenue
    FROM spend_unpivot s
    JOIN daily_total_spend d USING (date)
    JOIN revenue_clean r USING (date)
)
SELECT
    channel,
    SUM(spend) AS total_spend,
    SUM(attributed_revenue) AS total_revenue_attributed
FROM same_day_attribution
GROUP BY channel
ORDER BY total_spend DESC;

-- ===================================================================
-- 2.2 Attribution windows (PARAMETRIC VERSION) - set :ATTR_WINDOW_DAYS to 7, 14 or 30
-- ===================================================================
--Window "K" (7/14/30): revenue(t) divided based on the rolling spend value of the last K days (day "t" included).
--e.g.: in in the last tot K days, a channel has 20% of spend cumulated, it will get ~20% of the revenue of the day "t".
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
    SELECT date, 'paid_search' AS channel, paid_search_spend AS spend FROM marketing_spend_clean
    UNION ALL SELECT date,'paid_social',paid_social_spend FROM marketing_spend_clean
    UNION ALL SELECT date,'display',display_spend FROM marketing_spend_clean
    UNION ALL SELECT date,'email',email_spend FROM marketing_spend_clean
    UNION ALL SELECT date,'affiliate',affiliate_spend FROM marketing_spend_clean
    UNION ALL SELECT date,'tv',tv_spend FROM marketing_spend_clean
),
channel_rolling AS (
    SELECT
        date, channel, spend,
        SUM(spend) OVER (
            PARTITION BY channel
            ORDER BY date
            ROWS BETWEEN :ATTR_WINDOW_DAYS-1 PRECEDING AND CURRENT ROW
        ) AS rolling_spend_k
    FROM spend_unpivot
),
daily_rolling_totals AS (
    SELECT date, SUM(rolling_spend_k) AS total_rolling_spend_k
    FROM channel_rolling GROUP BY date
),
attribution_k AS (
    SELECT
        c.date, c.channel, c.spend, c.rolling_spend_k, drt.total_rolling_spend_k, r.revenue,
        CASE WHEN drt.total_rolling_spend_k>0 THEN r.revenue*(c.rolling_spend_k*1.0/drt.total_rolling_spend_k) ELSE 0 END AS attributed_revenue_k
    FROM channel_rolling c
    JOIN daily_rolling_totals drt USING(date)
    JOIN revenue_clean r USING(date)
)
SELECT
    channel,
    SUM(spend) AS total_spend,
    SUM(attributed_revenue_k) AS total_revenue_attributed_k,
    CASE WHEN SUM(spend)>0 THEN SUM(attributed_revenue_k)/SUM(spend) END AS roas_k,
    :ATTR_WINDOW_DAYS AS attribution_window_days
FROM attribution_k
GROUP BY channel
ORDER BY roas_k DESC, channel;

-- ===================================================================
-- 2.2 Attribution windows - ONE QUERY FOR 7/14/30
-- ===================================================================
--This is rolling-window spend-share attribution.
--Each day’s revenue is allocated to channels in proportion to their cumulative spend over the last K days (not just today). 
--This is because spend can influence conversions with lag/carryover; 
--meaning that you proxy those effects—channels that have been investing 
--recently get credit even if the conversion happens a few days later.
--Pros: Simple, window-controlled lag proxy; more realistic than pure same-day.
--Cons: Still non-causal
--General rule: 7–14 days for lower-consideration products (shorter lag), 30+ days for higher-consideration (longer lag).
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
    SELECT date, 'paid_search' AS channel, paid_search_spend AS spend FROM marketing_spend_clean
    UNION ALL SELECT date,'paid_social',paid_social_spend FROM marketing_spend_clean
    UNION ALL SELECT date,'display',display_spend FROM marketing_spend_clean
    UNION ALL SELECT date,'email',email_spend FROM marketing_spend_clean
    UNION ALL SELECT date,'affiliate',affiliate_spend FROM marketing_spend_clean
    UNION ALL SELECT date,'tv',tv_spend FROM marketing_spend_clean
),
roll_7 AS (
    SELECT date, channel, spend,
           SUM(spend) OVER (PARTITION BY channel ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_spend_k
    FROM spend_unpivot
),
roll_14 AS (
    SELECT date, channel, spend,
           SUM(spend) OVER (PARTITION BY channel ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS rolling_spend_k
    FROM spend_unpivot
),
roll_30 AS (
    SELECT date, channel, spend,
           SUM(spend) OVER (PARTITION BY channel ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_spend_k
    FROM spend_unpivot
),
union_roll AS (
    SELECT date, channel, spend, rolling_spend_k, 7 AS k FROM roll_7
    UNION ALL SELECT date, channel, spend, rolling_spend_k, 14 FROM roll_14
    UNION ALL SELECT date, channel, spend, rolling_spend_k, 30 FROM roll_30
),
daily_totals AS (
    SELECT date, k, SUM(rolling_spend_k) AS total_rolling_spend_k
    FROM union_roll GROUP BY date, k
),
attrib AS (
    SELECT
        u.date, u.channel, u.spend, u.k,
        u.rolling_spend_k, d.total_rolling_spend_k, r.revenue,
        CASE WHEN d.total_rolling_spend_k>0 THEN r.revenue*(u.rolling_spend_k*1.0/d.total_rolling_spend_k) ELSE 0 END AS attributed_revenue_k
    FROM union_roll u
    JOIN daily_totals d USING(date,k)
    JOIN revenue_clean r USING(date)
)
SELECT
    channel, k AS attribution_window_days,
    SUM(spend) AS total_spend,
    SUM(attributed_revenue_k) AS total_revenue_attributed_k,
    CASE WHEN SUM(spend)>0 THEN SUM(attributed_revenue_k)/SUM(spend) END AS roas_k
FROM attrib
GROUP BY channel, k
ORDER BY k, roas_k DESC, channel;

-- ===================================================================
-- 2.3 Top and Bottom Channels (30d window) 
--> more balanced between upper and low funnel
-- ===================================================================
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
        SUM(spend) OVER (PARTITION BY channel ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_spend_k
    FROM spend_unpivot
),
daily_total AS (
    SELECT date, SUM(rolling_spend_k) AS total_rolling_spend_k
    FROM roll_30 GROUP BY date
),
attrib_30 AS (
    SELECT
        r30.channel, r30.spend,
        CASE WHEN d.total_rolling_spend_k>0 THEN rc.revenue*(r30.rolling_spend_k*1.0/d.total_rolling_spend_k) ELSE 0 END AS attributed_revenue_30
    FROM roll_30 r30
    JOIN daily_total d USING(date)
    JOIN revenue_clean rc USING(date)
),
agg AS (
    SELECT channel,
           SUM(spend) AS total_spend,
           SUM(attributed_revenue_30) AS total_revenue_attributed_30,
           CASE WHEN SUM(spend)>0 THEN SUM(attributed_revenue_30)/SUM(spend) END AS roas_30
    FROM attrib_30 GROUP BY channel
),
ranking AS (
    SELECT channel, total_spend, total_revenue_attributed_30, roas_30,
           RANK() OVER (ORDER BY total_revenue_attributed_30 DESC) AS rev_rank_desc,
           RANK() OVER (ORDER BY roas_30 DESC) AS roas_rank_desc
    FROM agg
)
SELECT 'top_revenue' AS metric, channel, total_revenue_attributed_30 AS value FROM ranking WHERE rev_rank_desc=1
UNION ALL
SELECT 'bottom_revenue', channel, total_revenue_attributed_30 FROM ranking WHERE rev_rank_desc=(SELECT MAX(rev_rank_desc) FROM ranking)
UNION ALL
SELECT 'top_roas', channel, roas_30 FROM ranking WHERE roas_rank_desc=1
UNION ALL
SELECT 'bottom_roas', channel, roas_30 FROM ranking WHERE roas_rank_desc=(SELECT MAX(roas_rank_desc) FROM ranking)
ORDER BY metric;


-- ===================================================================
-- 2.4 Month / Quarter / Weekend / Promo breakdown (30d window)
-- ===================================================================
-- CHANNEL PERFORMANCE BY MONTH 
-- ===================================================================
--How does each channel’s ROAS evolve month-by-month and quarter-by-quarter when we attribute 
--revenue based on the prior 30-day rolling spend window?
--A 30-day window helps capture lagged and carry-over effects, ensuring that channels that influence the customer journey 
--earlier still receive proportional credit when purchases happen later (even if, in this case we sow that attr windows are pretty "the same")
WITH
marketing_spend_clean AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
        FROM marketing_spend
    )
    WHERE rn = 1
),
revenue_clean AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
        FROM revenue
    )
    WHERE rn = 1
),
spend_unpivot AS (
    SELECT date, 'paid_search' AS channel, paid_search_spend AS spend FROM marketing_spend_clean
    UNION ALL SELECT date, 'paid_social', paid_social_spend FROM marketing_spend_clean
    UNION ALL SELECT date, 'display',     display_spend     FROM marketing_spend_clean
    UNION ALL SELECT date, 'email',       email_spend       FROM marketing_spend_clean
    UNION ALL SELECT date, 'affiliate',   affiliate_spend   FROM marketing_spend_clean
    UNION ALL SELECT date, 'tv',          tv_spend          FROM marketing_spend_clean
),
roll_30 AS (
    SELECT
        date, channel, spend,
        SUM(spend) OVER (PARTITION BY channel ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_spend_k
    FROM spend_unpivot
),
daily_total AS (
    SELECT date, SUM(rolling_spend_k) AS total_rolling_spend_k
    FROM roll_30
    GROUP BY date
),
attrib_30 AS (
    SELECT
        r30.date,
        r30.channel,
        r30.spend,
        CASE WHEN d.total_rolling_spend_k > 0
             THEN rc.revenue * (r30.rolling_spend_k * 1.0 / d.total_rolling_spend_k)
             ELSE 0 END AS attributed_revenue_30
    FROM roll_30 r30
    JOIN daily_total d USING (date)
    JOIN revenue_clean rc USING (date)
),
-- Mese e trimestre 
labeled AS (
    SELECT
        date,
        channel,
        spend,
        attributed_revenue_30,
        strftime('%Y-%m', date)                                     AS month,
        'Q' || ((cast(strftime('%m', date) as integer) + 2) / 3)     AS quarter,
        strftime('%Y', date)                                         AS year
    FROM attrib_30
)
SELECT
    strftime('%Y-%m', date) AS period_month,
    year || '-' || quarter AS period_quarter,
    channel,
    SUM(spend)                        AS total_spend,
    SUM(attributed_revenue_30)        AS total_revenue_attr_30,
    CASE WHEN SUM(spend) > 0
         THEN SUM(attributed_revenue_30) / SUM(spend)
         ELSE NULL END                AS roas_30
FROM labeled
GROUP BY period_month, channel
ORDER BY period_month, roas_30 DESC, channel;
-- ===================================================================
-- CHANNEL PERFORMANCE BY WEEKEND VS WEEKDAY
-- ===================================================================
--How does each channel’s ROAS is affected by weekend/no weekend
WITH
marketing_spend_clean AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
        FROM marketing_spend
    )
    WHERE rn = 1
),
revenue_clean AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
        FROM revenue
    )
    WHERE rn = 1
),
external_factors_clean AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
        FROM external_factors
    )
    WHERE rn = 1
),
spend_unpivot AS (
    SELECT date, 'paid_search' AS channel, paid_search_spend AS spend FROM marketing_spend_clean
    UNION ALL SELECT date, 'paid_social', paid_social_spend FROM marketing_spend_clean
    UNION ALL SELECT date, 'display',     display_spend     FROM marketing_spend_clean
    UNION ALL SELECT date, 'email',       email_spend       FROM marketing_spend_clean
    UNION ALL SELECT date, 'affiliate',   affiliate_spend   FROM marketing_spend_clean
    UNION ALL SELECT date, 'tv',          tv_spend          FROM marketing_spend_clean
),
roll_30 AS (
    SELECT
        date, channel, spend,
        SUM(spend) OVER (PARTITION BY channel ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_spend_k
    FROM spend_unpivot
),
daily_total AS (
    SELECT date, SUM(rolling_spend_k) AS total_rolling_spend_k
    FROM roll_30
    GROUP BY date
),
attrib_30 AS (
    SELECT
        r30.date,
        r30.channel,
        r30.spend,
        CASE WHEN d.total_rolling_spend_k > 0
             THEN rc.revenue * (r30.rolling_spend_k * 1.0 / d.total_rolling_spend_k)
             ELSE 0 END AS attributed_revenue_30
    FROM roll_30 r30
    JOIN daily_total d USING (date)
    JOIN revenue_clean rc USING (date)
),
labeled AS (
    SELECT
        a.date,
        a.channel,
        a.spend,
        a.attributed_revenue_30,
        e.is_weekend
    FROM attrib_30 a
    JOIN external_factors_clean e USING (date)
)
SELECT
    CASE WHEN is_weekend = 1 THEN 'weekend' ELSE 'weekday' END AS day_type,
    channel,
    SUM(spend)                 AS total_spend,
    SUM(attributed_revenue_30) AS total_revenue_attr_30,
    CASE WHEN SUM(spend) > 0
         THEN SUM(attributed_revenue_30) / SUM(spend)
         ELSE NULL END         AS roas_30
FROM labeled
GROUP BY day_type, channel
ORDER BY day_type, roas_30 DESC, channel;
-- ===================================================================
-- CHANNEL PERFORMANCE BY PROMOTIONS VS NO PROMOTIONS
-- ===================================================================
--How does each channel’s ROAS is affected by promo/no promo
WITH
marketing_spend_clean AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
        FROM marketing_spend
    )
    WHERE rn = 1
),
revenue_clean AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
        FROM revenue
    )
    WHERE rn = 1
),
external_factors_clean AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY date ORDER BY ROWID) AS rn
        FROM external_factors
    )
    WHERE rn = 1
),
spend_unpivot AS (
    SELECT date, 'paid_search' AS channel, paid_search_spend AS spend FROM marketing_spend_clean
    UNION ALL SELECT date, 'paid_social', paid_social_spend FROM marketing_spend_clean
    UNION ALL SELECT date, 'display',     display_spend     FROM marketing_spend_clean
    UNION ALL SELECT date, 'email',       email_spend       FROM marketing_spend_clean
    UNION ALL SELECT date, 'affiliate',   affiliate_spend   FROM marketing_spend_clean
    UNION ALL SELECT date, 'tv',          tv_spend          FROM marketing_spend_clean
),
roll_30 AS (
    SELECT
        date, channel, spend,
        SUM(spend) OVER (PARTITION BY channel ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_spend_k
    FROM spend_unpivot
),
daily_total AS (
    SELECT date, SUM(rolling_spend_k) AS total_rolling_spend_k
    FROM roll_30
    GROUP BY date
),
attrib_30 AS (
    SELECT
        r30.date,
        r30.channel,
        r30.spend,
        CASE WHEN d.total_rolling_spend_k > 0
             THEN rc.revenue * (r30.rolling_spend_k * 1.0 / d.total_rolling_spend_k)
             ELSE 0 END AS attributed_revenue_30
    FROM roll_30 r30
    JOIN daily_total d USING (date)
    JOIN revenue_clean rc USING (date)
),
labeled AS (
    SELECT
        a.date,
        a.channel,
        a.spend,
        a.attributed_revenue_30,
        e.promotion_active
    FROM attrib_30 a
    JOIN external_factors_clean e USING (date)
)
SELECT
    CASE WHEN promotion_active = 1 THEN 'promo' ELSE 'non_promo' END AS promo_flag,
    channel,
    SUM(spend)                 AS total_spend,
    SUM(attributed_revenue_30) AS total_revenue_attr_30,
    CASE WHEN SUM(spend) > 0
         THEN SUM(attributed_revenue_30) / SUM(spend)
         ELSE NULL END         AS roas_30
FROM labeled
GROUP BY promo_flag, channel
ORDER BY promo_flag, roas_30 DESC, channel;




