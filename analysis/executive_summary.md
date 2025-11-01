# Executive Summary — Marketing Analytics Case Study

**Period analyzed:** 2023‑01‑01 → 2024‑12‑30 (728 days)  
**Files:** `marketing_spend` (marketing_spend.csv), `revenue` (revenue.csv), `external_factors` (external_factors.csv)

## 1) Key findings

- **Paid Search** is the best both in terms of **revenue** (≈ €11.408.155) and for ROAS (ROAS 30d ≈ 4.741)**. Also: Paid_social is the most stable while email is the most variable.

- ROAS values and channel rankings remained extremely stable across all windows, indicating low sensitivity to conversion lag and a consistent relationship between spend and revenue across channels. This suggests that, within this dataset, conclusions around channel efficiency are reliable and not driven by attribution method choice. This stability would suggest a short purchase cycle (lag ≈ negligible). Also, this consistency signals a synthetic or very smooth dataset.

-The correlation between daily spend and revenue is positive and relatively strong across all channels, ranging ~0.73–0.77. Paid social and email show the highest correlation while TV the lowest correlation, though still strong. This indicates that spend levels move broadly in line with revenue trends across the media mix.

- **External factors**: Holiday **+18.2%**, Promo **+15.7%**, Weekend **+9.6%**. **Seasonality index** è highly correlated to revenue (**r ≈ 0.90**). 
In details: The highest impact on revenue lift is the holiday one, followed by promotions. Also, weekend, the worst, shows an impact of +10% on revenue. About seasonality, revenue scales strongly with the seasonality index meaning that higher seasonal demand consistently delivers higher revenue levels, with a roughly 50%+ uplift from low to high season periods. 
Seasonality is therefore a significant and reliable driver that should be explicitly accounted for budget allocation.

- All channels show positive incremental lift when they spend more (confirmed by both quartile and decile analysis). Differences between channels are small, no channel has dramatically higher incremental impact. So, spending more across channels consistently increases attributed revenue, and, by looking at the marginal return for different spend levels (quartiles), marginal efficiency does not deteriorate suggesting budget can scale without hitting diminishing returns (consistent with a smooth/synthetic dataset). By observing the cohort analysis results, efficiency erodes over time, suggesting external market pressures or gradual saturation in time.
 
- The budget reallocation exercise shows a small but positive expected gain when shifting 10% of spend away from the lowest-ROAS channels (TV & Affiliate) toward the highest-ROAS channels (Search & Email). This supports the idea that reallocating budget toward more efficient channels increases total return, even if the effect is modest, consistent with the overall pattern in the dataset where marginal returns do not decline at higher spend levels.

## 2) Channel performance rankings (30d attribution)
Classifica per **efficienza (ROAS 30d)** e **ricavi attribuiti**. Attribution: la revenue del giorno *t* viene ripartita sui canali in proporzione alla **quota di spesa cumulata negli ultimi _N_ giorni** (rolling window che include il giorno *t*).

| Rank | Channel | Total Spend | Attributed Revenue (30d) | ROAS 7d | ROAS 14d | ROAS 30d | Corr(spend, revenue) |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | paid_search_spend | €2.406.438 | €11.408.155 | 4.739 | 4.740 | 4.741 | 0.752 |
| 2 | email_spend | €398.603 | €1.889.404 | 4.740 | 4.740 | 4.740 | 0.755 |
| 3 | display_spend | €1.195.037 | €5.664.208 | 4.740 | 4.740 | 4.740 | 0.746 |
| 4 | paid_social_spend | €2.003.083 | €9.493.168 | 4.740 | 4.740 | 4.739 | 0.767 |
| 5 | tv_spend | €1.594.725 | €7.553.786 | 4.737 | 4.737 | 4.737 | 0.736 |
| 6 | affiliate_spend | €800.602 | €3.791.083 | 4.737 | 4.736 | 4.735 | 0.747 |

Top ROAS and Top Revenue —> PAID SEARCH
Bottom ROAS —> affiliate
Bottom Revenue —> email

## 3) Budget optimization  
Given the alignment in ROAS across channels, the “static” optimization potential is limited. However:
-Reallocate 10% of spend from less efficient channels (TV, Affiliate) toward more efficient ones (Paid Search, Email), split equally across the two recipients.
-Controlled scalability tests: gradually increase investment in channels with improving ROAS at higher spend deciles (e.g., Paid Social, Display, Search) while monitoring marginal performance week-over-week.
-Leverage external demand peaks: concentrate incremental budget during holiday / promo / weekend periods, where average revenue lifts by +18.2%, +15.7%, and +9.6%, respectively.

## 4) Data quality issues / limitations
- Duplicate dates across all datasets: 2023-03-26 and 2024-03-31 → kept the first occurrence to align with the brief (final rows: 728 per dataset).
- Two missing calendar dates: 2023-10-29 and 2024-10-27 (likely due to DST changes). Effective coverage: 2023-01-01 → 2024-12-30.
- No zero-spend days per channel → prevents direct “spend vs. zero-spend” incrementality inference.
- Synthetic data: performance levels are very even across channels → limits the scale of reallocation recommendations.

## 5) Suggested next steps
- Lightweight MMM (Bayesian with informed priors) to estimate elasticity, adstock, and saturation for each channel; calibrate using external signals (holiday, promo, seasonality, competitor intensity).
- Controlled experiments (geo-split / holdout) for 1–2 channels to measure causal incrementality and anchor the MMM.
- Creative & audience granularity: enrich logs with placement/format, funnel stage, and target; measure marginal ROAS by segment.
- LTV-aware bidding: incorporate LTV for new vs returning users (dataset includes new_customers); prioritize channels with stronger LTV/CAC ratios.
- Calendar-based orchestration: deploy bursts in high-yield periods (holiday/promo/weekend) with frequency caps and real-time saturation monitoring.
