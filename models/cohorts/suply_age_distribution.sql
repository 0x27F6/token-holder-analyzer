/*
Purpose: Measure holder conviction by tracking supply age (time since last movement)
Model Type: Supply Age Analysis
Dependencies: continuous_state_for_supply_analysis.sql (query_XXXXX)
Output: Daily distribution of supply across age buckets

Age Buckets (days since segment start):
- Active:       Day 0 (tokens moved today, entered position)
- Sub 1 Month:  1-30 days unmoved
- 1-3 Months:   31-90 days unmoved
- 3-6 Months:   91-180 days unmoved
- 6+ Months:    181+ days unmoved (strong conviction signal)

Key Insight: Supply age reveals holder conviction independent of price
- Rising 6+ month supply = Strengthening holder base (bullish)
- Shrinking active supply = Reduced selling pressure (coiling)
- Supply aging during decline = Holders absorbing dips (accumulation)
- Supply becoming active during rally = Profit-taking (distribution)

Methodology: 
Supply age = days since segment_start (most recent entry/accumulation).
This differs from "holding period" - we're measuring dormancy, not total ownership duration.
*/

WITH continuous_state AS (
    SELECT * FROM query_XXXXX  -- Replace with actual query ID from continuous_state_for_supply_analysis
),

-- Calculate supply age: days since wallet entered current holding segment
supply_age AS (
    SELECT
        day, 
        owner, 
        balance_filled,
        balance_normalized,
        DATE_DIFF('day', segment_start, day) AS supply_age 
    FROM continuous_state
),

-- Bucket supply by age ranges
supply_age_buckets AS (
    SELECT 
        day,
        owner, 
        balance_normalized, 
        CASE
            WHEN supply_age IS NULL THEN 'Unknown'
            WHEN supply_age = 0 THEN 'active'
            WHEN supply_age BETWEEN 1 AND 30 THEN 'Sub 1 Month'
            WHEN supply_age BETWEEN 31 AND 90 THEN '1-3 Months'
            WHEN supply_age BETWEEN 91 AND 180 THEN '3-6 Months'
            ELSE '6+ Months'
        END AS age_bucket
    FROM supply_age
),

-- Aggregate supply by age bucket per day
supply_age_buckets_pivot AS (
    SELECT
        day,
        SUM(CASE WHEN age_bucket = 'active'        THEN balance_normalized ELSE 0 END) AS active,
        SUM(CASE WHEN age_bucket = 'Sub 1 Month'   THEN balance_normalized ELSE 0 END) AS sub_1_month,
        SUM(CASE WHEN age_bucket = '1-3 Months'    THEN balance_normalized ELSE 0 END) AS m1_3,
        SUM(CASE WHEN age_bucket = '3-6 Months'    THEN balance_normalized ELSE 0 END) AS m3_6,
        SUM(CASE WHEN age_bucket = '6+ Months'     THEN balance_normalized ELSE 0 END) AS m6_plus,
        SUM(CASE WHEN age_bucket = 'Unknown'       THEN balance_normalized ELSE 0 END) AS unknown
    FROM supply_age_buckets
    GROUP BY day
)

SELECT 
    day,
    active,
    sub_1_month,
    m1_3,
    m3_6,
    m6_plus,
    unknown
FROM supply_age_buckets_pivot
ORDER BY day;
