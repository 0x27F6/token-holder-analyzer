/*
Purpose: Classify holders into size-based cohorts by percentage of supply held
Model Type: Cohort Classification
Dependencies: continuous_state_for_supply_analysis.sql (query_XXXXX)
Output: Daily aggregate supply distribution across holder cohorts

Cohort Definitions (% of total supply):
- Krill:   < 0.01%  (less than 1 in 10,000 tokens)
- Fish:    0.01% - 0.1%  (1 in 10,000 to 1 in 1,000)
- Dolphin: 0.1% - 1%  (1 in 1,000 to 1 in 100)
- Whale:   >= 1%  (1 in 100 or more)

Key Insight: Supply concentration dynamics predict price movements
- Rising whale % during decline = Accumulation (bullish)
- Falling whale % during rally = Distribution (bearish)
- Stable concentration + low turnover = Strong holder base forming
*/

WITH 
  continuous_state AS (
    SELECT * FROM query_XXXXX  -- need to figure out what the typical structuring for github
),

-- Calculate each wallet's percentage of total supply
wallet_supply_pct AS (
    SELECT 
        day,
        owner, 
        balance_normalized AS balance,
        (balance_normalized / total_supply) AS pct_supply_held
    FROM continuous_state
),

-- Define cohort boundaries
bucket_definitions AS (
    SELECT * FROM (
        VALUES
            (0.0001, 'krill'),   -- < 0.01%
            (0.001,  'fish'),    -- 0.01% - 0.1%
            (0.01,   'dolphin'), -- 0.1% - 1%
            (1.0,    'whale')    -- >= 1%
    ) AS t(max_pct, bucket)
),

-- Assign each wallet to the smallest bucket that contains it
bucketed_wallets AS (
    SELECT
        w.day,
        w.owner,
        w.balance,
        w.pct_supply_held,
        b.bucket,
        ROW_NUMBER() OVER (
            PARTITION BY w.day, w.owner
            ORDER BY b.max_pct
        ) AS rn
    FROM wallet_supply_pct w
    JOIN bucket_definitions b
        ON w.pct_supply_held < b.max_pct
),

-- Take first matching bucket (smallest qualifying cohort)
wallet_cohorts AS (
    SELECT
        day,
        owner,
        balance,
        pct_supply_held,
        bucket
    FROM bucketed_wallets
    WHERE rn = 1
),

-- Aggregate supply by cohort per day
balance_by_cohort AS (
    SELECT 
        day,
        -- Absolute token amounts per cohort
        SUM(CASE WHEN bucket = 'krill'   THEN balance ELSE 0 END) AS krill_balance,
        SUM(CASE WHEN bucket = 'fish'    THEN balance ELSE 0 END) AS fish_balance,
        SUM(CASE WHEN bucket = 'dolphin' THEN balance ELSE 0 END) AS dolphin_balance,
        SUM(CASE WHEN bucket = 'whale'   THEN balance ELSE 0 END) AS whale_balance,
        
        -- Percentage of total supply per cohort
        SUM(CASE WHEN bucket = 'krill'   THEN pct_supply_held ELSE 0 END) * 100 AS krill_supply_pct,
        SUM(CASE WHEN bucket = 'fish'    THEN pct_supply_held ELSE 0 END) * 100 AS fish_supply_pct,
        SUM(CASE WHEN bucket = 'dolphin' THEN pct_supply_held ELSE 0 END) * 100 AS dolphin_supply_pct,
        SUM(CASE WHEN bucket = 'whale'   THEN pct_supply_held ELSE 0 END) * 100 AS whale_supply_pct
    FROM wallet_cohorts 
    GROUP BY day 
)

SELECT
    day,
    krill_balance,
    fish_balance,
    dolphin_balance,
    whale_balance,
    krill_supply_pct,
    fish_supply_pct,
    dolphin_supply_pct,
    whale_supply_pct
FROM balance_by_cohort
ORDER BY day;
