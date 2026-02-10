/*
Purpose: Calculate daily holder counts and flow dynamics (acquisition/churn)
Model Type: Holder Flow Analysis
Dependencies: wallet_classification.sql (query_XXXXX)
Output: Daily holder counts, acquired wallets, churned wallets, and net change
Query Link: https://dune.com/queries/6628974

Methodology:
- Uses AS OF join to forward-fill wallet balances across sparse transaction days
- Tracks threshold crossings to identify new holders (acquired) and exits (churn)
- Net change = acquired + churn (churn is negative, so this shows holder base growth/decline)

Key Metrics:
- holders: Total wallets above threshold on each day
- acquired: Wallets crossing from below to above threshold (entries)
- churn: Wallets crossing from above to below threshold (exits, represented as negative)
- net_change: Daily holder base growth (acquired - |churn|)
*/

WITH

balance_threshold AS(
  100 AS threshold
),

-- ============================================================================
-- HOLDER METRICS CALCULATION
-- ============================================================================

-- Generate continuous calendar for AS OF joins
calendar AS (
    SELECT DATE(day) AS day
    FROM UNNEST(SEQUENCE(
        (SELECT start_date FROM dates), 
        (SELECT end_date FROM dates),
        INTERVAL '1' DAY
    )) AS t(day)
),

-- Count total holders using AS OF join to forward-fill sparse state
holder_count AS (
    SELECT 
        c.day,
        COUNT(*) AS holders
    FROM calendar c
    JOIN stateful_wallets sw
        -- AS OF join: Forward-fill each wallet's last known balance to current calendar day
        -- Handles sparse updates - wallets without transactions retain previous balance
        ON sw.day = ( 
            SELECT MAX(sw2.day)
            FROM stateful_wallets sw2
            WHERE sw.owner = sw2.owner 
              AND sw2.day <= c.day
        )
    -- Count only holders currently above threshold
    WHERE sw.eod_balance > (SELECT threshold FROM balance_threshold)
    GROUP BY c.day 
),

-- Track daily holder acquisition and churn by detecting threshold crossings
wallet_flows AS (
    SELECT 
        sw.day,
        
        -- Acquired: Wallets crossing above threshold (new holders or re-entries)
        -- Counts first-time holders (prev_balance IS NULL) and wallets buying back in
        SUM(
            CASE
                WHEN (sw.prev_balance IS NULL OR sw.prev_balance <= ((SELECT threshold FROM balance_threshold))
                 AND sw.eod_balance > (SELECT threshold FROM balance_threshold)
                THEN 1 ELSE 0
            END
        ) AS acquired,
        
        -- Churn: Wallets crossing below threshold (exits via sell or transfer)
        -- Negative value to represent holder base contraction
        SUM(
            CASE
                WHEN sw.prev_balance > (SELECT threshold FROM balance_threshold)
                 AND sw.eod_balance <= (SELECT threshold FROM balance_threshold)
                THEN -1 ELSE 0
            END
        ) AS churn
    FROM stateful_wallets sw
    GROUP BY sw.day
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

-- Combine holder counts and flows into daily summary
daily_holder_summary AS (
    SELECT 
        hc.day,
        hc.holders,
        COALESCE(wf.acquired, 0) AS acquired,
        COALESCE(wf.churn, 0) AS churn,
        COALESCE(wf.acquired, 0) + COALESCE(wf.churn, 0) AS net_change
    FROM holder_count hc
    LEFT JOIN wallet_flows wf 
        ON hc.day = wf.day
)

SELECT 
  day,
  holders,
  acquired,
  churn,
  net_change
FROM daily_holder_summary;
  
