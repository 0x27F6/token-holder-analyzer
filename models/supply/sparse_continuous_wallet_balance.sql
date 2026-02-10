/*
Purpose: Reconstruct continuous wallet holding segments from sparse transaction events
Model Type: Dense Continuous State with Segmentation
Dependencies: solana_utils.daily_balances
Output: One row per wallet-day with forward-filled balances within holding segments

Key Insight: Wallets enter and exit positions multiple times. Traditional AS OF joins
incorrectly forward-fill balances across exit→re-entry boundaries. This model segments
each wallet's timeline into discrete holding episodes to prevent false continuity.

Segmentation Logic:
1. Identify entry events (balance > 0 AND prev_balance <= 0)
2. Assign segment IDs using cumulative sum of entry flags
3. Bound each segment (first day, last day)
4. Forward-fill balances only within segment boundaries
5. Filter segments that had actual holdings (eliminate false positives)
*/

WITH 

total_supply AS(
  1e9 AS total_supply
),

prev_state AS(
    SELECT 
        day, 
        owner,
        eod_balance,
        LAG(eod_balance) OVER(PARTITION BY owner ORDER BY day) AS prev_balance
    FROM daily_state.daily_wallet_state.sql 
),

wallet_lifetime AS (
    SELECT
        owner,
        MAX(eod_balance) AS max_balance
    FROM prev_state
    GROUP BY owner
),

labeled_wallet_types AS (
    SELECT
        ps.day,
        ps.owner,
        ps.eod_balance,
        ps.prev_balance,
        CASE
            -- wallets that NEVER held >= 100 tokens at any EOD snapshot
            -- everything <100 is filtered as noise / minimal dust traders -- not classified as a holder
            WHEN wl.max_balance <= 100 THEN 'pure_trader'
            ELSE 'stateful'
        END AS wallet_type
    FROM prev_state ps
    JOIN wallet_lifetime wl
        ON ps.owner = wl.owner
),

stateful_wallets AS(
    SELECT 
        day,
        owner,
        eod_balance,
        prev_balance
    FROM labeled_wallet_types
    WHERE wallet_type = 'stateful'
),

transition_labels AS(
    SELECT 
        day,
        owner,
        eod_balance,
        prev_balance,
    
        --- debugging logic 
        CASE 
            WHEN eod_balance > 0 AND (prev_balance <= 0 OR prev_balance IS NULL)  THEN 'entry'  
            WHEN eod_balance > prev_balance THEN 'added'
            WHEN eod_balance < prev_balance AND eod_balance > 0 THEN 'partial_sell'
            WHEN eod_balance = 0 AND prev_balance > 0 THEN 'exit'
            ELSE 'inactive'
        END AS debug ,
    
        CASE WHEN eod_balance > 0 AND (prev_balance <= 0 OR prev_balance IS NULL) THEN true
             ELSE false
        END as is_entry
        
    FROM stateful_wallets
),

segment_ids AS(
    SELECT 
        day,
        owner,
        eod_balance,
        prev_balance,
        debug,
        SUM(CASE WHEN is_entry THEN 1 ELSE 0 END) OVER(
                PARTITION BY owner ORDER BY day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS holding_segment_id 
    FROM transition_labels 
),

segment_bounds AS (
    SELECT 
        owner,
        holding_segment_id,
        MIN(day) AS segment_start,
        MAX(day) AS segment_end,
        -- segment flag 
        DATE_DIFF('day', MIN(day), MAX(day)) + 1 AS segment_length_days
    FROM segment_ids
    GROUP BY owner, holding_segment_id
),

calendar AS(
    SELECT DATE(day) AS day
    FROM(UNNEST(SEQUENCE(
        (SELECT start_date FROM params),
        (SELECT end_date FROM params),
        INTERVAL '1' DAY)
        )) AS sub(day)
),

bounded_wallet_expansion AS(
    SELECT *
    FROM calendar c
    JOIN segment_bounds sb
        ON c.day>= sb.segment_start 
            AND c.day <= sb.segment_end
),

wallet_balances AS(
    SELECT 
        day,
        owner,
        eod_balance 
    FROM stateful_wallets
),

wallet_segment_spine AS(
    SELECT
        b.day,
        b.owner,
        w.eod_balance,
        b.holding_segment_id,
        b.segment_start,
        b.segment_end,
        b.segment_length_days
    FROM bounded_wallet_expansion b
    LEFT JOIN wallet_balances w
        ON b.day = w.day AND b.owner = w.owner 
),

wallet_segment_spine_filled AS(
    SELECT 
        day,
        owner,
        eod_balance,
        holding_segment_id,
        segment_start,
        segment_end,
        segment_length_days,
        LAST_VALUE(eod_balance) IGNORE NULLS OVER(
            PARTITION BY owner, holding_segment_id ORDER BY day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS balance_filled
    FROM wallet_segment_spine
),

valid_segments AS (
    SELECT
        owner,
        holding_segment_id
    FROM wallet_segment_spine_filled
    GROUP BY owner, holding_segment_id
    HAVING SUM(eod_balance) > 0
),

continuous_wallet_balance_state AS(
    SELECT 
        w.day,
        w.owner,
        w.eod_balance,
        w.balance_filled,
        w.holding_segment_id,
        w.segment_start,
        w.segment_end,
        w.segment_length_days
    FROM wallet_segment_spine_filled w
    JOIN valid_segments v
      ON w.owner = v.owner
     AND w.holding_segment_id = v.holding_segment_id
),

daily_balance_summary AS(
    SELECT
        day,
        owner,
        eod_balance,
        balance_filled AS balance,
        holding_segment_id,
        segment_start,
        segment_end,
        segment_length_days
    FROM continuous_wallet_balance_state
)

SELECT 
  day,
  owner,
  eod_balance AS raw_balance,
  balance,
  holding_segment_id,
  segment_start,
  segment_end,
  segment_length_days
FROM daily_balance_summary 
ORDER by day
