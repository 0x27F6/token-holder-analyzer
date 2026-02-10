/*
Purpose: Reconstruct continuous wallet holding segments for holder metrics
Model Type: Dense Continuous State with Segmentation
Dependencies: wallet_classification.sql (query_XXXXX)
Output: Daily wallet balances with forward-filled values within holding segments
Query Link: https://dune.com/queries/XXXXXX

Segmentation Algorithm:
1. Identify entry events (balance > 0 AND prev_balance <= 0)
2. Assign segment IDs via cumulative sum of entry flags
3. Define segment bounds (first day to last day of each holding episode)
4. Expand calendar within segment boundaries
5. Forward-fill balances using LAST_VALUE within each segment
6. Filter to segments with actual holdings (eliminates data artifacts)

Boundary Logic: Includes exit day (day <= segment_end)
Rationale: A wallet that exits on day N was still a holder at the start of day N.
This version is used for holder counting, where we need to count wallets on their exit day.

Key Difference from Supply Analysis Version:
- Includes exit day in bounds (day <= segment_end vs day < segment_end)
- No normalization (used for counting wallets, not measuring supply distribution)
- Simpler output (no need to scale to total_supply)
*/

WITH

stateful_wallets AS (
    SELECT * FROM query_XXXXX  -- Replace with actual query ID from wallet_classification
),

dates AS (
    SELECT 
        DATE '2024-10-18' AS start_date,
        CURRENT_DATE AS end_date
),

-- ============================================================================
-- HOLDING SEGMENT IDENTIFICATION
-- ============================================================================

-- Label each day's transition type and flag entry events
transition_labels AS (
    SELECT 
        day,
        owner,
        eod_balance,
        prev_balance,
        
        -- Debug column for manual inspection (optional)
        CASE 
            WHEN eod_balance > 0 AND (prev_balance <= 0 OR prev_balance IS NULL) THEN 'entry'  
            WHEN eod_balance > prev_balance THEN 'added'
            WHEN eod_balance < prev_balance AND eod_balance > 0 THEN 'partial_sell'
            WHEN eod_balance = 0 AND prev_balance > 0 THEN 'exit'
            ELSE 'inactive'
        END AS debug,
        
        -- Entry flag: Used to create segment boundaries
        -- True when wallet moves from zero/null balance to positive balance
        CASE 
            WHEN eod_balance > 0 AND (prev_balance <= 0 OR prev_balance IS NULL) THEN true
            ELSE false
        END AS is_entry
    FROM stateful_wallets
),

-- Assign segment IDs: Cumulative sum of entry flags creates holding episodes
-- Each time a wallet enters (balance goes from 0 to >0), segment_id increments
segment_ids AS (
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

-- Define segment boundaries: start = first day, end = last day of holding episode
-- Note: Includes the exit day (a wallet exiting on day N held tokens at start of day N)
segment_bounds AS (
    SELECT 
        owner,
        holding_segment_id,
        MIN(day) AS segment_start,
        MAX(day) AS segment_end,
        DATE_DIFF('day', MIN(day), MAX(day)) + 1 AS segment_length_days
    FROM segment_ids
    GROUP BY owner, holding_segment_id
),

-- ============================================================================
-- CALENDAR EXPANSION & FORWARD-FILL
-- ============================================================================

-- Generate continuous calendar for the analysis period
calendar AS (
    SELECT DATE(day) AS day
    FROM UNNEST(SEQUENCE(
        (SELECT start_date FROM dates),
        (SELECT end_date FROM dates),
        INTERVAL '1' DAY
    )) AS sub(day)
),

-- Expand calendar only within each segment's boundaries
-- CRITICAL: day <= segment_end (includes exit day for holder counting)
bounded_wallet_expansion AS (
    SELECT *
    FROM calendar c
    JOIN segment_bounds sb
        ON c.day >= sb.segment_start 
            AND c.day <= sb.segment_end
),

-- Get actual recorded balances (sparse - only days with transactions)
wallet_balances AS (
    SELECT 
        day,
        owner,
        eod_balance 
    FROM stateful_wallets
),

-- Create spine: All calendar days per segment, with actual balances where available
wallet_segment_spine AS (
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

-- Forward-fill balances within each segment using LAST_VALUE
-- This handles sparse updates: wallets without transactions retain previous balance
wallet_segment_spine_filled AS (
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

-- Filter to valid segments: Must have at least one non-zero balance record
-- Eliminates false segments created by data gaps or edge cases
valid_segments AS (
    SELECT
        owner,
        holding_segment_id
    FROM wallet_segment_spine_filled
    GROUP BY owner, holding_segment_id
    HAVING SUM(eod_balance) > 0
),

-- Join back to get only valid segment data
continuous_wallet_balance_state AS (
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

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

daily_balance_summary AS (
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
    eod_balance AS raw_balance,        -- Actual balance (only present on transaction days)
    balance AS filled_balance,          -- Forward-filled balance (present every day in segment)
    holding_segment_id,
    segment_start,
    segment_end,
    segment_length_days
FROM daily_balance_summary 
ORDER BY day, owner;
