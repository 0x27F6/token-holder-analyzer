/*
Purpose: Reconstruct continuous wallet holding segments with normalized supply distribution
Model Type: Dense Continuous State with Segmentation & Normalization
Dependencies: solana_utils.daily_balances
Output: Daily wallet balances normalized to total supply, with segment metadata

Key Features:
1. Segmentation: Tracks discrete holding episodes per wallet (entry → exit → re-entry)
2. Normalization: Scales balances to sum to total_supply for accurate percentage calculations
3. Deduplication: Handles overlapping segments by selecting most recent

Boundary Logic: Excludes exit day (day < segment_end)
Rationale: Once supply exits a wallet, it should not be counted in that wallet's distribution

Why Normalization?
We only track "stateful" wallets (those exceeding threshold historically). This means
SUM(balance_filled) < total_supply. To enable accurate cohort percentage calculations,
we scale all balances proportionally so they sum to total_supply.

Segmentation Algorithm:
1. Identify entry events (balance > 0 AND prev_balance <= 0)
2. Assign segment IDs via cumulative sum of entry flags
3. Define segment bounds (start = first day, end = exit day or current date)
4. Expand calendar only within segment boundaries
5. Forward-fill balances using LAST_VALUE within each segment
6. Filter to segments that had actual holdings
*/

WITH

params AS (
    SELECT 
        '9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump' AS token_address,
        DATE '2024-10-18' AS start_date,
        CURRENT_DATE AS end_date,
        1 AS threshold,
        1e9 AS total_supply
),

-- ============================================================================
-- PART 1: BASE STATE & WALLET CLASSIFICATION
-- ============================================================================

-- Aggregate daily balances per owner (some owners have multiple addresses)
daily_state AS (
    SELECT 
        day,
        token_balance_owner AS owner,
        SUM(token_balance) AS eod_balance
    FROM solana_utils.daily_balances
    WHERE token_mint_address = (SELECT token_address FROM params)
      AND day BETWEEN (SELECT start_date FROM params) AND (SELECT end_date FROM params)
    GROUP BY day, token_balance_owner
),

-- Add previous day's balance for transition detection
prev_state AS (
    SELECT 
        day, 
        owner,
        eod_balance,
        LAG(eod_balance) OVER(PARTITION BY owner ORDER BY day) AS prev_balance
    FROM daily_state
),

-- Calculate each wallet's historical peak balance
wallet_lifetime AS (
    SELECT
        owner,
        MAX(eod_balance) AS max_balance
    FROM prev_state
    GROUP BY owner
),

-- Filter noise: Only track wallets that held meaningful positions at some point
-- "stateless" wallets never crossed threshold - exclude as dust/MEV bots
labeled_wallet_types AS (
    SELECT
        ps.day,
        ps.owner,
        ps.eod_balance,
        ps.prev_balance,
        CASE
            WHEN wl.max_balance <= 10 THEN 'stateless'
            ELSE 'stateful'
        END AS wallet_type
    FROM prev_state ps
    JOIN wallet_lifetime wl
        ON ps.owner = wl.owner
),

stateful_wallets AS (
    SELECT 
        day,
        owner,
        eod_balance,
        prev_balance
    FROM labeled_wallet_types
    WHERE wallet_type = 'stateful'
),

-- ============================================================================
-- PART 2: HOLDING SEGMENT IDENTIFICATION
-- ============================================================================

-- Label each day's transition type and flag entries
transition_labels AS (
    SELECT 
        day,
        owner,
        eod_balance,
        prev_balance,
        
        -- Debug column for manual inspection (optional, can remove in production)
        CASE 
            WHEN eod_balance > 0 AND (prev_balance <= 0 OR prev_balance IS NULL) THEN 'entry'  
            WHEN eod_balance > prev_balance THEN 'added'
            WHEN eod_balance < prev_balance AND eod_balance > 0 THEN 'partial_sell'
            WHEN eod_balance = 0 AND prev_balance > 0 THEN 'exit'
            ELSE 'inactive'
        END AS debug,
        
        -- Entry flag: Used to create segment boundaries
        CASE 
            WHEN eod_balance > 0 AND (prev_balance <= 0 OR prev_balance IS NULL) THEN true
            ELSE false
        END as is_entry
    FROM stateful_wallets
),

-- Assign segment IDs: Cumulative sum of entry flags creates holding episodes
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
        ) AS segment_id 
    FROM transition_labels 
),

-- Identify exit days for each segment
segment_exit_days AS (
    SELECT
        owner,
        segment_id,
        MIN(day) AS exit_day
    FROM segment_ids
    WHERE eod_balance = 0 
      AND prev_balance > 0
    GROUP BY owner, segment_id
),

-- Define segment boundaries: start = first day, end = exit day or current date
segment_bounds AS (
    SELECT 
        s.owner,
        s.segment_id,
        MIN(s.day) AS segment_start,
        COALESCE(e.exit_day, (SELECT end_date FROM params)) AS segment_end,
        DATE_DIFF(
            'day',
            MIN(s.day),
            COALESCE(e.exit_day, (SELECT end_date FROM params))
        ) AS segment_length_days
    FROM segment_ids s
    LEFT JOIN segment_exit_days e 
        ON s.owner = e.owner
        AND s.segment_id = e.segment_id
    GROUP BY s.owner, s.segment_id, e.exit_day
),

-- ============================================================================
-- PART 3: CALENDAR EXPANSION & FORWARD-FILL
-- ============================================================================

-- Generate continuous calendar for the analysis period
calendar AS (
    SELECT DATE(day) AS day
    FROM UNNEST(SEQUENCE(
        (SELECT start_date FROM params),
        (SELECT end_date FROM params),
        INTERVAL '1' DAY
    )) AS sub(day)
),

-- Expand calendar only within each segment's boundaries
-- Note: day < segment_end (excludes exit day - supply no longer in wallet)
bounded_wallet_expansion AS (
    SELECT *
    FROM calendar c
    JOIN segment_bounds sb
        ON c.day >= sb.segment_start 
            AND c.day < sb.segment_end
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
        b.segment_id,
        b.segment_start,
        b.segment_end,
        b.segment_length_days
    FROM bounded_wallet_expansion b
    LEFT JOIN wallet_balances w
        ON b.day = w.day AND b.owner = w.owner 
),

-- Forward-fill balances within each segment using LAST_VALUE
wallet_segment_spine_filled AS (
    SELECT 
        day,
        owner,
        eod_balance,
        segment_id,
        segment_start,
        segment_end,
        segment_length_days,
        LAST_VALUE(eod_balance) IGNORE NULLS OVER(
            PARTITION BY owner, segment_id ORDER BY day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS balance_filled
    FROM wallet_segment_spine
),

-- Filter to valid segments: Must have at least one non-zero balance record
-- Eliminates false segments created by data gaps
valid_segments AS (
    SELECT
        owner,
        segment_id
    FROM wallet_segment_spine_filled
    GROUP BY owner, segment_id
    HAVING SUM(eod_balance) > 0
),

continuous_wallet_balance_state AS (
    SELECT 
        w.day,
        w.owner,
        w.eod_balance,
        w.balance_filled,
        w.segment_id,
        w.segment_start,
        w.segment_end,
        w.segment_length_days
    FROM wallet_segment_spine_filled w
    JOIN valid_segments v
        ON w.owner = v.owner
        AND w.segment_id = v.segment_id
),

-- ============================================================================
-- PART 4: DEDUPLICATION & NORMALIZATION
-- ============================================================================

-- Handle overlapping segments: Extended exit bounds can create duplicates per wallet-day
-- Keep most recent segment (highest segment_start)
wallet_day_segments AS (
    SELECT
        day,
        owner,
        eod_balance,
        balance_filled AS balance,
        segment_id,
        segment_start,
        segment_end,
        segment_length_days,
        ROW_NUMBER() OVER (
            PARTITION BY day, owner
            ORDER BY segment_start DESC
        ) AS seg_rank
    FROM continuous_wallet_balance_state
    WHERE segment_id != 0
      AND balance_filled > 0
      AND day >= segment_start 
      AND day < segment_end
),

daily_balance_summary AS (
    SELECT
        day,
        owner,
        eod_balance,
        balance,
        segment_id,
        segment_start,
        segment_end,
        segment_length_days
    FROM wallet_day_segments
    WHERE seg_rank = 1
),

-- Calculate total filled balance per day (will be < total_supply)
daily_filled_totals AS (
    SELECT
        day,
        SUM(balance) AS filled_total
    FROM daily_balance_summary
    GROUP BY day
),

-- Compute normalization factor to scale balances to total_supply
daily_normalization_factor AS (
    SELECT
        f.day,
        (p.total_supply / f.filled_total) AS norm_factor,
        p.total_supply
    FROM daily_filled_totals f
    CROSS JOIN params p
),

-- Apply normalization: balance_normalized = balance × (total_supply / filled_total)
daily_balance_normalized AS (
    SELECT
        d.day,
        d.owner,
        d.eod_balance,
        d.balance AS balance_filled,
        d.balance * n.norm_factor AS balance_normalized,
        d.segment_id,
        d.segment_start,
        d.segment_end,
        d.segment_length_days,
        n.total_supply
    FROM daily_balance_summary d
    JOIN daily_normalization_factor n
        ON d.day = n.day
)

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

SELECT
    day,
    owner,
    eod_balance,           -- Raw balance (only present on transaction days)
    balance_filled,        -- Forward-filled balance within segment
    balance_normalized,    -- Scaled to ensure sum = total_supply
    segment_id,
    segment_start,
    segment_end,
    segment_length_days,
    total_supply
FROM daily_balance_normalized
ORDER BY day, owner;
