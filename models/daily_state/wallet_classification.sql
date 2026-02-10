
-- ============================================================================
-- STATE ENRICHMENT & WALLET CLASSIFICATION
-- ============================================================================

-- Add previous day's balance for threshold crossing detection
prev_state AS (
    SELECT 
        day, 
        owner,
        eod_balance,
        LAG(eod_balance) OVER(PARTITION BY owner ORDER BY day) AS prev_balance
    FROM daily_state
),

-- Determine each wallet's historical peak balance
wallet_lifetime AS (
    SELECT
        owner,
        MAX(eod_balance) AS max_balance
    FROM prev_state
    GROUP BY owner
),

-- Label wallets as 'stateful' (meaningful holders) vs 'pure_trader' (never crossed threshold)
-- This filters noise from wallets that only trade dust amounts
labeled_wallet_types AS (
    SELECT
        ps.day,
        ps.owner,
        ps.eod_balance,
        ps.prev_balance,
        CASE
            WHEN wl.max_balance <= (SELECT threshold FROM balance_threshold) THEN 'pure_trader'
            ELSE 'stateful'
        END AS wallet_type
    FROM prev_state ps
    JOIN wallet_lifetime wl
        ON ps.owner = wl.owner
),

-- Filter to stateful wallets only - these are economically relevant holders
stateful_wallets AS (
    SELECT 
        day,
        owner,
        eod_balance,
        prev_balance,
        CASE WHEN eod_balance > (SELECT threshold FROM balance_threshold) THEN true ELSE false END AS is_holder
    FROM labeled_wallet_types
    WHERE wallet_type = 'stateful'
)
-- ============================================================================
-- STATE ENRICHMENT & WALLET CLASSIFICATION
-- ============================================================================

-- Add previous day's balance for threshold crossing detection
prev_state AS (
    SELECT 
        day, 
        owner,
        eod_balance,
        LAG(eod_balance) OVER(PARTITION BY owner ORDER BY day) AS prev_balance
    FROM daily_state
),

-- Determine each wallet's historical peak balance
wallet_lifetime AS (
    SELECT
        owner,
        MAX(eod_balance) AS max_balance
    FROM prev_state
    GROUP BY owner
),

-- Label wallets as 'stateful' (meaningful holders) vs 'pure_trader' (never crossed threshold)
-- This filters noise from wallets that only trade dust amounts
labeled_wallet_types AS (
    SELECT
        ps.day,
        ps.owner,
        ps.eod_balance,
        ps.prev_balance,
        CASE
            WHEN wl.max_balance <= (SELECT threshold FROM balance_threshold) THEN 'pure_trader'
            ELSE 'stateful'
        END AS wallet_type
    FROM prev_state ps
    JOIN wallet_lifetime wl
        ON ps.owner = wl.owner
),

-- Filter to stateful wallets only - these are economically relevant holders
stateful_wallets AS (
    SELECT 
        day,
        owner,
        eod_balance,
        prev_balance,
        CASE WHEN eod_balance > (SELECT threshold FROM balance_threshold) THEN true ELSE false END AS is_holder
    FROM labeled_wallet_types
    WHERE wallet_type = 'stateful'
)

SELECT
  day,
  owner,
  eod_balance,
  prev_balance
FROM stateful_wallets 
