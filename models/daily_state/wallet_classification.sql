/*
Purpose: Filter blockchain balances to economically relevant holders
Model Type: Wallet Classification & State Preparation
Dependencies: daily_wallet_state.sql (query_XXXXX)
Output: Daily wallet state for wallets that crossed economic relevance threshold
Query Link: https://dune.com/queries/XXXXXX

Classification Logic:
- "Stateful" wallets: Ever held >= threshold tokens (economically meaningful)
- "Pure trader" wallets: Never crossed threshold (dust/MEV/noise) - excluded

This pre-filtering dramatically reduces data volume for downstream models while
preserving all analytically relevant holder behavior.

Why This Matters:
Blockchain data contains millions of wallets with negligible balances (airdrops, dust,
MEV bots). By identifying and excluding wallets that never held meaningful positions,
we reduce dataset size by ~90% while losing zero analytical signal. This materialized
layer eliminates redundant classification logic across all downstream models.

Output: ~3-4M rows (from billions in raw data)
*/

WITH

daily_wallet_state AS (
    SELECT * FROM query_XXXXX  -- Replace with actual query ID from daily_wallet_state
),

balance_threshold AS (
    SELECT 100 AS threshold
),

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
    FROM daily_wallet_state
),

-- Determine each wallet's historical peak balance
-- Used to classify wallets based on their maximum ever holding
wallet_lifetime AS (
    SELECT
        owner,
        MAX(eod_balance) AS max_balance
    FROM prev_state
    GROUP BY owner
),

-- Label wallets as 'stateful' (meaningful holders) vs 'pure_trader' (never crossed threshold)
-- This filters noise from wallets that trade intra-day with a remaining balance below threshold
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
-- Excludes ~90% of wallets while retaining all meaningful holder behavior
stateful_wallets AS (
    SELECT 
        day,
        owner,
        eod_balance,
        prev_balance
    FROM labeled_wallet_types
    WHERE wallet_type = 'stateful'
)

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================

SELECT
    day,
    owner,
    eod_balance,
    prev_balance
FROM stateful_wallets
ORDER BY day, owner; 
