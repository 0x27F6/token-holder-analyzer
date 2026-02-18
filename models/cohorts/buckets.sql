/*
Purpose: Continuous balance state reconstruction with supply decomposition metrics. Transforms sparse 
EOD balance events into a dense daily time series by forward-filling balances across holding segments.
This enables accurate supply age tracking, concentration analysis, and wallet type attribution over time.

The model reconstructs what each wallet held on every single day (not just days with balance changes),
then decomposes total supply across multiple dimensions:
  - Size cohorts (krill/fish/dolphin/whale by % of observed supply)
  - Supply age (active/fresh/medium/aged by days since last balance change)
  - Wallet type (dex_trader/lp/transfer_only from base model classification)
  - Threshold split (above/below economic significance threshold)

DUNE: https://dune.com/queries/6681563

Tables:
  None directly — reads from materialized base model

Dependencies:
  1. Base Model: dune.research_onchain.result_meta_dao_model_base

Output:
  One row per day with:
    - Cohort balances and supply percentages (krill/fish/dolphin/whale)
    - Supply by wallet type (trader/lp/transfer)
    - Supply age distribution (active/fresh/medium/aged)
    - Aged supply decomposed by wallet type (who are the long-term holders?)
    - Threshold supply split (above/below)

Key Concepts:
  - Segment: a continuous period where a wallet holds a non-zero balance
  - Forward-fill: on days without balance events, carry the last known balance
  - Balance_filled vs eod_balance: filled is the reconstructed continuous state,
    eod_balance is NULL on days without events (sparse source data)
*/

WITH

params AS (
    SELECT 
        start_date,
        end_date,
        threshold,
        total_supply,
        token_address
    FROM dune.research_onchain.result_meta_dao_model_base
    LIMIT 1
),

base_data AS (
    SELECT 
        day,
        owner,
        eod_balance,
        prev_balance,
        wallet_type
    FROM dune.research_onchain.result_meta_dao_model_base
),

-- ============================================================================
-- SEGMENT DETECTION
-- Identify continuous holding periods per wallet
-- A new segment begins each time a wallet goes from zero to non-zero balance
-- ============================================================================
transition_labels AS (
    SELECT 
        day,
        owner,
        eod_balance,
        prev_balance,
        wallet_type,
        CASE 
            WHEN eod_balance > 0 
             AND (prev_balance = 0 OR prev_balance IS NULL)
            THEN true
            ELSE false
        END AS is_entry
    FROM base_data
),

segment_ids AS (
    SELECT 
        day,
        owner,
        eod_balance,
        prev_balance,
        wallet_type,
        -- Running sum of entries creates unique segment IDs per wallet
        SUM(CASE WHEN is_entry THEN 1 ELSE 0 END) OVER (
            PARTITION BY owner 
            ORDER BY day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS segment_id
    FROM transition_labels
),

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

segment_bounds AS (
    SELECT 
        s.owner,
        s.segment_id,
        MIN(s.day) AS segment_start,
        COALESCE(
            e.exit_day,
            (SELECT end_date FROM params)
        ) AS segment_end,
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
-- DENSE STATE RECONSTRUCTION
-- Expand each segment into a daily time series and forward-fill balances
-- ============================================================================
calendar AS (
    SELECT DATE(day) AS day
    FROM UNNEST(
        SEQUENCE(
            (SELECT start_date FROM params),
            (SELECT end_date FROM params),
            INTERVAL '1' DAY
        )
    ) AS sub(day)
),

bounded_wallet_expansion AS (
    SELECT *
    FROM calendar c
    JOIN segment_bounds sb
      ON c.day >= sb.segment_start
     AND c.day < sb.segment_end
),

wallet_balances AS (
    SELECT 
        day,
