/*
Purpose: Holder lifecycle analytics with wallet type decomposition. Tracks holder counts, 
acquisition/churn flows, and turnover velocity across three wallet classifications 
(dex_trader, lp, transfer_only). Measures how the holder base grows, shrinks, and 
churns over time — and which wallet types are driving those changes.

Uses a correlated subquery against a calendar spine to produce accurate daily holder counts
even on days without balance events (forward-looking: "who held tokens as of this date?").

Velocity metrics normalize holder turnover against the existing holder base, with a 30-day 
rolling median baseline to identify periods of abnormally high or low churn relative to 
recent history.

DUNE: https://dune.com/queries/6681406

Tables:
  None directly — reads from materialized base model

Dependencies:
  1. Base Model: dune.research_onchain.result_meta_dao_model_base

Output:
  One row per day with:
    - Holder counts: all, threshold, and split by wallet type (trader/lp/transfer)
    - Flow dynamics: acquired, churned, net change — total and per wallet type
    - Velocity: daily holder turnover as a ratio of the holder base
    - Velocity normalized: turnover relative to 30-day rolling median (1.0 = baseline)

Key Concepts:
  - Acquired: wallet crossed above threshold today (new holder)
  - Churned: wallet dropped below threshold today (lost holder)
  - Net change: acquired + churn (churn is negative)
  - Holder velocity: net change / holder count (directional pressure)
  - Gross velocity: (|acquired| + |churn|) / holder count (total turnover)
  - Normalized velocity: current gross velocity / 30-day median (regime detection)
*/

WITH
params AS(
    SELECT
        start_date AS start_date,
        end_date   AS end_date,
        threshold  AS threshold
    FROM dune.research_onchain.result_meta_dao_model_base
    LIMIT 1 
),

base_data AS(
    SELECT 
        day,
        owner,
        eod_balance,
        prev_balance,
        wallet_type
    FROM dune.research_onchain.result_meta_dao_model_base
),

-- Calendar spine ensures every day has a row even if no balance events occurred
calendar AS (
    SELECT DATE(day) AS day
    FROM UNNEST(SEQUENCE(
        (SELECT start_date FROM params), 
        (SELECT end_date FROM params),
        INTERVAL '1' DAY
    )) AS t(day)
),

-- ============================================================================
-- HOLDER COUNTS
-- Correlated subquery finds each wallet's most recent balance as of each calendar day
-- Produces accurate point-in-time holder counts with wallet type decomposition
-- ============================================================================
holder_count AS (
    SELECT 
        c.day,
        COUNT(DISTINCT bd.owner) AS all_holders,
        COUNT(DISTINCT 
                CASE WHEN bd.eod_balance >= (SELECT threshold FROM params) 
                     THEN bd.owner 
                END) AS threshold_holders,
        COUNT(DISTINCT 
                CASE WHEN bd.wallet_type = 'dex_trader' AND bd.eod_balance >= (SELECT threshold FROM params)
                     THEN bd.owner 
                END) AS trader_holders,
        COUNT(DISTINCT 
                CASE WHEN bd.wallet_type = 'lp' AND bd.eod_balance >= (SELECT threshold FROM params)
                     THEN bd.owner 
                END) AS lp_holders,
        COUNT(DISTINCT 
                CASE WHEN bd.wallet_type = 'transfer_only' AND bd.eod_balance >= (SELECT threshold FROM params)
                     THEN bd.owner 
                END) AS transfer_holders
    FROM calendar c
    JOIN base_data bd
        ON bd.day = ( 
            SELECT MAX(bd2.day)
            FROM base_data bd2
            WHERE bd2.owner = bd.owner 
              AND bd2.day <= c.day
        )
    WHERE bd.eod_balance > 0
    GROUP BY c.day 
),

-- ============================================================================
-- HOLDER FLOWS
-- Detect threshold crossings per day: wallets entering or exiting the holder universe
-- Split by wallet type to see which actors drive holder base changes
-- ============================================================================
wallet_flows AS (
    SELECT 
        bd.day,
        bd.wallet_type,
        SUM(
            CASE
                WHEN (bd.prev_balance IS NULL OR bd.prev_balance <= (SELECT threshold FROM params))
                 AND bd.eod_balance > (SELECT threshold FROM params)
                THEN 1 ELSE 0
            END
        ) AS acquired,
        SUM(
            CASE
                WHEN bd.prev_balance > (SELECT threshold FROM params)
                 AND bd.eod_balance <= (SELECT threshold FROM params)
                THEN -1 ELSE 0
            END
        ) AS churn
    FROM base_data bd
    GROUP BY bd.day, bd.wallet_type
),

-- Reaggregate: total flows plus per-type breakdowns in a single row per day
wallet_flows_agg AS (
    SELECT
        day,
        SUM(acquired) AS acquired,
        SUM(churn) AS churn,
        SUM(CASE WHEN wallet_type = 'dex_trader' THEN acquired ELSE 0 END) AS trader_acquired,
        SUM(CASE WHEN wallet_type = 'dex_trader' THEN churn ELSE 0 END) AS trader_churn,
        SUM(CASE WHEN wallet_type = 'lp' THEN acquired ELSE 0 END) AS lp_acquired,
        SUM(CASE WHEN wallet_type = 'lp' THEN churn ELSE 0 END) AS lp_churn,
        SUM(CASE WHEN wallet_type = 'transfer_only' THEN acquired ELSE 0 END) AS transfer_acquired,
        SUM(CASE WHEN wallet_type = 'transfer_only' THEN churn ELSE 0 END) AS transfer_churn
    FROM wallet_flows
    GROUP BY day
),

daily_holder_summary AS(
    SELECT 
        hc.day,
        hc.all_holders,
        hc.threshold_holders,
        hc.trader_holders,
        hc.lp_holders,
        hc.transfer_holders,
        COALESCE(wf.acquired, 0) AS acquired,
        COALESCE(wf.churn, 0) AS churn,
        COALESCE(wf.acquired, 0) + COALESCE(wf.churn, 0) AS net_change,
        COALESCE(wf.trader_acquired, 0) AS trader_acquired,
        COALESCE(wf.trader_churn, 0) AS trader_churn,
        COALESCE(wf.lp_acquired, 0) AS lp_acquired,
        COALESCE(wf.lp_churn, 0) AS lp_churn,
        COALESCE(wf.transfer_acquired, 0) AS transfer_acquired,
        COALESCE(wf.transfer_churn, 0) AS transfer_churn
    FROM holder_count hc
    LEFT JOIN wallet_flows_agg wf 
        ON hc.day = wf.day
),

-- ============================================================================
-- VELOCITY METRICS
-- Normalize holder turnover against the size of the holder base
-- ============================================================================
velocity AS(
    SELECT 
        day,
        -- Directional: positive = net growth, negative = net decline
        CAST(net_change AS DOUBLE)
            / NULLIF(CAST(threshold_holders AS DOUBLE), 0) AS holder_velocity,
        -- Absolute: total churn regardless of direction
        CAST(ABS(acquired) + ABS(churn) AS DOUBLE)
            / NULLIF(CAST(threshold_holders AS DOUBLE), 0) AS gross_holder_velocity
    FROM daily_holder_summary
),

-- Normalize to 30-day rolling median baseline
-- Values > 1 = above-average turnover, < 1 = below-average
velocity_normalized AS(
    SELECT 
        day,
        gross_holder_velocity /
            NULLIF(
                APPROX_PERCENTILE(gross_holder_velocity, 0.5) OVER(
                    ORDER BY day
                    ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS gross_velo_normalized,
        holder_velocity /
            NULLIF(
                APPROX_PERCENTILE(holder_velocity, 0.5) OVER(
                    ORDER BY day
                    ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS velo_normalized
    FROM velocity
),

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================
final_holder_metrics AS (
    SELECT 
        h.day,
        h.all_holders,
        h.threshold_holders,
        h.trader_holders,
        h.lp_holders,
        h.transfer_holders,
        h.acquired,
        h.churn,
        h.net_change,
        h.trader_acquired,
        h.trader_churn,
        h.lp_acquired,
        h.lp_churn,
        h.transfer_acquired,
        h.transfer_churn,
        ROUND(v.holder_velocity, 4) AS holder_velocity,
        ROUND(v.gross_holder_velocity, 4) AS gross_holder_velocity,
        ROUND(vn.velo_normalized, 2) AS velocity_normalized,
        ROUND(vn.gross_velo_normalized, 2) AS gross_velocity_normalized,
        1 AS baseline
    FROM daily_holder_summary h
    LEFT JOIN velocity v
        ON h.day = v.day
    LEFT JOIN velocity_normalized vn
        ON h.day = vn.day
)

SELECT * FROM final_holder_metrics
ORDER BY day
