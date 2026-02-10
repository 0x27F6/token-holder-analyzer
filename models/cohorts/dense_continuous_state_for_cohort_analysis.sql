
/*



Boundary Logic: Excludes exit day (day < segment_end)
Rationale: Supply exited the wallet, should not be counted in distribution

Normalization: Scales all balances to sum to total_supply
Rationale: Enables accurate cohort percentage calculations despite incomplete coverage

Deduplication: Uses ROW_NUMBER to handle overlapping segments
Rationale: Extended exit bounds can create overlaps; keep most recent segment
*/
WITH 

prev_state AS(
SELECT 
    day, 
    owner,
    eod_balance,
    LAG(eod_balance) OVER(PARTITION BY owner ORDER BY day) AS prev_balance
FROM daily_state
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
            -- wallets that NEVER hold above threshold
            WHEN wl.max_balance <= 10 THEN 'stateless'
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
    END AS debug,

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
        ) AS segment_id 
FROM transition_labels 
),

segment_exit_days AS(
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
    COALESCE 
        (e.exit_day, 
         (SELECT end_date FROM params)) AS segment_end,
    -- segment flag 
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
        AND c.day < sb.segment_end
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
    b.segment_id,
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

valid_segments AS (
    SELECT
        owner,
        segment_id
    FROM wallet_segment_spine_filled
    GROUP BY owner, segment_id
    HAVING SUM(eod_balance) > 0
),

continuous_wallet_balance_state AS(
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

dense_daily_balance_summary AS (
    SELECT
        day,
        owner,
        eod_balance, -- raw balances no fill
        balance,
        segment_id,
        segment_start,
        segment_end,
        segment_length_days
    FROM wallet_day_segments
    WHERE seg_rank = 1
)

SELECT
  day,
  owner,
  eod_balance,
  balance,
  segment_id,
  segment_start,
  segment_end,
  segment_length_days
FROM dense_daily_balance_summary
