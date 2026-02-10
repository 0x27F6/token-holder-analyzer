/*
Purpose: Calculate daily token supply movements (inflows, outflows, net change)
Model Type: Flow Analysis
Dependencies: continuous_wallet_state.sql
Output: Daily aggregate supply flows in absolute units

Flow Definitions:
- Inflow: Positive balance deltas (accumulation)
- Outflow: Negative balance deltas (distribution)
- Net: Directional sum (inflow - outflow)
- Gross: Total magnitude (|inflow| + |outflow|) - measures velocity
*/

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
    FROM supply_flow.segmented_continuous_wallet_balance_state.sql
    ORDER BY owner, day 
),


-- supply circulated today

previous_filled_balances AS(
    SELECT 
        day,
        owner, 
        balance,
        LAG(balance) OVER(PARTITION BY owner ORDER BY day) AS prev_balance
    FROM daily_balance_summary
),


balance_deltas AS (
    SELECT 
        day, 
        owner,
        balance,
        prev_balance,
        COALESCE(balance - prev_balance, balance) AS balance_delta
    FROM previous_filled_balances
),

daily_supply_flows AS (
    SELECT
        day,
        SUM(balance_delta)                                                      AS net_balance_change,     -- +in - |out|  (directional net)
        -- Inflows 
        SUM(CASE WHEN balance_delta > 0 THEN balance_delta ELSE 0 END)          AS total_inflow,
        
        -- Outflows 
        SUM(CASE WHEN balance_delta < 0 THEN ABS(balance_delta) ELSE 0 END)     AS total_outflow,          -- negative values
        -- Gross = inflows + magnitude of outflows  
        SUM(CASE WHEN balance_delta > 0 THEN balance_delta ELSE 0 END)
      + SUM(CASE WHEN balance_delta < 0 THEN ABS(balance_delta) ELSE 0 END)     AS daily_gross_supply_flow
    FROM balance_deltas
    GROUP BY day
)

SELECT 
  day,
  net_balance_change,
  total_inflow,
  total_outflow,
  daily_gross_supply_flow
FROM daily_supply_flows;
  
