/*
Purpose: Normalize supply flows as % of total supply and calculate 7-day rolling metrics
Model Type: Derived Metrics
Dependencies: supply_flows.sql
Output: Supply velocity metrics (daily and 7-day rolling)

Metrics:
- gross_flow_pct_supply: Daily turnover as % of supply (velocity measure)
- gross_supply_flow_7d: 7-day rolling absolute token movement
- gross_supply_flow_7d_pct_supply: 7-day velocity as % of supply
*/
WITH
  
total_supply AS(
  1e9 AS token_supply
),
  
daily_supply_flow_metrics AS (
    SELECT
        f.day,

        /* raw flow metrics (absolute units) */
        ROUND(f.net_balance_change, 0)      AS net_supply_flow,     -- net change in held supply
        ROUND(f.total_inflow, 0)            AS inflows,             -- tokens accumulated by holders
        ROUND(f.total_outflow, 0)           AS outflows,            -- tokens distributed by holders
        ROUND(f.daily_gross_supply_flow, 0) AS gross_supply_flow,   -- total token flow

        /* normalized (percent of total supply) */
        ROUND(
            (f.daily_gross_supply_flow         
                / NULLIF(t.total_supply, 0)) * 100.0,
            2
        ) AS gross_flow_pct_supply,                                 -- velocity as a % of supply

        -- experimental; keep precision but easy to kill later
        ROUND(
            (f.net_balance_change
                / NULLIF(t.total_supply, 0)) * 100.0,
            4
        ) AS net_flow_pct_supply,                                   -- directional change as a % of supply

        /* rolling 7d flows (absolute) */
        ROUND(
            SUM(f.daily_gross_supply_flow) OVER (
                ORDER BY f.day
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ),
            0
        ) AS gross_supply_flow_7d,                                  --7-day rolling total movement

        /* rolling 7d flows (percent of supply) */
        ROUND(
            (
                SUM(f.daily_gross_supply_flow) OVER (
                    ORDER BY f.day
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                )
                / NULLIF(t.total_supply, 0)
            ) * 100.0,
            4
        ) AS gross_supply_flow_7d_pct_supply                        -- 7-day velocity as % of supply

    FROM supply.daily_supply_flows.sql f
    CROSS JOIN (
        SELECT token_supply
        FROM total_supply
    ) t
)

SELECT 
    day,
    net_supply_flow,
    inflows,
    outflows,
    gross_supply_flow,
    gross_flow_pct_supply,
    gross_supply_flow_7d,
    gross_supply_flow_7d_pct_supply
FROM daily_supply_flow_metrics
