/*
Purpose: Determine the known liquidity pools to better classify real users versus blockchain infrastructure 

Table: dex_solana.trades
  
*/

WITH
params AS(
    SELECT 
        'METAwkXcqyXKy1AtsSgJ8JiUHwGCafnZL38n3vYmeta' AS address,
        DATE '2025-08-15' AS start_date
),

lps AS(
    SELECT DISTINCT 
        trade_source,
        project_program_id,
        project_main_id, 
        MIN(tx_id) AS example_txn
    FROM dex_solana.trades
    WHERE (token_bought_mint_address = (SELECT address FROM params)
       OR token_sold_mint_address = (SELECT address FROM params))
       AND block_date BETWEEN (SELECT start_date FROM params) AND CURRENT_DATE
    GROUP BY 1,2,3
        
)

SELECT *
FROM lps
