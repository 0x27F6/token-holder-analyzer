/*
Purpose: Isolate all verified liquidity pool(LP) infrastructure to further attribute real holders versus infrastructure. Creates a list
of distinct wallet adressed linked to LP:Token pairs. 

DUNE: https://dune.com/queries/6703395

Tables:
  1. dex_solana.trades

Dependencies:
  None (standalone)

Output:
  trade_source: where the trade came from. Direct swap versus aggregator transaction.
  project_program_id: wallet address linked to the individual LP token pairs. 
  project_main_id: owner program of each individual token pair LP. 
  example_txn: an example txn_id to verify they data on solscan or other block explorers.

Note: After some validation it appears project_program_id is what appears at the balance level. It's the main datapoint we are looking to pool. The
other columns are produced for validation, debugging, and futute flexibility. For example if I wanted to filter out all known LP infrastructure 
I would use project_main_id instead of project_program_id. The result would be me filtering out all Raydium(a service provider) owned transactions
rather than a specific pool.
  
*/

WITH
params AS(
    SELECT 
        'METAwkXcqyXKy1AtsSgJ8JiUHwGCafnZL38n3vYmeta' AS address,
        DATE '2025-08-15' AS start_date
),

-- Pull every unique LP infrastructure combination that has facilitated a trade for this token
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
