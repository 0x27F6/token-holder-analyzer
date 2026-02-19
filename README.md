# token-holder-analyzer

# Advanced On-Chain Behavioral Modeling for High-Throughput Blockchains

This repository implements principled, sparsity-aware models for analyzing wallet behavior on Solana-based tokens. It addresses core challenges in ledger data: event-driven updates, extreme sparsity, and prohibitive state reconstruction costs.

The approach separates lightweight inference from precise historical state tracking, enabling accurate attribution, cohort analysis, and behavioral segmentation — techniques broadly applicable to event-sourced systems in finance, gaming, and user analytics.

Built primarily on Dune Analytics using optimized SQL against compressed daily balance tables.

---

## Blockchain Data Primer

Most blockchain dashboards rely on naïve aggregation strategies that break down when applied to decentralized ledgers.

Blockchain data is **event-rich but state-sparse**.

On Dune Analytics, tables do not store a balance for every wallet, token, and day. This is intentional, for two fundamental reasons:

1. Blockchains operate as real-time event streams validated across decentralized nodes. Only events — transfers, mints, burns — are natively recorded.
2. Storing full daily state across all wallets, tokens, and time would require **O(U × D × T)** storage and compute complexity, which is infeasible at chain scale.

Blockchains are best represented as **ledgers of events**, not snapshots of state.

A wallet may interact with many tokens over time, but only the token involved in a given transaction receives a balance update. Most wallet–token pairs remain unchanged for long periods, or indefinitely.

This creates a dataset with:

- Extremely sparse state updates
- Highly uneven activity distribution across wallets
- Long-tailed participation patterns

---

## Implications for Analysis

This structure introduces several analytical challenges:

- Naïve aggregation cannot correctly reconstruct historical balances
- Most recorded events are irrelevant to any specific analytical question
- Wallets are pseudonymous, so inference must be behavior-based rather than identity-based
- Strong invariants must be enforced to preserve model correctness
- Storage and compute costs escalate rapidly when state is reconstructed improperly

---

## Data Sources

### `solana_utils.daily_balances`:

Dune Link: https://dune.com/data/solana_utils.daily_balances

The primary data source for this project. This table compresses intra-day balance deltas per wallet into a single end-of-day (EOD) balance snapshot. It captures every transfer delta without requiring full ledger reconstruction, and serves as the foundation for all state-dependent models.

### `dex_solana.trades`: 
Dune Link: https://dune.com/data/dex_solana.trades

Used for trader attribution and liquidity pool identification. Any wallet appearing in this table has, by definition, executed a swap via a DEX or AMM — an unambiguous signal of trading intent. Because `solana_utils.daily_balances` captures all balance deltas but carries no intent signal, cross-referencing with `dex_solana.trades` enables separation of trading activity from transfers, airdrops, and other non-market interactions.

Known liquidity pool addresses were identified through this table and excluded from holder analysis to prevent LP positions from distorting supply distribution metrics and behavioral classifications.

Prior to adopting `solana_utils.daily_balances`, early iterations relied on `solana.transfers` — a complete archive of all Solana transfer events. That approach required reconstructing daily balances by computing deltas for every interacting wallet and compressing them into EOD snapshots. Due to high computational cost, `solana.transfers` was reduced to being no more than an honorable mention in this section. However, for a more robust and well labeled model `solana.transfers` would be invaluable. Empirically, differences between this reconstruction and `daily_balances` were minimal and primarily attributable to snapshot timing.

---

## Modeling Approach

This project answers different behavioral questions using two purpose-built model types.

### Model Types

**1. State Inference Model**

- Lightweight
- Infers holder counts from events that cross a defined balance threshold
- Does not store balances
- Path-dependent; suitable for approximate counts and trend detection

**2. Dense Continuous Model**

- Heavyweight
- Uses AS-OF logic to forward-fill wallet balances from their last recorded update
- Tracks the full evolution of holder balances over time
- Required for holder classification, cohort analysis, and behavioral segmentation

This approach enables:

- Correct as-of attribution using last-known balances
- Accurate holder classification and cohort analysis
- Clean separation of state estimation from behavioral modeling
- Significantly lower marginal cost for additional downstream metrics

---

## What This Answers

- How many economically relevant wallets are holding a token
- How supply is distributed across distinct holder classes
- Whether the holder base is expanding or contracting, and at what rate
- Which wallets are actively accumulating or distributing
- How much supply is held versus actively traded

## Live Dashboard

https://dune.com/research_onchain/spl-token-analyzer

## Author

Indigo Yuzna (LinkedIn)
@shageuh on X
@0x27F6 Git 
@research_onchain Dune 
