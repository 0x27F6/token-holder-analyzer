# token-holder-analyzer

This project analyzes on-chain user behavior for tradeable tokens on the Solana network.
Rather than relying on surface-level metrics, it models wallet behavior using first principles derived from how blockchains actually store data.

## Blockchain Data Primer

Most blockchain dashboards use naïve aggregation strategies that work well for traditional financial datasets but break down in decentralized ledgers.

Blockchain data is **event-rich but state-sparse**.

On Dune (a public, GitHub-like analytics platform for blockchain data), tables do not store a user's balance for every wallet, token, and day. This is intentional, and for two fundamental reasons:

1. Blockchains operate as real-time event streams, validated across decentralized validators. Only events (transfers, mints, burns) are natively recorded.
2. Storing full daily state across all wallets, tokens, and time would require **O(U × D × T)** storage and compute complexity — which is infeasible at chain scale.

Instead, blockchains are best represented as **ledgers of events**, not snapshots of state.

A wallet may interact with many tokens over time, but only the token involved in a transaction receives a balance update. Most wallet–token pairs remain unchanged for long periods, or forever.

This creates a dataset with:

- Extremely sparse state updates
- Highly uneven activity across wallets
- Long-tailed participation patterns

---

## Implications for Analysis

This structure introduces several analytical challenges:

- Naïve aggregation cannot correctly reconstruct historical balances
- Most recorded events are irrelevant to any specific analytical question
- Wallets are anonymous, so inference must be behavior-based rather than identity-based
- Strong invariants must be enforced to preserve model correctness
- Storage and compute costs increase rapidly when state is reconstructed improperly

---

## Modeling Approach

To address these analytical challenges, this project answers different behavioral questions using three purpose-built models.

### Model Types

**1. State Inference Model**

- Lightweight
- Infers holder counts from events that cross a defined threshold
- Does not store balances
- Path dependent

**2. Bounded Continuous State**

- Medium weight
- Forward-fills user balances to their most recent transaction
- Does not track full balance history
- Well-suited for tracking daily flows
- Path independent

**3. Dense Continuous Models**

- Heavy weight
- Utilizes AS-OF logic to forward-fill user balances based on their last balance update
- Tracks the evolution of holder balances over time
- Required for holder classification and cohort analysis

This approach enables:

- Correct as-of attribution using last-known balances
- Accurate holder classification and cohort analysis
- Separation of state estimation from behavioral modeling
- Significantly lower marginal cost for additional metrics

The result is a dashboard that measures **holder conviction, supply dynamics, and behavioral regimes** — not just price movement.

---

## Data Utilized

The models in this project are primarily derived from the `solana_utils.daily_balances` table on Dune. This table compresses intra-day balance deltas per user into a single end-of-day (EOD) balance snapshot.

Prior to discovering this table, early iterations relied on `solana.transfers`, a complete archive of all transfer events on Solana. That approach required reconstructing daily balances by computing deltas for every interacting wallet and compressing them into EOD balances.

Due to the high computational cost, `solana.transfers` was only used for the state inference model. Empirically, the differences between this reconstruction and `daily_balances` were minimal and primarily attributable to snapshot timing differences.

---

## What This Answers

- How many economically relevant users are holding a token
- How supply is distributed across different classes of holders
- Whether the holder base is expanding or contracting, and at what rate
- Which users are actively accumulating or distributing
- How much supply is actively traded versus held

## Live Dashboard

## Author

Indigo Yuzna (LinkedIn)
@shageuh on X
@0x27F6 Git 
@research_onchain Dune 
