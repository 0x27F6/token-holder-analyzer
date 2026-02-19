# Model Documentation: Canonical Daily Wallet State

## Overview

This is the base data model upon which every other model depends. It leverages `solana_utils.daily_balances` as a daily snapshot of each observed wallet's balance changes. The source table only emits a row when a wallet's balance has changed, which can be utilized to reconstruct historical balances in downstream models. This model is parameterized to support analysis of any SPL token.

---

## Output Schema

| Column | Type | Description |
|---|---|---|
| `day` | date | The day a recorded balance change occurred |
| `owner` | varchar | The wallet address that emitted a balance change |
| `eod_balance` | double | The resulting end-of-day balance for the owner |
| `prev_balance` | double | The prior day's balance for the owner |
| `threshold_label` | varchar | `above_threshold` or `below_threshold` based on the defined threshold parameter |
| `wallet_type` | varchar | Classification of the wallet: `lp`, `dex_trader`, or `transfer_only` |
| `eod_price` | double | The last observed trade price for the token on that day, derived from `dex_solana.trades` |
| `start_date` | date | Analysis start date carried from params |
| `end_date` | date | Analysis end date carried from params |
| `threshold` | integer | Token threshold value carried from params |
| `total_supply` | integer | Known total token supply carried from params |
| `token_address` | varchar | Token contract address carried from params |

---

## Parameters

| Parameter | Description |
|---|---|
| `token_address` | The contract address of the token being analyzed. Every token and owner has a unique address on Solana. |
| `start_date` | The start of the observation window, typically the token's creation date. |
| `end_date` | The final day of the analysis. Can be the current date or any date after `start_date`. |
| `threshold` | An arbitrary balance threshold used to label wallets as above or below a minimum holding size. |
| `total_supply` | The known total number of tokens in existence. |

Parameters are scalar values cross-joined near the end of the query so they are carried through into the final output. The base layer exists in a materialized state — rather than scanning billions of rows on every query, the data of interest is sliced and frozen from the full dataset, greatly reducing computational cost. Parameters are included in the output at minimal additional compute cost to keep dependent models portable. Rather than updating parameters across every downstream query, a single change in the base layer propagates through all dependencies, enabling rapid analysis of any token.

---

## Model Logic

### `daily_state`

`solana_utils.daily_balances` emits a single row per `owner` for balance changes end of day. An owner might transact 100–200 times intraday, but the source table compresses all of that activity into one EOD snapshot. For example, if owner Andy had no prior balance and bought 100 tokens across 100 transactions, the only row emitted would be Andy with a balance of 100 tokens.

`daily_state` establishes the EOD balance per owner per day as the reference point for all downstream state reconstruction. `SUM(token_balance)` is applied as a precautionary measure against duplicate rows that could theoretically arise from upstream data anomalies or edge cases in how `solana_utils.daily_balances` emits snapshots, though under normal conditions a single row per owner per day is expected.

---

### `prev_state`

`prev_state` utilizes `LAG(eod_balance)` partitioned by `owner` and ordered by `day`. LAG retrieves the previous row's value within each owner's ordered time series, effectively attaching the prior day's balance to the current day's record.

This CTE exists to support accurate threshold crossing attribution downstream. Without a prior balance anchor, current state is uninterpretable — a balance of 100 tokens tells you nothing about whether the wallet just entered, accumulated, or has held for weeks. For example, if an owner has a `prev_balance` of 0 or NULL but a nonzero `eod_balance`, that event can be correctly classified as an entry. Previous state is a necessary precondition for inferring the meaning of any current observation.

---

### `state_filter`

`state_filter` addresses a specific edge case in `solana_utils.daily_balances`. A wallet with no prior recorded balance that buys and fully exits a position within the same day will still emit an EOD snapshot — recorded with a balance of zero. For example, Ryan has never interacted with the analyzed token. They buy 50 tokens via an AMM but reduce their holdings to zero before the EOD snapshot occurs. The source table records a zero balance for Ryan with no prior balance on record.

These are net zero events — wallets with no economic presence at EOD and no prior history with the token. Including them would introduce ghost participants into downstream models without any meaningful signal. This CTE eliminates that noise using `(eod_balance = 0 AND (prev_balance = 0 OR prev_balance IS NULL))`, filtering any record where both the current and prior balance are zero or absent.

---

### `lp_addresses`

`lp_addresses` exists because `solana_utils.daily_balances` is an accurate observer but a poor attributer. It will correctly record the EOD balance for every address that experienced a balance change on a given day, but carries no signal as to whether that address is a liquidity pool, a bot, or a human wallet. Without explicit exclusion, LP infrastructure would appear as holders and distort supply distribution and behavioral metrics.

By cross-referencing `dex_solana.trades` via `result_lp_finder`, each liquidity pool's associated addresses can be identified and excluded. Three address types are captured via UNION: `trade_source`, the pool address that appears as counterparty in swap events; `project_program_id`, the DEX program controlling the pool; and `project_main_id`, the primary program identifier for the AMM protocol. UNIONing all three ensures that any address playing an infrastructure role in any capacity is excluded.

The MetaDAO AMM (`CUPoiqkK4hx...`) was not labeled by Dune's standard infrastructure and required manual addition after its balance behavior could not be reconciled with any known wallet type during validation.

---

### `dex_traders`

`dex_traders` uses `dex_solana.trades` to label any wallet that has ever executed a swap via an AMM as a trader. Any wallet appearing in this table has, by definition, interacted with a DEX — making it a reliable heuristic for market participation.

This distinction matters because a large volume of balance changes in `solana_utils.daily_balances` originate outside of AMM activity. A wallet dispersing tokens triggers a balance change, but the source table carries no signal about where those tokens came from or how they were acquired. DEX labeling provides a meaningful lens into population behavior and supply movement without requiring full transaction attribution.

Importantly, the inverse is equally useful: wallets that have never appeared in `dex_solana.trades` must have received their supply through non-market means — transfers, airdrops, or OTC. This classification does not provide granular attribution, but it cleanly segments the holder population by market participation, which anchors several downstream behavioral metrics.

#### Limitations

- Does not distinguish between a wallet that traded once and one that trades continuously — both receive the same `dex_trader` label. Frequency and volume of DEX activity is captured in downstream behavioral models.
- Venue coverage is limited to what `dex_solana.trades` captures. Swaps routed through unlisted or novel AMMs may be missed, as was the case with the MetaDAO AMM which required manual identification.

> **Note:** If the full token history is not used, transfer attribution may be incomplete. Classification will be accurate within the observed window but cannot account for activity outside of it.

---

### `classified_state`

`classified_state` applies two orthogonal labels to each owner record: a threshold label and a wallet type classification.

The threshold label is straightforward — any owner with an `eod_balance` below the defined `threshold` parameter receives `below_threshold`, all others receive `above_threshold`. This allows downstream models to filter or segment by economically meaningful holding sizes.

Wallet type is assigned via LEFT JOIN with explicit precedence. LP status is checked first, DEX trader second. If neither matches, the wallet falls into the `transfer_only` residual class. See Classification Hierarchy below for full definitions.

---

### `end_day_price`

`end_day_price` derives a daily price for the token from `dex_solana.trades` by computing a per-trade spot price as `amount_usd` divided by token quantity. `MAX_BY(..., block_time)` selects the last observed trade of each day, using the most recent market-observed price before midnight as a proxy for EOD valuation. The `NULLIF` guard on the denominator prevents division by zero on dust trades. On low-volume days this price may be stale or unrepresentative — see Known Limitations.

---

## Classification Hierarchy

Wallet type is exclusive and ordered: `lp` → `dex_trader` → `transfer_only`.

**`lp`** — Infrastructure addresses. Excluded from all holder and behavioral analysis. Includes any address identified as a liquidity pool, AMM program, or pool counterparty. These are not economic participants in the traditional sense and their inclusion would misrepresent supply distribution.

**`dex_trader`** — Wallets with confirmed market participation. Has executed at least one swap via an AMM. May also hold, accumulate, or distribute but is distinguished by having interacted with the market directly.

**`transfer_only`** — Residual class. No observed DEX interaction within the analysis window. Supply was received via transfer, airdrop, or OTC. This is not a negative signal — some of the largest conviction holders may never touch a DEX. Requires careful interpretation.

**Why ordering matters** — an LP address could theoretically appear in `dex_solana.trades` as well. By checking LP membership first, infrastructure is always excluded before the trader label is applied, preventing any pool address from being misclassified as a participant.

---

## Known Limitations & Edge Cases

- A full granular reconstruction of the ledger is possible but prohibitively costly in compute and storage. The minimum viable daily-level snapshot was chosen to ensure accuracy within that lens. Per-event reconstruction and transfer attribution are out of scope for this model.

- Compressing all intraday deltas into a single EOD snapshot is a deliberate tradeoff for compute and storage efficiency. Intraday behavior — entry and exit within the same day — is not observable at this resolution.

- This model observes every actor within the defined `start_date` and `end_date` window. It is strongly recommended to use the full token history to ensure accurate observation of all owners. Partial windows will miss wallets that entered and exited outside the observed range, leading to incomplete classification.

- The `transfer_only` label indicates that no DEX interaction was observed for that wallet — not how the supply was originally received. More granular event-level data would be required for complete transfer attribution.

- EOD price derived from `dex_solana.trades` may be stale or unrepresentative on low-volume days. Price gaps should be handled in downstream models before any USD-denominated metrics are computed.

---

## Downstream Dependencies

_To be completed._

