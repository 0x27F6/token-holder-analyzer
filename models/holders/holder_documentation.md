# Model Documentation: Holder Lifecycle & Transition Analytics

## Overview

This model produces daily holder counts with full wallet type decomposition, acquisition and
churn flows, and velocity metrics that normalize turnover against a rolling baseline. It
answers questions the base layer cannot — specifically, how the holder base evolves over time,
which wallet types are driving growth or decline, and whether current turnover is abnormal
relative to recent history.

---

## Dependencies

Reads entirely from the materialized base model:

- `dune.research_onchain.result_meta_dao_model_base`

No raw tables are queried directly. All classification, filtering, and parameter definitions
are inherited from the base layer.

---

## Output Schema

| Column | Type | Description |
|---|---|---|
| `day` | date | Calendar day |
| `all_holders` | bigint | All wallets with a nonzero balance as of this day |
| `threshold_holders` | bigint | Wallets with a balance at or above the defined threshold |
| `trader_holders` | bigint | `dex_trader` wallets at or above threshold |
| `lp_holders` | bigint | `lp` wallets at or above threshold |
| `transfer_holders` | bigint | `transfer_only` wallets at or above threshold |
| `acquired` | bigint | Wallets that crossed above threshold today |
| `churn` | bigint | Wallets that dropped below threshold today (negative value) |
| `net_change` | bigint | `acquired + churn` — net holder base change for the day |
| `trader_acquired` | bigint | `dex_trader` wallets acquired today |
| `trader_churn` | bigint | `dex_trader` wallets churned today |
| `lp_acquired` | bigint | `lp` wallets acquired today |
| `lp_churn` | bigint | `lp` wallets churned today |
| `transfer_acquired` | bigint | `transfer_only` wallets acquired today |
| `transfer_churn` | bigint | `transfer_only` wallets churned today |
| `holder_velocity` | double | `net_change / threshold_holders` — directional pressure on the holder base |
| `gross_holder_velocity` | double | `(acquired + churn) / threshold_holders` — total turnover regardless of direction |
| `velocity_normalized` | double | `holder_velocity` relative to 30-day rolling median. 1.0 = baseline |
| `gross_velocity_normalized` | double | `gross_holder_velocity` relative to 30-day rolling median. 1.0 = baseline |
| `baseline` | integer | Constant value of 1, used as a reference line in visualizations |

---

## Key Concepts

**Acquired** — a wallet whose `prev_balance` was NULL, zero, or below threshold and whose
`eod_balance` is above threshold. Represents a new entry into the holder universe for that day.

**Churn** — a wallet whose `prev_balance` was above threshold and whose `eod_balance` is at
or below threshold. Represents an exit. Stored as a negative value so it can be summed
directly with `acquired` to produce `net_change`.

**Net change** — `acquired + churn`. Positive means the holder base grew, negative means it
contracted.

**Holder velocity** — `net_change / threshold_holders`. A directional ratio expressing the
rate of growth or decline relative to the existing holder base. Positive = growth pressure,
negative = contraction pressure.

**Gross holder velocity** — `(|acquired| + |churn|) / threshold_holders`. Measures total
turnover regardless of direction. A day with 50 entries and 50 exits has zero net change but
high gross velocity — the holder base is churning even if the count is flat.

**Normalized velocity** — current velocity divided by the 30-day rolling median. A value of
1.0 represents baseline activity. Values above 1.0 indicate abnormally high turnover relative
to recent history, values below 1.0 indicate suppressed activity. Used for regime detection.

---

## Model Logic

### `params` / `base_data`

Parameters are pulled directly from the materialized base model rather than redefined here.
As documented in `base_model_documentation.md`, this design allows a single upstream change
to propagate through all dependent models. If the threshold or date range needs adjustment it
can be changed once in the base layer.

That said, parameters can be overridden locally in this model without touching the base layer.
For example, if one wants to see how holder counts shift at a different threshold, the param
can be redefined here directly. This allows consistency to be maintained as a default while
retaining flexibility for exploratory analysis.

`base_data` selects only the columns required by this model — `day`, `owner`, `eod_balance`,
`prev_balance`, and `wallet_type` — keeping the working dataset lean.

---

### `calendar`

In order to fully track wallet-level threshold transitions, a calendar spine is necessary.
It acts as a scaffold for each day of the token's existence, defined by `start_date` and
`end_date` with an interval of one day. If a token has existed since October 18th 2024, the
calendar spine will contain one row for every day from `2024-10-18` through the defined
`end_date`.

This spine is the required foundation for point-in-time state reconstruction. Rather than
only producing rows on days where balance events occurred, joining against the calendar
ensures every day is represented — including days where a wallet's balance was unchanged.
The mechanism for this join and why a simple LEFT JOIN is insufficient is covered in the
`holder_count` section below.

---

### `holder_count`

Before examining the counting logic it is worth understanding the AS-OF correlated subquery
that powers it. This will be the most complex topic in this section of the docs — the entire
model is contingent on it.

The goal of this model is to track holder counts by correctly identifying when a wallet's
balance has crossed above zero, classifying it as a holder, or been zeroed out, removing it
from the holder count. To do this accurately, three pieces of information are required for
each wallet on each day:

- What was their previous balance state
- What is their current balance state
- Are they holding a non-zero or zero balance in either state

These three pieces of information allow us to reconstruct each wallet's holding status over
time. The following example walks through a single wallet's lifecycle.

Mia first buys 100 tokens on `2024-10-20`. She holds for one month until `2024-11-20`. Over
that period her balance oscillates — sometimes increasing, sometimes decreasing — but she
never registers an EOD snapshot of zero. Her threshold transitions are as follows:

1. **Entry Event — `2024-10-20`**
   - `prev_balance` is NULL (she has never interacted with the token prior)
   - `eod_balance` = 100

2. **Quiet Period**
   - No threshold transitions are recorded. For each day in this period she is counted
     as a holder regardless of how many balance events she emits.

3. **Exit Event — `2024-11-20`**
   - `prev_balance` is non-zero (could be 100, could be 0.01)
   - `eod_balance` = 0

There may be hundreds or thousands of balance events for a given wallet in any time frame,
but this model is only concerned with threshold transitions. A holder is defined as any
wallet with a token balance greater than zero. A non-holder is a wallet that previously held
and has since zeroed their balance. Holder states in this model are binary — this model does
not count all wallets that have never interacted with the token, only those that have
interacted and signaled exit by zeroing their holdings.

#### The AS-OF Correlated Subquery

Blockchains do not store a balance for every wallet for every day — only days where a balance
event occurred. However, accurate holder counting requires knowing each wallet's balance on
every day, including days with no activity. If Mia's last balance event was Monday, she still
needs to be counted as a holder on Tuesday and Wednesday if her balance remains nonzero.

A standard join against the calendar spine would produce no row for Mia on days she emitted
no event. The AS-OF correlated subquery solves this by finding each wallet's most recent
known balance on or before each calendar day:

```sql
JOIN base_data bd
    ON bd.day = (
        SELECT MAX(bd2.day)
        FROM base_data bd2
        WHERE bd2.owner = bd.owner
          AND bd2.day <= c.day
    )
WHERE bd.eod_balance > 0
```

For each calendar day `c.day`, and for each owner in `base_data`, the subquery finds the
most recent day that owner emitted a balance event up to and including the current calendar
day. This effectively forward-fills each wallet's last known balance across days with no
activity, reconstructing continuous state from sparse events.

The outer `WHERE bd.eod_balance > 0` cuts any wallet whose most recently known balance is
zero — removing them from the holder count for that day. Once a wallet zeros its balance it
stops being counted until a new entry event is observed.

The AS-OF pattern then allows holder counts to be derived in a single pass. `COUNT(DISTINCT bd.owner)`
counts all wallets with a nonzero balance. `COUNT(DISTINCT CASE WHEN bd.eod_balance >= threshold THEN bd.owner END)`
counts only those at or above the defined threshold. This `CASE WHEN` logic is further
extended across each `wallet_type` — `dex_trader`, `lp`, and `transfer_only` — to produce
the per-type holder counts. For full details on how wallet types are defined see
[base_model_documentation.md](https://github.com/0x27F6/token-holder-analyzer/blob/main/models/base_framework/base_model_documentation.md).

---

### `wallet_flows`

`wallet_flows` detects threshold crossings from `base_data` directly — it does not use the
calendar spine or the AS-OF subquery. It operates on days where balance events actually
occurred, identifying wallets that crossed the threshold in either direction on that day.

**Acquired** is flagged when `prev_balance` is NULL or at or below threshold and `eod_balance`
is above threshold — the wallet has just entered the holder universe. **Churn** is flagged
when `prev_balance` is above threshold and `eod_balance` is at or below threshold — the
wallet has just exited. Churn is stored as -1 so it subtracts naturally when summed against
acquired.

Results are grouped by `day` and `wallet_type` so each type's contribution to daily flows is
preserved for the aggregation step.

---

### `wallet_flows_agg`

`wallet_flows_agg` reaggregates `wallet_flows` from one row per `(day, wallet_type)` into
one row per `day`. Total acquired and churn are summed across all wallet types, and
per-type breakdowns are pivoted into individual columns using
`SUM(CASE WHEN wallet_type = '...' THEN acquired ELSE 0 END)`.

This produces a single wide row per day that can be joined cleanly onto `holder_count` in
the next step.

---

### `daily_holder_summary`

`daily_holder_summary` joins `holder_count` and `wallet_flows_agg` on `day` and assembles
the final set of count and flow columns. The join is a LEFT JOIN from `holder_count` — the
calendar spine guarantees a row for every day, while `wallet_flows_agg` only has rows on
days where threshold crossings occurred. `COALESCE(..., 0)` handles days with no flow
events, replacing NULL with zero so downstream arithmetic is not disrupted. `net_change` is
computed here as `acquired + churn`.

---

### `velocity`

`velocity` normalizes holder flows against the size of the holder base to produce two
complementary ratios.

**`holder_velocity`** divides `net_change` by `threshold_holders`. This is a directional
metric — positive values indicate the holder base is growing relative to its size, negative
values indicate contraction. A net change of +10 on a base of 1000 holders is a much weaker
signal than +10 on a base of 50.

**`gross_holder_velocity`** divides the sum of absolute acquired and absolute churn by
`threshold_holders`. This captures total turnover regardless of direction. A day where 100
wallets enter and 100 exit has a `holder_velocity` of zero but a meaningful
`gross_holder_velocity` — the holder base is highly active even though the count is unchanged.

`NULLIF` guards against division by zero on days with no threshold holders.

---

### `velocity_normalized`

`velocity_normalized` expresses each velocity metric relative to a 30-day rolling median
baseline using `APPROX_PERCENTILE(..., 0.5)` as a window function.

```sql
gross_holder_velocity /
    NULLIF(
        APPROX_PERCENTILE(gross_holder_velocity, 0.5) OVER(
            ORDER BY day
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ), 0)
```

The window looks back 30 days excluding the current day, computing the median gross velocity
over that period. Dividing the current day's velocity by this median produces a normalized
value where 1.0 represents baseline activity. Values above 1.0 indicate turnover is elevated
relative to recent history, values below 1.0 indicate suppressed activity. This is used for
regime detection — identifying periods of abnormally high or low holder churn that may
warrant further investigation.

`APPROX_PERCENTILE` is used rather than exact median for performance reasons. On large
datasets exact median computation is expensive; the approximation is sufficient for regime
detection purposes.

The first 30 days of the analysis window will have insufficient lookback to produce a stable
baseline and normalized values during this period should be interpreted with caution.

---

## Known Limitations & Edge Cases

- The AS-OF correlated subquery is computationally expensive at scale. Performance degrades
  as the number of unique owners and calendar days grows. The materialized base layer
  mitigates this by pre-filtering to the token and date range of interest.

- Velocity normalization requires 30 days of prior history to produce a meaningful baseline.
  Normalized values in the first 30 days of the analysis window are unreliable.

- `wallet_flows` operates on EOD snapshots only. A wallet that crosses the threshold intraday
  and returns below it before EOD will not register a flow event, consistent with the EOD
  resolution tradeoff documented in the base model.

- Churn is defined as crossing below threshold, not zeroing out entirely. A wallet that drops
  from above threshold to below threshold but retains a nonzero balance is counted as churned
  for the purposes of this model.

---

## Downstream Dependencies

_To be completed._
