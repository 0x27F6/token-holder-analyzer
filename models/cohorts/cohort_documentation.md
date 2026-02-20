Overview
What this model produces and why it exists — supply distribution analysis across size-based cohorts, wallet type, and supply age. 
What questions it answers that the holder count model cannot.


Dependencies
Reads entirely from the materialized base model.
Dune link : https://dune.com/queries/6683290
Git link: 

Output Schema
All 23 output columns across three logical groups — cohort balances/percentages, supply age buckets, and threshold/wallet type supply splits.
Key Concepts

Dedicated glossary before model logic — these need defining upfront:

Segment — a single continuous holding period for a wallet. A wallet that buys, sells completely, and buys again has two distinct segments.

Segment ID — how segments are identified and why the cumulative SUM entry flag approach works

Supply age — how long a token has been held within the current segment, not the wallet's lifetime

Size cohorts — krill / fish / dolphin / whale definitions and what percentage thresholds mean

Observed supply — why supply percentages are calculated against observed supply rather than total supply

Model Logic
# Model Documentation: Cohort & Supply Distribution Analytics

---

## Model Logic

### `transition_labels`

`transition_labels` exists to define when a wallet has entered a holding state. To describe
this succinctly, two states and two state transitions are defined.

#### States

**Non-holder** — an `owner` whose `eod_balance` is zero. In the broader blockchain universe,
most wallets are non-holders. Non-holder state is inferred either by the absence of a record
in `solana_utils.daily_balances` or by the presence of a recorded balance of zero.

**Holder** — an `owner` whose `eod_balance` is non-zero.

#### State Transitions

**Non-holder → Holder** — a transition from a `prev_balance` of zero or NULL to an
`eod_balance > 0`. If an owner's previously recorded balance was zero or absent and their
current EOD snapshot is nonzero, they have entered the holding state. This is flagged as
`is_entry = true`.

**Holder → Non-holder** — the inverse. An owner whose `prev_balance > 0` and
`eod_balance = 0` has exited the holding state.

#### The State Reconstruction Problem

The goal of this model is to reconstruct the EOD balance for each wallet for each day they
were holding. To illustrate why this is non-trivial, consider May — a trader who has been
trading the token's volatility using the time-tested buy-high, sell-low method. Her recorded
balance events look like this:

| day | prev_balance | eod_balance |
|---|---|---|
| 2024-10-01 | NULL | 500 |
| 2024-10-15 | 500 | 0 |
| 2024-11-03 | 0 | 250 |
| 2024-11-22 | 250 | 0 |
| 2024-12-10 | 0 | 750 |
| 2024-12-31 | 750 | 750 |

May first entered on `2024-10-01` and held for 14 days until `2024-10-15`. Her data is event
sparse — there is no row to query between recorded events. To know her status on an individual
day like October 8th, a continuous state must be reconstructed from her transitions.

This is the core problem this model solves — not just for one wallet, but across an entire
population. Populations ranging from a few thousand transient users to hundreds of thousands
or more. Each wallet's state must be reconstructed continuously for every day they were
holding.

To do this efficiently, two things must be avoided. First, carrying forward zero balances for
inactive wallets — if a wallet is in non-holder status, there is no analytical value in
explicitly storing a zero balance row for every dormant day. Second, a cartesian explosion of
`owner × day` where every wallet is expanded across every calendar day regardless of whether
they were ever holding.

Both are addressed through entry flagging and segmentation:

1. Entry events are flagged via `is_entry`. If an owner was in non-holder status and
transitioned to holder status, that day is flagged `is_entry = true`. This marks the
beginning of a new holding period.

2. `is_entry` enables segmentation. If a wallet has multiple entry events it means they
exited and re-entered — transitioning from holder to non-holder and back. Each distinct
holding period is assigned a unique `segment_id`, explained in full in the next section.

---

### `segment_ids`

`segment_ids` assigns each row a `segment_id` that identifies which distinct holding period
it belongs to. This is the mechanism that allows the model to track multiple independent
holding periods per wallet without conflating them.

The approach uses a cumulative SUM window function over the `is_entry` flag:

```sql
SUM(CASE WHEN is_entry THEN 1 ELSE 0 END) OVER (
    PARTITION BY owner
    ORDER BY day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS segment_id
```

For each owner, this sums the number of entry events seen up to and including each row.
On the first entry event the sum becomes 1 — `segment_id = 1`. During the quiet holding
period that follows, no new entry events occur so the sum stays at 1. When the wallet exits
and re-enters, a new `is_entry = true` increments the sum to 2 — `segment_id = 2`. Each
subsequent re-entry produces a new segment ID.

Using May as the example:

| day | prev_balance | eod_balance | is_entry | segment_id |
|---|---|---|---|---|
| 2024-10-01 | NULL | 500 | true | 1 |
| 2024-10-15 | 500 | 0 | false | 1 |
| 2024-11-03 | 0 | 250 | true | 2 |
| 2024-11-22 | 250 | 0 | false | 2 |
| 2024-12-10 | 0 | 750 | true | 3 |
| 2024-12-31 | 750 | 750 | false | 3 |

May has three distinct holding segments. Each can be analyzed independently — its own start
date, end date, length, and balance trajectory. Without segment IDs, her three holding periods
would be indistinguishable from one another and any supply age or cohort calculation would
be incorrect.

Note that rows where `eod_balance = 0` also carry a `segment_id`. These exit rows are
necessary to mark the end of a segment and are handled in `segment_exit_days` and
`segment_bounds` — covered in the next section.


### `segment_exit_days`

Segments are now identified and numerically labeled, but there is still a missing bound.
The goal of this model is to reconstruct the balance state of each owner using `LAST_VALUE`
forward-fill, covered in detail in the `wallet_segment_spine_filled` section. However,
`LAST_VALUE` will fill indefinitely unless explicitly bounded. `segment_exit_days` provides
that bound — the start of each segment is already known, this CTE derives the end.
```sql
segment_exit_days AS (
    SELECT
        owner,
        segment_id,
        MIN(day) AS exit_day
    FROM segment_ids
    WHERE eod_balance = 0
      AND prev_balance > 0
    GROUP BY owner, segment_id
)
```

The query filters for the Holder → Non-holder transition — `eod_balance = 0` and
`prev_balance > 0` — then groups by `owner` and `segment_id` to find the exit day for each
holding period. `MIN(day)` is used as a precaution against edge cases where tight timeframe
state transitions could produce overlapping segment rows. Segment IDs should be mutually
exclusive and well-bounded by design, but `MIN` ensures the earliest valid exit is always
selected. This is good hygiene rather than a structural requirement.

Segments without an exit day are open — the wallet is still holding as of `end_date`. These
are handled in `segment_bounds` via `COALESCE` to `end_date`, covered next.




/ segment_bounds — how segment start and end are determined, COALESCE to end_date for open segments

### `segment_bounds`

`segment_bounds` assembles the complete time boundary for each holding segment —
`segment_start`, `segment_end`, and `segment_length_days`.

`segment_start` is derived as `MIN(s.day)` per `owner` and `segment_id`, which is the first
day of each holding period. `segment_end` is the exit day from `segment_exit_days`, joined
via LEFT JOIN on `owner` and `segment_id`. The LEFT JOIN is intentional — segments without
a matching exit day are still holding as of the analysis window. `COALESCE` handles these
open segments by substituting `end_date` from params as the segment boundary.
```sql
COALESCE(e.exit_day, (SELECT end_date FROM params)) AS segment_end
```

`DATE_DIFF` then computes `segment_length_days` between `segment_start` and `segment_end`,
giving the total duration of each holding period in days.

`e.exit_day` appears in the `GROUP BY` clause alongside `owner` and `segment_id`. This is
required by the aggregation but does not affect the grouping in practice — each
`segment_id` per `owner` has exactly one exit day by construction, so including it in the
`GROUP BY` produces the same result as grouping on `owner` and `segment_id` alone.

The output of this CTE is one row per holding segment per owner — a complete map of every
discrete holding period with its start, end, and length, ready to be expanded across the
calendar spine.

### `calendar`

The calendar spine functions identically to the one documented in
`holder_count_model_docs.md` — a complete sequence of days from `start_date` to `end_date`
that ensures every day is represented regardless of whether balance events occurred.

The key difference in this model is how the spine is joined. Rather than using the AS-OF
correlated subquery to forward-fill balances across the full population, the calendar here
is joined against `segment_bounds` — expanding each wallet's holding period across only the
days it was active. This bounds the expansion to each segment's `segment_start` and
`segment_end`, avoiding the `owner × day` cartesian explosion described in
`transition_labels`.

bounded_wallet_expansion — expanding each segment across its active days

### `bounded_wallet_expansion`

`bounded_wallet_expansion` joins the calendar spine against `segment_bounds` to expand each
holding segment across every day it was active. The result is one row per owner per day per
segment, bounded by `segment_start` and `segment_end`.

| day | owner | segment_id | segment_start | segment_end | segment_length_days |
|---|---|---|---|---|---|
| 2024-10-20 | May | 1 | 2024-10-20 | 2024-10-30 | 10 |
| 2024-10-21 | May | 1 | 2024-10-20 | 2024-10-30 | 10 |
| 2024-10-22 | May | 1 | 2024-10-20 | 2024-10-30 | 10 |
| 2024-10-20 | Andy | 1 | 2024-10-20 | 2024-11-15 | 26 |
| 2024-10-21 | Andy | 1 | 2024-10-20 | 2024-11-15 | 26 |
| 2024-10-22 | Andy | 1 | 2024-10-20 | 2024-11-15 | 26 |

Extended to the full history, May's segment would produce 10 rows and Andy's 26 — one per
day between their respective `segment_start` and `segment_end`. This is the scaffold the
forward-fill will operate on.

---

### `wallet_segment_spine`

The expanded scaffold is complete but balance data is still missing. `wallet_segment_spine`
resolves this by joining `wallet_balances` — the raw EOD balance per owner per day — onto
`bounded_wallet_expansion`. Days where a balance event occurred will have a real
`eod_balance`. Days with no activity will have NULL, which the forward-fill in the next
step resolves.

| day | owner | eod_balance | segment_id | segment_start | segment_end | segment_length_days |
|---|---|---|---|---|---|---|
| 2024-10-20 | May | 500 | 1 | 2024-10-20 | 2024-10-30 | 10 |
| 2024-10-21 | May | NULL | 1 | 2024-10-20 | 2024-10-30 | 10 |
| 2024-10-22 | May | NULL | 1 | 2024-10-20 | 2024-10-30 | 10 |
| 2024-10-20 | Andy | 250 | 1 | 2024-10-20 | 2024-11-15 | 26 |
| 2024-10-21 | Andy | NULL | 1 | 2024-10-20 | 2024-11-15 | 26 |
| 2024-10-22 | Andy | NULL | 1 | 2024-10-20 | 2024-11-15 | 26 |

The NULLs are expected — they represent days where no balance event was emitted, not days
where the balance was actually zero. The next CTE -- `wallet_segment_spine_filled` fills them forward using `LAST_VALUE`.


/ wallet_segment_spine_filled — the LAST_VALUE forward-fill approach and why it's used here instead of the AS-OF correlated subquery

valid_segments — why segment_id = 0 is excluded and what it represents
continuous_wallet_balance_state / wallet_day_segments / daily_balance_summary — deduplication logic and seg_rank
supply_threshold_delta — supply split by threshold and wallet type
wallet_supply_pct — why percentage is against observed supply
bucket_definitions / bucketed_wallets / wallet_cohorts — the JOIN bucketing approach and ROW_NUMBER deduplication
balance_by_cohort — pivoting cohort and wallet type supply into columns
supply_age / supply_age_buckets / supply_age_buckets_pivot — age calculation, bucket definitions, and pivot

Known Limitations & Edge Cases
Downstream Dependencies
