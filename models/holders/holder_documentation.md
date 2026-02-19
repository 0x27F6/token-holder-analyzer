Overview
What this model produces, why it exists, and what questions it answers that the base layer cannot.
Dependencies
Reads entirely from the materialized base model — worth documenting explicitly.
Output Schema
All 20 output columns — the flow metrics and velocity columns especially need clear definitions since the naming isn't self-evident to an outside reader.
Key Concepts
A dedicated glossary section before the model logic since several terms need to be defined before the CTE walkthrough makes sense — acquired, churned, net change, holder velocity, gross velocity, normalized velocity.
Model Logic

params / base_data — why params are pulled from the base model rather than redefined
As mentioned in the `base_model_documentation.md` params are crystallized in the materialized base model to be used downstream for a few reasons:
1. they are used regularly across models, if one wants to adjust the token threshold it can be done by one input upstream rather than hunting down
every instance of token treshold in each model and adjusting accordingly.
2. it allows for portability. If once wants to see how counts change when the token treshold is adjusted it takes one model upstream. The downside
is you may be recalling the expensive query upstream to answer something downstream. However, this can be circumvented by adusting the earliest instance
of threshold in the downstream. For example, in this model if I want to see how holder counts adjust when I change token threshold I can simply redefine
the threshold param no problem. This allows us to maintain consistency while retaining flexibility.



calendar — what a calendar spine is and why it's necessary
### `calendar`

In order to fully track wallet-level threshold transitions, a calendar spine is necessary.
It acts as a scaffold for each day of the token's existence, defined by `start_date` and
`end_date` with an interval of one day. If a token has existed since October 18th 2024,
the calendar spine will contain one row for every day from `2024-10-18` through the defined
`end_date`.

This spine is the required foundation for point-in-time state reconstruction. Rather than
only producing rows on days where balance events occurred, joining against the calendar
ensures every day is represented — including days where a wallet's balance was unchanged.
The mechanism for this join and why a simple LEFT JOIN is insufficient is covered in the
`holder_count` section below.

holder_count — the correlated subquery pattern, why it's needed, and what it solves that a simple GROUP BY cannot

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

To linchpin of the aforementioned counting logic the the AS-OF correlated subquery that's been danced around. Without a calendary spine for 
every day x wallet combinations daily state cannot be reconstructed which is what the query below does.

#### The AS-OF Correlated Subquery

Blockchains do not store a balance for every wallet for every day — only days where a balance
event occurred. However, accurate holder counting requires knowing each wallet's balance on
every day, including days with no activity. If Mia's last balance event was Monday, she still
needs to be counted as a holder on Tuesday and Wednesday if her balance remains nonzero.

A standard join against the calendar spine would produce no row for Mia on days she emitted
no event. The AS-OF correlated subquery solves this by finding each wallet's most recent known
balance on or before each calendar day:

```
JOIN base_data bd
    ON bd.day = (
        SELECT MAX(bd2.day)
        FROM base_data bd2
        WHERE bd2.owner = bd.owner
          AND bd2.day <= c.day
    )
```

For each calendar day `c.day`, and for each owner in `base_data`, the subquery finds the
most recent day that owner emitted a balance event up to and including the current calendar
day. This effectively forward-fills each wallet's last known balance across days with no
activity, reconstructing continuous state from sparse events.

The outer `WHERE bd.eod_balance > 0` then cuts any wallet whose most recently known balance
is zero — removing them from the holder count for that day. Once a wallet zeros its balance
it stops being counted until a new entry event is observed.




wallet_flows — threshold crossing detection logic, how acquired and churn are defined
wallet_flows_agg — reaggregation pattern and why it's structured this way
daily_holder_summary — join logic and COALESCE handling
velocity — directional vs gross velocity and what each measures
velocity_normalized — the 30-day rolling median baseline, APPROX_PERCENTILE, what normalized values mean
