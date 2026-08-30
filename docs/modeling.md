# Data model

Phase 0 deliverable. **Write the TODO sections by hand.** They are the decisions
this whole project exists to be able to defend. A version generated for you is
worth nothing in an interview.

No dbt model or DDL gets written until the TODOs below are gone.

---

## Settled

### Unit of work: `(competition_id, season_id)`

StatsBomb open data is a static historical dump, not a daily feed. Nothing
arrives tomorrow. The unit the data actually arrives in is a competition-season,
so that is the DAG's parameter and the thing "backfill" refers to.

Rejected: a calendar-date parameter. It would mimic a conventional daily batch,
but the cadence would be fictional and most run dates would contain zero
matches.

### Fact structure: core fact plus subtype facts

StatsBomb events are heterogeneous. A shot carries xG, body part, technique, and
a freeze frame. A pass carries length, angle, height, recipient. A carry carries
an end location. One table holding all of it is mostly nulls.

- `fct_event`, the columns every event shares
- `fct_shot`, `fct_pass`, and later siblings, type-specific measures

Rejected: one wide fact with nullable type columns. Defensible on BigQuery's
columnar storage, since unqueried columns cost nothing to scan, but it
demonstrates less. Rejected: nested `ARRAY<STRUCT>` payloads, the most
BigQuery-native option and the least portable to a Snowflake or Redshift
interviewer.

### Fact partitioning: `match_date`

Deliberately decoupled from the unit of work. Partitioning serves query pruning,
the DAG parameter serves scheduling. Conflating them is the mistake.

Clustering keys: TODO, after the Phase 1 counts below.

---

## TODO, write these by hand

### Fact grain

State it so that "one row in `fct_event` means exactly \_\_\_" has one unambiguous
completion. If you can complete the sentence two ways, the grain is not decided.

> TODO

### Dimensions

For each of `dim_player`, `dim_team`, `dim_match`, `dim_competition`,
`dim_date`, `dim_event_type`: grain, primary key, Type 1 or Type 2, and why.

> TODO

Worth resolving specifically: does `dim_player` need SCD Type 2? Players change
clubs between seasons, which is the textbook trigger. But StatsBomb puts
`team_id` on the event, so team affiliation arrives from the fact, not the dim,
which may make Type 1 sufficient. Check whether anything else about a player
changes over time and matters here, jersey number or position, before letting
the `team_id` answer close the whole question. "I checked and did not need it"
is a much stronger answer than never having looked.

### Surrogate keys

> TODO

Leaning hashed natural keys (`dbt_utils.generate_surrogate_key`) over generated
integers. Hashing is deterministic, so a re-run produces identical keys, which
is what makes the idempotency claim hold. Generated integers need a
lookup-and-merge pattern that breaks on re-run, and BigQuery has no sequence
that behaves well under a distributed rebuild.

Answer the prior question first. StatsBomb already ships durable natural keys,
event UUIDs and integer player, team, and match ids. So why introduce a
surrogate at all? The standard answers are insulation from source-system key
changes and SCD Type 2 support. Decide which, if either, actually applies here.

### Freeze frames

> TODO

A shot's freeze frame is an array of every player's position at the moment of
the strike. It does not fit the event grain. Either its own fact at
one-row-per-player-per-shot, or cut from v1, with the reason written down.

If you cut it, have the answer ready for "why does your model drop one of
StatsBomb's signature data points?"

### Reporting layer shape

> TODO

Once the fact is split into core plus subtypes, decide whether `rpt_` models
pre-join across the split or whether the dashboard joins. Looker Studio joining
across two large tables on every filter change is the expensive answer. Write
down which you chose.

---

## Phase 1 measurements

Fill in from the extract. These numbers drive the clustering and incremental
decisions above, and they are what gets asked for when someone says "how big was
it?" Measure before designing for scale.

| Metric                             | Value |
| ---------------------------------- | ----- |
| Competitions / seasons available   | TODO  |
| Matches                            | TODO  |
| Events (total)                     | TODO  |
| Events per match (median)          | TODO  |
| Raw JSON bytes in GCS              | TODO  |
| BigQuery logical bytes after load  | TODO  |
| Full-refresh `dbt build` wall time | TODO  |
| Full-refresh bytes billed          | TODO  |
