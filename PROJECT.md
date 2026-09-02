# StatsBomb Warehouse

StatsBomb open data lands in GCS, loads to BigQuery, is modeled into a star
schema with dbt, orchestrated by Airflow, and served through Looker Studio.

Portfolio project. The goal is to be able to defend every modeling decision in
an interview, not just to have working code.

## Stack

- **Extract**: Python 3.11, `statsbombpy`
- **Object storage**: Google Cloud Storage
- **Warehouse**: BigQuery (free tier)
- **Transform**: dbt-core with `dbt-bigquery`, `dbt_utils` for surrogate keys
- **Orchestration**: Apache Airflow, local via Astro CLI
- **CI/CD**: GitHub Actions, `dbt build` plus tests against a CI dataset on
  every PR
- **Dashboard**: Looker Studio, native BigQuery connector

Out of scope: Spark, Cloud Composer, Next.js. Airflow runs locally.

Looker Studio was chosen over a custom frontend on purpose. Pointing a BI tool
at pre-aggregated reporting tables is the pattern real analytics teams use, and
a bespoke React app over a warehouse is not. It also removes the auth layer,
the query layer, and the API routes, none of which this project exists to
demonstrate.

## Layout

```
dags/                    Airflow DAGs
  football_pipeline.py
extract/                 Python extraction to GCS
load/                    GCS to BigQuery external tables
dbt/
  models/
    staging/             1:1 with raw, light cleaning only
    marts/               dims and facts
    reporting/           small pre-aggregated tables Looker Studio reads
  tests/
  dbt_project.yml
  packages.yml
.github/
  workflows/
    dbt-ci.yml
docs/
  modeling.md            Grain and dimension decisions, with reasoning
```

## Data model

Full detail in `docs/modeling.md`. Summary of what is settled:

**Unit of work**: `(competition_id, season_id)`. This is how StatsBomb data
actually arrives, and it is the DAG's parameter. Deliberately not a calendar
date. The source is a static historical dump, so a daily cadence would be
fictional and most run dates would be empty.

**Fact structure**: core fact plus subtype facts. `fct_event` carries the
columns every event shares. `fct_shot`, `fct_pass`, and any later siblings
carry type-specific measures. Rejected the single wide fact because the
shared/specific split is the skill being demonstrated. Rejected nested
`ARRAY<STRUCT>` payloads as the least portable answer to a Snowflake or
Redshift interviewer.

**Fact partitioning**: `match_date`. Clustering keys TBD pending the Phase 1
row counts. Partitioning is decoupled from the unit of work on purpose. One
serves query pruning, the other serves scheduling.

Fact grain, dimension grain and SCD types, surrogate key strategy, and freeze
frame handling are TODO in `docs/modeling.md`. No schema DDL or dbt models get
written until they are filled in.

## Conventions

### Warehouse

- Raw layer: `ext_<entity>`, BigQuery external tables over the GCS objects.
  Every one is a single `raw_json STRING` column plus hive partition columns
  read from the object path. The raw layer stores what landed and interprets
  nothing; typed extraction is the staging layer's job. Schema autodetect was
  rejected: one season of events has 205 distinct field paths, autodetect
  inferred 38 columns and then failed the read on `dribble.overrun`, and
  `ignore_unknown_values` would have dropped fields silently.
- Parsing the raw layer needs
  `SAFE.PARSE_JSON(raw_json, wide_number_mode => 'round')`. StatsBomb's xG and
  pass-angle floats do not round-trip exactly, and strict `PARSE_JSON` rejects
  267 of 139,030 rows in the pilot season.
- Staging models: `stg_<source>__<entity>`, one per raw table, no joins, no
  business logic
- Marts: `dim_<entity>`, `fct_<process>`
- Reporting models: `rpt_<thing>`, materialized as tables, small enough to
  query cheaply
- Snake case everywhere. Dates as `_date`, timestamps as `_at`.
- Every model gets a `schema.yml` entry with a description
- Primary key on every dim: `unique` plus `not_null` tests, no exceptions
- Foreign keys on facts: `relationships` test back to the dim

### Dashboard

- Looker Studio connects to `rpt_` tables only. Never a fact table, never a
  staging model.
- Turn on report caching. Looker Studio re-queries on filter changes, and an
  uncached report left open is the main runaway cost risk now that there is no
  custom frontend.
- Every `rpt_` model is a table, not a view. A view pushes the scan cost to
  every dashboard load.

### CI

- `dbt build` runs against a dedicated CI dataset on every PR, never against
  the production dataset
- Tests failing blocks the merge. That is the point of having them.
- CI authenticates through Workload Identity Federation. No key file in the
  repo, no key in GitHub secrets.

### Credentials

This repo is public. Use Application Default Credentials locally
(`gcloud auth application-default login`) so no service account key file exists
on disk at all. The key patterns in `.gitignore` are defence in depth, not the
primary control. A key never written is a key never leaked.

## Non-negotiables

These are the interview follow-up questions. Build them in, do not retrofit.

- **Idempotent**: rerunning any task for the same `(competition, season)`
  produces the same result. No duplicate rows on a second run.
- **Backfillable**: the DAG takes a `(competition, season)` parameter and can
  re-run any past season without manual edits.
- **Tested**: dbt tests pass before downstream tasks run, locally and in CI.
- **Incremental where it matters**: the fact table is built incrementally to
  demonstrate the pattern, not because volume demands it. StatsBomb open data
  is on the order of single-digit-millions of event rows and probably fits
  inside BigQuery's free storage tier, where a full refresh is measurably
  cheaper. Say that plainly rather than claiming a scale requirement a row
  count would disprove. Confirm against the real Phase 1 numbers.
- **Bounded query cost**: BigQuery bills per byte scanned. Partition and
  cluster the fact table, and know roughly what a full dashboard load costs.
  Set `maximum_bytes_billed` in `profiles.yml` and a per-day custom quota on
  the project, so a runaway query fails instead of billing. The real cost risk
  during development is repeated `dbt build --full-refresh`.
  Caveat measured in Phase 2: BigQuery cannot dry-run-estimate an external
  table, so a pre-flight cost preview returns 0 bytes and tells you nothing.
  `maximum_bytes_billed` still enforces a ceiling by aborting mid-query, but
  cost is only observable after the fact. Native tables restore estimation,
  which is one argument for materialising rather than querying externals.

## Working agreement

Build one phase at a time. Each phase must run end to end before starting the
next.

1. Extract to GCS (plain script, no Airflow)
2. Load raw to BigQuery staging
3. dbt models: staging, then dims, then fact
4. dbt reporting models for the dashboard to read
5. Wrap in an Airflow DAG
6. GitHub Actions CI
7. Looker Studio dashboard

For anything touching the data model, ask me before generating. I want to make
those calls myself and be questioned on them. Boilerplate, config, and glue
code can be written directly.

Keep this file current. If a decision changes, update it here in the same
commit.
