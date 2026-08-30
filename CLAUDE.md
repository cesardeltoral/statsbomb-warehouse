# CLAUDE.md

Read `PROJECT.md` for the full spec and `docs/modeling.md` for the data model
before doing anything. Both are authoritative. This file is the working
agreement.

## What this project is

A portfolio data warehouse. StatsBomb open data lands in GCS, loads to BigQuery,
is modeled into a star schema with dbt, is orchestrated by Airflow, and is served
through Looker Studio.

The goal is to be able to defend every modeling decision in an interview, not
just to have working code. That changes what "help" means here.

## Hard rules

**Do not make data-model decisions.** Fact grain, dimension grain, primary keys,
SCD type, surrogate key strategy, and what to do with freeze frames are mine.
If a TODO in `docs/modeling.md` is unfilled and you need it, stop and ask. Do
not propose a default and proceed. Do not fill one in "as a starting point."
Quiz me on it instead.

**Do not generate dbt models or DDL while TODOs remain in `docs/modeling.md`.**
The gate is in that file. Respect it.

**One phase at a time.** Each phase runs end to end before the next starts. Do
not scaffold ahead. Phase order is in `PROJECT.md`.

**Never write a service account key file.** Auth is Application Default
Credentials locally and Workload Identity Federation in CI. This repo is public.

**Never query the raw fact table from a dashboard or a reporting model
consumer.** BigQuery bills per byte scanned.

## What you can write directly

Boilerplate, config, and glue. Specifically:

- `.gitignore`, `requirements.txt`, venv setup
- GCS and BigQuery client wiring in `extract/`
- `dbt_project.yml`, `profiles.yml`, `packages.yml`, `schema.yml` scaffolding
- GitHub Actions workflows
- Astro CLI scaffolding merge when Phase 5 arrives
- Tests, docstrings, README updates

Ask before: anything in `dbt/models/marts/`, anything that decides a grain or a
key, anything that changes the phase order.

## Conventions

Naming, testing, and cost rules are in `PROJECT.md`. Follow them exactly.
Snake case. `stg_<source>__<entity>`, `dim_<entity>`, `fct_<process>`,
`rpt_<thing>`. Every dim gets `unique` + `not_null` on its PK. Every fact FK
gets a `relationships` test.

## Keeping docs current

If a decision changes, update `PROJECT.md` or `docs/modeling.md` in the same
commit as the code. A stale spec is worse than no spec, because I will act on
it.
