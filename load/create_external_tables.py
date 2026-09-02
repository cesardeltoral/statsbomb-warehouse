"""Define BigQuery external tables over the raw StatsBomb JSON in GCS.

Phase 2. Creates the dataset if absent, then one external table per entity
pointing at the objects Phase 1 landed. No data is copied: an external table
is a definition, so re-running this is free and idempotent.

Why every table is a single STRING column
-----------------------------------------
StatsBomb events are heterogeneous. One season alone contains 39 distinct
top-level keys and 205 distinct field paths, and schema autodetect — which
samples a subset of files — inferred 38 columns and then failed the read with
"No such field: dribble.overrun". It also missed ``off_camera`` entirely. The
full corpus has more paths than one league season, so autodetect gets less
reliable as more competitions land, not more.

The alternatives were rejected deliberately:

``ignore_unknown_values = TRUE``
    Makes the error vanish by silently discarding unmapped fields. In a layer
    whose entire purpose is losslessness this is the worst outcome, and it
    fails silently — the data is gone with no error to notice.

An exhaustive explicit schema
    205 hand-maintained field paths, re-broken by the first competition that
    ships a key this season did not contain.

So the raw layer stores exactly what landed and interprets nothing. Typed
extraction is the job of the ``stg_`` models in Phase 3, which is what a
staging layer is for. The same rule is applied to matches, lineups and
competitions even though autodetect currently succeeds on them: it succeeds
only because one season's sample happened to cover their fields, and the
identical silent-drift risk applies as more competitions land.

Format note
-----------
The files are newline-delimited JSON, but these tables are declared as CSV
with a tab delimiter and quoting disabled. That is the mechanism for reading
one whole line into one STRING column without BigQuery trying to interpret
it. It is safe because JSON escapes control characters, so a literal tab
cannot occur inside a compact JSON record — verified across all 139,030
events of the pilot season, zero rows contained one.

Reading it back
---------------
``PARSE_JSON`` rejects floats it cannot round-trip exactly, which StatsBomb's
xG and pass-angle values trip: 267 of 139,030 rows failed with default
settings and 0 failed with ``wide_number_mode => 'round'``. The raw layer
keeps the exact original text either way; only a consumer that parses has to
choose. Phase 3 will need::

    SAFE.PARSE_JSON(raw_json, wide_number_mode => 'round')

Credentials
-----------
Application Default Credentials only. Never a service account key file.

Usage::

    python -m load.create_external_tables
    python -m load.create_external_tables --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from google.api_core.exceptions import Conflict
from google.cloud import bigquery

LOG = logging.getLogger("create_external_tables")

DEFAULT_DATASET = "statsbomb_raw"
DEFAULT_PREFIX = "statsbomb"
DEFAULT_LOCATION = "US"

# hive_partitioned: whether the objects sit under key=value directories.
# Phase 1 writes match_id as a directory, not a filename, precisely so that
# BigQuery surfaces it here — hive partition keys come from directory
# components only.
ENTITIES = {
    "competitions": {"hive_partitioned": False},
    "matches": {"hive_partitioned": True},
    "events": {"hive_partitioned": True},
    "lineups": {"hive_partitioned": True},
}


def external_table_ddl(project: str, dataset: str, entity: str, bucket: str, prefix: str) -> str:
    base = f"gs://{bucket}/{prefix}/{entity}"
    partition_clause = "WITH PARTITION COLUMNS" if ENTITIES[entity]["hive_partitioned"] else ""
    hive_option = (
        f",\n  hive_partition_uri_prefix = '{base}/',\n  require_hive_partition_filter = false"
        if ENTITIES[entity]["hive_partitioned"]
        else ""
    )
    return f"""
CREATE OR REPLACE EXTERNAL TABLE `{project}.{dataset}.ext_{entity}` (
  raw_json STRING
)
{partition_clause}
OPTIONS (
  description = 'Raw StatsBomb {entity}, one JSON record per row, exactly as landed. Parse with SAFE.PARSE_JSON(raw_json, wide_number_mode => \\'round\\').',
  format = 'CSV',
  field_delimiter = '\\t',
  quote = '',
  uris = ['{base}/*']{hive_option}
)
""".strip()


def ensure_dataset(client: bigquery.Client, project: str, dataset: str, location: str) -> None:
    ref = bigquery.Dataset(f"{project}.{dataset}")
    ref.location = location
    ref.description = "Raw StatsBomb open data landed from GCS. 1:1 with source, no modelling."
    try:
        client.create_dataset(ref)
        LOG.info("created dataset %s.%s in %s", project, dataset, location)
    except Conflict:
        LOG.info("dataset %s.%s already exists", project, dataset)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m load.create_external_tables",
        description="Create BigQuery external tables over the raw StatsBomb JSON in GCS.",
    )
    p.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"), help="GCP project id (env: GOOGLE_CLOUD_PROJECT)")
    p.add_argument("--bucket", default=os.environ.get("STATSBOMB_GCS_BUCKET"), help="GCS bucket (env: STATSBOMB_GCS_BUCKET)")
    p.add_argument("--dataset", default=os.environ.get("STATSBOMB_BQ_DATASET", DEFAULT_DATASET))
    p.add_argument("--prefix", default=DEFAULT_PREFIX)
    p.add_argument("--location", default=DEFAULT_LOCATION)
    p.add_argument("--dry-run", action="store_true", help="Print the DDL, execute nothing")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    if not args.project or not args.bucket:
        LOG.error("Set --project/--bucket or GOOGLE_CLOUD_PROJECT/STATSBOMB_GCS_BUCKET.")
        return 2

    if args.dry_run:
        for entity in ENTITIES:
            print(external_table_ddl(args.project, args.dataset, entity, args.bucket, args.prefix), end=";\n\n")
        return 0

    client = bigquery.Client(project=args.project)
    ensure_dataset(client, args.project, args.dataset, args.location)

    for entity in ENTITIES:
        ddl = external_table_ddl(args.project, args.dataset, entity, args.bucket, args.prefix)
        client.query(ddl).result()
        LOG.info("created external table %s.%s.ext_%s", args.project, args.dataset, entity)

    return 0


if __name__ == "__main__":
    sys.exit(main())
