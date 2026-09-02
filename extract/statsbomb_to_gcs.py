"""Extract StatsBomb open data for one competition-season and land it in GCS.

Phase 1 of the pipeline. No Airflow, no BigQuery — this script only fetches
from the StatsBomb open data repository and writes objects to a bucket.

Design decisions this script implements (see PROJECT.md and docs/modeling.md):

Unit of work
    ``(competition_id, season_id)``, matching the parameter the Phase 5 DAG
    will take. Backfilling a season means re-running this with that season's
    ids and nothing else.

Payload fidelity
    Objects hold StatsBomb's payload with nothing added. ``statsbombpy``
    injects a ``match_id`` into every event record; we strip it, because the
    object path already carries that id and the raw layer should not contain
    derived columns. Verified by ``--verify``, which re-fetches the source
    files and asserts value equality.

    Objects are newline-delimited JSON: one record per line. This is forced,
    not chosen — BigQuery has no reader for a JSON array, and a file holding
    ``[{...},{...}]`` fails schema detection with "No object found when new
    array is started". Only the container changes; each record is still
    exactly what StatsBomb published.

    Note the payload is *value*-identical, not byte-identical: the source is
    pretty-printed by a Java JSON writer whose spacing Python cannot
    reproduce. We serialise canonically and compactly instead, which is
    deterministic — the same input always produces the same bytes — and
    materially smaller. Whitespace carries no meaning to BigQuery.

Layout
    Hive-style prefixes, one object per match::

        <prefix>/competitions/competitions.ndjson
        <prefix>/matches/competition_id=11/season_id=90/matches.ndjson
        <prefix>/events/competition_id=11/season_id=90/match_id=3773386/events.ndjson
        <prefix>/lineups/competition_id=11/season_id=90/match_id=3773386/lineups.ndjson

    Phase 2 reads these with hive partitioning enabled, so ``competition_id``,
    ``season_id`` and ``match_id`` arrive as real columns without the payload
    having to carry them.

    ``match_id`` is a *directory*, not part of the filename, and that is
    load-bearing. BigQuery derives hive partition keys from directory
    components only; with ``match_id=3773386.json`` as a filename it silently
    yields competition_id and season_id and no match_id at all. Verified
    against BigQuery before this layout was settled.

Idempotency
    Every path is a pure function of the ids in it, and uploads overwrite. A
    second run of the same season rewrites the same objects with the same
    bytes. Duplicates are impossible by construction rather than by
    bookkeeping.

Credentials
    Application Default Credentials only. Run ``gcloud auth
    application-default login`` first. This script never reads or writes a
    service account key file — see PROJECT.md, the repo is public.

Usage::

    python -m extract.statsbomb_to_gcs --list-competitions
    python -m extract.statsbomb_to_gcs --competition-id 11 --season-id 90
    python -m extract.statsbomb_to_gcs --competition-id 11 --season-id 90 --dry-run
    python -m extract.statsbomb_to_gcs --competition-id 11 --season-id 90 --verify
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import statistics
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

# statsbombpy warns on import that no StatsBomb credentials were supplied and
# it is therefore serving open data. That is exactly what we want, so the
# warning is noise.
import warnings

warnings.filterwarnings("ignore", module="statsbombpy")

from google.cloud import storage  # noqa: E402
from statsbombpy import sb  # noqa: E402

LOG = logging.getLogger("statsbomb_to_gcs")

DEFAULT_PREFIX = "statsbomb"
DEFAULT_WORKERS = 4
SOURCE_BASE = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"

# Keys statsbombpy adds that are not in StatsBomb's source payload. Stripped so
# the raw layer stays exactly what the provider published.
INJECTED_KEYS = {"events": ("match_id",)}


# --------------------------------------------------------------------------
# Serialisation
# --------------------------------------------------------------------------


def canonical_ndjson(records: list[dict[str, Any]]) -> bytes:
    """Serialise to newline-delimited JSON: one record per line.

    Deterministic — same records in, same bytes out — which is what lets us
    claim a re-run is a no-op at the byte level. Key order is preserved as
    StatsBomb published it rather than sorted, so a diff against the source
    shows only formatting.

    NDJSON rather than a JSON array because BigQuery cannot read an array;
    see this module's docstring.
    """
    lines = (json.dumps(r, separators=(",", ":"), ensure_ascii=False) for r in records)
    return ("\n".join(lines) + "\n").encode("utf-8")


def strip_injected(records: Iterable[dict[str, Any]], entity: str) -> list[dict[str, Any]]:
    """Remove keys statsbombpy adds, leaving StatsBomb's payload untouched."""
    injected = INJECTED_KEYS.get(entity, ())
    if not injected:
        return list(records)
    return [{k: v for k, v in r.items() if k not in injected} for r in records]


# --------------------------------------------------------------------------
# Fetching
# --------------------------------------------------------------------------


def with_retry(fn: Callable[[], Any], *, what: str, attempts: int = 4) -> Any:
    """Retry a fetch with exponential backoff.

    statsbombpy reads from raw.githubusercontent.com, which rate limits and
    occasionally drops connections partway through a few-thousand-match run.
    """
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - upstream raises many types
            if attempt == attempts:
                raise
            backoff = 2 ** (attempt - 1)
            LOG.warning("%s failed (%s), retrying in %ss [%d/%d]", what, exc, backoff, attempt, attempts)
            time.sleep(backoff)


def fetch_competitions() -> list[dict[str, Any]]:
    return list(with_retry(lambda: sb.competitions(fmt="dict"), what="competitions").values())


def fetch_matches(competition_id: int, season_id: int) -> list[dict[str, Any]]:
    return list(
        with_retry(
            lambda: sb.matches(competition_id=competition_id, season_id=season_id, fmt="dict"),
            what=f"matches c={competition_id} s={season_id}",
        ).values()
    )


def fetch_events(match_id: int) -> list[dict[str, Any]]:
    records = with_retry(lambda: sb.events(match_id=match_id, fmt="dict"), what=f"events m={match_id}").values()
    return strip_injected(records, "events")


def fetch_lineups(match_id: int) -> list[dict[str, Any]]:
    records = with_retry(lambda: sb.lineups(match_id=match_id, fmt="dict"), what=f"lineups m={match_id}").values()
    return strip_injected(records, "lineups")


# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------


def competitions_path(prefix: str) -> str:
    """Not season-scoped: the competitions index describes every season."""
    return f"{prefix}/competitions/competitions.ndjson"


def matches_path(prefix: str, competition_id: int, season_id: int) -> str:
    return f"{prefix}/matches/competition_id={competition_id}/season_id={season_id}/matches.ndjson"


def match_scoped_path(prefix: str, entity: str, competition_id: int, season_id: int, match_id: int) -> str:
    """match_id is a directory, not part of the filename.

    BigQuery reads hive partition keys from directory components only, so
    ``match_id=<id>.ndjson`` would not surface match_id as a column.
    """
    return (
        f"{prefix}/{entity}/competition_id={competition_id}"
        f"/season_id={season_id}/match_id={match_id}/{entity}.ndjson"
    )


# --------------------------------------------------------------------------
# Upload
# --------------------------------------------------------------------------


@dataclass
class RunStats:
    """Numbers that fill the Phase 1 measurements table in docs/modeling.md."""

    matches: int = 0
    events_per_match: list[int] = field(default_factory=list)
    lineup_rows: int = 0
    bytes_written: int = 0
    objects_written: int = 0
    objects_skipped: int = 0

    @property
    def events_total(self) -> int:
        return sum(self.events_per_match)


class Uploader:
    """Writes objects to GCS, or reports what it would write in dry-run mode."""

    def __init__(self, bucket_name: str, project: str | None, dry_run: bool, skip_existing: bool):
        self.dry_run = dry_run
        self.skip_existing = skip_existing
        self.bucket_name = bucket_name
        self._bucket = None
        if not dry_run:
            # Application Default Credentials. No key file, ever.
            client = storage.Client(project=project)
            self._bucket = client.bucket(bucket_name)

    def put(self, path: str, records: list[dict[str, Any]], stats: RunStats) -> int:
        payload = canonical_ndjson(records)
        if self.dry_run:
            LOG.info("[dry-run] gs://%s/%s (%d records, %d bytes)", self.bucket_name, path, len(records), len(payload))
            stats.objects_written += 1
            stats.bytes_written += len(payload)
            return len(payload)

        blob = self._bucket.blob(path)
        if self.skip_existing and blob.exists():
            LOG.info("skip (exists) gs://%s/%s", self.bucket_name, path)
            stats.objects_skipped += 1
            return 0

        # Overwrites by default. That is the idempotency guarantee: the path is
        # derived from the ids, so a re-run rewrites rather than appends.
        blob.upload_from_string(payload, content_type="application/x-ndjson")
        LOG.info("wrote gs://%s/%s (%d records, %d bytes)", self.bucket_name, path, len(records), len(payload))
        stats.objects_written += 1
        stats.bytes_written += len(payload)
        return len(payload)


# --------------------------------------------------------------------------
# Extraction
# --------------------------------------------------------------------------


def extract_season(
    competition_id: int,
    season_id: int,
    uploader: Uploader,
    prefix: str,
    workers: int,
) -> RunStats:
    stats = RunStats()

    LOG.info("fetching competitions index")
    uploader.put(competitions_path(prefix), fetch_competitions(), stats)

    LOG.info("fetching match list for competition_id=%s season_id=%s", competition_id, season_id)
    matches = fetch_matches(competition_id, season_id)
    if not matches:
        raise SystemExit(
            f"No matches for competition_id={competition_id} season_id={season_id}. "
            "Run --list-competitions to see valid pairs."
        )
    uploader.put(matches_path(prefix, competition_id, season_id), matches, stats)
    stats.matches = len(matches)
    LOG.info("%d matches to extract", len(matches))

    def one_match(match: dict[str, Any]) -> tuple[int, int]:
        match_id = match["match_id"]
        events = fetch_events(match_id)
        lineups = fetch_lineups(match_id)
        uploader.put(
            match_scoped_path(prefix, "events", competition_id, season_id, match_id), events, stats
        )
        uploader.put(
            match_scoped_path(prefix, "lineups", competition_id, season_id, match_id), lineups, stats
        )
        return len(events), sum(len(t.get("lineup", [])) for t in lineups)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for event_count, lineup_count in pool.map(one_match, matches):
            stats.events_per_match.append(event_count)
            stats.lineup_rows += lineup_count

    return stats


# --------------------------------------------------------------------------
# Fidelity verification
# --------------------------------------------------------------------------


def _fetch_source(url: str) -> Any:
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read())


def verify_fidelity(competition_id: int, season_id: int, sample: int) -> int:
    """Assert what we would upload equals StatsBomb's published payload.

    Re-fetches the source files directly, bypassing statsbombpy, and compares
    parsed values. Backs the claim in this module's docstring instead of
    leaving it as an assertion in a comment. Returns a process exit code.
    """
    failures = 0

    def check(label: str, got: list[dict[str, Any]], url: str) -> None:
        nonlocal failures
        want = _fetch_source(url)
        norm = lambda rs: sorted(json.dumps(r, sort_keys=True, default=str) for r in rs)  # noqa: E731
        if norm(got) == norm(want):
            LOG.info("PASS %-28s %d records identical to source", label, len(want))
        else:
            failures += 1
            extra = set().union(*(set(r) for r in got)) - set().union(*(set(r) for r in want))
            missing = set().union(*(set(r) for r in want)) - set().union(*(set(r) for r in got))
            LOG.error(
                "FAIL %-28s ours=%d source=%d extra_keys=%s missing_keys=%s",
                label, len(got), len(want), sorted(extra) or "none", sorted(missing) or "none",
            )

    check("competitions", fetch_competitions(), f"{SOURCE_BASE}/competitions.json")

    matches = fetch_matches(competition_id, season_id)
    check(
        f"matches c={competition_id} s={season_id}",
        matches,
        f"{SOURCE_BASE}/matches/{competition_id}/{season_id}.json",
    )

    for match in matches[:sample]:
        mid = match["match_id"]
        check(f"events m={mid}", fetch_events(mid), f"{SOURCE_BASE}/events/{mid}.json")
        check(f"lineups m={mid}", fetch_lineups(mid), f"{SOURCE_BASE}/lineups/{mid}.json")

    if failures:
        LOG.error("%d fidelity check(s) failed", failures)
    else:
        LOG.info("all fidelity checks passed")
    return 1 if failures else 0


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def list_competitions() -> None:
    rows = fetch_competitions()
    rows.sort(key=lambda r: (r["country_name"], r["competition_name"], str(r["season_name"])))
    print(f"{'comp':>5} {'season':>7}  {'country':<22} {'competition':<32} season      360")
    print("-" * 96)
    for r in rows:
        has_360 = "yes" if r.get("match_available_360") else ""
        print(
            f"{r['competition_id']:>5} {r['season_id']:>7}  {r['country_name'][:22]:<22} "
            f"{r['competition_name'][:32]:<32} {str(r['season_name']):<11} {has_360}"
        )
    print(f"\n{len(rows)} competition-seasons available.")


def report(stats: RunStats, bucket: str, prefix: str, competition_id: int, season_id: int) -> None:
    """Print the numbers docs/modeling.md's Phase 1 table asks for."""
    per_match = stats.events_per_match
    median = int(statistics.median(per_match)) if per_match else 0
    mb = stats.bytes_written / 1_048_576
    print()
    print("=" * 62)
    print(f"  competition_id={competition_id} season_id={season_id}")
    print(f"  gs://{bucket}/{prefix}/")
    print("=" * 62)
    print(f"  Matches                        {stats.matches:>12,}")
    print(f"  Events (total)                 {stats.events_total:>12,}")
    print(f"  Events per match (median)      {median:>12,}")
    print(f"  Events per match (min / max)   "
          f"{(min(per_match) if per_match else 0):>5,} / {(max(per_match) if per_match else 0):<6,}")
    print(f"  Lineup rows (player-match)     {stats.lineup_rows:>12,}")
    print(f"  Objects written                {stats.objects_written:>12,}")
    print(f"  Objects skipped                {stats.objects_skipped:>12,}")
    print(f"  Bytes written                  {stats.bytes_written:>12,}  ({mb:,.1f} MiB)")
    print("=" * 62)
    print("  These fill the Phase 1 measurements table in docs/modeling.md.")
    print("  Note: for one season only. Run every season for the full figures.")
    print()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m extract.statsbomb_to_gcs",
        description="Extract one StatsBomb competition-season to GCS.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Auth is Application Default Credentials:\n"
            "  gcloud auth application-default login\n"
            "This script never reads or writes a service account key file."
        ),
    )
    p.add_argument("--competition-id", type=int, help="StatsBomb competition_id")
    p.add_argument("--season-id", type=int, help="StatsBomb season_id")
    p.add_argument(
        "--bucket",
        default=os.environ.get("STATSBOMB_GCS_BUCKET"),
        help="Target GCS bucket (env: STATSBOMB_GCS_BUCKET)",
    )
    p.add_argument(
        "--project",
        default=os.environ.get("GOOGLE_CLOUD_PROJECT"),
        help="GCP project id (env: GOOGLE_CLOUD_PROJECT)",
    )
    p.add_argument("--prefix", default=DEFAULT_PREFIX, help=f"Object prefix (default: {DEFAULT_PREFIX})")
    p.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"Parallel match fetches (default: {DEFAULT_WORKERS})")
    p.add_argument("--dry-run", action="store_true", help="Fetch and report, upload nothing. No bucket needed.")
    p.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip objects already in the bucket. A resume aid, not required for correctness.",
    )
    p.add_argument("--list-competitions", action="store_true", help="Print available (competition_id, season_id) pairs and exit")
    p.add_argument("--verify", action="store_true", help="Check payloads against the source files and exit")
    p.add_argument("--verify-sample", type=int, default=2, help="Matches to verify (default: 2)")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.list_competitions:
        list_competitions()
        return 0

    if args.competition_id is None or args.season_id is None:
        LOG.error("--competition-id and --season-id are required. Try --list-competitions.")
        return 2

    if args.verify:
        return verify_fidelity(args.competition_id, args.season_id, args.verify_sample)

    if not args.dry_run and not args.bucket:
        LOG.error("No bucket. Pass --bucket or set STATSBOMB_GCS_BUCKET (or use --dry-run).")
        return 2

    uploader = Uploader(
        bucket_name=args.bucket or "(dry-run)",
        project=args.project,
        dry_run=args.dry_run,
        skip_existing=args.skip_existing,
    )
    started = time.monotonic()
    stats = extract_season(args.competition_id, args.season_id, uploader, args.prefix, args.workers)
    LOG.info("done in %.1fs", time.monotonic() - started)
    report(stats, args.bucket or "(dry-run)", args.prefix, args.competition_id, args.season_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
