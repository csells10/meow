"""Run the Packet 2 read-only Schedule-to-snapshot coverage audit."""

from __future__ import annotations

import argparse
import json
from datetime import date

from google.cloud import bigquery

from runtime_config import load_runtime_config
from services.gamelens_snapshot_coverage import (
    BigQuerySnapshotCoverageAuditor,
)


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "expected an ISO date in YYYY-MM-DD format"
        ) from exc


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compare production Schedule with GameLens_dev snapshots and "
            "per-game attempt evidence. This command never writes."
        )
    )
    parser.add_argument("--start-date", required=True, type=_date)
    parser.add_argument("--end-date", required=True, type=_date)
    args = parser.parse_args(argv)

    runtime_config = load_runtime_config()
    if not runtime_config.is_dev:
        raise ValueError("Snapshot coverage audit is allowed only in dev")

    client = bigquery.Client(project=runtime_config.project_id)
    auditor = BigQuerySnapshotCoverageAuditor(
        client=client,
        runtime_config=runtime_config,
    )
    result = auditor.audit(
        start_date=args.start_date,
        end_date=args.end_date,
        season=runtime_config.active_season,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

