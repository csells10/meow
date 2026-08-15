"""Run Packet 3 Level 1 for one approved development capture."""

from __future__ import annotations

import argparse
import json

from google.cloud import bigquery

from runtime_config import load_runtime_config
from services.gamelens_claim_storage import BigQueryClaimStorage
from services.gamelens_level1_service import extract_level1_from_capture
from services.gamelens_snapshot_storage import BigQuerySnapshotStorage


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plan or write Level 1 claims for one canonical capture."
    )
    parser.add_argument("--capture-id", required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument(
        "--write",
        action="store_true",
        help="Execute the dev-only claim MERGE and save a per-game receipt.",
    )
    return parser


def _display_result(result: dict) -> dict:
    visible = {key: value for key, value in result.items() if key != "rows"}
    visible["claim_sample"] = [
        {
            "claim_key": row.get("claim_key"),
            "claim_type": row.get("claim_type"),
            "claim_layer": row.get("claim_layer"),
            "claimed_team": row.get("claimed_team"),
            "source_field_path": row.get("source_field_path"),
        }
        for row in result.get("rows", [])[:5]
    ]
    return visible


def main(argv=None) -> int:
    args = _parser().parse_args(argv)
    runtime_config = load_runtime_config()
    if not runtime_config.is_dev:
        raise ValueError("Packet 3 Level 1 is allowed only in dev")

    client = bigquery.Client(project=runtime_config.project_id)
    result = extract_level1_from_capture(
        args.capture_id,
        args.attempt_id,
        snapshot_storage=BigQuerySnapshotStorage(
            client=client,
            runtime_config=runtime_config,
        ),
        claim_storage=BigQueryClaimStorage(
            client=client,
            runtime_config=runtime_config,
        ),
        write=args.write,
    )
    print(json.dumps(_display_result(result), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
