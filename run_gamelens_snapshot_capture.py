"""Manual, one-game LL-3 dry-run/write/read-back command."""

from __future__ import annotations

import argparse
import json
from typing import Optional, Sequence

from google.cloud import bigquery

from queries.gamelens_snapshot_queries import BigQuerySingleGameEvidenceLoader
from runtime_config import load_runtime_config
from services.gamelens_snapshot_capture import SnapshotCaptureService
from services.gamelens_snapshot_storage import BigQuerySnapshotStorage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture one immutable LL-3 development snapshot."
    )
    parser.add_argument("--game-id", required=True)
    parser.add_argument("--learning-run-id", required=True)
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--ruleset-version", required=True)
    parser.add_argument(
        "--metric-pipeline-run-id",
        default=None,
        help="Observed upstream run identity when one is available.",
    )
    parser.add_argument(
        "--evidence-source",
        choices=("configured", "production"),
        default="production",
        help="Read-only evidence source; every write remains in GameLens_dev.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Perform the insert-only reconciliation; default is dry-run.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    runtime_config = load_runtime_config()
    if not runtime_config.is_dev:
        raise ValueError("Set GAMELENS_ENVIRONMENT=dev before LL-3 capture")

    client = bigquery.Client(project=runtime_config.project_id)
    loader = BigQuerySingleGameEvidenceLoader(
        client=client,
        runtime_config=runtime_config,
        evidence_source=args.evidence_source,
    )
    storage = BigQuerySnapshotStorage(
        client=client,
        runtime_config=runtime_config,
    )
    service = SnapshotCaptureService(
        runtime_config=runtime_config,
        evidence_loader=loader,
        storage=storage,
    )
    result = service.capture_one(
        game_id=args.game_id,
        learning_run_id=args.learning_run_id,
        model_version=args.model_version,
        ruleset_version=args.ruleset_version,
        metric_pipeline_run_id=args.metric_pipeline_run_id,
        write=args.write,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
