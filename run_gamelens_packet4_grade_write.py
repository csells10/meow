"""Deliberate development-only Packet 4 grade write with read-back proof."""

from __future__ import annotations

import argparse
import json

from qa_gamelens_packet4_dry_grade import (
    build_final_score,
    load_canonical_snapshot,
    load_final_score_rows,
)
from services.gamelens_postgame_grading import grade_frozen_capture
from services.gamelens_postgame_storage import BigQueryPostgameOutcomeStorage


def execute_packet4_grade_write(
    *,
    client,
    bigquery,
    runtime_config,
    game_id: str,
    attempt_id: str,
    outcome_builder=None,
    trust_builder=None,
    storage_factory=BigQueryPostgameOutcomeStorage,
) -> dict:
    """Grade and reconcile one game through the dev-only insert boundary."""
    if not runtime_config.is_dev:
        raise ValueError("Packet 4 grade writes are allowed only in dev")
    attempt = str(attempt_id or "").strip()
    if not attempt:
        raise ValueError("attempt_id is required")

    snapshot = load_canonical_snapshot(
        client=client,
        bigquery=bigquery,
        project_id=runtime_config.project_id,
        game_id=game_id,
    )
    score_rows = load_final_score_rows(
        client=client,
        bigquery=bigquery,
        project_id=runtime_config.project_id,
        game_id=game_id,
    )
    final_score = build_final_score(score_rows)
    grade = grade_frozen_capture(
        snapshot=snapshot,
        final_score=final_score,
        outcome_builder=outcome_builder,
        trust_builder=trust_builder,
    )
    grade_storage = storage_factory(
        client=client,
        runtime_config=runtime_config,
        bigquery_module=bigquery,
    )
    before_plan = grade_storage.plan_grade(grade)
    if before_plan["conflict_count"]:
        raise ValueError("Packet 4 grade write found an immutable conflict")
    reconciliation = grade_storage.store_grade(grade, attempt_id=attempt)
    return {
        "environment": "dev",
        "attempt_id": attempt,
        "game_id": game_id,
        "source_counts": {
            "canonical_snapshot_rows": 1,
            "final_score_rows": len(score_rows),
        },
        "capture": {
            "learning_run_id": grade["learning_run_id"],
            "capture_id": grade["capture_id"],
            "pipeline_run_id": grade.get("pipeline_run_id"),
            "source_payload_sha256": grade["source_payload_sha256"],
        },
        "final_score_sha256": grade["final_score_sha256"],
        "grade_version": grade["grade_version"],
        "model_outcome": grade["model_outcome"],
        "model_trust": grade["model_trust"],
        "before_plan": before_plan,
        "reconciliation": reconciliation,
        "write_performed": reconciliation["write_performed"],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Write one reviewed Packet 4 grade to GameLens_dev."
    )
    parser.add_argument("--game-id", required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument(
        "--confirm-dev-write",
        action="store_true",
        help="Confirm one game-scoped write to the Packet 4 development ledger.",
    )
    args = parser.parse_args(argv)
    if not args.confirm_dev_write:
        raise ValueError(
            "Pass --confirm-dev-write to allow the Packet 4 development write"
        )

    from google.cloud import bigquery
    from runtime_config import load_runtime_config

    runtime_config = load_runtime_config()
    if not runtime_config.is_dev:
        raise ValueError("Set GAMELENS_ENVIRONMENT=dev before writing a grade")
    result = execute_packet4_grade_write(
        client=bigquery.Client(project=runtime_config.project_id),
        bigquery=bigquery,
        runtime_config=runtime_config,
        game_id=args.game_id,
        attempt_id=args.attempt_id,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
