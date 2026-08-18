"""Deliberate development-only Packet 4 Level 3 update with read-back."""

from __future__ import annotations

import argparse
import json

from qa_gamelens_packet4_level3 import build_packet4_level3_preview
from services.gamelens_level3_storage import BigQueryLevel3FeatureStorage


def execute_packet4_level3_write(
    *,
    client,
    bigquery,
    runtime_config,
    game_id: str,
    attempt_id: str,
    level2_calculation_module=None,
    level3_calculation_module=None,
    storage_factory=BigQueryLevel3FeatureStorage,
) -> dict:
    if not runtime_config.is_dev:
        raise ValueError("Packet 4 Level 3 writes are allowed only in dev")
    attempt = str(attempt_id or "").strip()
    if not attempt:
        raise ValueError("attempt_id is required")

    preview = build_packet4_level3_preview(
        client=client,
        bigquery=bigquery,
        project_id=runtime_config.project_id,
        game_id=game_id,
        level2_calculation_module=level2_calculation_module,
        level3_calculation_module=level3_calculation_module,
    )
    if preview["status"] == "blocked":
        raise ValueError("Packet 4 Level 3 is blocked by the Level 2 gate")
    storage = storage_factory(
        client=client,
        runtime_config=runtime_config,
        bigquery_module=bigquery,
        calculation_module=level3_calculation_module,
    )
    identity = {
        "learning_run_id": preview["learning_run_id"],
        "capture_id": preview["capture_id"],
        "game_id": preview["game_id"],
    }
    before_plan = storage.plan_features(
        **identity,
        feature_rows=preview["feature_rows"],
    )
    if before_plan["conflict_count"] or before_plan["rejected_count"]:
        raise ValueError("Packet 4 Level 3 write found a stored-row conflict")
    reconciliation = storage.store_features(
        **identity,
        feature_rows=preview["feature_rows"],
        attempt_id=attempt,
    )
    return {
        "environment": "dev",
        "attempt_id": attempt,
        "access_mode": "development_write",
        **identity,
        "final_score": preview["final_score"],
        "facts_counts": preview["facts_counts"],
        "level2_gate": preview["level2_gate"],
        "feature_status": preview["status"],
        "feature_reason": preview["reason"],
        "formula_version": preview["formula_version"],
        "source_counts": preview["source_counts"],
        "feature_reconciliation": preview["reconciliation"],
        "before_plan": before_plan,
        "reconciliation": reconciliation,
        "write_performed": reconciliation["write_performed"],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Update one reviewed Packet 4 Level 3 capture in dev."
    )
    parser.add_argument("--game-id", required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument(
        "--confirm-dev-write",
        action="store_true",
        help="Confirm one capture-scoped Level 3 development update.",
    )
    args = parser.parse_args(argv)
    if not args.confirm_dev_write:
        raise ValueError(
            "Pass --confirm-dev-write to allow the Level 3 development update"
        )

    from google.cloud import bigquery
    from runtime_config import load_runtime_config

    runtime_config = load_runtime_config()
    if not runtime_config.is_dev:
        raise ValueError("Set GAMELENS_ENVIRONMENT=dev before Level 3 writes")
    result = execute_packet4_level3_write(
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
