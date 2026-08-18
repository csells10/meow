"""Read-only Packet 4 Level 3 preview for one canonical capture."""

from __future__ import annotations

import argparse
import json
from typing import Any

from qa_gamelens_packet4_level2 import (
    build_packet4_level2_preview,
    load_bounded_claim_rows,
)
from qa_gamelens_packet4_schema_inventory import DEFAULT_PROJECT_ID
from services.gamelens_level3_features import run_bounded_level3_features


def build_packet4_level3_preview(
    *,
    client,
    bigquery,
    project_id: str,
    game_id: str,
    level2_calculation_module: Any = None,
    level3_calculation_module: Any = None,
) -> dict[str, Any]:
    level2 = build_packet4_level2_preview(
        client=client,
        bigquery=bigquery,
        project_id=project_id,
        game_id=game_id,
        calculation_module=level2_calculation_module,
    )
    claims = load_bounded_claim_rows(
        client=client,
        bigquery=bigquery,
        project_id=project_id,
        learning_run_id=level2["learning_run_id"],
        capture_id=level2["capture_id"],
        game_id=level2["game_id"],
    )
    result = run_bounded_level3_features(
        learning_run_id=level2["learning_run_id"],
        capture_id=level2["capture_id"],
        game_id=level2["game_id"],
        claim_rows=claims,
        level2_status=level2["status"],
        calculation_module=level3_calculation_module,
    )
    return {
        "access_mode": "read_only",
        "environment": "dev",
        "final_score": level2["final_score"],
        "facts_counts": level2["source_counts"],
        "level2_gate": {
            "status": level2["status"],
            "reason": level2["reason"],
            "reconciliation": level2["reconciliation"],
        },
        **result,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Preview bounded Packet 4 Level 3 features."
    )
    parser.add_argument("--game-id", required=True)
    parser.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    parser.add_argument(
        "--dev-read-only",
        action="store_true",
        help="Confirm this Level 3 preview cannot write BigQuery.",
    )
    args = parser.parse_args(argv)
    if not args.dev_read_only:
        raise ValueError(
            "Pass --dev-read-only to confirm Packet 4 Level 3 is read-only"
        )

    from google.cloud import bigquery

    result = build_packet4_level3_preview(
        client=bigquery.Client(project=args.project_id),
        bigquery=bigquery,
        project_id=args.project_id,
        game_id=args.game_id,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
