"""Read-only BigQuery inventory for the Packet 4 storage decision.

The command inspects table metadata and optional per-game row counts.  It does
not create, alter, update, merge, insert, or delete any BigQuery object.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping, Sequence


BASE_TABLE_SPECS = (
    ("GameLens_dev", "pregame_snapshots", "packet2_canonical_snapshots", True),
    ("Scores", "scores", "final_score_source", True),
    ("Analytics", "game_model_outcomes", "existing_game_outcomes", True),
    ("Analytics", "game_model_trust_details", "existing_model_trust_details", True),
    ("GameLens_dev", "claim_training_examples", "packet3_claim_rows", True),
    ("GameLens_dev", "stage_runs", "attempt_receipts", True),
    ("GameLens_dev", "stage_game_results", "game_stage_receipts", True),
    ("GameLens_dev", "game_model_outcomes", "proposed_packet4_outcomes", False),
)

LINEAGE_FIELDS = (
    "pipeline_run_id",
    "learning_run_id",
    "capture_id",
    "game_id",
    "claim_key",
    "source_payload_sha256",
    "payload_sha256",
    "final_score_sha256",
    "grade_version",
)

_FORBIDDEN_SQL = re.compile(
    r"\b(ALTER|CREATE|DELETE|DROP|EXPORT|INSERT|LOAD|MERGE|TRUNCATE|UPDATE)\b",
    re.IGNORECASE,
)


def _table_id(project_id: str, dataset: str, table: str) -> str:
    project = str(project_id or "").strip()
    if not project:
        raise ValueError("project_id is required")
    return f"{project}.{dataset}.{table}"


def table_specs_for_season(season: str):
    value = str(season or "").strip()
    if not re.fullmatch(r"20\d{2}", value):
        raise ValueError("season must be a four-digit NFL season")
    return BASE_TABLE_SPECS + (
        (
            "Analytics",
            f"game_team_metric_facts_{value}",
            "level2_actual_facts",
            True,
        ),
    )


def assert_read_only_sql(sql: str) -> None:
    """Fail locally if an inventory query contains a mutation keyword."""
    match = _FORBIDDEN_SQL.search(sql or "")
    if match:
        raise ValueError(f"inventory SQL is not read-only: {match.group(1).upper()}")


def build_game_count_query(table_id: str, field_names: Iterable[str]) -> str:
    """Build one parameterized, read-only selected-game reconciliation query."""
    fields = set(field_names)
    if "game_id" not in fields:
        raise ValueError("game_id field is required for selected-game counts")

    select_parts = ["game_id", "COUNT(*) AS row_count"]
    for field in LINEAGE_FIELDS:
        if field != "game_id" and field in fields:
            select_parts.append(
                f"COUNTIF({field} IS NOT NULL) AS {field}_populated_count"
            )

    query = f"""
        SELECT
            {',\n            '.join(select_parts)}
        FROM `{table_id}`
        WHERE game_id IN UNNEST(@game_ids)
        GROUP BY game_id
        ORDER BY game_id
    """
    assert_read_only_sql(query)
    return query


def _schema_row(field: Any) -> Dict[str, Any]:
    return {
        "name": str(field.name),
        "type": str(field.field_type),
        "mode": str(field.mode),
    }


def inspect_table(
    *,
    client: Any,
    bigquery: Any,
    table_id: str,
    role: str,
    required: bool,
    game_ids: Sequence[str],
) -> Dict[str, Any]:
    """Inspect one table's metadata and optional selected-game counts."""
    try:
        table = client.get_table(table_id)
    except Exception as exc:
        return {
            "table": table_id,
            "role": role,
            "required": required,
            "status": "unavailable",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }

    schema = [_schema_row(field) for field in table.schema]
    field_names = [field["name"] for field in schema]
    result: Dict[str, Any] = {
        "table": table_id,
        "role": role,
        "required": required,
        "status": "available",
        "row_count": int(getattr(table, "num_rows", 0) or 0),
        "field_count": len(schema),
        "schema": schema,
        "lineage_fields_present": [
            field for field in LINEAGE_FIELDS if field in field_names
        ],
        "lineage_fields_missing": [
            field for field in LINEAGE_FIELDS if field not in field_names
        ],
        "selected_game_counts": [],
    }

    selected = sorted({str(game_id).strip() for game_id in game_ids if str(game_id).strip()})
    if not selected or "game_id" not in field_names:
        return result

    query = build_game_count_query(table_id, field_names)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("game_ids", "STRING", selected)
        ]
    )
    rows = client.query(query, job_config=job_config).result()
    result["selected_game_counts"] = [dict(row) for row in rows]
    return result


def inventory_packet4_tables(
    *,
    client: Any,
    bigquery: Any,
    project_id: str,
    season: str,
    game_ids: Sequence[str] = (),
) -> Dict[str, Any]:
    """Return the complete read-only inventory used before Packet 4 storage."""
    tables = [
        inspect_table(
            client=client,
            bigquery=bigquery,
            table_id=_table_id(project_id, dataset, table),
            role=role,
            required=required,
            game_ids=game_ids,
        )
        for dataset, table, role, required in table_specs_for_season(season)
    ]
    return {
        "access_mode": "read_only",
        "audited_at": datetime.now(timezone.utc).isoformat(),
        "project_id": project_id,
        "season": str(season),
        "requested_game_ids": sorted(set(game_ids)),
        "available_table_count": sum(
            table["status"] == "available" for table in tables
        ),
        "unavailable_table_count": sum(
            table["status"] != "available" for table in tables
        ),
        "required_unavailable_count": sum(
            table["required"] and table["status"] != "available"
            for table in tables
        ),
        "tables": tables,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect Packet 4 source/target schemas and optional game counts. "
            "This command is read-only."
        )
    )
    parser.add_argument(
        "--game-id",
        action="append",
        default=[],
        help="Optional game ID to count in each table; repeat for multiple games.",
    )
    args = parser.parse_args(argv)

    from google.cloud import bigquery
    from runtime_config import load_runtime_config

    runtime_config = load_runtime_config()
    if not runtime_config.is_dev:
        raise ValueError("Packet 4 inventory is allowed only in dev")

    client = bigquery.Client(project=runtime_config.project_id)
    result = inventory_packet4_tables(
        client=client,
        bigquery=bigquery,
        project_id=runtime_config.project_id,
        season=runtime_config.active_season,
        game_ids=args.game_id,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0 if result["required_unavailable_count"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
