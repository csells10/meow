"""Read-only Packet 4 grade and storage projection for one frozen capture."""

from __future__ import annotations

import argparse
import json
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

from qa_gamelens_packet4_schema_inventory import (
    DEFAULT_PROJECT_ID,
    assert_read_only_sql,
)
from services.gamelens_postgame_grading import grade_frozen_capture
from services.gamelens_postgame_storage import BigQueryPostgameOutcomeStorage


def build_final_score(score_rows: Sequence[Mapping[str, Any]]) -> dict:
    """Reuse the established one-row-per-team score shape."""
    rows = [dict(row) for row in score_rows]
    if len(rows) != 2:
        raise ValueError("Packet 4 requires exactly two final score rows")
    away_rows = [row for row in rows if row.get("team_type") == "away"]
    home_rows = [row for row in rows if row.get("team_type") == "home"]
    if len(away_rows) != 1 or len(home_rows) != 1:
        raise ValueError("Packet 4 requires one away and one home score row")

    away = away_rows[0]
    home = home_rows[0]
    if away.get("awayPts") is None or home.get("homePts") is None:
        raise ValueError("Packet 4 final score totals are required")
    return {
        "away": {
            "q1": away.get("Q1", 0),
            "q2": away.get("Q2", 0),
            "q3": away.get("Q3", 0),
            "q4": away.get("Q4", 0),
            "ot": away.get("OT", 0),
            "total": away["awayPts"],
        },
        "home": {
            "q1": home.get("Q1", 0),
            "q2": home.get("Q2", 0),
            "q3": home.get("Q3", 0),
            "q4": home.get("Q4", 0),
            "ot": home.get("OT", 0),
            "total": home["homePts"],
        },
    }


def _query_rows(*, client, bigquery, sql: str, game_id: str) -> list[dict]:
    assert_read_only_sql(sql)
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("game_id", "STRING", game_id)
        ]
    )
    return [dict(row) for row in client.query(sql, job_config=config).result()]


def load_canonical_snapshot(*, client, bigquery, project_id: str, game_id: str):
    query = f"""
        SELECT *
        FROM `{project_id}.GameLens_dev.pregame_snapshots`
        WHERE game_id = @game_id
          AND capture_status = 'captured'
        ORDER BY captured_at
    """
    rows = _query_rows(
        client=client,
        bigquery=bigquery,
        sql=query,
        game_id=game_id,
    )
    if len(rows) != 1:
        raise ValueError(
            "Packet 4 requires exactly one canonical snapshot for the game"
        )
    return rows[0]


def load_final_score_rows(*, client, bigquery, project_id: str, game_id: str):
    query = f"""
        SELECT team_type, Q1, Q2, Q3, Q4, OT, homePts, awayPts
        FROM `{project_id}.Scores.scores`
        WHERE gameID = @game_id
        ORDER BY team_type
    """
    return _query_rows(
        client=client,
        bigquery=bigquery,
        sql=query,
        game_id=game_id,
    )


def build_packet4_dry_grade_plan(
    *,
    client,
    bigquery,
    project_id: str,
    game_id: str,
    outcome_builder=None,
    trust_builder=None,
    storage_factory=BigQueryPostgameOutcomeStorage,
) -> dict:
    """Read, grade, and project one row without any storage mutation."""
    snapshot = load_canonical_snapshot(
        client=client,
        bigquery=bigquery,
        project_id=project_id,
        game_id=game_id,
    )
    score_rows = load_final_score_rows(
        client=client,
        bigquery=bigquery,
        project_id=project_id,
        game_id=game_id,
    )
    final_score = build_final_score(score_rows)
    grade = grade_frozen_capture(
        snapshot=snapshot,
        final_score=final_score,
        outcome_builder=outcome_builder,
        trust_builder=trust_builder,
    )
    runtime = SimpleNamespace(is_dev=True, project_id=project_id)
    grade_storage = storage_factory(
        client=client,
        runtime_config=runtime,
        bigquery_module=bigquery,
    )
    storage_plan = grade_storage.plan_grade(grade)
    if storage_plan["conflict_count"]:
        raise ValueError("Packet 4 dry grade found an immutable storage conflict")
    return {
        "access_mode": "read_only",
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
        "final_score": final_score,
        "grade": grade,
        "storage_plan": storage_plan,
        "write_performed": False,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Build one read-only Packet 4 frozen-capture grade plan."
    )
    parser.add_argument("--game-id", required=True)
    parser.add_argument(
        "--dev-read-only",
        action="store_true",
        help="Confirm that this Packet 4 run cannot write a grade.",
    )
    parser.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    args = parser.parse_args(argv)
    if not args.dev_read_only:
        raise ValueError(
            "Pass --dev-read-only to confirm this Packet 4 grade is read-only"
        )

    from google.cloud import bigquery

    result = build_packet4_dry_grade_plan(
        client=bigquery.Client(project=args.project_id),
        bigquery=bigquery,
        project_id=args.project_id,
        game_id=args.game_id,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
