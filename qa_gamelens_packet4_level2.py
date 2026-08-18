"""Read-only Packet 4 Level 2 preview for one canonical capture."""

from __future__ import annotations

import argparse
import json
from typing import Any

from qa_gamelens_packet4_dry_grade import (
    build_final_score,
    load_canonical_snapshot,
    load_final_score_rows,
)
from qa_gamelens_packet4_schema_inventory import (
    DEFAULT_PROJECT_ID,
    assert_read_only_sql,
)
from services.gamelens_level2_validation import (
    run_bounded_level2_validation,
)


def _query_rows(*, client, bigquery, sql: str, parameters) -> list[dict]:
    assert_read_only_sql(sql)
    config = bigquery.QueryJobConfig(query_parameters=list(parameters))
    return [dict(row) for row in client.query(sql, job_config=config).result()]


def load_bounded_claim_rows(
    *, client, bigquery, project_id: str, learning_run_id: str,
    capture_id: str, game_id: str
) -> list[dict]:
    query = f"""
        SELECT *
        FROM `{project_id}.GameLens_dev.claim_training_examples`
        WHERE learning_run_id = @learning_run_id
          AND capture_id = @capture_id
          AND game_id = @game_id
        ORDER BY claim_key
    """
    return _query_rows(
        client=client,
        bigquery=bigquery,
        sql=query,
        parameters=[
            bigquery.ScalarQueryParameter(
                "learning_run_id", "STRING", learning_run_id
            ),
            bigquery.ScalarQueryParameter("capture_id", "STRING", capture_id),
            bigquery.ScalarQueryParameter("game_id", "STRING", game_id),
        ],
    )


def load_accepted_fact_rows(
    *, client, bigquery, project_id: str, season: int, game_id: str
) -> list[dict]:
    query = f"""
        SELECT
            CAST(season AS STRING) AS season,
            game_id,
            game_date,
            game_week,
            team_id,
            team_abv,
            team_type,
            metric,
            SAFE_CAST(value AS FLOAT64) AS value,
            label,
            category,
            core_area,
            comparison_direction
        FROM `{project_id}.Analytics.game_team_metric_facts_{int(season)}`
        WHERE game_id = @game_id
          AND value IS NOT NULL
          AND comparison_direction IN ('higher', 'lower')
        ORDER BY team_type, metric
    """
    return _query_rows(
        client=client,
        bigquery=bigquery,
        sql=query,
        parameters=[
            bigquery.ScalarQueryParameter("game_id", "STRING", game_id)
        ],
    )


def build_packet4_level2_preview(
    *, client, bigquery, project_id: str, game_id: str,
    calculation_module: Any = None
) -> dict[str, Any]:
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
    learning_run_id = str(snapshot.get("learning_run_id") or "")
    capture_id = str(snapshot.get("capture_id") or "")
    season = int(snapshot.get("season"))
    claims = load_bounded_claim_rows(
        client=client,
        bigquery=bigquery,
        project_id=project_id,
        learning_run_id=learning_run_id,
        capture_id=capture_id,
        game_id=game_id,
    )
    facts = load_accepted_fact_rows(
        client=client,
        bigquery=bigquery,
        project_id=project_id,
        season=season,
        game_id=game_id,
    )
    result = run_bounded_level2_validation(
        learning_run_id=learning_run_id,
        capture_id=capture_id,
        game_id=game_id,
        claim_rows=claims,
        actual_fact_rows=facts,
        facts_accepted=bool(facts),
        calculation_module=calculation_module,
    )
    return {
        "access_mode": "read_only",
        "environment": "dev",
        "final_score": final_score,
        **result,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Preview bounded Packet 4 Level 2 validation."
    )
    parser.add_argument("--game-id", required=True)
    parser.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    parser.add_argument(
        "--dev-read-only",
        action="store_true",
        help="Confirm this Level 2 preview cannot write BigQuery.",
    )
    args = parser.parse_args(argv)
    if not args.dev_read_only:
        raise ValueError(
            "Pass --dev-read-only to confirm Packet 4 Level 2 is read-only"
        )

    from google.cloud import bigquery

    result = build_packet4_level2_preview(
        client=bigquery.Client(project=args.project_id),
        bigquery=bigquery,
        project_id=args.project_id,
        game_id=args.game_id,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
