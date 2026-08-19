"""Packet 5 Slice 1: read-only Admin source inventory and Game Journey sample.

The command never creates or mutates BigQuery objects.  It inventories the six
canonical GameLens_dev tables, checks their logical grains, and renders a
Schedule-anchored per-game journey for visual review before an API is added.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

from qa_gamelens_packet4_schema_inventory import assert_read_only_sql


PROJECT_ID = "nfl-stream-406420"
GAMELENS_DATASET = "GameLens_dev"
ALLOWED_STAGE_STATUSES = frozenset(
    {
        "complete",
        "waiting",
        "no_work_needed",
        "not_applicable",
        "warning",
        "failed",
    }
)
MAX_RUN_SUMMARY_ROWS = 12
KNOWN_GAP_REASONS = frozenset(
    {
        "kickoff_reached",
        "before_packet_2_capture_program",
        "level1_receipt_missing_after_kickoff",
    }
)


@dataclass(frozen=True)
class TableSpec:
    table_name: str
    role: str
    logical_key: tuple[str, ...]
    required_fields: tuple[str, ...]

    @property
    def table_id(self) -> str:
        return f"{PROJECT_ID}.{GAMELENS_DATASET}.{self.table_name}"


TABLE_SPECS = (
    TableSpec(
        "pregame_snapshots",
        "frozen_pregame_evidence",
        ("capture_id",),
        (
            "capture_id",
            "learning_run_id",
            "game_id",
            "captured_at",
            "payload_sha256",
        ),
    ),
    TableSpec(
        "stage_runs",
        "packet2_attempt_stage_receipts",
        ("attempt_id", "stage_name"),
        ("attempt_id", "stage_name", "status", "started_at", "finished_at"),
    ),
    TableSpec(
        "stage_game_results",
        "packet2_game_stage_receipts",
        ("attempt_id", "stage_name", "game_id"),
        (
            "attempt_id",
            "stage_name",
            "game_id",
            "status",
            "input_count",
            "output_count",
            "recorded_at",
        ),
    ),
    TableSpec(
        "claim_training_examples",
        "canonical_claim_rows",
        ("learning_run_id", "claim_key"),
        ("learning_run_id", "capture_id", "game_id", "claim_key", "run_id"),
    ),
    TableSpec(
        "game_model_outcomes",
        "canonical_game_grades",
        ("learning_run_id", "capture_id"),
        (
            "learning_run_id",
            "capture_id",
            "game_id",
            "grade_version",
            "graded_at",
        ),
    ),
    TableSpec(
        "postgame_learning_stage_receipts",
        "packet4_attempt_and_game_stage_receipts",
        ("attempt_id", "receipt_scope", "game_id", "stage_name"),
        (
            "attempt_id",
            "receipt_scope",
            "status",
            "recorded_at",
            "retryable",
        ),
    ),
)


def _validate_season(value: str) -> str:
    season = str(value or "").strip()
    if not re.fullmatch(r"20\d{2}", season):
        raise ValueError("season must be a four-digit NFL season")
    return season


def _window_type_for_phase(season_type: str) -> str:
    phase = str(season_type or "").strip().casefold()
    if phase == "preseason":
        return "preseason_to_date"
    if phase in {"regular season", "regular_season"}:
        return "regular_season_to_date"
    if phase in {"postseason", "post season"}:
        return "regular_plus_postseason_to_date"
    raise ValueError("season_type must be Preseason, Regular Season, or Postseason")


def _text(value: Any) -> str | None:
    value = str(value or "").strip()
    return value or None


def _iso(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def build_grain_query(spec: TableSpec) -> str:
    """Return a read-only logical-key reconciliation query for one table."""
    struct = ", ".join(f"`{field}` AS `{field}`" for field in spec.logical_key)
    required_missing = lambda field: (
        f"`{field}` IS NULL OR TRIM(CAST(`{field}` AS STRING)) = ''"
    )
    if spec.table_name == "postgame_learning_stage_receipts":
        attempt_missing = " OR ".join(
            required_missing(field) for field in ("attempt_id", "receipt_scope")
        )
        game_stage_missing = " OR ".join(
            required_missing(field) for field in spec.logical_key
        )
        missing = (
            "(receipt_scope = 'attempt' AND ("
            f"{attempt_missing} OR game_id IS NOT NULL OR stage_name IS NOT NULL)) "
            "OR (receipt_scope = 'game_stage' AND ("
            f"{game_stage_missing})) "
            "OR receipt_scope NOT IN ('attempt', 'game_stage')"
        )
    else:
        missing = " OR ".join(
            required_missing(field) for field in spec.logical_key
        )
    query = f"""
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT TO_JSON_STRING(STRUCT({struct})))
                AS logical_key_count,
            COUNT(*) - COUNT(DISTINCT TO_JSON_STRING(STRUCT({struct})))
                AS duplicate_key_count,
            COUNTIF({missing}) AS missing_key_row_count
        FROM `{spec.table_id}`
    """
    assert_read_only_sql(query)
    return query


def inspect_table(*, client: Any, spec: TableSpec) -> dict[str, Any]:
    """Inspect schema and logical-key counts without changing the source."""
    try:
        table = client.get_table(spec.table_id)
    except Exception as exc:
        return {
            "table": spec.table_id,
            "role": spec.role,
            "status": "unavailable",
            "logical_key": list(spec.logical_key),
            "error_type": type(exc).__name__,
            "error": str(exc),
        }

    fields = [str(field.name) for field in table.schema]
    missing_fields = sorted(set(spec.required_fields) - set(fields))
    result = {
        "table": spec.table_id,
        "role": spec.role,
        "status": "available" if not missing_fields else "schema_warning",
        "logical_key": list(spec.logical_key),
        "metadata_row_count": int(getattr(table, "num_rows", 0) or 0),
        "field_count": len(fields),
        "required_fields_missing": missing_fields,
    }
    if any(field not in fields for field in spec.logical_key):
        result.update(
            duplicate_key_count=None,
            missing_key_row_count=None,
            query_status="not_run_missing_logical_key_field",
        )
        return result

    try:
        rows = list(client.query(build_grain_query(spec)).result())
        counts = dict(rows[0]) if rows else {}
        result.update(
            row_count=int(counts.get("row_count") or 0),
            logical_key_count=int(counts.get("logical_key_count") or 0),
            duplicate_key_count=int(counts.get("duplicate_key_count") or 0),
            missing_key_row_count=int(counts.get("missing_key_row_count") or 0),
            query_status="complete",
        )
    except Exception as exc:
        result.update(
            query_status="failed",
            query_error_type=type(exc).__name__,
            query_error=str(exc),
        )
    return result


def build_game_journey_query(season: str) -> str:
    """Build the clear Schedule-anchored joins used for the visual sample."""
    season = _validate_season(season)
    query = f"""
        WITH canonical_snapshots AS (
            SELECT
                game_id,
                learning_run_id,
                capture_id,
                captured_at,
                payload_sha256,
                metric_source_date,
                ranking_as_of_date,
                ranking_context_available,
                ranking_context_reason,
                metric_pipeline_run_id
            FROM `{PROJECT_ID}.{GAMELENS_DATASET}.pregame_snapshots`
            WHERE learning_run_id = @learning_run_id
              AND capture_status = 'captured'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY learning_run_id, game_id
                ORDER BY captured_at, capture_id
            ) = 1
        ),
        latest_capture_stages AS (
            SELECT * EXCEPT(row_number)
            FROM (
                SELECT
                    game_id,
                    stage_name,
                    attempt_id,
                    status,
                    reason,
                    capture_id,
                    learning_run_id,
                    input_count,
                    output_count,
                    recorded_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY game_id, stage_name
                        ORDER BY recorded_at DESC, attempt_id DESC
                    ) AS row_number
                FROM `{PROJECT_ID}.{GAMELENS_DATASET}.stage_game_results`
                WHERE stage_name IN ('snapshot_capture', 'level1_claim_extraction')
                  AND (learning_run_id = @learning_run_id OR learning_run_id IS NULL)
            )
            WHERE row_number = 1
        ),
        capture_stage_summary AS (
            SELECT
                game_id,
                MAX(IF(stage_name = 'snapshot_capture', status, NULL))
                    AS snapshot_receipt_status,
                MAX(IF(stage_name = 'snapshot_capture', reason, NULL))
                    AS snapshot_receipt_reason,
                MAX(IF(stage_name = 'snapshot_capture', attempt_id, NULL))
                    AS snapshot_attempt_id,
                MAX(IF(stage_name = 'level1_claim_extraction', status, NULL))
                    AS level1_receipt_status,
                MAX(IF(stage_name = 'level1_claim_extraction', reason, NULL))
                    AS level1_receipt_reason,
                MAX(IF(stage_name = 'level1_claim_extraction', output_count, NULL))
                    AS level1_output_count,
                MAX(IF(stage_name = 'level1_claim_extraction', attempt_id, NULL))
                    AS level1_attempt_id
            FROM latest_capture_stages
            GROUP BY game_id
        ),
        claim_counts AS (
            SELECT
                learning_run_id,
                capture_id,
                game_id,
                COUNT(*) AS claim_count,
                COUNTIF(validation_result IS NOT NULL) AS level2_count,
                COUNTIF(feature_formula_version IS NOT NULL) AS level3_count,
                COUNTIF(run_id != learning_run_id) AS run_alias_disagreement_count
            FROM `{PROJECT_ID}.{GAMELENS_DATASET}.claim_training_examples`
            WHERE learning_run_id = @learning_run_id
            GROUP BY learning_run_id, capture_id, game_id
        ),
        grade_counts AS (
            SELECT
                learning_run_id,
                capture_id,
                game_id,
                COUNT(*) AS grade_count,
                ANY_VALUE(grade_version) AS grade_version,
                MAX(graded_at) AS graded_at
            FROM `{PROJECT_ID}.{GAMELENS_DATASET}.game_model_outcomes`
            WHERE learning_run_id = @learning_run_id
            GROUP BY learning_run_id, capture_id, game_id
        ),
        latest_postgame_stages AS (
            SELECT * EXCEPT(row_number)
            FROM (
                SELECT
                    game_id,
                    stage_name,
                    attempt_id,
                    status,
                    reason,
                    learning_run_id,
                    capture_id,
                    input_count,
                    output_count,
                    retryable,
                    failed_boundary,
                    message,
                    log_reference,
                    recorded_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY game_id, stage_name
                        ORDER BY recorded_at DESC, attempt_id DESC
                    ) AS row_number
                FROM `{PROJECT_ID}.{GAMELENS_DATASET}.postgame_learning_stage_receipts`
                WHERE receipt_scope = 'game_stage'
                  AND stage_name IN ('game_grade', 'level2_validation', 'level3_features')
                  AND (learning_run_id = @learning_run_id OR learning_run_id IS NULL)
            )
            WHERE row_number = 1
        ),
        postgame_stage_summary AS (
            SELECT
                game_id,
                MAX(IF(stage_name = 'game_grade', status, NULL)) AS grade_status,
                MAX(IF(stage_name = 'game_grade', reason, NULL)) AS grade_reason,
                MAX(IF(stage_name = 'game_grade', attempt_id, NULL)) AS grade_attempt_id,
                MAX(IF(stage_name = 'level2_validation', status, NULL)) AS level2_status,
                MAX(IF(stage_name = 'level2_validation', reason, NULL)) AS level2_reason,
                MAX(IF(stage_name = 'level3_features', status, NULL)) AS level3_status,
                MAX(IF(stage_name = 'level3_features', reason, NULL)) AS level3_reason,
                MAX(IF(status = 'failed', failed_boundary, NULL)) AS failed_boundary,
                MAX(IF(status = 'failed', retryable, NULL)) AS retryable,
                MAX(IF(status = 'failed', message, NULL)) AS failure_message,
                MAX(IF(status = 'failed', log_reference, NULL)) AS log_reference
            FROM latest_postgame_stages
            GROUP BY game_id
        ),
        score_counts AS (
            SELECT CAST(gameID AS STRING) AS game_id, COUNT(*) AS score_row_count
            FROM `{PROJECT_ID}.Scores.scores`
            WHERE CAST(gameID AS STRING) IN (
                SELECT CAST(gameID AS STRING)
                FROM `{PROJECT_ID}.League.schedule`
                WHERE gameDate BETWEEN @start_date AND @end_date
                  AND CAST(season AS STRING) = @season
                  AND seasonType = @season_type
            )
            GROUP BY game_id
        ),
        fact_counts AS (
            SELECT game_id, COUNT(*) AS fact_row_count
            FROM `{PROJECT_ID}.Analytics.game_team_metric_facts_{season}`
            WHERE game_id IN (
                SELECT CAST(gameID AS STRING)
                FROM `{PROJECT_ID}.League.schedule`
                WHERE gameDate BETWEEN @start_date AND @end_date
                  AND CAST(season AS STRING) = @season
                  AND seasonType = @season_type
            )
            GROUP BY game_id
        ),
        windowed_counts AS (
            SELECT
                latest_included_game_id AS game_id,
                COUNT(*) AS windowed_row_count
            FROM `{PROJECT_ID}.Analytics.team_metrics_windowed_{season}`
            WHERE latest_included_game_id IN (
                SELECT CAST(gameID AS STRING)
                FROM `{PROJECT_ID}.League.schedule`
                WHERE gameDate BETWEEN @start_date AND @end_date
                  AND CAST(season AS STRING) = @season
                  AND seasonType = @season_type
            )
            GROUP BY game_id
        ),
        ranking_counts AS (
            SELECT
                CAST(schedule.gameID AS STRING) AS game_id,
                COUNT(rankings.metric) AS ranking_row_count,
                COUNT(DISTINCT rankings.team_abv) AS ranking_team_count
            FROM `{PROJECT_ID}.League.schedule` AS schedule
            LEFT JOIN `{PROJECT_ID}.Analytics.team_metric_rankings_{season}` AS rankings
                ON rankings.as_of_date = schedule.gameDate
               AND rankings.team_abv IN (schedule.away, schedule.home)
               AND rankings.window_type = @window_type
            WHERE schedule.gameDate BETWEEN @start_date AND @end_date
              AND CAST(schedule.season AS STRING) = @season
              AND schedule.seasonType = @season_type
            GROUP BY game_id
        )
        SELECT
            CAST(schedule.gameID AS STRING) AS game_id,
            schedule.gameDate AS game_date,
            SAFE_CAST(schedule.gameTime_epoch AS TIMESTAMP) AS scheduled_kickoff,
            schedule.gameStatus AS game_status,
            CAST(schedule.season AS STRING) AS season,
            schedule.seasonType AS season_type,
            schedule.away,
            schedule.home,
            snapshots.* EXCEPT(game_id, learning_run_id),
            @learning_run_id AS learning_run_id,
            capture_stages.* EXCEPT(game_id),
            COALESCE(claims.claim_count, 0) AS claim_count,
            COALESCE(claims.level2_count, 0) AS level2_count,
            COALESCE(claims.level3_count, 0) AS level3_count,
            COALESCE(claims.run_alias_disagreement_count, 0)
                AS run_alias_disagreement_count,
            COALESCE(grades.grade_count, 0) AS grade_count,
            grades.grade_version,
            grades.graded_at,
            postgame.* EXCEPT(game_id),
            COALESCE(scores.score_row_count, 0) AS score_row_count,
            COALESCE(facts.fact_row_count, 0) AS fact_row_count,
            COALESCE(windowed.windowed_row_count, 0) AS windowed_row_count,
            COALESCE(rankings.ranking_row_count, 0) AS ranking_row_count,
            COALESCE(rankings.ranking_team_count, 0) AS ranking_team_count
        FROM `{PROJECT_ID}.League.schedule` AS schedule
        LEFT JOIN canonical_snapshots AS snapshots
            ON CAST(schedule.gameID AS STRING) = snapshots.game_id
        LEFT JOIN capture_stage_summary AS capture_stages
            ON CAST(schedule.gameID AS STRING) = capture_stages.game_id
        LEFT JOIN claim_counts AS claims
            ON snapshots.learning_run_id = claims.learning_run_id
           AND snapshots.capture_id = claims.capture_id
           AND CAST(schedule.gameID AS STRING) = claims.game_id
        LEFT JOIN grade_counts AS grades
            ON snapshots.learning_run_id = grades.learning_run_id
           AND snapshots.capture_id = grades.capture_id
           AND CAST(schedule.gameID AS STRING) = grades.game_id
        LEFT JOIN postgame_stage_summary AS postgame
            ON CAST(schedule.gameID AS STRING) = postgame.game_id
        LEFT JOIN score_counts AS scores
            ON CAST(schedule.gameID AS STRING) = scores.game_id
        LEFT JOIN fact_counts AS facts
            ON CAST(schedule.gameID AS STRING) = facts.game_id
        LEFT JOIN windowed_counts AS windowed
            ON CAST(schedule.gameID AS STRING) = windowed.game_id
        LEFT JOIN ranking_counts AS rankings
            ON CAST(schedule.gameID AS STRING) = rankings.game_id
        WHERE schedule.gameDate BETWEEN @start_date AND @end_date
          AND CAST(schedule.season AS STRING) = @season
          AND schedule.seasonType = @season_type
        ORDER BY schedule.gameDate, scheduled_kickoff, game_id
    """
    assert_read_only_sql(query)
    return query


def build_run_summary_query() -> str:
    """Return a bounded attempt-grain summary without forcing runs onto games."""
    query = f"""
        WITH packet2_attempts AS (
            SELECT
                'stage_runs' AS source,
                attempt_id,
                stage_name,
                status,
                reason,
                input_count,
                output_count,
                duration_ms,
                started_at,
                finished_at
            FROM `{PROJECT_ID}.{GAMELENS_DATASET}.stage_runs`
            WHERE season = @season
              AND season_type = @season_type
        ),
        packet4_attempts AS (
            SELECT
                'postgame_learning_stage_receipts' AS source,
                attempt.attempt_id,
                'postgame_learning' AS stage_name,
                attempt.status,
                attempt.reason,
                attempt.input_count,
                attempt.output_count,
                attempt.duration_ms,
                attempt.started_at,
                attempt.finished_at
            FROM `{PROJECT_ID}.{GAMELENS_DATASET}.postgame_learning_stage_receipts`
                AS attempt
            WHERE attempt.receipt_scope = 'attempt'
              AND EXISTS (
                  SELECT 1
                  FROM `{PROJECT_ID}.{GAMELENS_DATASET}.postgame_learning_stage_receipts`
                      AS game_stage
                  WHERE game_stage.attempt_id = attempt.attempt_id
                    AND game_stage.receipt_scope = 'game_stage'
                    AND game_stage.learning_run_id = @learning_run_id
              )
        )
        SELECT *
        FROM (
            SELECT * FROM packet2_attempts
            UNION ALL
            SELECT * FROM packet4_attempts
        )
        ORDER BY finished_at DESC, attempt_id DESC, stage_name DESC
        LIMIT @run_limit
    """
    assert_read_only_sql(query)
    return query


def _stage(
    name: str,
    status: str,
    reason: str,
    *,
    source: str,
    count: int | None = None,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if status not in ALLOWED_STAGE_STATUSES:
        raise ValueError(f"unsupported Game Journey status: {status}")
    attention = "none"
    if status == "failed":
        attention = "action_required"
    elif status == "warning":
        attention = "known_gap" if reason in KNOWN_GAP_REASONS else "action_required"
    return {
        "stage": name,
        "status": status,
        "reason": reason,
        "attention": attention,
        "source": source,
        "count": count,
        "details": dict(details or {}),
    }


def _receipt_status(value: Any, *, absent: str = "waiting") -> str:
    status = _text(value)
    if status in {"success", "no_op", "complete", "completed"}:
        return "complete"
    if status in {"waiting", "deferred"}:
        return "waiting"
    if status == "skipped":
        return "not_applicable"
    if status in {"failure", "failed", "partial_failure"}:
        return "failed"
    return absent


def build_game_journey(row: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    """Translate one joined row into honest pregame and postgame clocks."""
    raw = dict(row)
    game_id = str(raw.get("game_id") or "")
    capture_id = _text(raw.get("capture_id"))
    kickoff_value = raw.get("scheduled_kickoff")
    if isinstance(kickoff_value, datetime):
        kickoff = kickoff_value
    elif kickoff_value:
        kickoff = datetime.fromisoformat(str(kickoff_value).replace("Z", "+00:00"))
    else:
        kickoff = None
    if kickoff is not None and kickoff.tzinfo is None:
        kickoff = kickoff.replace(tzinfo=timezone.utc)
    kickoff_passed = bool(kickoff and kickoff <= now)
    game_final = str(raw.get("game_status") or "").casefold() == "final"

    score_count = int(raw.get("score_row_count") or 0)
    fact_count = int(raw.get("fact_row_count") or 0)
    windowed_count = int(raw.get("windowed_row_count") or 0)
    ranking_count = int(raw.get("ranking_row_count") or 0)
    ranking_team_count = int(raw.get("ranking_team_count") or 0)
    data_load = [
        _stage(
            "schedule",
            "complete",
            "scheduled_game_row_present",
            source="League.schedule",
        ),
        _stage(
            "final_score_stats",
            "complete" if score_count == 2 else (
                "warning" if score_count else "waiting"
            ),
            "two_final_score_rows_present" if score_count == 2 else (
                "partial_final_score_rows" if score_count else (
                    "final_score_etl_pending" if game_final else "game_not_final"
                )
            ),
            source="Scores.scores",
            count=score_count,
        ),
        _stage(
            "target_facts",
            "complete" if fact_count > 0 else "waiting",
            "target_game_facts_present" if fact_count > 0 else (
                "facts_etl_pending" if game_final else "game_not_final"
            ),
            source=f"Analytics.game_team_metric_facts_{raw.get('season')}",
            count=fact_count,
        ),
        _stage(
            "target_windowed",
            "complete" if windowed_count > 0 else "waiting",
            "target_game_window_rows_present" if windowed_count > 0 else (
                "windowed_etl_pending" if fact_count else "target_facts_required"
            ),
            source=f"Analytics.team_metrics_windowed_{raw.get('season')}",
            count=windowed_count,
        ),
        _stage(
            "target_rankings",
            "complete" if ranking_team_count == 2 and ranking_count > 0 else (
                "warning" if ranking_count > 0 else "waiting"
            ),
            "both_game_teams_ranked_for_game_date" if ranking_team_count == 2 else (
                "partial_game_team_ranking_coverage" if ranking_count > 0 else (
                    "rankings_etl_pending" if windowed_count else "windowed_rows_required"
                )
            ),
            source=f"Analytics.team_metric_rankings_{raw.get('season')}",
            count=ranking_count,
            details={"team_count": ranking_team_count},
        ),
    ]

    pregame = []
    if capture_id:
        snapshot_status, snapshot_reason = "complete", "canonical_capture_present"
    else:
        raw_snapshot_status = _text(raw.get("snapshot_receipt_status"))
        receipt_status = _receipt_status(raw_snapshot_status)
        if receipt_status == "failed":
            snapshot_status, snapshot_reason = "failed", (
                _text(raw.get("snapshot_receipt_reason")) or "snapshot_capture_failed"
            )
        elif raw_snapshot_status == "skipped" or kickoff_passed:
            snapshot_status, snapshot_reason = "warning", (
                _text(raw.get("snapshot_receipt_reason")) or "capture_missing_after_kickoff"
            )
        else:
            snapshot_status, snapshot_reason = "waiting", (
                _text(raw.get("snapshot_receipt_reason")) or "capture_not_due_or_not_attempted"
            )
    pregame.append(
        _stage(
            "snapshot",
            snapshot_status,
            snapshot_reason,
            source="pregame_snapshots + stage_game_results",
            details={
                "capture_id": capture_id,
                "attempt_id": _text(raw.get("snapshot_attempt_id")),
                "captured_at": _iso(raw.get("captured_at")),
            },
        )
    )
    pipeline_run_id = _text(raw.get("metric_pipeline_run_id"))
    ranking_available = raw.get("ranking_context_available")
    if not capture_id:
        context_status, context_reason = "not_applicable", "canonical_snapshot_required"
    elif raw.get("metric_source_date") or pipeline_run_id:
        context_status = "complete"
        context_reason = (
            "frozen_context_recorded"
            if ranking_available is not False
            else _text(raw.get("ranking_context_reason"))
            or "frozen_context_recorded_without_rankings"
        )
    else:
        context_status, context_reason = "warning", "snapshot_lineage_incomplete"
    pregame.append(
        _stage(
            "frozen_context",
            context_status,
            context_reason,
            source="pregame_snapshots.evidence_context + source lineage",
            details={
                "metric_source_date": _iso(raw.get("metric_source_date")),
                "ranking_as_of_date": _iso(raw.get("ranking_as_of_date")),
                "metric_pipeline_run_id": pipeline_run_id,
                "ranking_context_available": ranking_available,
                "ranking_context_reason": _text(raw.get("ranking_context_reason")),
            },
        )
    )
    raw_level1_status = _text(raw.get("level1_receipt_status"))
    level1_status = _receipt_status(raw_level1_status)
    if not raw_level1_status and not capture_id:
        level1_status = "not_applicable"
    elif not raw_level1_status and capture_id and kickoff_passed:
        level1_status = "warning"
    level1_count = int(raw.get("claim_count") or 0)
    level1_reason = _text(raw.get("level1_receipt_reason"))
    if level1_status == "complete" and level1_count == 0:
        level1_reason = level1_reason or "processed_with_zero_claims"
    elif not level1_reason:
        if level1_status == "warning":
            level1_reason = "level1_receipt_missing_after_kickoff"
        else:
            level1_reason = "awaiting_level1" if capture_id else "capture_required"
    pregame.append(
        _stage(
            "level1",
            level1_status,
            level1_reason,
            source="stage_game_results + claim_training_examples",
            count=level1_count,
            details={"attempt_id": _text(raw.get("level1_attempt_id"))},
        )
    )

    postgame = []
    grade_count = int(raw.get("grade_count") or 0)
    if grade_count == 1:
        grade_status, grade_reason = "complete", "canonical_frozen_grade_present"
    elif _receipt_status(raw.get("grade_status")) == "failed":
        grade_status = "failed"
        grade_reason = _text(raw.get("grade_reason")) or "game_grade_failed"
    elif not capture_id:
        grade_status, grade_reason = "not_applicable", "canonical_capture_required"
    else:
        grade_status = _receipt_status(raw.get("grade_status"))
        grade_reason = _text(raw.get("grade_reason")) or "awaiting_game_grade"
    postgame.append(
        _stage(
            "game_grade",
            grade_status,
            grade_reason,
            source="game_model_outcomes + postgame_learning_stage_receipts",
            count=grade_count,
            details={
                "grade_version": _text(raw.get("grade_version")),
                "graded_at": _iso(raw.get("graded_at")),
                "attempt_id": _text(raw.get("grade_attempt_id")),
            },
        )
    )
    for stage_name, count_field, receipt_field, reason_field in (
        ("level2", "level2_count", "level2_status", "level2_reason"),
        ("level3", "level3_count", "level3_status", "level3_reason"),
    ):
        count = int(raw.get(count_field) or 0)
        claim_count = int(raw.get("claim_count") or 0)
        raw_receipt_status = _text(raw.get(receipt_field))
        raw_reason = _text(raw.get(reason_field))
        if not capture_id:
            status, reason = "not_applicable", "canonical_capture_required"
        elif raw_receipt_status == "no_op" and raw_reason == "zero_claims":
            status, reason = "complete", "completed_zero_claims"
        elif claim_count == 0 and level1_status == "complete":
            status, reason = "no_work_needed", "zero_level1_claims"
        else:
            status = _receipt_status(raw_receipt_status)
            reason = raw_reason or f"awaiting_{stage_name}"
        postgame.append(
            _stage(
                stage_name,
                status,
                reason,
                source="claim_training_examples + postgame_learning_stage_receipts",
                count=count,
                details={
                    "receipt_status": raw_receipt_status,
                    "receipt_reason": raw_reason,
                },
            )
        )
    postgame.append(
        _stage(
            "level4",
            "not_applicable",
            "packet5_does_not_implement_weekly_level4",
            source="future_weekly_batch_contract",
            details={"batch_state": "not_eligible"},
        )
    )

    stages = data_load + pregame + postgame
    first_issue = next(
        (
            {
                "stage": stage["stage"],
                "status": stage["status"],
                "reason": stage["reason"],
                "attention": stage["attention"],
            }
            for stage in stages
            if stage["attention"] == "action_required"
        ),
        None,
    ) or next(
        (
            {
                "stage": stage["stage"],
                "status": stage["status"],
                "reason": stage["reason"],
                "attention": stage["attention"],
            }
            for stage in stages
            if stage["attention"] == "known_gap"
        ),
        None,
    )
    return {
        "game_id": game_id,
        "matchup": f"{raw.get('away') or '?'} @ {raw.get('home') or '?'}",
        "game_date": _iso(raw.get("game_date")),
        "scheduled_kickoff": _iso(kickoff_value),
        "game_status": raw.get("game_status"),
        "season": str(raw.get("season") or ""),
        "season_type": raw.get("season_type"),
        "learning_run_id": raw.get("learning_run_id"),
        "capture_id": capture_id,
        "data_load": data_load,
        "pregame": pregame,
        "postgame": postgame,
        "first_issue": first_issue,
        "traceability": {
            "failed_boundary": _text(raw.get("failed_boundary")),
            "retryable": raw.get("retryable"),
            "failure_message": _text(raw.get("failure_message")),
            "log_reference": _text(raw.get("log_reference")),
            "run_alias_disagreement_count": int(
                raw.get("run_alias_disagreement_count") or 0
            ),
        },
    }


def load_game_journeys(
    *, client: Any, bigquery: Any, season: str, season_type: str,
    learning_run_id: str, start_date: date, end_date: date,
    now: datetime,
) -> tuple[list[dict[str, Any]], str]:
    query = build_game_journey_query(season)
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("learning_run_id", "STRING", learning_run_id),
            bigquery.ScalarQueryParameter("season", "STRING", season),
            bigquery.ScalarQueryParameter("season_type", "STRING", season_type),
            bigquery.ScalarQueryParameter(
                "window_type", "STRING", _window_type_for_phase(season_type)
            ),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )
    rows = client.query(query, job_config=config).result()
    return [build_game_journey(dict(row), now=now) for row in rows], query


def load_run_summary(
    *, client: Any, bigquery: Any, season: str, season_type: str,
    learning_run_id: str, run_limit: int = MAX_RUN_SUMMARY_ROWS,
) -> tuple[list[dict[str, Any]], str]:
    if not 1 <= run_limit <= MAX_RUN_SUMMARY_ROWS:
        raise ValueError(f"run_limit must be between 1 and {MAX_RUN_SUMMARY_ROWS}")
    query = build_run_summary_query()
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("season", "STRING", season),
            bigquery.ScalarQueryParameter("season_type", "STRING", season_type),
            bigquery.ScalarQueryParameter(
                "learning_run_id", "STRING", learning_run_id
            ),
            bigquery.ScalarQueryParameter("run_limit", "INT64", run_limit),
        ]
    )
    rows = [dict(row) for row in client.query(query, job_config=config).result()]
    return rows, query


def build_packet5_inventory(
    *, client: Any, bigquery: Any, season: str, season_type: str,
    learning_run_id: str, start_date: date, end_date: date,
    now: datetime | None = None,
) -> dict[str, Any]:
    season = _validate_season(season)
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")
    cohort = str(learning_run_id or "").strip()
    if not cohort:
        raise ValueError("learning_run_id is required")
    phase = str(season_type or "").strip()
    if not phase:
        raise ValueError("season_type is required")
    audited_at = now or datetime.now(timezone.utc)
    tables = [inspect_table(client=client, spec=spec) for spec in TABLE_SPECS]
    games, _ = load_game_journeys(
        client=client,
        bigquery=bigquery,
        season=season,
        season_type=phase,
        learning_run_id=cohort,
        start_date=start_date,
        end_date=end_date,
        now=audited_at,
    )
    run_summary, _ = load_run_summary(
        client=client,
        bigquery=bigquery,
        season=season,
        season_type=phase,
        learning_run_id=cohort,
    )
    action_required_game_count = sum(
        any(
            stage["attention"] == "action_required"
            for clock in ("data_load", "pregame", "postgame")
            for stage in game[clock]
        )
        for game in games
    )
    known_gap_game_count = sum(
        any(
            stage["attention"] == "known_gap"
            for clock in ("data_load", "pregame", "postgame")
            for stage in game[clock]
        )
        for game in games
    )
    return {
        "access_mode": "read_only",
        "checkpoint": "packet5_slice1_inventory_and_game_journey",
        "audited_at": audited_at.isoformat(),
        "filters": {
            "season": season,
            "season_type": phase,
            "learning_run_id": cohort,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
        "inventory_summary": {
            "table_count": len(tables),
            "available_count": sum(t["status"] == "available" for t in tables),
            "warning_count": sum(t["status"] != "available" for t in tables),
            "duplicate_key_count": sum(
                int(t.get("duplicate_key_count") or 0) for t in tables
            ),
            "missing_key_row_count": sum(
                int(t.get("missing_key_row_count") or 0) for t in tables
            ),
        },
        "tables": tables,
        "run_summary": run_summary,
        "game_summary": {
            "scheduled_game_count": len(games),
            "captured_game_count": sum(bool(g["capture_id"]) for g in games),
            "game_with_issue_count": sum(bool(g["first_issue"]) for g in games),
            "action_required_game_count": action_required_game_count,
            "known_gap_game_count": known_gap_game_count,
        },
        "games": games,
        "write_performed": False,
    }


_STATUS_TOKEN = {
    "complete": "OK",
    "waiting": "WAIT",
    "no_work_needed": "NO WORK",
    "not_applicable": "N/A",
    "warning": "WARN",
    "failed": "FAIL",
}


def _stage_map(game: Mapping[str, Any], clock: str) -> dict[str, str]:
    return {
        stage["stage"]: (
            "OK (0)"
            if stage["status"] == "complete" and stage.get("count") == 0
            else _STATUS_TOKEN[stage["status"]]
        )
        for stage in game.get(clock, [])
    }


def _render_rows(
    headers: Sequence[str], rows: Iterable[Sequence[Any]], *, max_width: int = 38
) -> str:
    values = [[str(value if value is not None else "") for value in row] for row in rows]
    widths = [len(header) for header in headers]
    for row in values:
        for index, value in enumerate(row):
            widths[index] = min(max_width, max(widths[index], len(value)))
    def line(row: Sequence[str]) -> str:
        return " | ".join(value[: widths[i]].ljust(widths[i]) for i, value in enumerate(row))
    divider = "-+-".join("-" * width for width in widths)
    return "\n".join([line(headers), divider, *(line(row) for row in values)])


def render_visual_report(report: Mapping[str, Any]) -> str:
    """Render compact tables backed by the exact JSON report."""
    inventory = report["inventory_summary"]
    game_summary = report["game_summary"]
    lines = [
        "PACKET 5 / CHECKPOINT 1 / READ ONLY",
        (
            f"Tables: {inventory['available_count']}/{inventory['table_count']} available | "
            f"duplicate keys: {inventory['duplicate_key_count']} | "
            f"missing keys: {inventory['missing_key_row_count']}"
        ),
        (
            f"Games: {game_summary['scheduled_game_count']} scheduled | "
            f"{game_summary['captured_game_count']} captured | "
            f"{game_summary.get('action_required_game_count', 0)} need attention | "
            f"{game_summary.get('known_gap_game_count', 0)} known gaps"
        ),
        "",
        "SOURCE GRAINS",
    ]
    source_headers = (
        "Table", "Rows", "Logical keys", "Duplicates", "Invalid keys", "Status",
    )
    source_rows = [
        (
            str(table["table"]).rsplit(".", 1)[-1],
            table.get("row_count", table.get("metadata_row_count", "")),
            table.get("logical_key_count", ""),
            table.get("duplicate_key_count", ""),
            table.get("missing_key_row_count", ""),
            table.get("status", ""),
        )
        for table in report.get("tables", [])
    ]
    lines.extend([
        _render_rows(source_headers, source_rows),
        "",
        "RUN SUMMARY",
    ])
    run_headers = (
        "Source", "Attempt", "Stage", "Status", "Reason", "In", "Out",
        "Duration ms", "Finished",
    )
    run_rows = [
        (
            row.get("source", ""),
            row.get("attempt_id", ""),
            row.get("stage_name", ""),
            str(row.get("status") or "").upper(),
            row.get("reason", ""),
            row.get("input_count", ""),
            row.get("output_count", ""),
            row.get("duration_ms", ""),
            _iso(row.get("finished_at")) or "",
        )
        for row in report.get("run_summary", [])
    ]
    lines.extend([
        _render_rows(run_headers, run_rows, max_width=34) if run_rows else "None.",
        "",
        "DAILY DATA LOAD CLOCK",
    ])
    load_headers = (
        "Game", "Matchup", "Schedule", "Score/stats", "Facts", "Windowed",
        "Rankings",
    )
    load_rows = []
    for game in report["games"]:
        stages = _stage_map(game, "data_load")
        load_rows.append(
            (
                game["game_id"], game["matchup"], stages.get("schedule"),
                stages.get("final_score_stats"), stages.get("target_facts"),
                stages.get("target_windowed"), stages.get("target_rankings"),
            )
        )
    lines.extend([
        _render_rows(load_headers, load_rows),
        "",
        "GAMELENS PREGAME CLOCK",
    ])
    pre_headers = ("Game", "Snapshot", "Frozen context", "L1", "First issue")
    pre_rows = []
    for game in report["games"]:
        stages = _stage_map(game, "pregame")
        issue = game.get("first_issue") or {}
        pre_rows.append(
            (
                game["game_id"], stages.get("snapshot"),
                stages.get("frozen_context"), stages.get("level1"),
                f"{issue.get('stage', '')}: {issue.get('reason', '')}".strip(": "),
            )
        )
    lines.extend([
        _render_rows(pre_headers, pre_rows),
        "",
        "POSTGAME LEARNING CLOCK",
    ])
    post_headers = (
        "Game", "Grade", "L2", "L3", "L4",
    )
    post_rows = []
    for game in report["games"]:
        stages = _stage_map(game, "postgame")
        post_rows.append(
            (
                game["game_id"], stages.get("game_grade"),
                stages.get("level2"), stages.get("level3"), stages.get("level4"),
            )
        )
    lines.append(_render_rows(post_headers, post_rows))
    attention_rows = []
    known_gap_rows = []
    for game in report["games"]:
        for clock in ("data_load", "pregame", "postgame"):
            for stage in game.get(clock, []):
                row = (
                    game["game_id"],
                    clock,
                    stage["stage"],
                    _STATUS_TOKEN[stage["status"]],
                    stage["reason"],
                )
                if stage.get("attention") == "action_required":
                    attention_rows.append(row)
                elif stage.get("attention") == "known_gap":
                    known_gap_rows.append(row)
    lines.extend(
        [
            "",
            "NEEDS ATTENTION",
            (
                _render_rows(
                    ("Game", "Clock", "Stage", "Status", "Reason"),
                    attention_rows,
                    max_width=72,
                )
                if attention_rows
                else "None."
            ),
            "",
            "KNOWN GAPS",
            (
                _render_rows(
                    ("Game", "Clock", "Stage", "Status", "Reason"),
                    known_gap_rows,
                    max_width=72,
                )
                if known_gap_rows
                else "None."
            ),
        ]
    )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only Packet 5 inventory and Game Journey checkpoint."
    )
    parser.add_argument("--season", default="2026")
    parser.add_argument("--season-type", required=True)
    parser.add_argument("--learning-run-id", required=True)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--dev-read-only",
        action="store_true",
        help="Confirm that this command may only read approved sources.",
    )
