"""Read-only Schedule-to-snapshot coverage audit for Packet 2."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timezone
from typing import Any, Mapping, Optional

from google.cloud import bigquery

from runtime_config import PRODUCTION_DATASETS, RuntimeConfig
from services.gamelens_snapshot_storage import (
    GAMELENS_DEV_DATASET,
    PREGAME_SNAPSHOTS_TABLE,
    STAGE_GAME_RESULTS_TABLE,
)


PACKET_2_CAPTURE_STARTED_AT = datetime(
    2026,
    8,
    12,
    18,
    48,
    49,
    tzinfo=timezone.utc,
)


def _as_utc(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if not isinstance(value, datetime):
        value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def classify_snapshot_coverage(
    row: Mapping[str, Any],
    *,
    now: datetime,
    capture_started_at: datetime = PACKET_2_CAPTURE_STARTED_AT,
) -> dict:
    """Classify coverage without ever reconstructing a past pregame read."""
    now_utc = _as_utc(now)
    started_utc = _as_utc(capture_started_at)
    if now_utc is None or started_utc is None:
        raise ValueError("audit clocks are required")

    game_id = str(row.get("game_id") or "")
    kickoff = _as_utc(row.get("scheduled_kickoff"))
    capture_id = str(row.get("capture_id") or "").strip() or None
    attempt_status = (
        str(row.get("latest_attempt_status") or "").strip()
        or "not_attempted"
    )
    attempt_reason = row.get("latest_attempt_reason")

    if capture_id:
        coverage_status = "captured"
        coverage_reason = None
    elif kickoff is None:
        coverage_status = "capture_missing"
        coverage_reason = "scheduled_kickoff_missing"
    elif kickoff > now_utc:
        coverage_status = "upcoming"
        coverage_reason = "capture_window_open"
    else:
        coverage_status = "capture_missing"
        if kickoff < started_utc:
            coverage_reason = "before_packet_2_capture_program"
        elif attempt_status == "failure":
            coverage_reason = "latest_attempt_failed"
        elif attempt_status == "waiting":
            coverage_reason = "latest_attempt_waiting_at_cutoff"
        elif attempt_status == "skipped":
            coverage_reason = "latest_attempt_skipped"
        elif attempt_status == "success":
            coverage_reason = "canonical_snapshot_missing_after_success"
        elif attempt_status == "no_op":
            coverage_reason = "canonical_snapshot_missing_after_no_op"
        else:
            coverage_reason = "no_successful_capture_before_kickoff"

    away = str(row.get("away") or "").strip()
    home = str(row.get("home") or "").strip()
    game_date = row.get("game_date")
    captured_at = _as_utc(row.get("captured_at"))
    return {
        "game_id": game_id,
        "matchup": (
            f"{away} @ {home}" if away or home else None
        ),
        "game_date": str(game_date) if game_date is not None else None,
        "scheduled_kickoff": kickoff.isoformat() if kickoff else None,
        "game_status": row.get("game_status"),
        "season": str(row.get("season") or "") or None,
        "season_type": row.get("season_type"),
        "capture_id": capture_id,
        "learning_run_id": row.get("learning_run_id"),
        "captured_at": captured_at.isoformat() if captured_at else None,
        "coverage_status": coverage_status,
        "coverage_reason": coverage_reason,
        "latest_attempt_id": row.get("latest_attempt_id"),
        "latest_attempt_status": attempt_status,
        "latest_attempt_reason": attempt_reason,
    }


class BigQuerySnapshotCoverageAuditor:
    """Read production Schedule and development evidence without writing."""

    def __init__(
        self,
        *,
        client: bigquery.Client,
        runtime_config: RuntimeConfig,
    ):
        if not runtime_config.is_dev:
            raise ValueError("Snapshot coverage audit is dev-only")
        self.client = client
        self.runtime_config = runtime_config
        project = runtime_config.project_id
        self.schedule_table = (
            f"{project}.{PRODUCTION_DATASETS['league']}.schedule"
        )
        self.snapshots_table = (
            f"{project}.{GAMELENS_DEV_DATASET}.{PREGAME_SNAPSHOTS_TABLE}"
        )
        self.stage_game_results_table = (
            f"{project}.{GAMELENS_DEV_DATASET}."
            f"{STAGE_GAME_RESULTS_TABLE}"
        )

    def audit(
        self,
        *,
        start_date: date,
        end_date: date,
        season: str,
        now: Optional[datetime] = None,
    ) -> dict:
        if end_date < start_date:
            raise ValueError("end_date must be on or after start_date")
        audit_time = _as_utc(now or datetime.now(timezone.utc))
        query = f"""
            WITH latest_snapshots AS (
                SELECT
                    game_id, capture_id, learning_run_id, captured_at
                FROM `{self.snapshots_table}`
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY game_id
                    ORDER BY captured_at DESC, capture_id DESC
                ) = 1
            ),
            latest_results AS (
                SELECT
                    game_id,
                    attempt_id AS latest_attempt_id,
                    status AS latest_attempt_status,
                    reason AS latest_attempt_reason,
                    recorded_at
                FROM `{self.stage_game_results_table}`
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY game_id
                    ORDER BY recorded_at DESC, attempt_id DESC
                ) = 1
            )
            SELECT
                CAST(s.gameID AS STRING) AS game_id,
                s.gameDate AS game_date,
                SAFE_CAST(s.gameTime_epoch AS TIMESTAMP)
                    AS scheduled_kickoff,
                s.gameStatus AS game_status,
                CAST(s.season AS STRING) AS season,
                s.seasonType AS season_type,
                s.away,
                s.home,
                p.capture_id,
                p.learning_run_id,
                p.captured_at,
                r.latest_attempt_id,
                r.latest_attempt_status,
                r.latest_attempt_reason
            FROM `{self.schedule_table}` s
            LEFT JOIN latest_snapshots p
                ON CAST(s.gameID AS STRING) = p.game_id
            LEFT JOIN latest_results r
                ON CAST(s.gameID AS STRING) = r.game_id
            WHERE s.gameDate BETWEEN @start_date AND @end_date
              AND CAST(s.season AS STRING) = @season
            ORDER BY s.gameDate, scheduled_kickoff, game_id
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "start_date", "DATE", start_date
                ),
                bigquery.ScalarQueryParameter(
                    "end_date", "DATE", end_date
                ),
                bigquery.ScalarQueryParameter(
                    "season", "STRING", str(season)
                ),
            ]
        )
        raw_rows = self.client.query(
            query,
            job_config=config,
        ).result()
        games = [
            classify_snapshot_coverage(
                dict(row),
                now=audit_time,
            )
            for row in raw_rows
        ]
        return {
            "status": "complete",
            "access_mode": "read_only",
            "audited_at": audit_time.isoformat(),
            "season": str(season),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "sources": {
                "schedule": self.schedule_table,
                "snapshots": self.snapshots_table,
                "stage_game_results": self.stage_game_results_table,
            },
            "games_checked": len(games),
            "coverage_counts": dict(Counter(
                game["coverage_status"] for game in games
            )),
            "capture_missing_count": sum(
                game["coverage_status"] == "capture_missing"
                for game in games
            ),
            "games": games,
        }

