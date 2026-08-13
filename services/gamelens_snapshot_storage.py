"""Append-only BigQuery storage for the required Packet 2 shadow proof."""

from __future__ import annotations

import json
from hashlib import sha256
from typing import Any, Mapping, Optional, Sequence

from google.cloud import bigquery

from runtime_config import RuntimeConfig


GAMELENS_DEV_DATASET = "GameLens_dev"
PREGAME_SNAPSHOTS_TABLE = "pregame_snapshots"
STAGE_RUNS_TABLE = "stage_runs"
STAGE_GAME_RESULTS_TABLE = "stage_game_results"
STAGE_GAME_RESULT_STATUSES = frozenset({
    "success",
    "no_op",
    "failure",
    "waiting",
    "skipped",
})


class SnapshotStorageError(RuntimeError):
    pass


class BigQueryBufferPending(SnapshotStorageError):
    """The requested operation is retryable after BigQuery settles."""


def _is_streaming_buffer_error(value: Any) -> bool:
    text = str(value or "").casefold()
    return "streaming buffer" in text or "streamingbuffer" in text


def pregame_snapshot_schema():
    return [
        bigquery.SchemaField("capture_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("learning_run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("game_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("environment", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("season", "STRING"),
        bigquery.SchemaField("season_type", "STRING"),
        bigquery.SchemaField("game_week", "STRING"),
        bigquery.SchemaField("game_status", "STRING"),
        bigquery.SchemaField("scheduled_kickoff", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("captured_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("capture_status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("payload_sha256", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("response_payload", "JSON", mode="REQUIRED"),
        bigquery.SchemaField("evidence_context", "JSON", mode="REQUIRED"),
        bigquery.SchemaField("lens_tags", "STRING", mode="REPEATED"),
        bigquery.SchemaField("ranking_context_available", "BOOLEAN"),
        bigquery.SchemaField("ranking_context_reason", "STRING"),
        bigquery.SchemaField("metric_source_date", "DATE"),
        bigquery.SchemaField("ranking_as_of_date", "DATE"),
        bigquery.SchemaField("metric_pipeline_run_id", "STRING"),
        bigquery.SchemaField("model_version", "STRING"),
        bigquery.SchemaField("ruleset_version", "STRING"),
    ]


def stage_run_schema():
    return [
        bigquery.SchemaField("attempt_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("stage_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("game_id", "STRING"),
        bigquery.SchemaField("season", "STRING"),
        bigquery.SchemaField("season_type", "STRING"),
        bigquery.SchemaField("input_count", "INTEGER"),
        bigquery.SchemaField("output_count", "INTEGER"),
        bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("finished_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("duration_ms", "INTEGER"),
        bigquery.SchemaField("upstream_run_id", "STRING"),
        bigquery.SchemaField("reason", "STRING"),
    ]


def stage_game_result_schema():
    """Per-game audit rows supplement, rather than replace, stage_runs."""
    return [
        bigquery.SchemaField("attempt_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("stage_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("game_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("capture_id", "STRING"),
        bigquery.SchemaField("learning_run_id", "STRING"),
        bigquery.SchemaField("season", "STRING"),
        bigquery.SchemaField("season_type", "STRING"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("reason", "STRING"),
        bigquery.SchemaField("eligible", "BOOLEAN"),
        bigquery.SchemaField("rebuilt", "BOOLEAN"),
        bigquery.SchemaField("input_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("output_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("upstream_run_id", "STRING"),
        bigquery.SchemaField("recorded_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("is_backfill", "BOOLEAN", mode="REQUIRED"),
        bigquery.SchemaField("backfill_source", "STRING"),
    ]


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _json_insert_value(value: Any) -> str:
    """Encode a native BigQuery JSON value for the streaming insert API."""
    if isinstance(value, str):
        # Preserve already-serialized JSON, but fail locally on invalid text.
        json.loads(value)
        return value
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


class BigQuerySnapshotStorage:
    """Required Packet 2 persistence: insert once, then read and verify."""

    def __init__(
        self,
        *,
        client: bigquery.Client,
        runtime_config: RuntimeConfig,
    ):
        if not runtime_config.is_dev:
            raise ValueError("Packet 2 snapshot storage is dev-only")
        self.client = client
        self.runtime_config = runtime_config
        project = runtime_config.project_id
        self.snapshots_table = (
            f"{project}.{GAMELENS_DEV_DATASET}.{PREGAME_SNAPSHOTS_TABLE}"
        )
        self.stage_runs_table = (
            f"{project}.{GAMELENS_DEV_DATASET}.{STAGE_RUNS_TABLE}"
        )
        self.stage_game_results_table = (
            f"{project}.{GAMELENS_DEV_DATASET}.{STAGE_GAME_RESULTS_TABLE}"
        )

    def find_capture(self, capture_id: str) -> Optional[dict]:
        query = f"""
            SELECT *
            FROM `{self.snapshots_table}`
            WHERE capture_id = @capture_id
            LIMIT 1
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "capture_id", "STRING", str(capture_id)
                )
            ]
        )
        try:
            rows = list(self.client.query(query, job_config=config).result())
        except Exception as exc:
            self._raise_storage_error(exc)
        if not rows:
            return None
        return self._normalize_snapshot(dict(rows[0]))

    def save_snapshot(self, row: Mapping[str, Any]) -> None:
        try:
            insert_row = dict(row)
            for field in ("response_payload", "evidence_context"):
                insert_row[field] = _json_insert_value(insert_row[field])
            errors = self.client.insert_rows_json(
                self.snapshots_table,
                [insert_row],
                row_ids=[str(row["capture_id"])],
            )
        except Exception as exc:
            self._raise_storage_error(exc)
        if errors:
            self._raise_storage_error(errors)

    def read_snapshot(self, capture_id: str) -> Optional[dict]:
        return self.find_capture(capture_id)

    def write_stage_receipt(self, receipt: Mapping[str, Any]) -> None:
        try:
            errors = self.client.insert_rows_json(
                self.stage_runs_table,
                [dict(receipt)],
            )
        except Exception as exc:
            self._raise_storage_error(exc)
        if errors:
            self._raise_storage_error(errors)

    def write_stage_game_results(
        self,
        rows: Sequence[Mapping[str, Any]],
    ) -> dict:
        """Insert only missing per-game logical keys for one stage attempt."""
        normalized = [self._validate_stage_game_result(row) for row in rows]
        if not normalized:
            return {
                "input_count": 0,
                "inserted_count": 0,
                "existing_count": 0,
            }

        attempts = {row["attempt_id"] for row in normalized}
        stages = {row["stage_name"] for row in normalized}
        if len(attempts) != 1 or len(stages) != 1:
            raise ValueError(
                "stage game results must belong to one attempt and stage"
            )

        logical_keys = [
            (row["attempt_id"], row["stage_name"], row["game_id"])
            for row in normalized
        ]
        if len(set(logical_keys)) != len(logical_keys):
            raise ValueError("duplicate stage game result logical key")

        attempt_id = normalized[0]["attempt_id"]
        stage_name = normalized[0]["stage_name"]
        existing_game_ids = self._find_stage_game_result_game_ids(
            attempt_id=attempt_id,
            stage_name=stage_name,
            game_ids=[row["game_id"] for row in normalized],
        )
        pending = [
            row for row in normalized
            if row["game_id"] not in existing_game_ids
        ]
        if pending:
            try:
                errors = self.client.insert_rows_json(
                    self.stage_game_results_table,
                    pending,
                    row_ids=[
                        _stage_game_result_row_id(row) for row in pending
                    ],
                )
            except Exception as exc:
                self._raise_storage_error(exc)
            if errors:
                self._raise_storage_error(errors)

        return {
            "input_count": len(normalized),
            "inserted_count": len(pending),
            "existing_count": len(normalized) - len(pending),
        }

    def _find_stage_game_result_game_ids(
        self,
        *,
        attempt_id: str,
        stage_name: str,
        game_ids: Sequence[str],
    ) -> set:
        query = f"""
            SELECT game_id
            FROM `{self.stage_game_results_table}`
            WHERE attempt_id = @attempt_id
              AND stage_name = @stage_name
              AND game_id IN UNNEST(@game_ids)
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "attempt_id", "STRING", attempt_id
                ),
                bigquery.ScalarQueryParameter(
                    "stage_name", "STRING", stage_name
                ),
                bigquery.ArrayQueryParameter(
                    "game_ids", "STRING", sorted(set(game_ids))
                ),
            ]
        )
        try:
            rows = self.client.query(query, job_config=config).result()
            return {str(dict(row)["game_id"]) for row in rows}
        except Exception as exc:
            self._raise_storage_error(exc)

    @staticmethod
    def _validate_stage_game_result(row: Mapping[str, Any]) -> dict:
        normalized = dict(row)
        required_text = (
            "attempt_id",
            "stage_name",
            "game_id",
            "status",
        )
        missing = [
            field for field in required_text
            if not str(normalized.get(field) or "").strip()
        ]
        if not normalized.get("recorded_at"):
            missing.append("recorded_at")
        if missing:
            raise ValueError(
                "stage game result missing required fields: "
                + ",".join(missing)
            )
        if normalized["status"] not in STAGE_GAME_RESULT_STATUSES:
            raise ValueError(
                "unsupported stage game result status: "
                f"{normalized['status']}"
            )
        if not isinstance(normalized.get("is_backfill"), bool):
            raise ValueError(
                "stage game result is_backfill must be boolean"
            )
        for field in ("input_count", "output_count"):
            value = normalized.get(field)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                raise ValueError(
                    f"stage game result {field} must be a "
                    "non-negative integer"
                )
        return normalized

    @staticmethod
    def _normalize_snapshot(row: dict) -> dict:
        row["response_payload"] = _json_value(row.get("response_payload"))
        row["evidence_context"] = _json_value(row.get("evidence_context"))
        row["lens_tags"] = list(row.get("lens_tags") or [])
        return row

    @staticmethod
    def _raise_storage_error(error: Any):
        if _is_streaming_buffer_error(error):
            raise BigQueryBufferPending(str(error))
        raise SnapshotStorageError(str(error))


def _stage_game_result_row_id(row: Mapping[str, Any]) -> str:
    logical_key = "|".join(
        str(row[field])
        for field in ("attempt_id", "stage_name", "game_id")
    )
    return sha256(logical_key.encode("utf-8")).hexdigest()
