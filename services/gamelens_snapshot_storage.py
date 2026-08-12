"""Append-only BigQuery storage for the required Packet 2 shadow proof."""

from __future__ import annotations

import json
from typing import Any, Mapping, Optional

from google.cloud import bigquery

from runtime_config import RuntimeConfig


GAMELENS_DEV_DATASET = "GameLens_dev"
PREGAME_SNAPSHOTS_TABLE = "pregame_snapshots"
STAGE_RUNS_TABLE = "stage_runs"


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


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


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
            errors = self.client.insert_rows_json(
                self.snapshots_table,
                [dict(row)],
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
