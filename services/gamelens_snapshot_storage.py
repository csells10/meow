"""Single-table, insert-only storage for Learning Lite LL-3."""

from __future__ import annotations

import json
from typing import Any, Mapping, Optional

from google.cloud import bigquery

from runtime_config import RuntimeConfig
from services.gamelens_pregame_contract import (
    canonical_payload_json,
    payload_sha256,
    require_eligible_pregame_payload,
)


GAMELENS_DEV_DATASET = "GameLens_dev"
PREGAME_SNAPSHOTS_TABLE = "pregame_snapshots"

SNAPSHOT_SCHEMA_SIGNATURE = (
    ("capture_id", "STRING", "REQUIRED"),
    ("learning_run_id", "STRING", "REQUIRED"),
    ("game_id", "STRING", "REQUIRED"),
    ("environment", "STRING", "REQUIRED"),
    ("season", "STRING", "NULLABLE"),
    ("season_type", "STRING", "NULLABLE"),
    ("game_week", "STRING", "NULLABLE"),
    ("game_status", "STRING", "NULLABLE"),
    ("scheduled_kickoff", "TIMESTAMP", "REQUIRED"),
    ("captured_at", "TIMESTAMP", "REQUIRED"),
    ("capture_status", "STRING", "REQUIRED"),
    ("payload_sha256", "STRING", "REQUIRED"),
    ("response_payload", "JSON", "REQUIRED"),
    ("evidence_context", "JSON", "REQUIRED"),
    ("lens_tags", "STRING", "REPEATED"),
    ("ranking_context_available", "BOOLEAN", "NULLABLE"),
    ("ranking_context_reason", "STRING", "NULLABLE"),
    ("metric_source_date", "DATE", "NULLABLE"),
    ("ranking_as_of_date", "DATE", "NULLABLE"),
    ("metric_pipeline_run_id", "STRING", "NULLABLE"),
    ("model_version", "STRING", "NULLABLE"),
    ("ruleset_version", "STRING", "NULLABLE"),
)

SNAPSHOT_COLUMNS = tuple(field[0] for field in SNAPSHOT_SCHEMA_SIGNATURE)


class SnapshotStorageError(RuntimeError):
    pass


class SnapshotConflictError(SnapshotStorageError):
    pass


class SnapshotIntegrityError(SnapshotStorageError):
    pass


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _canonical_evidence_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _schema_signature(schema) -> tuple:
    return tuple(
        (field.name, field.field_type, field.mode)
        for field in schema
    )


class BigQuerySnapshotStorage:
    """Verify and reconcile one immutable row in the approved dev table."""

    def __init__(
        self,
        *,
        client: bigquery.Client,
        runtime_config: RuntimeConfig,
    ):
        if not runtime_config.is_dev:
            raise ValueError("LL-3 snapshot storage is dev-only")
        self.client = client
        self.runtime_config = runtime_config
        self.table_id = (
            f"{runtime_config.project_id}."
            f"{GAMELENS_DEV_DATASET}.{PREGAME_SNAPSHOTS_TABLE}"
        )

    def verify_table_contract(self) -> dict:
        """Fail closed unless the existing table matches the approved shape."""
        table = self.client.get_table(self.table_id)
        actual_schema = _schema_signature(table.schema)
        if actual_schema != SNAPSHOT_SCHEMA_SIGNATURE:
            raise SnapshotIntegrityError(
                f"snapshot_schema_mismatch:{actual_schema}"
            )

        partitioning = getattr(table, "time_partitioning", None)
        partition_field = getattr(partitioning, "field", None)
        if partition_field != "captured_at":
            raise SnapshotIntegrityError(
                f"snapshot_partition_mismatch:{partition_field}"
            )

        clustering = list(getattr(table, "clustering_fields", None) or [])
        if clustering != ["game_id", "season_type"]:
            raise SnapshotIntegrityError(
                f"snapshot_clustering_mismatch:{clustering}"
            )

        return {
            "status": "verified",
            "table": self.table_id,
            "field_count": len(actual_schema),
            "partition_field": partition_field,
            "clustering_fields": clustering,
        }

    def read_capture_rows(self, capture_id: str) -> list[dict]:
        columns = ", ".join(SNAPSHOT_COLUMNS)
        query = f"""
            SELECT {columns}
            FROM `{self.table_id}`
            WHERE capture_id = @capture_id
            ORDER BY captured_at
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "capture_id", "STRING", str(capture_id)
                )
            ]
        )
        rows = self.client.query(query, job_config=config).result()
        return [self._normalize_row(dict(row)) for row in rows]

    def read_snapshot(self, capture_id: str) -> Optional[dict]:
        rows = self.read_capture_rows(capture_id)
        if len(rows) > 1:
            raise SnapshotIntegrityError(
                f"duplicate_canonical_capture:{capture_id}:{len(rows)}"
            )
        return rows[0] if rows else None

    def reconcile_snapshot(self, row: Mapping[str, Any]) -> dict:
        """Insert once; return no-op for identity; reject every conflict."""
        candidate = self._validate_candidate_row(row)
        self.verify_table_contract()

        existing_rows = self.read_capture_rows(candidate["capture_id"])
        if len(existing_rows) > 1:
            raise SnapshotIntegrityError(
                "duplicate_canonical_capture:"
                f"{candidate['capture_id']}:{len(existing_rows)}"
            )
        if existing_rows:
            return self._classify_existing(candidate, existing_rows[0])

        job = self.client.query(
            self._insert_if_absent_sql(),
            job_config=self._insert_job_config(candidate),
        )
        job.result()
        affected = getattr(job, "num_dml_affected_rows", None)
        if affected not in {0, 1}:
            raise SnapshotIntegrityError(
                f"unexpected_insert_count:{affected}"
            )

        saved_rows = self.read_capture_rows(candidate["capture_id"])
        if len(saved_rows) != 1:
            raise SnapshotIntegrityError(
                "canonical_row_count_after_insert:"
                f"{candidate['capture_id']}:{len(saved_rows)}"
            )

        classification = self._classify_existing(candidate, saved_rows[0])
        if classification["status"] != "identical_no_op":
            raise SnapshotIntegrityError("insert_readback_not_identical")
        if affected == 1:
            classification["status"] = "inserted"
            classification["material_change"] = True
        return classification

    def _classify_existing(self, candidate: dict, saved: dict) -> dict:
        saved_payload_json = self._validate_saved_row(saved)
        candidate_payload_json = canonical_payload_json(
            candidate["response_payload"]
        )
        same_hash = (
            saved["payload_sha256"] == candidate["payload_sha256"]
        )
        same_payload = saved_payload_json == candidate_payload_json
        if not same_hash or not same_payload:
            raise SnapshotConflictError(
                "canonical_capture_conflict:"
                f"{candidate['capture_id']}:"
                f"stored={saved['payload_sha256']}:"
                f"candidate={candidate['payload_sha256']}"
            )

        return {
            "status": "identical_no_op",
            "material_change": False,
            "table": self.table_id,
            "capture_id": candidate["capture_id"],
            "payload_sha256": candidate["payload_sha256"],
            "row_count": 1,
            "stored_captured_at": saved["captured_at"],
        }

    def _validate_candidate_row(self, row: Mapping[str, Any]) -> dict:
        candidate = dict(row)
        missing = [
            field
            for field in (
                "capture_id",
                "learning_run_id",
                "game_id",
                "environment",
                "scheduled_kickoff",
                "captured_at",
                "capture_status",
                "payload_sha256",
                "response_payload",
                "evidence_context",
                "model_version",
                "ruleset_version",
            )
            if candidate.get(field) in (None, "")
        ]
        if missing:
            raise ValueError(
                "snapshot row missing required fields: " + ",".join(missing)
            )
        if candidate["environment"] != "dev":
            raise ValueError("snapshot row environment must be dev")
        if candidate["capture_status"] != "captured":
            raise ValueError("snapshot row capture_status must be captured")
        if not isinstance(candidate["response_payload"], Mapping):
            raise TypeError("response_payload must be a mapping")
        if not isinstance(candidate["evidence_context"], Mapping):
            raise TypeError("evidence_context must be a mapping")

        tags = candidate.get("lens_tags") or []
        if not isinstance(tags, list) or any(
            not isinstance(tag, str) or not tag.strip()
            for tag in tags
        ):
            raise ValueError("lens_tags must be nonblank strings")
        candidate["lens_tags"] = tags

        require_eligible_pregame_payload(
            payload=candidate["response_payload"],
            game_id=candidate["game_id"],
            captured_at=candidate["captured_at"],
            scheduled_kickoff=candidate["scheduled_kickoff"],
        )
        fresh_hash = payload_sha256(candidate["response_payload"])
        if candidate["payload_sha256"] != fresh_hash:
            raise ValueError(
                "candidate_payload_hash_mismatch:"
                f"stored={candidate['payload_sha256']}:fresh={fresh_hash}"
            )
        _canonical_evidence_json(candidate["evidence_context"])
        return candidate

    def _validate_saved_row(self, saved: dict) -> str:
        payload = saved.get("response_payload")
        if not isinstance(payload, Mapping):
            raise SnapshotIntegrityError("stored_payload_not_json_object")
        require_eligible_pregame_payload(
            payload=payload,
            game_id=saved.get("game_id"),
            captured_at=saved.get("captured_at"),
            scheduled_kickoff=saved.get("scheduled_kickoff"),
        )
        fresh_hash = payload_sha256(payload)
        if saved.get("payload_sha256") != fresh_hash:
            raise SnapshotIntegrityError(
                "stored_payload_hash_mismatch:"
                f"stored={saved.get('payload_sha256')}:fresh={fresh_hash}"
            )
        return canonical_payload_json(payload)

    def _insert_job_config(self, row: dict):
        values = {
            "capture_id": ("STRING", row["capture_id"]),
            "learning_run_id": ("STRING", row["learning_run_id"]),
            "game_id": ("STRING", row["game_id"]),
            "environment": ("STRING", row["environment"]),
            "season": ("STRING", row.get("season")),
            "season_type": ("STRING", row.get("season_type")),
            "game_week": ("STRING", row.get("game_week")),
            "game_status": ("STRING", row.get("game_status")),
            "scheduled_kickoff": ("TIMESTAMP", row["scheduled_kickoff"]),
            "captured_at": ("TIMESTAMP", row["captured_at"]),
            "capture_status": ("STRING", row["capture_status"]),
            "payload_sha256": ("STRING", row["payload_sha256"]),
            "response_payload": (
                "STRING",
                canonical_payload_json(row["response_payload"]),
            ),
            "evidence_context": (
                "STRING",
                _canonical_evidence_json(row["evidence_context"]),
            ),
            "ranking_context_available": (
                "BOOL",
                row.get("ranking_context_available"),
            ),
            "ranking_context_reason": (
                "STRING",
                row.get("ranking_context_reason"),
            ),
            "metric_source_date": ("DATE", row.get("metric_source_date")),
            "ranking_as_of_date": ("DATE", row.get("ranking_as_of_date")),
            "metric_pipeline_run_id": (
                "STRING",
                row.get("metric_pipeline_run_id"),
            ),
            "model_version": ("STRING", row.get("model_version")),
            "ruleset_version": ("STRING", row.get("ruleset_version")),
        }
        parameters = [
            bigquery.ScalarQueryParameter(name, type_name, value)
            for name, (type_name, value) in values.items()
        ]
        parameters.append(
            bigquery.ArrayQueryParameter(
                "lens_tags", "STRING", row.get("lens_tags") or []
            )
        )
        return bigquery.QueryJobConfig(query_parameters=parameters)

    def _insert_if_absent_sql(self) -> str:
        return f"""
            INSERT INTO `{self.table_id}` (
                {", ".join(SNAPSHOT_COLUMNS)}
            )
            SELECT
                @capture_id,
                @learning_run_id,
                @game_id,
                @environment,
                @season,
                @season_type,
                @game_week,
                @game_status,
                @scheduled_kickoff,
                @captured_at,
                @capture_status,
                @payload_sha256,
                PARSE_JSON(@response_payload),
                PARSE_JSON(@evidence_context),
                @lens_tags,
                @ranking_context_available,
                @ranking_context_reason,
                @metric_source_date,
                @ranking_as_of_date,
                @metric_pipeline_run_id,
                @model_version,
                @ruleset_version
            WHERE NOT EXISTS (
                SELECT 1
                FROM `{self.table_id}`
                WHERE capture_id = @capture_id
            )
        """

    @staticmethod
    def _normalize_row(row: dict) -> dict:
        row["response_payload"] = _json_value(row.get("response_payload"))
        row["evidence_context"] = _json_value(row.get("evidence_context"))
        row["lens_tags"] = list(row.get("lens_tags") or [])
        return row
