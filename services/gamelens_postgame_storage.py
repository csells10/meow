"""Development-only, capture-aware storage for Packet 4 game grades."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
from typing import Any, Mapping, Sequence


GAMELENS_DEV_DATASET = "GameLens_dev"
POSTGAME_OUTCOME_TABLE = "game_model_outcomes"
POSTGAME_PARTITION_FIELD = "graded_at"
POSTGAME_CLUSTERING_FIELDS = ["learning_run_id", "game_id", "grade_version"]
POSTGAME_MERGE_KEY = ["learning_run_id", "capture_id"]

POSTGAME_FIELD_SPECS = (
    ("learning_run_id", "STRING", "REQUIRED"),
    ("capture_id", "STRING", "REQUIRED"),
    ("game_id", "STRING", "REQUIRED"),
    ("pipeline_run_id", "STRING", "NULLABLE"),
    ("season", "STRING", "NULLABLE"),
    ("season_type", "STRING", "NULLABLE"),
    ("game_week", "STRING", "NULLABLE"),
    ("source_payload_sha256", "STRING", "REQUIRED"),
    ("final_score_sha256", "STRING", "REQUIRED"),
    ("grade_version", "STRING", "REQUIRED"),
    ("model_outcome", "JSON", "REQUIRED"),
    ("model_trust", "JSON", "REQUIRED"),
    ("graded_at", "TIMESTAMP", "REQUIRED"),
)
POSTGAME_IMMUTABLE_FIELDS = tuple(
    name for name, _, _ in POSTGAME_FIELD_SPECS if name != "graded_at"
)


class PostgameOutcomeStorageConflictError(RuntimeError):
    """Stored evidence disagrees with the incoming frozen-capture grade."""


def _bigquery(module=None):
    if module is not None:
        return module
    from google.cloud import bigquery

    return bigquery


def postgame_outcome_schema(bigquery_module=None):
    bigquery = _bigquery(bigquery_module)
    return [
        bigquery.SchemaField(name, field_type, mode=mode)
        for name, field_type, mode in POSTGAME_FIELD_SPECS
    ]


def _schema_signature(schema):
    return [(field.name, field.field_type, field.mode) for field in schema]


def _verify_table_layout(*, table, expected_schema, table_id: str) -> None:
    if _schema_signature(table.schema) != _schema_signature(expected_schema):
        raise ValueError(
            f"Existing table schema does not match Packet 4: {table_id}"
        )
    partition_field = getattr(table.time_partitioning, "field", None)
    if partition_field != POSTGAME_PARTITION_FIELD:
        raise ValueError(
            f"Existing table partition does not match Packet 4: {table_id}"
        )
    if list(table.clustering_fields or []) != POSTGAME_CLUSTERING_FIELDS:
        raise ValueError(
            f"Existing table clustering does not match Packet 4: {table_id}"
        )


def ensure_gamelens_postgame_outcome_table(
    *, client, runtime_config, bigquery_module=None
) -> dict:
    """Idempotently create and verify the one approved Packet 4 grade table."""
    if not runtime_config.is_dev:
        raise ValueError("GameLens postgame outcome setup is allowed only in dev")

    bigquery = _bigquery(bigquery_module)
    dataset_id = f"{runtime_config.project_id}.{GAMELENS_DEV_DATASET}"
    dataset = client.get_dataset(dataset_id)
    if not getattr(dataset, "location", None):
        raise ValueError("GameLens development dataset location is required")

    table_id = f"{dataset_id}.{POSTGAME_OUTCOME_TABLE}"
    schema = postgame_outcome_schema(bigquery)
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=POSTGAME_PARTITION_FIELD,
    )
    table.clustering_fields = list(POSTGAME_CLUSTERING_FIELDS)
    client.create_table(table, exists_ok=True)
    actual = client.get_table(table_id)
    _verify_table_layout(
        table=actual,
        expected_schema=schema,
        table_id=table_id,
    )
    return {
        "status": "verified",
        "dataset": dataset_id,
        "table": table_id,
        "field_count": len(schema),
        "partition_field": POSTGAME_PARTITION_FIELD,
        "clustering_fields": list(POSTGAME_CLUSTERING_FIELDS),
        "merge_key": list(POSTGAME_MERGE_KEY),
    }


def _required_text(row: Mapping[str, Any], field: str) -> str:
    value = str(row.get(field) or "").strip()
    if not value:
        raise ValueError(f"{field} is required")
    return value


def _optional_text(value: Any):
    text = str(value or "").strip()
    return text or None


def normalize_postgame_grade(grade: Mapping[str, Any]) -> dict:
    """Restrict one dry grade to the frozen Packet 4 storage contract."""
    if not isinstance(grade, Mapping):
        raise TypeError("grade must be an object")
    allowed = {name for name, _, _ in POSTGAME_FIELD_SPECS} - {"graded_at"}
    unknown = sorted(set(grade) - allowed)
    if unknown:
        raise ValueError(
            "postgame grade contains fields outside canonical schema: "
            + ",".join(unknown)
        )
    model_outcome = grade.get("model_outcome")
    model_trust = grade.get("model_trust")
    if not isinstance(model_outcome, Mapping) or not model_outcome:
        raise ValueError("model_outcome is required")
    if not isinstance(model_trust, Mapping):
        raise ValueError("model_trust is required")
    return {
        "learning_run_id": _required_text(grade, "learning_run_id"),
        "capture_id": _required_text(grade, "capture_id"),
        "game_id": _required_text(grade, "game_id"),
        "pipeline_run_id": _optional_text(grade.get("pipeline_run_id")),
        "season": _optional_text(grade.get("season")),
        "season_type": _optional_text(grade.get("season_type")),
        "game_week": _optional_text(grade.get("game_week")),
        "source_payload_sha256": _required_text(
            grade, "source_payload_sha256"
        ),
        "final_score_sha256": _required_text(grade, "final_score_sha256"),
        "grade_version": _required_text(grade, "grade_version"),
        "model_outcome": dict(model_outcome),
        "model_trust": dict(model_trust),
    }


def _comparable(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value))
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    if isinstance(value, (list, tuple)):
        return tuple(_comparable(item) for item in value)
    return str(value)


def plan_postgame_grade_merge(
    *, grade: Mapping[str, Any], existing_rows: Sequence[Mapping[str, Any]]
) -> dict:
    """Plan insert/unchanged/conflict for one capture without writing."""
    incoming = normalize_postgame_grade(grade)
    existing = [dict(row) for row in existing_rows]
    if len(existing) > 1:
        return {
            "expected_inserted_count": 0,
            "expected_unchanged_count": 0,
            "conflict_count": 1,
            "conflicts": [{"fields": ["duplicate_stored_merge_key"]}],
            "pending_row": None,
        }
    if not existing:
        return {
            "expected_inserted_count": 1,
            "expected_unchanged_count": 0,
            "conflict_count": 0,
            "conflicts": [],
            "pending_row": incoming,
        }

    stored = existing[0]
    differing = [
        field
        for field in POSTGAME_IMMUTABLE_FIELDS
        if _comparable(incoming.get(field)) != _comparable(stored.get(field))
    ]
    if differing:
        return {
            "expected_inserted_count": 0,
            "expected_unchanged_count": 0,
            "conflict_count": 1,
            "conflicts": [{"fields": differing}],
            "pending_row": None,
        }
    return {
        "expected_inserted_count": 0,
        "expected_unchanged_count": 1,
        "conflict_count": 0,
        "conflicts": [],
        "pending_row": None,
    }


def reconcile_postgame_grade_write(
    *, inserted_count: int, unchanged_count: int, conflict_count: int,
    selected_capture_row_count: int
) -> dict:
    counts = {
        "inserted_count": inserted_count,
        "unchanged_count": unchanged_count,
        "conflict_count": conflict_count,
        "selected_capture_row_count": selected_capture_row_count,
    }
    for name, value in counts.items():
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} must be a non-negative integer")
    if inserted_count + unchanged_count != 1:
        raise ValueError("Packet 4 grade reconciliation requires one input grade")
    if conflict_count:
        raise ValueError("Packet 4 grade reconciliation found a conflict")
    if selected_capture_row_count != 1:
        raise ValueError("Packet 4 grade reconciliation requires one stored grade")
    return {
        "status": "matched",
        "grades_in": 1,
        "inserted": inserted_count,
        "unchanged": unchanged_count,
        "conflicts": conflict_count,
        "grades_out": selected_capture_row_count,
    }


class BigQueryPostgameOutcomeStorage:
    """Insert-only grade ledger keyed by learning cohort and frozen capture."""

    def __init__(self, *, client, runtime_config, bigquery_module=None):
        if not runtime_config.is_dev:
            raise ValueError("Packet 4 postgame storage is dev-only")
        self.client = client
        self.runtime_config = runtime_config
        self.bigquery = _bigquery(bigquery_module)
        self.schema = postgame_outcome_schema(self.bigquery)
        self.field_names = [field.name for field in self.schema]
        self.table = (
            f"{runtime_config.project_id}.{GAMELENS_DEV_DATASET}."
            f"{POSTGAME_OUTCOME_TABLE}"
        )
        actual = client.get_table(self.table)
        _verify_table_layout(
            table=actual,
            expected_schema=self.schema,
            table_id=self.table,
        )

    def plan_grade(self, grade: Mapping[str, Any]) -> dict:
        incoming = normalize_postgame_grade(grade)
        existing = self._read_grades(incoming)
        plan = plan_postgame_grade_merge(grade=incoming, existing_rows=existing)
        return {
            key: value for key, value in plan.items() if key != "pending_row"
        } | {
            "status": "conflict" if plan["conflict_count"] else "ready",
            "existing_capture_row_count": len(existing),
            "projected_capture_row_count": (
                len(existing) + plan["expected_inserted_count"]
            ),
            "target_table": self.table,
            "merge_key": list(POSTGAME_MERGE_KEY),
            "write_performed": False,
        }

    def store_grade(self, grade: Mapping[str, Any], *, attempt_id: str) -> dict:
        attempt = str(attempt_id or "").strip()
        if not attempt:
            raise ValueError("attempt_id is required")
        incoming = normalize_postgame_grade(grade)
        existing = self._read_grades(incoming)
        plan = plan_postgame_grade_merge(grade=incoming, existing_rows=existing)
        if plan["conflict_count"]:
            fields = ",".join(plan["conflicts"][0]["fields"])
            raise PostgameOutcomeStorageConflictError(
                f"immutable Packet 4 grade conflict: {fields}"
            )

        inserted_count = 0
        if plan["pending_row"] is not None:
            row = {
                **plan["pending_row"],
                "graded_at": datetime.now(timezone.utc).isoformat(),
            }
            staging_table = self._staging_table(
                attempt_id=attempt,
                capture_id=incoming["capture_id"],
            )
            try:
                config = self.bigquery.LoadJobConfig(
                    schema=self.schema,
                    write_disposition=self.bigquery.WriteDisposition.WRITE_TRUNCATE,
                )
                self.client.load_table_from_json(
                    [row], staging_table, job_config=config
                ).result()
                merge_job = self.client.query(self._merge_sql(staging_table))
                merge_job.result()
                inserted_count = merge_job.num_dml_affected_rows
                if inserted_count != 1:
                    raise RuntimeError(
                        "Packet 4 MERGE did not insert exactly one grade"
                    )
            finally:
                self.client.delete_table(staging_table, not_found_ok=True)

        stored = self._read_grades(incoming)
        replay = plan_postgame_grade_merge(grade=incoming, existing_rows=stored)
        if replay["conflict_count"] or replay["expected_inserted_count"]:
            raise RuntimeError("Packet 4 read-back does not match the dry grade")
        reconciliation = reconcile_postgame_grade_write(
            inserted_count=inserted_count,
            unchanged_count=1 - inserted_count,
            conflict_count=0,
            selected_capture_row_count=len(stored),
        )
        return {
            **reconciliation,
            "target_table": self.table,
            "merge_key": list(POSTGAME_MERGE_KEY),
            "write_performed": bool(inserted_count),
        }

    def _read_grades(self, grade: Mapping[str, Any]) -> list[dict]:
        query = f"""
            SELECT *
            FROM `{self.table}`
            WHERE learning_run_id = @learning_run_id
              AND capture_id = @capture_id
        """
        config = self.bigquery.QueryJobConfig(
            query_parameters=[
                self.bigquery.ScalarQueryParameter(
                    "learning_run_id", "STRING", grade["learning_run_id"]
                ),
                self.bigquery.ScalarQueryParameter(
                    "capture_id", "STRING", grade["capture_id"]
                ),
            ]
        )
        return [
            dict(row)
            for row in self.client.query(query, job_config=config).result()
        ]

    def _staging_table(self, *, attempt_id: str, capture_id: str) -> str:
        suffix = sha256(
            f"{attempt_id}|{capture_id}".encode("utf-8")
        ).hexdigest()[:20]
        return (
            f"{self.runtime_config.project_id}.{GAMELENS_DEV_DATASET}."
            f"_packet4_outcome_stage_{suffix}"
        )

    def _merge_sql(self, staging_table: str) -> str:
        columns = ", ".join(f"`{name}`" for name in self.field_names)
        values = ", ".join(f"source.`{name}`" for name in self.field_names)
        return f"""
            MERGE `{self.table}` AS target
            USING `{staging_table}` AS source
            ON target.learning_run_id = source.learning_run_id
               AND target.capture_id = source.capture_id
            WHEN NOT MATCHED THEN
              INSERT ({columns})
              VALUES ({values})
        """
