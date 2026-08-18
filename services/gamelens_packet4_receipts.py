"""Durable development receipts for the Packet 4 coordinator."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Mapping, Sequence


GAMELENS_DEV_DATASET = "GameLens_dev"
PACKET4_RECEIPTS_TABLE = "postgame_learning_stage_receipts"
RECEIPT_FIELD_SPECS = (
    ("receipt_key", "STRING", "REQUIRED"),
    ("attempt_id", "STRING", "REQUIRED"),
    ("receipt_scope", "STRING", "REQUIRED"),
    ("game_id", "STRING", "NULLABLE"),
    ("stage_name", "STRING", "NULLABLE"),
    ("stage_order", "INTEGER", "NULLABLE"),
    ("status", "STRING", "REQUIRED"),
    ("reason", "STRING", "NULLABLE"),
    ("learning_run_id", "STRING", "NULLABLE"),
    ("capture_id", "STRING", "NULLABLE"),
    ("pipeline_run_id", "STRING", "NULLABLE"),
    ("source_payload_sha256", "STRING", "NULLABLE"),
    ("started_at", "TIMESTAMP", "REQUIRED"),
    ("finished_at", "TIMESTAMP", "REQUIRED"),
    ("duration_ms", "INTEGER", "REQUIRED"),
    ("input_count", "INTEGER", "REQUIRED"),
    ("output_count", "INTEGER", "REQUIRED"),
    ("inserted_count", "INTEGER", "REQUIRED"),
    ("updated_count", "INTEGER", "REQUIRED"),
    ("unchanged_count", "INTEGER", "REQUIRED"),
    ("conflict_count", "INTEGER", "REQUIRED"),
    ("unavailable_count", "INTEGER", "REQUIRED"),
    ("rejected_count", "INTEGER", "REQUIRED"),
    ("write_performed", "BOOLEAN", "REQUIRED"),
    ("failed_boundary", "STRING", "NULLABLE"),
    ("retryable", "BOOLEAN", "REQUIRED"),
    ("exception_class", "STRING", "NULLABLE"),
    ("message", "STRING", "NULLABLE"),
    ("log_reference", "STRING", "NULLABLE"),
    ("details_json", "STRING", "REQUIRED"),
    ("recorded_at", "TIMESTAMP", "REQUIRED"),
)
MATERIAL_FIELDS = tuple(
    name
    for name, _, _ in RECEIPT_FIELD_SPECS
    if name
    not in {
        "started_at",
        "finished_at",
        "duration_ms",
        "details_json",
        "recorded_at",
    }
)


class Packet4ReceiptConflictError(RuntimeError):
    """An immutable logical receipt key already has different evidence."""


def packet4_receipt_schema(bigquery: Any):
    return [
        bigquery.SchemaField(name, field_type, mode=mode)
        for name, field_type, mode in RECEIPT_FIELD_SPECS
    ]


def _schema_signature(schema) -> list[tuple[str, str, str]]:
    return [
        (field.name, str(field.field_type).upper(), str(field.mode).upper())
        for field in schema
    ]


def ensure_packet4_receipt_table(
    *, client: Any, runtime_config: Any, bigquery_module: Any
) -> dict[str, Any]:
    if not runtime_config.is_dev:
        raise ValueError("Packet 4 receipt setup is allowed only in dev")
    project = runtime_config.project_id
    dataset_id = f"{project}.{GAMELENS_DEV_DATASET}"
    source = client.get_dataset(
        f"{project}.{runtime_config.league_dataset}"
    )
    if not source.location:
        raise ValueError("Source BigQuery dataset location is required")
    dataset = bigquery_module.Dataset(dataset_id)
    dataset.location = source.location
    client.create_dataset(dataset, exists_ok=True)

    table_id = f"{dataset_id}.{PACKET4_RECEIPTS_TABLE}"
    schema = packet4_receipt_schema(bigquery_module)
    table = bigquery_module.Table(table_id, schema=schema)
    table.time_partitioning = bigquery_module.TimePartitioning(
        type_=bigquery_module.TimePartitioningType.DAY,
        field="recorded_at",
    )
    table.clustering_fields = ["attempt_id", "game_id", "stage_name", "status"]
    client.create_table(table, exists_ok=True)
    actual = client.get_table(table_id)
    if _schema_signature(actual.schema) != _schema_signature(schema):
        raise ValueError(
            "Existing table schema does not match Packet 4 receipts: "
            + table_id
        )
    return {
        "environment": "dev",
        "status": "verified",
        "dataset": dataset_id,
        "table": table_id,
        "field_count": len(schema),
        "partition_field": "recorded_at",
        "logical_key": ["attempt_id", "receipt_scope", "game_id", "stage_name"],
    }


def build_receipt_key(row: Mapping[str, Any]) -> str:
    logical = "|".join(
        str(row.get(field) or "")
        for field in ("attempt_id", "receipt_scope", "game_id", "stage_name")
    )
    return sha256(logical.encode("utf-8")).hexdigest()


def normalize_receipt_row(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    for field in ("attempt_id", "receipt_scope", "status"):
        value = str(normalized.get(field) or "").strip()
        if not value:
            raise ValueError(f"Packet 4 receipt {field} is required")
        normalized[field] = value
    if normalized["receipt_scope"] not in {"attempt", "game_stage"}:
        raise ValueError("Packet 4 receipt_scope is unsupported")
    if normalized["receipt_scope"] == "game_stage":
        for field in ("game_id", "stage_name"):
            if not str(normalized.get(field) or "").strip():
                raise ValueError(f"Packet 4 stage receipt {field} is required")
    for field in (
        "duration_ms",
        "input_count",
        "output_count",
        "inserted_count",
        "updated_count",
        "unchanged_count",
        "conflict_count",
        "unavailable_count",
        "rejected_count",
    ):
        value = normalized.get(field, 0)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"Packet 4 receipt {field} must be non-negative")
        normalized[field] = value
    for field in ("write_performed", "retryable"):
        if not isinstance(normalized.get(field), bool):
            raise ValueError(f"Packet 4 receipt {field} must be boolean")
    for field in ("started_at", "finished_at"):
        if not normalized.get(field):
            raise ValueError(f"Packet 4 receipt {field} is required")
    details = normalized.pop("details", None)
    if "details_json" not in normalized:
        normalized["details_json"] = json.dumps(
            details or {}, sort_keys=True, separators=(",", ":"), default=str
        )
    normalized["recorded_at"] = normalized.get("recorded_at") or datetime.now(
        timezone.utc
    ).isoformat()
    normalized["receipt_key"] = build_receipt_key(normalized)
    return {
        name: normalized.get(name)
        for name, _, _ in RECEIPT_FIELD_SPECS
    }


def plan_receipt_merge(
    *, incoming_rows: Sequence[Mapping[str, Any]], existing_rows: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    incoming = [normalize_receipt_row(row) for row in incoming_rows]
    if len({row["receipt_key"] for row in incoming}) != len(incoming):
        raise ValueError("duplicate Packet 4 receipt key")
    existing = {str(row["receipt_key"]): dict(row) for row in existing_rows}
    pending = []
    unchanged = []
    conflicts = []
    for row in incoming:
        stored = existing.get(row["receipt_key"])
        if stored is None:
            pending.append(row)
            continue
        differing = [
            field
            for field in MATERIAL_FIELDS
            if stored.get(field) != row.get(field)
        ]
        if differing:
            conflicts.append({"receipt_key": row["receipt_key"], "fields": differing})
        else:
            unchanged.append(row["receipt_key"])
    return {
        "receipts_in": len(incoming),
        "expected_inserted": len(pending),
        "expected_unchanged": len(unchanged),
        "conflict_count": len(conflicts),
        "conflicts": conflicts,
        "pending_rows": pending,
    }


class BigQueryPacket4ReceiptStorage:
    def __init__(self, *, client: Any, runtime_config: Any, bigquery_module: Any):
        if not runtime_config.is_dev:
            raise ValueError("Packet 4 receipt writes are allowed only in dev")
        self.client = client
        self.runtime_config = runtime_config
        self.bigquery = bigquery_module
        self.schema = packet4_receipt_schema(bigquery_module)
        self.table = (
            f"{runtime_config.project_id}.{GAMELENS_DEV_DATASET}."
            f"{PACKET4_RECEIPTS_TABLE}"
        )
        actual = client.get_table(self.table)
        if _schema_signature(actual.schema) != _schema_signature(self.schema):
            raise ValueError("Packet 4 receipt table schema disagrees")

    def store_receipts(
        self, *, rows: Sequence[Mapping[str, Any]], attempt_id: str
    ) -> dict[str, Any]:
        attempt = str(attempt_id or "").strip()
        if not attempt:
            raise ValueError("attempt_id is required")
        normalized = [normalize_receipt_row(row) for row in rows]
        if any(row["attempt_id"] != attempt for row in normalized):
            raise ValueError("Packet 4 receipt attempt_id disagrees")
        existing = self._read_receipts([row["receipt_key"] for row in normalized])
        plan = plan_receipt_merge(incoming_rows=normalized, existing_rows=existing)
        if plan["conflict_count"]:
            raise Packet4ReceiptConflictError(
                "Packet 4 receipt logical key has different evidence"
            )
        inserted = 0
        pending = plan["pending_rows"]
        if pending:
            suffix = sha256(attempt.encode("utf-8")).hexdigest()[:20]
            staging = (
                f"{self.runtime_config.project_id}.{GAMELENS_DEV_DATASET}."
                f"_packet4_receipt_stage_{suffix}"
            )
            try:
                config = self.bigquery.LoadJobConfig(
                    schema=self.schema,
                    write_disposition=self.bigquery.WriteDisposition.WRITE_TRUNCATE,
                )
                self.client.load_table_from_json(
                    pending, staging, job_config=config
                ).result()
                job = self.client.query(self._merge_sql(staging))
                job.result()
                inserted = int(job.num_dml_affected_rows or 0)
                if inserted != len(pending):
                    raise RuntimeError("Packet 4 receipt MERGE count disagrees")
            finally:
                self.client.delete_table(staging, not_found_ok=True)
        after = self._read_receipts([row["receipt_key"] for row in normalized])
        replay = plan_receipt_merge(incoming_rows=normalized, existing_rows=after)
        if replay["conflict_count"] or replay["expected_inserted"]:
            raise RuntimeError("Packet 4 receipt read-back does not reconcile")
        return {
            "status": "matched",
            "receipts_in": len(normalized),
            "inserted": inserted,
            "unchanged": plan["expected_unchanged"],
            "conflicts": 0,
            "target_table": self.table,
            "write_performed": bool(inserted),
        }

    def _read_receipts(self, receipt_keys: Sequence[str]) -> list[dict[str, Any]]:
        if not receipt_keys:
            return []
        query = f"""
            SELECT *
            FROM `{self.table}`
            WHERE receipt_key IN UNNEST(@receipt_keys)
        """
        config = self.bigquery.QueryJobConfig(
            query_parameters=[
                self.bigquery.ArrayQueryParameter(
                    "receipt_keys", "STRING", list(receipt_keys)
                )
            ]
        )
        return [dict(row) for row in self.client.query(query, job_config=config).result()]

    def _merge_sql(self, staging_table: str) -> str:
        names = [name for name, _, _ in RECEIPT_FIELD_SPECS]
        columns = ", ".join(f"`{name}`" for name in names)
        values = ", ".join(f"source.`{name}`" for name in names)
        return f"""
            MERGE `{self.table}` AS target
            USING `{staging_table}` AS source
            ON target.receipt_key = source.receipt_key
            WHEN NOT MATCHED THEN
              INSERT ({columns})
              VALUES ({values})
        """
