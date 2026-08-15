"""Development-only storage contract for Packet 3 Level 1 claims."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any, Mapping, Sequence

from google.cloud import bigquery

from agg.gamelens_training.create_claim_training_examples_table import (
    claim_training_examples_schema,
)
from runtime_config import RuntimeConfig
from services.gamelens_snapshot_storage import GAMELENS_DEV_DATASET


CLAIM_TRAINING_EXAMPLES_TABLE = "claim_training_examples"
CLAIM_PARTITION_FIELD = "game_date"
CLAIM_CLUSTERING_FIELDS = [
    "learning_run_id",
    "game_id",
    "claim_type",
    "claim_layer",
]

# These columns are intentionally enriched after Level 1. They are excluded
# from replay-conflict checks so Packet 4 can grade the same canonical row.
LEVEL1_MUTABLE_OR_AUDIT_FIELDS = frozenset(
    {
        "feature_build_stage",
        "feature_status",
        "feature_notes",
        "pregame_abs_raw_gap",
        "pregame_abs_percentile_gap",
        "core_area_metric_count",
        "same_direction_metric_count",
        "opposing_signal_count",
        "near_even_metric_count",
        "core_area_agreement_rate",
        "directional_edge_flag",
        "elevated_candidate_flag",
        "strong_language_allowed_flag",
        "offense_finish_score",
        "defensive_suppression_score",
        "two_way_edge_score",
        "disruption_upside_score",
        "hidden_lean_score",
        "confidence_cap_reason",
        "feature_formula_version",
        "actual_team",
        "actual_side",
        "validation_result",
        "validated_flag",
        "elevated_deserved_flag",
        "actual_gap",
        "actual_rank_gap",
        "actual_percentile_gap",
        "actual_gap_bucket",
        "predicted_team",
        "actual_winner",
        "model_result",
        "is_tie",
        "final_away_total",
        "final_home_total",
        "final_margin_abs",
        "final_margin_bucket",
        "qa_read_v2",
        "headline_claim_validation_rate",
        "unique_claim_validation_rate",
        "created_at",
        "updated_at",
        "extracted_at",
    }
)


class ClaimStorageConflictError(RuntimeError):
    """An existing logical claim disagrees with immutable Level 1 evidence."""


def level1_claim_table_schema() -> list[bigquery.SchemaField]:
    """Reuse the broad claim row and add only Packet 3 lineage."""
    schema = list(claim_training_examples_schema())
    schema.extend(
        [
            bigquery.SchemaField("learning_run_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("capture_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("pipeline_run_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "source_payload_sha256",
                "STRING",
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                "extraction_version",
                "STRING",
                mode="REQUIRED",
            ),
            bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="REQUIRED"),
        ]
    )
    names = [field.name for field in schema]
    if len(names) != len(set(names)):
        raise ValueError("Packet 3 claim schema contains duplicate field names")
    return schema


def _schema_signature(schema):
    return [(field.name, field.field_type, field.mode) for field in schema]


def _verify_table_layout(*, table, expected_schema, table_id: str) -> None:
    if _schema_signature(table.schema) != _schema_signature(expected_schema):
        raise ValueError(
            f"Existing table schema does not match Packet 3: {table_id}"
        )

    partition_field = getattr(table.time_partitioning, "field", None)
    if partition_field != CLAIM_PARTITION_FIELD:
        raise ValueError(
            f"Existing table partition does not match Packet 3: {table_id}"
        )

    if list(table.clustering_fields or []) != CLAIM_CLUSTERING_FIELDS:
        raise ValueError(
            f"Existing table clustering does not match Packet 3: {table_id}"
        )


def ensure_gamelens_claim_table(
    *,
    client: bigquery.Client,
    runtime_config: RuntimeConfig,
) -> dict:
    """Idempotently create and verify the one approved Packet 3 table."""
    if not runtime_config.is_dev:
        raise ValueError("GameLens claim-table setup is allowed only in dev")

    dataset_id = f"{runtime_config.project_id}.{GAMELENS_DEV_DATASET}"
    dataset = client.get_dataset(dataset_id)
    if not getattr(dataset, "location", None):
        raise ValueError("GameLens development dataset location is required")

    table_id = f"{dataset_id}.{CLAIM_TRAINING_EXAMPLES_TABLE}"
    schema = level1_claim_table_schema()
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=CLAIM_PARTITION_FIELD,
    )
    table.clustering_fields = list(CLAIM_CLUSTERING_FIELDS)

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
        "lineage_field_count": 6,
        "partition_field": CLAIM_PARTITION_FIELD,
        "clustering_fields": list(CLAIM_CLUSTERING_FIELDS),
    }


def level1_immutable_fields() -> tuple[str, ...]:
    """Derive replay-protected fields from the one canonical row schema."""
    return tuple(
        field.name
        for field in level1_claim_table_schema()
        if field.name not in LEVEL1_MUTABLE_OR_AUDIT_FIELDS
    )


def _comparable(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value))
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return tuple(
            (str(key), _comparable(item))
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        )
    if isinstance(value, (list, tuple)):
        return tuple(_comparable(item) for item in value)
    return str(value)


def plan_level1_claim_merge(
    *,
    rows: Sequence[Mapping[str, Any]],
    existing_rows: Sequence[Mapping[str, Any]],
    learning_run_id: str,
    capture_id: str,
) -> dict:
    """Compare one prepared capture with its stored canonical claim rows."""
    cohort = str(learning_run_id or "").strip()
    capture = str(capture_id or "").strip()
    if not cohort or not capture:
        raise ValueError("learning_run_id and capture_id are required")

    incoming = [dict(row) for row in rows]
    existing = [dict(row) for row in existing_rows]
    for label, candidates in (("incoming", incoming), ("existing", existing)):
        for row in candidates:
            if row.get("learning_run_id") != cohort:
                raise ValueError(f"{label} claim learning_run_id disagrees")
            if row.get("run_id") != cohort:
                raise ValueError(f"{label} claim run_id alias disagrees")
            if row.get("capture_id") != capture:
                raise ValueError(f"{label} claim capture_id disagrees")
            if not str(row.get("claim_key") or "").strip():
                raise ValueError(f"{label} claim_key is required")

    incoming_by_key = {row["claim_key"]: row for row in incoming}
    existing_by_key = {row["claim_key"]: row for row in existing}
    if len(incoming_by_key) != len(incoming):
        raise ValueError("duplicate incoming logical claim key")
    if len(existing_by_key) != len(existing):
        raise ClaimStorageConflictError(
            "duplicate stored learning_run_id + claim_key"
        )

    immutable_fields = level1_immutable_fields()
    conflicts = []
    incoming_keys = set(incoming_by_key)
    for claim_key in sorted(set(existing_by_key) - incoming_keys):
        conflicts.append(
            {
                "claim_key": claim_key,
                "fields": ["claim_key_missing_from_replay"],
            }
        )
    for claim_key in sorted(incoming_keys & set(existing_by_key)):
        incoming_row = incoming_by_key[claim_key]
        stored_row = existing_by_key[claim_key]
        differing = [
            field
            for field in immutable_fields
            if _comparable(incoming_row.get(field))
            != _comparable(stored_row.get(field))
        ]
        if differing:
            conflicts.append({"claim_key": claim_key, "fields": differing})

    pending_rows = [
        incoming_by_key[key]
        for key in sorted(incoming_keys - set(existing_by_key))
    ]
    return {
        "extracted_claim_count": len(incoming),
        "unique_claim_key_count": len(incoming_by_key),
        "expected_inserted_count": len(pending_rows),
        "expected_unchanged_count": len(incoming) - len(pending_rows),
        "conflict_count": len(conflicts),
        "conflicts": conflicts,
        "pending_rows": pending_rows,
    }


class BigQueryClaimStorage:
    """Game-scoped, insert-only MERGE boundary for canonical Level 1 rows."""

    def __init__(
        self,
        *,
        client: bigquery.Client,
        runtime_config: RuntimeConfig,
    ):
        if not runtime_config.is_dev:
            raise ValueError("Packet 3 claim storage is dev-only")
        self.client = client
        self.runtime_config = runtime_config
        self.schema = level1_claim_table_schema()
        self.field_names = [field.name for field in self.schema]
        self.table = (
            f"{runtime_config.project_id}.{GAMELENS_DEV_DATASET}."
            f"{CLAIM_TRAINING_EXAMPLES_TABLE}"
        )
        actual = client.get_table(self.table)
        _verify_table_layout(
            table=actual,
            expected_schema=self.schema,
            table_id=self.table,
        )

    def plan_claims(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        learning_run_id: str,
        capture_id: str,
    ) -> dict:
        existing = self._read_capture_claims(
            learning_run_id=learning_run_id,
            capture_id=capture_id,
        )
        plan = plan_level1_claim_merge(
            rows=rows,
            existing_rows=existing,
            learning_run_id=learning_run_id,
            capture_id=capture_id,
        )
        return {
            key: value
            for key, value in plan.items()
            if key != "pending_rows"
        } | {
            "status": "conflict" if plan["conflict_count"] else "ready",
            "target_table": self.table,
            "merge_key": ["learning_run_id", "claim_key"],
            "write_performed": False,
        }

    def merge_claims(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        learning_run_id: str,
        capture_id: str,
        attempt_id: str,
    ) -> dict:
        """Insert only missing rows, preserve matches, and fail on conflicts."""
        attempt = str(attempt_id or "").strip()
        if not attempt:
            raise ValueError("attempt_id is required")
        existing = self._read_capture_claims(
            learning_run_id=learning_run_id,
            capture_id=capture_id,
        )
        plan = plan_level1_claim_merge(
            rows=rows,
            existing_rows=existing,
            learning_run_id=learning_run_id,
            capture_id=capture_id,
        )
        if plan["conflict_count"]:
            keys = ",".join(
                item["claim_key"] for item in plan["conflicts"]
            )
            raise ClaimStorageConflictError(
                f"immutable Level 1 claim conflict: {keys}"
            )

        pending = self._load_rows(plan["pending_rows"])
        inserted_count = 0
        if pending:
            staging_table = self._staging_table(
                attempt_id=attempt,
                capture_id=capture_id,
            )
            try:
                load_config = bigquery.LoadJobConfig(
                    schema=self.schema,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                )
                self.client.load_table_from_json(
                    pending,
                    staging_table,
                    job_config=load_config,
                ).result()
                merge_job = self.client.query(
                    self._merge_sql(staging_table)
                )
                merge_job.result()
                inserted_count = merge_job.num_dml_affected_rows
                if inserted_count is None:
                    raise RuntimeError("BigQuery MERGE did not report affected rows")
            finally:
                self.client.delete_table(staging_table, not_found_ok=True)

        stored = self._read_capture_claims(
            learning_run_id=learning_run_id,
            capture_id=capture_id,
        )
        replay = plan_level1_claim_merge(
            rows=rows,
            existing_rows=stored,
            learning_run_id=learning_run_id,
            capture_id=capture_id,
        )
        if replay["conflict_count"] or replay["expected_inserted_count"]:
            raise RuntimeError("Packet 3 read-back does not match prepared claims")

        unchanged_count = len(rows) - inserted_count
        reconciliation = reconcile_level1_claim_write(
            extracted_claim_count=len(rows),
            unique_claim_key_count=plan["unique_claim_key_count"],
            inserted_count=inserted_count,
            unchanged_count=unchanged_count,
            conflict_count=0,
            selected_capture_row_count=len(stored),
        )
        return {
            **reconciliation,
            "target_table": self.table,
            "merge_key": ["learning_run_id", "claim_key"],
            "write_performed": bool(inserted_count),
        }

    def _read_capture_claims(
        self,
        *,
        learning_run_id: str,
        capture_id: str,
    ) -> list[dict]:
        query = f"""
            SELECT *
            FROM `{self.table}`
            WHERE learning_run_id = @learning_run_id
              AND capture_id = @capture_id
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "learning_run_id", "STRING", learning_run_id
                ),
                bigquery.ScalarQueryParameter(
                    "capture_id", "STRING", capture_id
                ),
            ]
        )
        return [
            dict(row)
            for row in self.client.query(query, job_config=config).result()
        ]

    def _load_rows(
        self,
        rows: Sequence[Mapping[str, Any]],
    ) -> list[dict]:
        allowed = set(self.field_names)
        normalized = []
        for row in rows:
            unknown = sorted(set(row) - allowed)
            if unknown:
                raise ValueError(
                    "prepared claim contains fields outside canonical schema: "
                    + ",".join(unknown)
                )
            normalized.append(
                {field: row.get(field) for field in self.field_names}
            )
        return normalized

    def _staging_table(self, *, attempt_id: str, capture_id: str) -> str:
        suffix = sha256(
            f"{attempt_id}|{capture_id}".encode("utf-8")
        ).hexdigest()[:20]
        dataset = f"{self.runtime_config.project_id}.{GAMELENS_DEV_DATASET}"
        return f"{dataset}._packet3_claim_stage_{suffix}"

    def _merge_sql(self, staging_table: str) -> str:
        columns = ", ".join(f"`{name}`" for name in self.field_names)
        values = ", ".join(f"source.`{name}`" for name in self.field_names)
        return f"""
            MERGE `{self.table}` AS target
            USING `{staging_table}` AS source
            ON target.learning_run_id = source.learning_run_id
               AND target.claim_key = source.claim_key
            WHEN NOT MATCHED THEN
              INSERT ({columns})
              VALUES ({values})
        """


def reconcile_level1_claim_write(
    *,
    extracted_claim_count: int,
    unique_claim_key_count: int,
    inserted_count: int,
    unchanged_count: int,
    conflict_count: int,
    selected_capture_row_count: int,
) -> dict:
    """Fail closed unless claims entering and leaving storage reconcile."""
    counts = {
        "extracted_claim_count": extracted_claim_count,
        "unique_claim_key_count": unique_claim_key_count,
        "inserted_count": inserted_count,
        "unchanged_count": unchanged_count,
        "conflict_count": conflict_count,
        "selected_capture_row_count": selected_capture_row_count,
    }
    for name, value in counts.items():
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} must be a non-negative integer")

    failures = []
    if extracted_claim_count != unique_claim_key_count:
        failures.append("extractor output does not equal unique staged keys")
    if unique_claim_key_count != inserted_count + unchanged_count:
        failures.append("unique staged keys do not equal inserted plus unchanged")
    if selected_capture_row_count != unique_claim_key_count:
        failures.append("stored selected-capture rows do not equal unique staged keys")
    if conflict_count:
        failures.append("immutable Level 1 conflicts were detected")
    if failures:
        raise ValueError("Packet 3 claim reconciliation failed: " + "; ".join(failures))

    return {
        "status": "matched",
        "claims_in": extracted_claim_count,
        "unique_claim_keys": unique_claim_key_count,
        "inserted": inserted_count,
        "unchanged": unchanged_count,
        "conflicts": conflict_count,
        "claims_out": selected_capture_row_count,
    }
