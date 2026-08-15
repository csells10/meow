"""Development-only storage contract for Packet 3 Level 1 claims."""

from __future__ import annotations

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
