"""Create or verify Packet 2 development tables without replacing data."""

from __future__ import annotations

import json

from google.cloud import bigquery

from runtime_config import RuntimeConfig, load_runtime_config
from services.gamelens_snapshot_storage import (
    GAMELENS_DEV_DATASET,
    PREGAME_SNAPSHOTS_TABLE,
    STAGE_GAME_RESULTS_TABLE,
    STAGE_RUNS_TABLE,
    pregame_snapshot_schema,
    stage_game_result_schema,
    stage_run_schema,
)


def _schema_signature(schema):
    return [(field.name, field.field_type, field.mode) for field in schema]


def ensure_gamelens_snapshot_tables(
    *,
    client: bigquery.Client,
    runtime_config: RuntimeConfig,
) -> dict:
    """Idempotently create/verify only the approved development objects."""
    if not runtime_config.is_dev:
        raise ValueError("GameLens snapshot setup is allowed only in dev")

    project = runtime_config.project_id
    source_dataset = client.get_dataset(
        f"{project}.{runtime_config.league_dataset}"
    )
    location = source_dataset.location
    if not location:
        raise ValueError("Source BigQuery dataset location is required")

    dataset_id = f"{project}.{GAMELENS_DEV_DATASET}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)

    table_specs = {
        PREGAME_SNAPSHOTS_TABLE: pregame_snapshot_schema(),
        STAGE_RUNS_TABLE: stage_run_schema(),
        STAGE_GAME_RESULTS_TABLE: stage_game_result_schema(),
    }
    verified = []
    for table_name, expected_schema in table_specs.items():
        table_id = f"{dataset_id}.{table_name}"
        table = bigquery.Table(table_id, schema=expected_schema)
        if table_name == PREGAME_SNAPSHOTS_TABLE:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="captured_at",
            )
            table.clustering_fields = ["game_id", "season_type"]
        elif table_name == STAGE_GAME_RESULTS_TABLE:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="recorded_at",
            )
            table.clustering_fields = ["attempt_id", "game_id", "status"]
        client.create_table(table, exists_ok=True)
        actual = client.get_table(table_id)
        if _schema_signature(actual.schema) != _schema_signature(expected_schema):
            raise ValueError(
                f"Existing table schema does not match Packet 2: {table_id}"
            )
        verified.append(table_id)

    return {
        "status": "verified",
        "dataset": dataset_id,
        "location": location,
        "tables": verified,
    }


def main() -> int:
    runtime_config = load_runtime_config()
    if not runtime_config.is_dev:
        raise ValueError("Set GAMELENS_ENVIRONMENT=dev before running setup")
    client = bigquery.Client(project=runtime_config.project_id)
    result = ensure_gamelens_snapshot_tables(
        client=client,
        runtime_config=runtime_config,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
