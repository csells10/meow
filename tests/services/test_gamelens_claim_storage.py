from types import SimpleNamespace

import pytest

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from agg.gamelens_training.create_claim_training_examples_table import (
    claim_training_examples_schema,
)
from runtime_config import RuntimeConfig
from services.gamelens_claim_storage import (
    CLAIM_CLUSTERING_FIELDS,
    ensure_gamelens_claim_table,
    level1_claim_table_schema,
    reconcile_level1_claim_write,
)


def _runtime_config(environment="dev"):
    return RuntimeConfig(
        project_id="nfl-stream-406420",
        environment=environment,
        run_mode="daily",
        active_season="2026",
        league_dataset="League_dev" if environment == "dev" else "League",
        scores_dataset="Scores_dev" if environment == "dev" else "Scores",
        analytics_dataset=(
            "Analytics_dev" if environment == "dev" else "Analytics"
        ),
        raw_response_bucket=(
            "xtra-point-dev" if environment == "dev" else "xtra_point"
        ),
    )


def _signature(schema):
    return [(field.name, field.field_type, field.mode) for field in schema]


class FakeClient:
    def __init__(self):
        self.tables = {}
        self.create_calls = []
        self.dataset_reads = []

    def get_dataset(self, dataset_id):
        self.dataset_reads.append(dataset_id)
        return SimpleNamespace(location="US")

    def create_table(self, table, exists_ok=False):
        self.create_calls.append((table.table_id, exists_ok))
        self.tables.setdefault(table.table_id, table)
        return self.tables[table.table_id]

    def get_table(self, table_id):
        return self.tables[table_id]


def test_packet3_schema_reuses_existing_row_and_adds_only_lineage():
    base = claim_training_examples_schema()
    packet3 = level1_claim_table_schema()
    lineage = {
        field.name: (field.field_type, field.mode)
        for field in packet3[len(base) :]
    }

    assert _signature(packet3[: len(base)]) == _signature(base)
    assert lineage == {
        "learning_run_id": ("STRING", "REQUIRED"),
        "capture_id": ("STRING", "REQUIRED"),
        "pipeline_run_id": ("STRING", "NULLABLE"),
        "source_payload_sha256": ("STRING", "REQUIRED"),
        "extraction_version": ("STRING", "REQUIRED"),
        "extracted_at": ("TIMESTAMP", "REQUIRED"),
    }
    names = [field.name for field in packet3]
    assert len(names) == len(set(names))


def test_setup_is_dev_only_idempotent_and_verifies_layout():
    client = FakeClient()
    config = _runtime_config()

    first = ensure_gamelens_claim_table(client=client, runtime_config=config)
    second = ensure_gamelens_claim_table(client=client, runtime_config=config)

    expected_table = (
        "nfl-stream-406420.GameLens_dev.claim_training_examples"
    )
    assert first == second
    assert first["status"] == "verified"
    assert first["table"] == expected_table
    assert first["lineage_field_count"] == 6
    assert first["field_count"] == len(claim_training_examples_schema()) + 6
    assert first["partition_field"] == "game_date"
    assert first["clustering_fields"] == CLAIM_CLUSTERING_FIELDS
    assert client.create_calls == [(expected_table, True), (expected_table, True)]


def test_setup_refuses_production_before_any_client_call():
    client = FakeClient()

    with pytest.raises(ValueError, match="only in dev"):
        ensure_gamelens_claim_table(
            client=client,
            runtime_config=_runtime_config(environment="production"),
        )

    assert client.dataset_reads == []
    assert client.create_calls == []


def test_setup_fails_closed_on_existing_schema_mismatch():
    client = FakeClient()
    table_id = "nfl-stream-406420.GameLens_dev.claim_training_examples"
    from google.cloud import bigquery

    bad_table = bigquery.Table(table_id, schema=claim_training_examples_schema())
    bad_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="game_date",
    )
    bad_table.clustering_fields = list(CLAIM_CLUSTERING_FIELDS)
    client.tables[table_id] = bad_table

    with pytest.raises(ValueError, match="schema does not match Packet 3"):
        ensure_gamelens_claim_table(
            client=client,
            runtime_config=_runtime_config(),
        )


def test_claims_in_and_out_reconcile_visibly():
    assert reconcile_level1_claim_write(
        extracted_claim_count=8,
        unique_claim_key_count=8,
        inserted_count=5,
        unchanged_count=3,
        conflict_count=0,
        selected_capture_row_count=8,
    ) == {
        "status": "matched",
        "claims_in": 8,
        "unique_claim_keys": 8,
        "inserted": 5,
        "unchanged": 3,
        "conflicts": 0,
        "claims_out": 8,
    }


@pytest.mark.parametrize(
    "overrides",
    [
        {"unique_claim_key_count": 7},
        {"inserted_count": 4},
        {"conflict_count": 1},
        {"selected_capture_row_count": 7},
    ],
)
def test_claim_reconciliation_fails_closed_on_any_mismatch(overrides):
    counts = {
        "extracted_claim_count": 8,
        "unique_claim_key_count": 8,
        "inserted_count": 5,
        "unchanged_count": 3,
        "conflict_count": 0,
        "selected_capture_row_count": 8,
    }
    counts.update(overrides)

    with pytest.raises(ValueError, match="reconciliation failed"):
        reconcile_level1_claim_write(**counts)
