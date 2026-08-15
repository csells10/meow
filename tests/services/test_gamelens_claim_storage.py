from types import SimpleNamespace

import pytest

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from agg.gamelens_training.create_claim_training_examples_table import (
    claim_training_examples_schema,
)
from runtime_config import RuntimeConfig
from services.gamelens_claim_storage import (
    BigQueryClaimStorage,
    CLAIM_CLUSTERING_FIELDS,
    ClaimStorageConflictError,
    ensure_gamelens_claim_table,
    level1_claim_table_schema,
    plan_level1_claim_merge,
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


def _claim_row(
    claim_key="claim_one",
    *,
    capture_id="capture_one",
    game_id="20260813_ARI@LV",
    claim_text="ARI owns the passing profile edge.",
):
    return {
        "claim_key": claim_key,
        "run_id": "gamelens_2026_preseason_v1",
        "learning_run_id": "gamelens_2026_preseason_v1",
        "capture_id": capture_id,
        "pipeline_run_id": "metric_20260813",
        "source_payload_sha256": "frozen_hash",
        "extraction_version": "level1_snapshot_adapter_v1",
        "extracted_at": "2026-08-15T15:00:00+00:00",
        "created_at": "2026-08-15T15:00:00+00:00",
        "game_id": game_id,
        "season": "2026",
        "claimed_team": "ARI",
        "claim_type": "game_profile",
        "claim_layer": "headline",
        "claim_rank": 1,
        "claim_text": claim_text,
        "source_field_path": "game_profile[0]",
    }


class FakeJob:
    def __init__(self, rows=None, affected=None):
        self.rows = list(rows or [])
        self.num_dml_affected_rows = affected

    def result(self):
        return list(self.rows)


class FakeClaimClient:
    def __init__(self, target_rows=None):
        from google.cloud import bigquery

        self.table_id = (
            "nfl-stream-406420.GameLens_dev.claim_training_examples"
        )
        self.table = bigquery.Table(
            self.table_id,
            schema=level1_claim_table_schema(),
        )
        self.table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="game_date",
        )
        self.table.clustering_fields = list(CLAIM_CLUSTERING_FIELDS)
        self.target_rows = [dict(row) for row in target_rows or []]
        self.staged_rows = {}
        self.queries = []
        self.deleted_tables = []

    def get_table(self, table_id):
        assert table_id == self.table_id
        return self.table

    def load_table_from_json(self, rows, table_id, job_config):
        self.staged_rows[table_id] = [dict(row) for row in rows]
        return FakeJob()

    def query(self, query, job_config=None):
        self.queries.append(query)
        if "SELECT *" in query:
            parameters = {
                parameter.args[0]: parameter.args[2]
                for parameter in job_config.query_parameters
            }
            rows = [
                row
                for row in self.target_rows
                if row.get("learning_run_id")
                == parameters["learning_run_id"]
                and row.get("capture_id") == parameters["capture_id"]
            ]
            return FakeJob(rows)
        if "MERGE" in query:
            staging_table = next(
                table_id
                for table_id in self.staged_rows
                if f"USING `{table_id}`" in query
            )
            existing_keys = {
                (row["learning_run_id"], row["claim_key"])
                for row in self.target_rows
            }
            pending = [
                row
                for row in self.staged_rows[staging_table]
                if (row["learning_run_id"], row["claim_key"])
                not in existing_keys
            ]
            self.target_rows.extend(pending)
            return FakeJob(affected=len(pending))
        raise AssertionError(f"unexpected query: {query}")

    def delete_table(self, table_id, not_found_ok=False):
        self.deleted_tables.append((table_id, not_found_ok))
        self.staged_rows.pop(table_id, None)


def test_merge_plan_protects_immutable_claims_but_ignores_later_labels():
    incoming = _claim_row()
    stored = {**incoming, "validation_result": "validated"}

    clean = plan_level1_claim_merge(
        rows=[incoming],
        existing_rows=[stored],
        learning_run_id=incoming["learning_run_id"],
        capture_id=incoming["capture_id"],
    )
    assert clean["expected_inserted_count"] == 0
    assert clean["expected_unchanged_count"] == 1
    assert clean["conflict_count"] == 0

    conflict = plan_level1_claim_merge(
        rows=[incoming],
        existing_rows=[{**stored, "claim_text": "Different claim"}],
        learning_run_id=incoming["learning_run_id"],
        capture_id=incoming["capture_id"],
    )
    assert conflict["conflict_count"] == 1
    assert conflict["conflicts"][0]["fields"] == ["claim_text"]


def test_bigquery_merge_inserts_once_and_identical_retry_is_unchanged():
    unrelated = _claim_row(
        "other_claim",
        capture_id="capture_other",
        game_id="20260813_GB@PIT",
    )
    client = FakeClaimClient(target_rows=[unrelated])
    storage = BigQueryClaimStorage(
        client=client,
        runtime_config=_runtime_config(),
    )
    row = _claim_row()

    first = storage.merge_claims(
        [row],
        learning_run_id=row["learning_run_id"],
        capture_id=row["capture_id"],
        attempt_id="level1_attempt_one",
    )
    second = storage.merge_claims(
        [row],
        learning_run_id=row["learning_run_id"],
        capture_id=row["capture_id"],
        attempt_id="level1_attempt_two",
    )

    assert first["inserted"] == 1
    assert first["unchanged"] == 0
    assert first["claims_in"] == first["claims_out"] == 1
    assert second["inserted"] == 0
    assert second["unchanged"] == 1
    assert second["claims_in"] == second["claims_out"] == 1
    assert len(client.target_rows) == 2
    merge_sql = next(query for query in client.queries if "MERGE" in query)
    assert "target.learning_run_id = source.learning_run_id" in merge_sql
    assert "target.claim_key = source.claim_key" in merge_sql
    assert "DELETE" not in merge_sql
    assert client.deleted_tables


def test_bigquery_merge_fails_before_write_on_immutable_conflict():
    stored = _claim_row(claim_text="Stored wording")
    client = FakeClaimClient(target_rows=[stored])
    storage = BigQueryClaimStorage(
        client=client,
        runtime_config=_runtime_config(),
    )

    with pytest.raises(ClaimStorageConflictError, match="conflict"):
        storage.merge_claims(
            [_claim_row(claim_text="Changed wording")],
            learning_run_id=stored["learning_run_id"],
            capture_id=stored["capture_id"],
            attempt_id="level1_conflict",
        )

    assert not any("MERGE" in query for query in client.queries)
    assert client.deleted_tables == []


def test_zero_claim_capture_reconciles_without_a_claim_mutation():
    unrelated = _claim_row(
        "other_claim",
        capture_id="capture_other",
        game_id="20260813_GB@PIT",
    )
    client = FakeClaimClient(target_rows=[unrelated])
    storage = BigQueryClaimStorage(
        client=client,
        runtime_config=_runtime_config(),
    )

    result = storage.merge_claims(
        [],
        learning_run_id="gamelens_2026_preseason_v1",
        capture_id="capture_zero",
        attempt_id="level1_zero",
    )

    assert result["claims_in"] == result["claims_out"] == 0
    assert result["inserted"] == result["unchanged"] == 0
    assert not any("MERGE" in query for query in client.queries)
    assert client.target_rows == [unrelated]
