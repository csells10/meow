import json
import unittest
from datetime import date

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from runtime_config import RuntimeConfig
from services.gamelens_snapshot_storage import (
    BigQueryBufferPending,
    BigQuerySnapshotStorage,
    pregame_snapshot_schema,
)
from setup_gamelens_snapshot_tables import ensure_gamelens_snapshot_tables


def config(environment="dev"):
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


class QueryResult:
    def __init__(self, rows):
        self.rows = rows

    def result(self):
        return list(self.rows)


class StorageClient:
    def __init__(self):
        self.query_rows = []
        self.insert_errors = []
        self.insert_calls = []

    def query(self, query, job_config=None):
        return QueryResult(self.query_rows)

    def insert_rows_json(self, table, rows, **kwargs):
        self.insert_calls.append((table, rows, kwargs))
        return self.insert_errors


class SetupClient:
    def __init__(self):
        self.tables = {}
        self.dataset_create_calls = 0

    def get_dataset(self, dataset_id):
        source = type("Dataset", (), {})()
        source.location = "US"
        return source

    def create_dataset(self, dataset, exists_ok=False):
        self.dataset_create_calls += 1
        self.dataset = dataset
        self.dataset_exists_ok = exists_ok
        return dataset

    def create_table(self, table, exists_ok=False):
        self.tables.setdefault(table.table_id, table)
        self.table_exists_ok = exists_ok
        return self.tables[table.table_id]

    def get_table(self, table_id):
        return self.tables[table_id]


class TestSnapshotStorage(unittest.TestCase):
    def test_storage_is_fail_closed_outside_dev(self):
        with self.assertRaisesRegex(ValueError, "dev-only"):
            BigQuerySnapshotStorage(
                client=StorageClient(),
                runtime_config=config("production"),
            )

    def test_streaming_buffer_error_is_explicitly_retryable(self):
        client = StorageClient()
        client.insert_errors = [{"message": "rows are in the streaming buffer"}]
        storage = BigQuerySnapshotStorage(
            client=client,
            runtime_config=config(),
        )
        with self.assertRaises(BigQueryBufferPending):
            storage.save_snapshot({
                "capture_id": "capture_1",
                "response_payload": {},
                "evidence_context": {},
            })

    def test_readback_normalizes_json_and_repeated_tags(self):
        client = StorageClient()
        client.query_rows = [{
            "capture_id": "capture_1",
            "response_payload": '{"header": {}}',
            "evidence_context": '{"rankings": []}',
            "lens_tags": ("efficiency",),
        }]
        storage = BigQuerySnapshotStorage(
            client=client,
            runtime_config=config(),
        )
        row = storage.read_snapshot("capture_1")
        self.assertEqual(row["response_payload"], {"header": {}})
        self.assertEqual(row["evidence_context"], {"rankings": []})
        self.assertEqual(row["lens_tags"], ["efficiency"])

    def test_save_serializes_native_json_fields_for_streaming_insert(self):
        client = StorageClient()
        storage = BigQuerySnapshotStorage(
            client=client,
            runtime_config=config(),
        )
        row = {
            "capture_id": "capture_1",
            "response_payload": {"header": {"game_id": "game_1"}},
            "evidence_context": {
                "source_lineage": {"access_mode": "read_only"},
                "as_of_date": date(2026, 8, 6),
            },
        }

        storage.save_snapshot(row)

        inserted = client.insert_calls[0][1][0]
        self.assertIsInstance(inserted["response_payload"], str)
        self.assertIsInstance(inserted["evidence_context"], str)
        self.assertEqual(
            json.loads(inserted["response_payload"]),
            row["response_payload"],
        )
        self.assertEqual(
            json.loads(inserted["evidence_context"]),
            {
                "source_lineage": {"access_mode": "read_only"},
                "as_of_date": "2026-08-06",
            },
        )
        self.assertIsInstance(row["response_payload"], dict)
        self.assertIsInstance(row["evidence_context"], dict)


class TestSnapshotTableSetup(unittest.TestCase):
    def test_snapshot_schema_requires_explicit_learning_run_identity(self):
        fields = {field.name: field for field in pregame_snapshot_schema()}
        self.assertIn("learning_run_id", fields)
        self.assertEqual(fields["learning_run_id"].field_type, "STRING")
        self.assertEqual(fields["learning_run_id"].mode, "REQUIRED")

    def test_setup_is_idempotent_and_non_destructive(self):
        client = SetupClient()
        first = ensure_gamelens_snapshot_tables(
            client=client,
            runtime_config=config(),
        )
        second = ensure_gamelens_snapshot_tables(
            client=client,
            runtime_config=config(),
        )
        self.assertEqual(first, second)
        self.assertEqual(first["status"], "verified")
        self.assertEqual(first["location"], "US")
        self.assertEqual(len(client.tables), 2)
        self.assertTrue(client.dataset_exists_ok)
        self.assertTrue(client.table_exists_ok)

    def test_setup_refuses_production_before_any_create(self):
        client = SetupClient()
        with self.assertRaisesRegex(ValueError, "only in dev"):
            ensure_gamelens_snapshot_tables(
                client=client,
                runtime_config=config("production"),
            )
        self.assertEqual(client.dataset_create_calls, 0)
        self.assertEqual(client.tables, {})


if __name__ == "__main__":
    unittest.main()
