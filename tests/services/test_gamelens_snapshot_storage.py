import unittest

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from runtime_config import RuntimeConfig
from services.gamelens_snapshot_storage import (
    BigQueryBufferPending,
    BigQuerySnapshotStorage,
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

    def query(self, query, job_config=None):
        return QueryResult(self.query_rows)

    def insert_rows_json(self, table, rows, **kwargs):
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
            storage.save_snapshot({"capture_id": "capture_1"})

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


class TestSnapshotTableSetup(unittest.TestCase):
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
