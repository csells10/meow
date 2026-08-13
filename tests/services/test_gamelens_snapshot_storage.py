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
    stage_game_result_schema,
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

    def test_stage_game_results_insert_only_missing_logical_keys(self):
        client = StorageClient()
        client.query_rows = [{
            "attempt_id": "attempt_1",
            "stage_name": "snapshot_capture",
            "game_id": "game_1",
        }]
        storage = BigQuerySnapshotStorage(
            client=client,
            runtime_config=config(),
        )
        common = {
            "attempt_id": "attempt_1",
            "stage_name": "snapshot_capture",
            "season": "2026",
            "season_type": "Preseason",
            "status": "success",
            "reason": None,
            "eligible": True,
            "rebuilt": True,
            "input_count": 1,
            "output_count": 1,
            "upstream_run_id": "metric_1",
            "recorded_at": "2026-08-13T14:08:07+00:00",
            "is_backfill": False,
            "backfill_source": None,
        }

        result = storage.write_stage_game_results([
            {
                **common,
                "game_id": "game_1",
                "capture_id": "capture_1",
                "learning_run_id": "learning_1",
            },
            {
                **common,
                "game_id": "game_2",
                "capture_id": "capture_2",
                "learning_run_id": "learning_1",
            },
        ])

        self.assertEqual(
            result,
            {
                "input_count": 2,
                "inserted_count": 1,
                "existing_count": 1,
            },
        )
        table, inserted_rows, kwargs = client.insert_calls[0]
        self.assertTrue(table.endswith(".stage_game_results"))
        self.assertEqual(
            [row["game_id"] for row in inserted_rows],
            ["game_2"],
        )
        self.assertEqual(len(kwargs["row_ids"]), 1)
        self.assertEqual(len(kwargs["row_ids"][0]), 64)

    def test_stage_game_results_reject_duplicate_input_keys(self):
        storage = BigQuerySnapshotStorage(
            client=StorageClient(),
            runtime_config=config(),
        )
        row = {
            "attempt_id": "attempt_1",
            "stage_name": "snapshot_capture",
            "game_id": "game_1",
            "status": "no_op",
            "input_count": 1,
            "output_count": 0,
            "recorded_at": "2026-08-13T14:08:07+00:00",
            "is_backfill": False,
        }

        with self.assertRaisesRegex(
            ValueError,
            "duplicate stage game result logical key",
        ):
            storage.write_stage_game_results([row, dict(row)])


class TestSnapshotTableSetup(unittest.TestCase):
    def test_snapshot_schema_requires_explicit_learning_run_identity(self):
        fields = {field.name: field for field in pregame_snapshot_schema()}
        self.assertIn("learning_run_id", fields)
        self.assertEqual(fields["learning_run_id"].field_type, "STRING")
        self.assertEqual(fields["learning_run_id"].mode, "REQUIRED")

    def test_stage_game_result_schema_has_required_logical_grain(self):
        fields = {
            field.name: field for field in stage_game_result_schema()
        }
        for field in ("attempt_id", "stage_name", "game_id", "status"):
            self.assertEqual(fields[field].mode, "REQUIRED")
        self.assertEqual(fields["is_backfill"].field_type, "BOOLEAN")
        self.assertEqual(fields["recorded_at"].field_type, "TIMESTAMP")

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
        self.assertEqual(len(client.tables), 3)
        self.assertTrue(client.dataset_exists_ok)
        self.assertTrue(client.table_exists_ok)
        detail_table = client.tables[
            "nfl-stream-406420.GameLens_dev.stage_game_results"
        ]
        self.assertEqual(
            detail_table.time_partitioning.field,
            "recorded_at",
        )
        self.assertEqual(
            detail_table.clustering_fields,
            ["attempt_id", "game_id", "status"],
        )

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
