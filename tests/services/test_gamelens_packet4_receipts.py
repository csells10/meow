import unittest
from types import SimpleNamespace

from services import gamelens_packet4_receipts as receipts


def _row(**overrides):
    row = {
        "attempt_id": "attempt-1",
        "receipt_scope": "game_stage",
        "game_id": "game-1",
        "stage_name": "game_grade",
        "stage_order": 1,
        "status": "success",
        "reason": None,
        "learning_run_id": "cohort-1",
        "capture_id": "capture-1",
        "pipeline_run_id": "pipeline-1",
        "source_payload_sha256": "hash-1",
        "started_at": "2026-08-18T00:00:00+00:00",
        "finished_at": "2026-08-18T00:00:01+00:00",
        "duration_ms": 1000,
        "input_count": 1,
        "output_count": 1,
        "inserted_count": 0,
        "updated_count": 0,
        "unchanged_count": 1,
        "conflict_count": 0,
        "unavailable_count": 0,
        "rejected_count": 0,
        "write_performed": False,
        "failed_boundary": None,
        "retryable": False,
        "exception_class": None,
        "message": None,
        "log_reference": None,
        "details": {"proof": "matched"},
    }
    row.update(overrides)
    return row


class _Field:
    def __init__(self, name, field_type, mode="NULLABLE"):
        self.name = name
        self.field_type = field_type
        self.mode = mode


class _Table:
    def __init__(self, table_id, schema):
        self.table_id = table_id
        self.schema = schema
        self.time_partitioning = None
        self.clustering_fields = None


class _BigQuery:
    SchemaField = _Field
    Table = _Table

    class Dataset:
        def __init__(self, dataset_id):
            self.dataset_id = dataset_id
            self.location = None

    class TimePartitioning:
        def __init__(self, type_, field):
            self.type_ = type_
            self.field = field

    class TimePartitioningType:
        DAY = "DAY"


class _SetupClient:
    def __init__(self):
        self.tables = {}

    def get_dataset(self, _):
        return SimpleNamespace(location="US")

    def create_dataset(self, dataset, exists_ok):
        self.dataset = dataset
        self.dataset_exists_ok = exists_ok

    def create_table(self, table, exists_ok):
        self.tables.setdefault(table.table_id, table)
        self.table_exists_ok = exists_ok

    def get_table(self, table_id):
        return self.tables[table_id]


class ReceiptTests(unittest.TestCase):
    def test_setup_creates_one_dev_receipt_table(self):
        client = _SetupClient()
        result = receipts.ensure_packet4_receipt_table(
            client=client,
            runtime_config=SimpleNamespace(
                is_dev=True, project_id="project", league_dataset="League_dev"
            ),
            bigquery_module=_BigQuery,
        )
        self.assertEqual(result["status"], "verified")
        self.assertEqual(result["field_count"], len(receipts.RECEIPT_FIELD_SPECS))
        self.assertEqual(len(client.tables), 1)

    def test_setup_refuses_production_before_client_access(self):
        with self.assertRaisesRegex(ValueError, "only in dev"):
            receipts.ensure_packet4_receipt_table(
                client=object(),
                runtime_config=SimpleNamespace(is_dev=False),
                bigquery_module=_BigQuery,
            )

    def test_merge_plan_distinguishes_insert_retry_and_conflict(self):
        first = receipts.plan_receipt_merge(incoming_rows=[_row()], existing_rows=[])
        self.assertEqual(first["expected_inserted"], 1)
        stored = receipts.normalize_receipt_row(_row())
        retry = receipts.plan_receipt_merge(
            incoming_rows=[_row(duration_ms=2000)], existing_rows=[stored]
        )
        self.assertEqual(retry["expected_unchanged"], 1)
        conflict = receipts.plan_receipt_merge(
            incoming_rows=[_row(status="failed")], existing_rows=[stored]
        )
        self.assertEqual(conflict["conflict_count"], 1)
        self.assertIn("status", conflict["conflicts"][0]["fields"])

    def test_merge_sql_is_insert_only_by_receipt_key(self):
        storage = object.__new__(receipts.BigQueryPacket4ReceiptStorage)
        storage.table = "project.GameLens_dev.receipts"
        sql = storage._merge_sql("project.GameLens_dev.stage")
        self.assertIn("target.receipt_key = source.receipt_key", sql)
        self.assertIn("WHEN NOT MATCHED", sql)
        self.assertNotIn("WHEN MATCHED", sql)


if __name__ == "__main__":
    unittest.main()
