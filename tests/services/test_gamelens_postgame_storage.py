from __future__ import annotations

import unittest
from types import SimpleNamespace

from services import gamelens_postgame_storage as storage


class _SchemaField:
    def __init__(self, name, field_type, mode="NULLABLE"):
        self.name = name
        self.field_type = field_type
        self.mode = mode


class _Table:
    def __init__(self, table_id, schema):
        self.table_id = table_id
        self.schema = schema
        self.time_partitioning = None
        self.clustering_fields = []


class _TimePartitioning:
    def __init__(self, *, type_, field):
        self.type_ = type_
        self.field = field


class _Parameter:
    def __init__(self, *args):
        self.args = args


class _Config:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _BigQuery:
    SchemaField = _SchemaField
    Table = _Table
    TimePartitioning = _TimePartitioning
    QueryJobConfig = _Config
    LoadJobConfig = _Config
    ScalarQueryParameter = _Parameter

    class TimePartitioningType:
        DAY = "DAY"

    class WriteDisposition:
        WRITE_TRUNCATE = "WRITE_TRUNCATE"


def _runtime(environment="dev"):
    return SimpleNamespace(
        project_id="nfl-stream-406420",
        environment=environment,
        is_dev=environment == "dev",
    )


def _grade(**overrides):
    value = {
        "learning_run_id": "gamelens_2026_preseason_v1",
        "capture_id": "capture_dal_sea",
        "game_id": "20260815_DAL@SEA",
        "pipeline_run_id": "metric_20260816",
        "season": "2026",
        "season_type": "Preseason",
        "game_week": "Preseason 2",
        "source_payload_sha256": "pregame_hash",
        "final_score_sha256": "score_hash",
        "grade_version": "frozen_capture_outcome_v1",
        "model_outcome": {"result": "Correct", "predicted_team": "DAL"},
        "model_trust": {"learning_label": "Aligned"},
    }
    value.update(overrides)
    return value


class _Job:
    def __init__(self, rows=None, affected=None):
        self.rows = list(rows or [])
        self.num_dml_affected_rows = affected

    def result(self):
        return list(self.rows)


class _Client:
    def __init__(self, rows=None):
        self.table_id = (
            "nfl-stream-406420.GameLens_dev.game_model_outcomes"
        )
        self.table = _Table(
            self.table_id,
            storage.postgame_outcome_schema(_BigQuery),
        )
        self.table.time_partitioning = _TimePartitioning(
            type_="DAY", field=storage.POSTGAME_PARTITION_FIELD
        )
        self.table.clustering_fields = list(storage.POSTGAME_CLUSTERING_FIELDS)
        self.rows = [dict(row) for row in rows or []]
        self.staged = {}
        self.queries = []
        self.create_calls = []
        self.deleted = []

    def get_dataset(self, dataset_id):
        return SimpleNamespace(location="US")

    def create_table(self, table, exists_ok=False):
        self.create_calls.append((table.table_id, exists_ok))
        if table.table_id == self.table_id:
            self.table = table
        return self.table

    def get_table(self, table_id):
        self.assert_table(table_id)
        return self.table

    def assert_table(self, table_id):
        if table_id != self.table_id:
            raise AssertionError(table_id)

    def load_table_from_json(self, rows, table_id, job_config):
        self.staged[table_id] = [dict(row) for row in rows]
        return _Job()

    def query(self, query, job_config=None):
        self.queries.append(query)
        if "SELECT *" in query:
            params = {
                parameter.args[0]: parameter.args[2]
                for parameter in job_config.query_parameters
            }
            return _Job(
                row
                for row in self.rows
                if row["learning_run_id"] == params["learning_run_id"]
                and row["capture_id"] == params["capture_id"]
            )
        if "MERGE" in query:
            stage = next(
                table_id
                for table_id in self.staged
                if f"USING `{table_id}`" in query
            )
            existing = {
                (row["learning_run_id"], row["capture_id"])
                for row in self.rows
            }
            pending = [
                row
                for row in self.staged[stage]
                if (row["learning_run_id"], row["capture_id"])
                not in existing
            ]
            self.rows.extend(pending)
            return _Job(affected=len(pending))
        raise AssertionError(query)

    def delete_table(self, table_id, not_found_ok=False):
        self.deleted.append((table_id, not_found_ok))
        self.staged.pop(table_id, None)


class PostgameStorageTests(unittest.TestCase):
    def test_schema_and_setup_are_one_narrow_dev_ledger(self):
        client = _Client()
        result = storage.ensure_gamelens_postgame_outcome_table(
            client=client,
            runtime_config=_runtime(),
            bigquery_module=_BigQuery,
        )
        self.assertEqual(result["field_count"], 13)
        self.assertEqual(result["merge_key"], ["learning_run_id", "capture_id"])
        self.assertEqual(result["partition_field"], "graded_at")
        self.assertEqual(
            [field.name for field in client.table.schema],
            [name for name, _, _ in storage.POSTGAME_FIELD_SPECS],
        )

    def test_setup_refuses_production_before_client_access(self):
        class NoCalls:
            def get_dataset(self, _):
                raise AssertionError("client should not be called")

        with self.assertRaisesRegex(ValueError, "only in dev"):
            storage.ensure_gamelens_postgame_outcome_table(
                client=NoCalls(),
                runtime_config=_runtime("production"),
                bigquery_module=_BigQuery,
            )

    def test_plan_distinguishes_insert_retry_and_immutable_conflict(self):
        grade = _grade()
        insert = storage.plan_postgame_grade_merge(
            grade=grade, existing_rows=[]
        )
        self.assertEqual(insert["expected_inserted_count"], 1)

        stored = {**grade, "graded_at": "2026-08-17T20:00:00+00:00"}
        retry = storage.plan_postgame_grade_merge(
            grade=grade, existing_rows=[stored]
        )
        self.assertEqual(retry["expected_unchanged_count"], 1)

        conflict = storage.plan_postgame_grade_merge(
            grade=grade,
            existing_rows=[{**stored, "final_score_sha256": "different"}],
        )
        self.assertEqual(conflict["conflict_count"], 1)
        self.assertIn("final_score_sha256", conflict["conflicts"][0]["fields"])

    def test_dry_plan_projects_one_row_without_write(self):
        client = _Client()
        boundary = storage.BigQueryPostgameOutcomeStorage(
            client=client,
            runtime_config=_runtime(),
            bigquery_module=_BigQuery,
        )
        plan = boundary.plan_grade(_grade())
        self.assertEqual(plan["existing_capture_row_count"], 0)
        self.assertEqual(plan["projected_capture_row_count"], 1)
        self.assertFalse(plan["write_performed"])
        self.assertEqual(client.rows, [])

    def test_insert_once_and_identical_retry_is_unchanged(self):
        unrelated = {
            **_grade(
                capture_id="capture_other",
                game_id="20260815_LAC@LAR",
            ),
            "graded_at": "2026-08-17T20:00:00+00:00",
        }
        client = _Client(rows=[unrelated])
        boundary = storage.BigQueryPostgameOutcomeStorage(
            client=client,
            runtime_config=_runtime(),
            bigquery_module=_BigQuery,
        )
        first = boundary.store_grade(_grade(), attempt_id="packet4_one")
        second = boundary.store_grade(_grade(), attempt_id="packet4_two")

        self.assertEqual(first["inserted"], 1)
        self.assertEqual(second["inserted"], 0)
        self.assertEqual(second["unchanged"], 1)
        self.assertEqual(len(client.rows), 2)
        merge = next(query for query in client.queries if "MERGE" in query)
        self.assertIn(
            "target.learning_run_id = source.learning_run_id", merge
        )
        self.assertIn("target.capture_id = source.capture_id", merge)
        self.assertNotIn("DELETE", merge)

    def test_conflict_stops_before_merge(self):
        stored = {
            **_grade(final_score_sha256="stored_score"),
            "graded_at": "2026-08-17T20:00:00+00:00",
        }
        client = _Client(rows=[stored])
        boundary = storage.BigQueryPostgameOutcomeStorage(
            client=client,
            runtime_config=_runtime(),
            bigquery_module=_BigQuery,
        )
        with self.assertRaisesRegex(
            storage.PostgameOutcomeStorageConflictError,
            "final_score_sha256",
        ):
            boundary.store_grade(_grade(), attempt_id="packet4_conflict")
        self.assertFalse(any("MERGE" in query for query in client.queries))


if __name__ == "__main__":
    unittest.main()
