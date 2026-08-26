import copy
import json
import unittest
from datetime import datetime, timezone

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from runtime_config import RuntimeConfig
from services.gamelens_pregame_contract import payload_sha256
from services.gamelens_snapshot_storage import (
    BigQuerySnapshotStorage,
    SNAPSHOT_SCHEMA_SIGNATURE,
    SnapshotConflictError,
    SnapshotIntegrityError,
)


KICKOFF = datetime(2026, 9, 10, 0, 20, tzinfo=timezone.utc)
CAPTURED_AT = datetime(2026, 9, 9, 12, 0, tzinfo=timezone.utc)


def runtime_config(environment="dev"):
    return RuntimeConfig(
        project_id="nfl-stream-406420",
        environment=environment,
        run_mode="controlled_replay" if environment == "dev" else "daily",
        active_season="2026",
        league_dataset="League_dev" if environment == "dev" else "League",
        scores_dataset="Scores_dev" if environment == "dev" else "Scores",
        analytics_dataset=(
            "Analytics_dev" if environment == "dev" else "Analytics"
        ),
        raw_response_bucket=(
            "xtra-point-dev" if environment == "dev" else "xtra_point"
        ),
        replay_date="2026-09-09" if environment == "dev" else None,
    )


def payload():
    return {
        "header": {
            "game_id": "20260909_SEA@NE",
            "game_status": "Scheduled",
            "season": "2026",
            "season_type": "Regular Season",
            "game_week": "Week 1",
        },
        "final_score": None,
        "game_profile": [],
        "matchup_lean": {},
        "model_outcome": None,
        "model_trust": {},
        "team_comparison": [],
        "core_area_comparison": {},
        "ranking_context": {
            "available": False,
            "reason": "no_ranking_rows_found",
        },
        "claim_language_context": {},
        "matchup_breakdown": {},
    }


def snapshot_row():
    response = payload()
    return {
        "capture_id": "capture_ca1f097100b6563570b23464",
        "learning_run_id": "gamelens_2026_regular_season_v1",
        "game_id": "20260909_SEA@NE",
        "environment": "dev",
        "season": "2026",
        "season_type": "Regular Season",
        "game_week": "Week 1",
        "game_status": "Scheduled",
        "scheduled_kickoff": KICKOFF,
        "captured_at": CAPTURED_AT,
        "capture_status": "captured",
        "payload_sha256": payload_sha256(response),
        "response_payload": response,
        "evidence_context": {"source_lineage": {"access_mode": "read_only"}},
        "lens_tags": ["scoring-efficiency"],
        "ranking_context_available": False,
        "ranking_context_reason": "no_ranking_rows_found",
        "metric_source_date": None,
        "ranking_as_of_date": None,
        "metric_pipeline_run_id": None,
        "model_version": "game_service_v1",
        "ruleset_version": "v1",
    }


class Field:
    def __init__(self, name, field_type, mode):
        self.name = name
        self.field_type = field_type
        self.mode = mode


class Partitioning:
    field = "captured_at"


class Table:
    def __init__(self):
        self.schema = [Field(*field) for field in SNAPSHOT_SCHEMA_SIGNATURE]
        self.time_partitioning = Partitioning()
        self.clustering_fields = ["game_id", "season_type"]


class Job:
    def __init__(self, rows=None, affected=None):
        self.rows = rows or []
        self.num_dml_affected_rows = affected

    def result(self):
        return self.rows


def parameter_values(job_config):
    values = {}
    for parameter in job_config.query_parameters:
        name, _, value = parameter.args
        values[name] = value
    return values


class Client:
    def __init__(self):
        self.rows = []
        self.insert_calls = 0
        self.query_calls = 0
        self.table = Table()

    def get_table(self, table_id):
        self.table.table_id = table_id
        return self.table

    def query(self, query, job_config):
        self.query_calls += 1
        values = parameter_values(job_config)
        capture_id = values["capture_id"]
        if query.lstrip().startswith("SELECT"):
            rows = [
                copy.deepcopy(row)
                for row in self.rows
                if row["capture_id"] == capture_id
            ]
            return Job(rows=rows)

        self.insert_calls += 1
        if any(row["capture_id"] == capture_id for row in self.rows):
            return Job(affected=0)
        saved = dict(values)
        saved["response_payload"] = json.loads(saved["response_payload"])
        saved["evidence_context"] = json.loads(saved["evidence_context"])
        self.rows.append(saved)
        return Job(affected=1)


class TestSnapshotStorage(unittest.TestCase):
    def setUp(self):
        self.client = Client()
        self.storage = BigQuerySnapshotStorage(
            client=self.client,
            runtime_config=runtime_config(),
        )

    def test_storage_is_fail_closed_outside_dev(self):
        with self.assertRaisesRegex(ValueError, "dev-only"):
            BigQuerySnapshotStorage(
                client=Client(),
                runtime_config=runtime_config("production"),
            )

    def test_existing_table_contract_is_exact(self):
        result = self.storage.verify_table_contract()

        self.assertEqual(result["field_count"], 22)
        self.assertEqual(result["partition_field"], "captured_at")
        self.assertEqual(
            result["clustering_fields"],
            ["game_id", "season_type"],
        )

    def test_first_insert_is_readable_and_hash_verified(self):
        row = snapshot_row()

        result = self.storage.reconcile_snapshot(row)
        saved = self.storage.read_snapshot(row["capture_id"])

        self.assertEqual(result["status"], "inserted")
        self.assertTrue(result["material_change"])
        self.assertEqual(len(self.client.rows), 1)
        self.assertEqual(saved["response_payload"], row["response_payload"])
        self.assertEqual(
            set(saved["response_payload"]),
            {
                "header",
                "final_score",
                "game_profile",
                "matchup_lean",
                "model_outcome",
                "model_trust",
                "team_comparison",
                "core_area_comparison",
                "ranking_context",
                "claim_language_context",
                "matchup_breakdown",
            },
        )
        self.assertEqual(saved["payload_sha256"], row["payload_sha256"])
        self.assertEqual(saved["lens_tags"], ["scoring-efficiency"])
        self.assertFalse(saved["ranking_context_available"])
        self.assertEqual(
            saved["ranking_context_reason"],
            "no_ranking_rows_found",
        )

    def test_identical_retry_is_no_op_and_preserves_original_row(self):
        row = snapshot_row()
        self.storage.reconcile_snapshot(row)
        original = copy.deepcopy(self.client.rows[0])

        retry = copy.deepcopy(row)
        retry["captured_at"] = datetime(
            2026, 9, 9, 13, 0, tzinfo=timezone.utc
        )
        result = self.storage.reconcile_snapshot(retry)

        self.assertEqual(result["status"], "identical_no_op")
        self.assertFalse(result["material_change"])
        self.assertEqual(self.client.insert_calls, 1)
        self.assertEqual(self.client.rows, [original])

    def test_conflicting_retry_cannot_overwrite(self):
        row = snapshot_row()
        self.storage.reconcile_snapshot(row)
        original = copy.deepcopy(self.client.rows[0])

        conflict = copy.deepcopy(row)
        conflict["response_payload"]["ranking_context"]["reason"] = "changed"
        conflict["payload_sha256"] = payload_sha256(
            conflict["response_payload"]
        )
        with self.assertRaisesRegex(
            SnapshotConflictError,
            "canonical_capture_conflict",
        ):
            self.storage.reconcile_snapshot(conflict)

        self.assertEqual(self.client.rows, [original])
        self.assertEqual(self.client.insert_calls, 1)

    def test_duplicate_existing_rows_fail_closed(self):
        row = snapshot_row()
        self.client.rows = [copy.deepcopy(row), copy.deepcopy(row)]

        with self.assertRaisesRegex(
            SnapshotIntegrityError,
            "duplicate_canonical_capture",
        ):
            self.storage.reconcile_snapshot(row)

    def test_corrupt_stored_hash_fails_closed(self):
        row = snapshot_row()
        corrupt = copy.deepcopy(row)
        corrupt["payload_sha256"] = "0" * 64
        self.client.rows = [corrupt]

        with self.assertRaisesRegex(
            SnapshotIntegrityError,
            "stored_payload_hash_mismatch",
        ):
            self.storage.reconcile_snapshot(row)

    def test_postgame_payload_is_rejected_before_storage_access(self):
        row = snapshot_row()
        row["response_payload"]["final_score"] = {"away": {"total": 20}}
        row["payload_sha256"] = "0" * 64

        with self.assertRaisesRegex(ValueError, "postgame_fields_populated"):
            self.storage.reconcile_snapshot(row)

        self.assertEqual(self.client.query_calls, 0)
        self.assertEqual(self.client.insert_calls, 0)

    def test_schema_mismatch_fails_without_dml(self):
        self.client.table.schema = self.client.table.schema[:-1]

        with self.assertRaisesRegex(
            SnapshotIntegrityError,
            "snapshot_schema_mismatch",
        ):
            self.storage.reconcile_snapshot(snapshot_row())

        self.assertEqual(self.client.insert_calls, 0)


if __name__ == "__main__":
    unittest.main()
