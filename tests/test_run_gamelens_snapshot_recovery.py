import copy
import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from run_gamelens_snapshot_recovery import (
    CORRECTED_READBACK_PAYLOAD_SHA256,
    EXPECTED_TABLE_ID,
    FAILED_CAPTURED_AT,
    FAILED_CAPTURE_ID,
    FAILED_GAME_ID,
    FAILED_LEARNING_RUN_ID,
    FAILED_MODEL_VERSION,
    FAILED_RULESET_VERSION,
    FAILED_STORED_PAYLOAD_SHA256,
    SnapshotRecoveryError,
    _delete_sql,
    build_parser,
    recover_failed_capture,
)


def failed_row():
    return {
        "capture_id": FAILED_CAPTURE_ID,
        "learning_run_id": FAILED_LEARNING_RUN_ID,
        "game_id": FAILED_GAME_ID,
        "environment": "dev",
        "season": "2026",
        "season_type": "Preseason",
        "game_week": "Preseason Week 3",
        "game_status": "Scheduled",
        "scheduled_kickoff": datetime(
            2026, 8, 27, 23, 0, tzinfo=timezone.utc
        ),
        "captured_at": FAILED_CAPTURED_AT,
        "capture_status": "captured",
        "payload_sha256": FAILED_STORED_PAYLOAD_SHA256,
        "response_payload": {
            "header": {
                "game_id": FAILED_GAME_ID,
                "game_status": "Scheduled",
                "season": "2026",
                "season_type": "Preseason",
            },
            "final_score": None,
            "model_outcome": None,
            "ranking_context": {"available": True},
            "proof": {"value": 5},
        },
        "evidence_context": {"source": "production"},
        "lens_tags": [f"tag-{index}" for index in range(96)],
        "ranking_context_available": True,
        "ranking_context_reason": None,
        "metric_source_date": None,
        "ranking_as_of_date": None,
        "metric_pipeline_run_id": None,
        "model_version": FAILED_MODEL_VERSION,
        "ruleset_version": FAILED_RULESET_VERSION,
    }


class Storage:
    table_id = EXPECTED_TABLE_ID

    def __init__(self, row):
        self.rows = [copy.deepcopy(row)]
        self.verify_calls = 0

    def verify_table_contract(self):
        self.verify_calls += 1
        return {"status": "verified"}

    def read_capture_rows(self, capture_id):
        return [
            copy.deepcopy(row)
            for row in self.rows
            if row["capture_id"] == capture_id
        ]


class Job:
    num_dml_affected_rows = 1

    def result(self):
        return []


class Client:
    def __init__(self, storage):
        self.storage = storage
        self.calls = []

    def query(self, query, job_config):
        self.calls.append((query, job_config))
        self.storage.rows = []
        return Job()


class TestSnapshotRecovery(unittest.TestCase):
    def setUp(self):
        self.row = failed_row()
        self.storage = Storage(self.row)
        self.client = Client(self.storage)

    def _patch_expected_fresh_hash(self):
        import run_gamelens_snapshot_recovery as recovery

        original = recovery.CORRECTED_READBACK_PAYLOAD_SHA256
        recovery.CORRECTED_READBACK_PAYLOAD_SHA256 = recovery.payload_sha256(
            self.row["response_payload"]
        )
        self.addCleanup(
            setattr,
            recovery,
            "CORRECTED_READBACK_PAYLOAD_SHA256",
            original,
        )

    def test_parser_is_export_only_by_default(self):
        args = build_parser().parse_args(["--evidence-file", "proof.json"])

        self.assertFalse(args.delete)
        self.assertIsNone(args.expected_evidence_sha256)

    def test_delete_sql_is_locked_to_exact_row_and_single_capture(self):
        sql = _delete_sql(EXPECTED_TABLE_ID)

        self.assertIn(f"DELETE FROM `{EXPECTED_TABLE_ID}`", sql)
        self.assertIn("capture_id = @capture_id", sql)
        self.assertIn("game_id = @game_id", sql)
        self.assertIn("payload_sha256 = @payload_sha256", sql)
        self.assertIn("captured_at = @captured_at", sql)
        self.assertIn("SELECT COUNT(*)", sql)
        self.assertNotIn("UPDATE", sql)
        self.assertNotIn("TRUNCATE", sql)

    def test_export_is_read_only_and_exclusive(self):
        self._patch_expected_fresh_hash()
        with tempfile.TemporaryDirectory() as temp_dir:
            evidence_file = Path(temp_dir) / "failed_capture.json"
            result = recover_failed_capture(
                storage=self.storage,
                client=self.client,
                evidence_file=evidence_file,
            )

            self.assertEqual(result["status"], "evidence_exported")
            self.assertFalse(result["material_change"])
            self.assertTrue(evidence_file.is_file())
            self.assertEqual(self.client.calls, [])
            content = evidence_file.read_bytes()
            self.assertEqual(
                result["evidence_sha256"],
                hashlib.sha256(content).hexdigest(),
            )
            evidence = json.loads(content)
            self.assertEqual(evidence["row"]["capture_id"], FAILED_CAPTURE_ID)

            with self.assertRaises(FileExistsError):
                recover_failed_capture(
                    storage=self.storage,
                    client=self.client,
                    evidence_file=evidence_file,
                )

    def test_delete_requires_matching_unchanged_evidence(self):
        self._patch_expected_fresh_hash()
        with tempfile.TemporaryDirectory() as temp_dir:
            evidence_file = Path(temp_dir) / "failed_capture.json"
            exported = recover_failed_capture(
                storage=self.storage,
                client=self.client,
                evidence_file=evidence_file,
            )
            evidence_file.write_text("changed\n", encoding="utf-8")

            with self.assertRaisesRegex(
                SnapshotRecoveryError,
                "evidence_file_does_not_match_live_row",
            ):
                recover_failed_capture(
                    storage=self.storage,
                    client=self.client,
                    evidence_file=evidence_file,
                    delete=True,
                    expected_evidence_sha256=exported["evidence_sha256"],
                )
            self.assertEqual(self.client.calls, [])

    def test_guarded_delete_removes_one_verified_failed_row(self):
        self._patch_expected_fresh_hash()
        with tempfile.TemporaryDirectory() as temp_dir:
            evidence_file = Path(temp_dir) / "failed_capture.json"
            exported = recover_failed_capture(
                storage=self.storage,
                client=self.client,
                evidence_file=evidence_file,
            )

            result = recover_failed_capture(
                storage=self.storage,
                client=self.client,
                evidence_file=evidence_file,
                delete=True,
                expected_evidence_sha256=exported["evidence_sha256"],
            )

            self.assertEqual(result["status"], "failed_proof_deleted")
            self.assertTrue(result["material_change"])
            self.assertEqual(result["affected_rows"], 1)
            self.assertEqual(result["row_count_after"], 0)
            self.assertEqual(len(self.client.calls), 1)

    def test_unexpected_live_row_fails_before_export_or_delete(self):
        self._patch_expected_fresh_hash()
        self.storage.rows[0]["game_id"] = "different"
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(
                SnapshotRecoveryError,
                "failed_capture_game_id_mismatch",
            ):
                recover_failed_capture(
                    storage=self.storage,
                    client=self.client,
                    evidence_file=Path(temp_dir) / "failed_capture.json",
                )
        self.assertEqual(self.client.calls, [])

    def test_recorded_real_corrected_hash_remains_locked(self):
        self.assertEqual(
            CORRECTED_READBACK_PAYLOAD_SHA256,
            "115187701cad1a5fffe511e1108f07234cc8c7a3db461fcb20478a0e2d944c46",
        )


if __name__ == "__main__":
    unittest.main()
