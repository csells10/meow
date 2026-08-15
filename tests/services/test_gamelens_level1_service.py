import copy
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from services.gamelens_learning_contract import (
    LEVEL1_POSTGAME_NULL_FIELDS,
    payload_sha256,
)
from services.gamelens_level1_service import (
    EXTRACTION_VERSION,
    LEVEL1_STAGE_NAME,
    Level1PreparationError,
    extract_level1_from_capture,
    prepare_level1_claims,
)


EXTRACTED_AT = datetime(2026, 8, 15, 15, 0, tzinfo=timezone.utc)
GAME_ID = "20260813_ARI@LV"


def payload():
    return {
        "header": {
            "game_id": GAME_ID,
            "season": "2026",
            "season_type": "Preseason",
            "game_week": "Preseason Week 2",
            "game_status": "Scheduled",
            "away_team": {"abbreviation": "ARI"},
            "home_team": {"abbreviation": "LV"},
        },
        "game_profile": [
            {
                "category": "Passing",
                "tilt_team": "away",
                "tilt_text": "ARI owns the passing profile edge.",
                "level": "Strong",
                "level_index": 3,
            }
        ],
        "core_area_comparison": [],
        "matchup_breakdown": {
            "core_area_summaries": [],
            "category_summaries": [],
            "metric_highlights": [],
        },
        "team_comparison": [],
        "matchup_lean": {"target_side": "away"},
        "ranking_context": {"available": True, "as_of_date": "2026-08-12"},
        "final_score": None,
        "model_outcome": None,
    }


def snapshot():
    response = payload()
    return {
        "capture_id": "capture_packet3_test",
        "learning_run_id": "gamelens_2026_preseason_v1",
        "game_id": GAME_ID,
        "environment": "dev",
        "capture_status": "captured",
        "payload_sha256": payload_sha256(response),
        "response_payload": response,
        "metric_pipeline_run_id": "metric_20260813",
        "model_version": "game_service_v1",
        "lens_tags": ["passing-production", "offensive-output"],
    }


class TestPrepareLevel1Claims(unittest.TestCase):
    def test_prepares_capture_aware_rows_and_visual_summary(self):
        result = prepare_level1_claims(snapshot(), extracted_at=EXTRACTED_AT)

        self.assertEqual(result["claim_count"], 1)
        self.assertEqual(result["unique_claim_key_count"], 1)
        self.assertEqual(result["by_claim_type"], {"game_profile": 1})
        self.assertEqual(result["by_claim_layer"], {"headline": 1})
        self.assertFalse(result["write_performed"])

        row = result["rows"][0]
        self.assertTrue(row["claim_key"].startswith("claim_"))
        self.assertEqual(row["run_id"], result["learning_run_id"])
        self.assertEqual(row["learning_run_id"], result["learning_run_id"])
        self.assertEqual(row["capture_id"], result["capture_id"])
        self.assertEqual(row["pipeline_run_id"], "metric_20260813")
        self.assertEqual(row["source_payload_sha256"], result["payload_sha256"])
        self.assertEqual(row["extraction_version"], EXTRACTION_VERSION)
        self.assertIn(result["capture_id"], row["source_payload_path"])
        self.assertTrue(
            all(row[field] is None for field in LEVEL1_POSTGAME_NULL_FIELDS)
        )

    def test_same_capture_produces_same_claim_identity(self):
        first = prepare_level1_claims(snapshot(), extracted_at=EXTRACTED_AT)
        second = prepare_level1_claims(snapshot(), extracted_at=EXTRACTED_AT)
        self.assertEqual(
            [row["claim_key"] for row in first["rows"]],
            [row["claim_key"] for row in second["rows"]],
        )

    def test_hash_mismatch_fails_before_extraction(self):
        selected = snapshot()
        selected["payload_sha256"] = "not-the-frozen-payload-hash"
        with self.assertRaisesRegex(Level1PreparationError, "payload_sha256"):
            prepare_level1_claims(selected, extracted_at=EXTRACTED_AT)

    def test_row_and_header_game_identity_mismatch_fails(self):
        selected = snapshot()
        selected["response_payload"]["header"]["game_id"] = "different_game"
        selected["payload_sha256"] = payload_sha256(selected["response_payload"])
        with self.assertRaisesRegex(Level1PreparationError, "game_id disagree"):
            prepare_level1_claims(selected, extracted_at=EXTRACTED_AT)

    def test_postgame_leakage_fails_before_extraction(self):
        selected = snapshot()
        selected["response_payload"]["final_score"] = {
            "away": {"total": 24},
            "home": {"total": 17},
        }
        selected["payload_sha256"] = payload_sha256(selected["response_payload"])
        with self.assertRaisesRegex(Level1PreparationError, "not pregame-safe"):
            prepare_level1_claims(selected, extracted_at=EXTRACTED_AT)

    def test_duplicate_canonical_claim_keys_fail_closed(self):
        duplicate = {
            "source_field_path": "game_profile[0]",
            "claim_type": "game_profile",
            "claim_rank": 1,
            "claim_layer": "headline",
        }

        def duplicate_rows(*, context, **kwargs):
            return [
                {**context, **copy.deepcopy(duplicate), "claimed_team": "ARI"},
                {**context, **copy.deepcopy(duplicate), "claimed_team": "ARI"},
            ]

        with patch(
            "services.gamelens_level1_service.extract_claim_rows",
            side_effect=duplicate_rows,
        ):
            with self.assertRaisesRegex(Level1PreparationError, "duplicate"):
                prepare_level1_claims(snapshot(), extracted_at=EXTRACTED_AT)

    def test_context_builder_failure_uses_level1_error_boundary(self):
        with patch(
            "services.gamelens_level1_service.extract_payload_context",
            side_effect=ValueError("bad canonical context"),
        ):
            with self.assertRaisesRegex(
                Level1PreparationError,
                "Level 1 preparation failed",
            ):
                prepare_level1_claims(snapshot(), extracted_at=EXTRACTED_AT)

    def test_non_dev_and_uncaptured_snapshots_fail_closed(self):
        for field, value, message in (
            ("environment", "prod", "must be from dev"),
            ("capture_status", "failure", "must be captured"),
        ):
            with self.subTest(field=field):
                selected = snapshot()
                selected[field] = value
                with self.assertRaisesRegex(Level1PreparationError, message):
                    prepare_level1_claims(selected, extracted_at=EXTRACTED_AT)

    def test_honest_zero_claim_capture_is_a_successful_preparation(self):
        selected = snapshot()
        selected["response_payload"]["game_profile"] = []
        selected["payload_sha256"] = payload_sha256(selected["response_payload"])

        result = prepare_level1_claims(selected, extracted_at=EXTRACTED_AT)

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["claim_count"], 0)
        self.assertEqual(result["unique_claim_key_count"], 0)
        self.assertEqual(result["rows"], [])


class FakeSnapshotStorage:
    def __init__(self, selected, *, receipt_already_exists=False):
        self.selected = selected
        self.receipt_already_exists = receipt_already_exists
        self.receipts = []

    def read_snapshot(self, capture_id):
        if self.selected and self.selected["capture_id"] == capture_id:
            return copy.deepcopy(self.selected)
        return None

    def write_stage_game_results(self, rows):
        self.receipts.extend(copy.deepcopy(rows))
        return {
            "input_count": len(rows),
            "inserted_count": 0 if self.receipt_already_exists else len(rows),
            "existing_count": len(rows) if self.receipt_already_exists else 0,
        }


class FakeClaimStorage:
    def __init__(self, *, inserted=1, unchanged=0):
        self.inserted = inserted
        self.unchanged = unchanged
        self.plan_calls = []
        self.merge_calls = []

    def plan_claims(self, rows, **identity):
        self.plan_calls.append((copy.deepcopy(rows), dict(identity)))
        return {
            "status": "ready",
            "expected_inserted_count": len(rows),
            "expected_unchanged_count": 0,
            "conflict_count": 0,
            "existing_capture_row_count": 0,
            "projected_capture_row_count": len(rows),
            "target_table": "nfl-stream-406420.GameLens_dev.claim_training_examples",
            "merge_key": ["learning_run_id", "claim_key"],
            "write_performed": False,
        }

    def merge_claims(self, rows, **identity):
        self.merge_calls.append((copy.deepcopy(rows), dict(identity)))
        return {
            "status": "matched",
            "claims_in": len(rows),
            "unique_claim_keys": len(rows),
            "inserted": self.inserted if rows else 0,
            "unchanged": self.unchanged if rows else 0,
            "conflicts": 0,
            "claims_out": len(rows),
            "target_table": "nfl-stream-406420.GameLens_dev.claim_training_examples",
            "merge_key": ["learning_run_id", "claim_key"],
            "write_performed": bool(rows and self.inserted),
        }


class TestExtractLevel1FromCapture(unittest.TestCase):
    def test_dry_write_plan_has_counts_and_no_receipt(self):
        snapshots = FakeSnapshotStorage(snapshot())
        claims = FakeClaimStorage()

        result = extract_level1_from_capture(
            "capture_packet3_test",
            "level1_dry_run",
            snapshot_storage=snapshots,
            claim_storage=claims,
            write=False,
            extracted_at=EXTRACTED_AT,
        )

        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["claim_count"], 1)
        self.assertEqual(result["inserted_count"], 1)
        self.assertEqual(result["unchanged_count"], 0)
        self.assertEqual(result["claims_out"], 0)
        self.assertEqual(result["projected_claims_out"], 1)
        self.assertFalse(result["write_requested"])
        self.assertFalse(result["write_performed"])
        self.assertFalse(result["receipt_saved"])
        self.assertEqual(len(claims.plan_calls), 1)
        self.assertEqual(snapshots.receipts, [])

    def test_deliberate_write_merges_and_saves_canonical_output_count(self):
        snapshots = FakeSnapshotStorage(snapshot())
        claims = FakeClaimStorage(inserted=1, unchanged=0)

        result = extract_level1_from_capture(
            "capture_packet3_test",
            "level1_write_one",
            snapshot_storage=snapshots,
            claim_storage=claims,
            write=True,
            extracted_at=EXTRACTED_AT,
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["reason"], "claims_merged")
        self.assertEqual(result["inserted_count"], 1)
        self.assertTrue(result["receipt_saved"])
        receipt = snapshots.receipts[0]
        self.assertEqual(receipt["stage_name"], LEVEL1_STAGE_NAME)
        self.assertEqual(receipt["input_count"], 1)
        self.assertEqual(receipt["output_count"], result["claim_count"])
        self.assertEqual(receipt["capture_id"], result["capture_id"])
        self.assertEqual(receipt["learning_run_id"], result["learning_run_id"])
        self.assertEqual(receipt["upstream_run_id"], "metric_20260813")

    def test_identical_retry_is_no_op_but_still_gets_a_receipt(self):
        snapshots = FakeSnapshotStorage(snapshot())
        claims = FakeClaimStorage(inserted=0, unchanged=1)

        result = extract_level1_from_capture(
            "capture_packet3_test",
            "level1_retry",
            snapshot_storage=snapshots,
            claim_storage=claims,
            write=True,
            extracted_at=EXTRACTED_AT,
        )

        self.assertEqual(result["status"], "no_op")
        self.assertEqual(result["reason"], "claims_unchanged")
        self.assertEqual(result["inserted_count"], 0)
        self.assertEqual(result["unchanged_count"], 1)
        self.assertFalse(result["write_performed"])
        self.assertTrue(result["receipt_saved"])

    def test_zero_claim_capture_is_visible_in_receipt(self):
        selected = snapshot()
        selected["response_payload"]["game_profile"] = []
        selected["payload_sha256"] = payload_sha256(
            selected["response_payload"]
        )
        snapshots = FakeSnapshotStorage(selected)

        result = extract_level1_from_capture(
            "capture_packet3_test",
            "level1_zero",
            snapshot_storage=snapshots,
            claim_storage=FakeClaimStorage(inserted=0, unchanged=0),
            write=True,
            extracted_at=EXTRACTED_AT,
        )

        self.assertEqual(result["claim_count"], 0)
        self.assertEqual(result["reason"], "zero_claims_extracted")
        self.assertEqual(snapshots.receipts[0]["output_count"], 0)
        self.assertEqual(snapshots.receipts[0]["status"], "success")

    def test_zero_claim_identical_retry_is_a_visible_no_op(self):
        selected = snapshot()
        selected["response_payload"]["game_profile"] = []
        selected["payload_sha256"] = payload_sha256(
            selected["response_payload"]
        )
        snapshots = FakeSnapshotStorage(
            selected,
            receipt_already_exists=True,
        )

        result = extract_level1_from_capture(
            "capture_packet3_test",
            "level1_zero_retry",
            snapshot_storage=snapshots,
            claim_storage=FakeClaimStorage(inserted=0, unchanged=0),
            write=True,
            extracted_at=EXTRACTED_AT,
        )

        self.assertEqual(result["status"], "no_op")
        self.assertEqual(result["reason"], "zero_claims_already_recorded")
        self.assertTrue(result["receipt_saved"])
        self.assertEqual(result["receipt_result"]["inserted_count"], 0)
        self.assertEqual(result["receipt_result"]["existing_count"], 1)

    def test_missing_capture_fails_before_claim_or_receipt_write(self):
        snapshots = FakeSnapshotStorage(None)
        claims = FakeClaimStorage()

        with self.assertRaisesRegex(Level1PreparationError, "not found"):
            extract_level1_from_capture(
                "missing_capture",
                "level1_missing",
                snapshot_storage=snapshots,
                claim_storage=claims,
                write=True,
            )

        self.assertEqual(claims.merge_calls, [])
        self.assertEqual(snapshots.receipts, [])


if __name__ == "__main__":
    unittest.main()
