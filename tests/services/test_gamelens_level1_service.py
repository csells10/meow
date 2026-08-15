import copy
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from services.gamelens_learning_contract import payload_sha256
from services.gamelens_level1_service import (
    EXTRACTION_VERSION,
    POSTGAME_NULL_FIELDS,
    Level1PreparationError,
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
        self.assertTrue(all(row[field] is None for field in POSTGAME_NULL_FIELDS))

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
        with patch(
            "services.gamelens_level1_service.extract_claim_rows",
            return_value=[copy.deepcopy(duplicate), copy.deepcopy(duplicate)],
        ):
            with self.assertRaisesRegex(Level1PreparationError, "duplicate"):
                prepare_level1_claims(snapshot(), extracted_at=EXTRACTED_AT)

    def test_honest_zero_claim_capture_is_a_successful_preparation(self):
        selected = snapshot()
        selected["response_payload"]["game_profile"] = []
        selected["payload_sha256"] = payload_sha256(selected["response_payload"])

        result = prepare_level1_claims(selected, extracted_at=EXTRACTED_AT)

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["claim_count"], 0)
        self.assertEqual(result["unique_claim_key_count"], 0)
        self.assertEqual(result["rows"], [])


if __name__ == "__main__":
    unittest.main()
