import json
import math
import unittest
from datetime import datetime, timezone

from services.gamelens_pregame_contract import (
    build_capture_id,
    identify_pregame_payload,
    payload_sha256,
    require_pregame_payload,
    validate_pregame_payload,
)


KICKOFF = datetime(2026, 9, 10, 0, 20, tzinfo=timezone.utc)
CAPTURED_AT = datetime(2026, 9, 9, 12, 0, tzinfo=timezone.utc)
GAME_ID = "20260909_SEA@NE"
LEARNING_RUN_ID = "gamelens_2026_regular_season_v1"


def pregame_payload():
    return {
        "header": {
            "game_id": GAME_ID,
            "game_status": "Scheduled",
            "season": "2026",
            "season_type": "Regular Season",
        },
        "final_score": None,
        "model_outcome": None,
        "ranking_context": {
            "available": True,
            "featured_metrics": [
                {
                    "metric": "points_per_play",
                    "lens_tags": ["scoring-efficiency", "strong-signal"],
                }
            ],
        },
    }


class TestPregameContract(unittest.TestCase):
    def test_deterministic_capture_identity_and_payload_hash(self):
        payload = pregame_payload()
        first = identify_pregame_payload(
            payload=payload,
            learning_run_id=LEARNING_RUN_ID,
            game_id=GAME_ID,
            captured_at=CAPTURED_AT,
            scheduled_kickoff=KICKOFF,
        )
        second = identify_pregame_payload(
            payload=dict(reversed(list(payload.items()))),
            learning_run_id=LEARNING_RUN_ID,
            game_id=GAME_ID,
            captured_at=CAPTURED_AT,
            scheduled_kickoff=KICKOFF,
        )

        self.assertEqual(first, second)
        self.assertRegex(first["capture_id"], r"^capture_[0-9a-f]{24}$")
        self.assertRegex(first["payload_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(first["payload_sha256"], payload_sha256(payload))
        self.assertEqual(
            first["capture_id"],
            build_capture_id(
                learning_run_id=LEARNING_RUN_ID,
                game_id=GAME_ID,
                scheduled_kickoff=KICKOFF,
            ),
        )

    def test_identity_rejects_noneligible_fixture(self):
        with self.assertRaisesRegex(ValueError, "kickoff_reached"):
            identify_pregame_payload(
                payload=pregame_payload(),
                learning_run_id=LEARNING_RUN_ID,
                game_id=GAME_ID,
                captured_at=KICKOFF,
                scheduled_kickoff=KICKOFF,
            )

        mismatched = pregame_payload()
        mismatched["header"]["game_id"] = "different_game"
        with self.assertRaisesRegex(ValueError, "payload_game_id_mismatch"):
            identify_pregame_payload(
                payload=mismatched,
                learning_run_id=LEARNING_RUN_ID,
                game_id=GAME_ID,
                captured_at=CAPTURED_AT,
                scheduled_kickoff=KICKOFF,
            )

    def test_postgame_shaped_payload_fails_closed(self):
        payload = pregame_payload()
        payload["final_score"] = {"away": {"total": 20}}
        payload["audit"] = {
            "actual_winner": "SEA",
            "model_result": "Correct",
        }

        result = validate_pregame_payload(payload)
        self.assertEqual(
            result,
            {
                "valid": False,
                "reason": "postgame_fields_populated",
                "violations": [
                    "audit.actual_winner",
                    "audit.model_result",
                    "final_score",
                ],
            },
        )
        with self.assertRaisesRegex(ValueError, "postgame_fields_populated"):
            require_pregame_payload(payload)
        with self.assertRaisesRegex(ValueError, "postgame_fields_populated"):
            payload_sha256(payload)

    def test_honest_missing_rankings_remain_valid(self):
        payload = pregame_payload()
        payload["ranking_context"] = {
            "available": False,
            "reason": "no_ranking_rows_found",
        }

        result = validate_pregame_payload(payload)
        self.assertTrue(result["valid"])
        self.assertFalse(result["ranking_context_available"])
        self.assertEqual(
            result["ranking_context_reason"],
            "no_ranking_rows_found",
        )

    def test_lens_tags_remain_a_json_array(self):
        tags = pregame_payload()["ranking_context"]["featured_metrics"][0][
            "lens_tags"
        ]
        self.assertIsInstance(tags, list)
        serialized = json.loads(json.dumps({"lens_tags": tags}))
        self.assertEqual(
            serialized["lens_tags"],
            ["scoring-efficiency", "strong-signal"],
        )
        self.assertIsInstance(serialized["lens_tags"], list)

    def test_canonical_payload_rejects_non_json_values(self):
        payload = pregame_payload()
        payload["unsupported"] = object()

        with self.assertRaises(TypeError):
            payload_sha256(payload)

    def test_canonical_payload_rejects_non_finite_numbers(self):
        for value in (math.nan, math.inf, -math.inf):
            with self.subTest(value=value):
                payload = pregame_payload()
                payload["ranking_context"]["unsafe_number"] = value
                with self.assertRaisesRegex(ValueError, "JSON compliant"):
                    payload_sha256(payload)

    def test_canonical_payload_preserves_list_order_and_numeric_identity(self):
        first = pregame_payload()
        first["ranking_context"]["ordered"] = [1, 1.0, True, None]
        second = pregame_payload()
        second["ranking_context"]["ordered"] = [1.0, 1, True, None]

        self.assertNotEqual(payload_sha256(first), payload_sha256(second))


if __name__ == "__main__":
    unittest.main()
