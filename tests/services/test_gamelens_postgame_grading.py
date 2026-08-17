from __future__ import annotations

import unittest

from services import gamelens_postgame_grading as grading
from services.gamelens_learning_contract import payload_sha256


def _snapshot(payload=None, **overrides):
    payload = payload or {
        "header": {
            "game_id": "20260815_DAL@SEA",
            "away_team": {"abbreviation": "DAL"},
            "home_team": {"abbreviation": "SEA"},
        },
        "game_profile": [{"category": "Pressure", "tilt_team": "away"}],
        "team_comparison": [{"label": "Pressure", "better": "away"}],
        "matchup_lean": {
            "target_team": "DAL edge",
            "outcome_confidence": "Medium",
        },
        "final_score": None,
        "model_outcome": None,
    }
    row = {
        "capture_id": "capture_dal_sea",
        "learning_run_id": "gamelens_2026_preseason_v1",
        "game_id": "20260815_DAL@SEA",
        "environment": "dev",
        "capture_status": "captured",
        "payload_sha256": payload_sha256(payload),
        "response_payload": payload,
    }
    row.update(overrides)
    return row


class FrozenCaptureGradeTests(unittest.TestCase):
    def test_reuses_frozen_sections_and_returns_lineage(self):
        calls = {}

        def outcome_builder(**kwargs):
            calls["outcome"] = kwargs
            return {
                "result": "Correct",
                "predicted_team": "DAL",
                "actual_winner": "DAL",
            }

        def trust_builder(**kwargs):
            calls["trust"] = kwargs
            return {"learning_label": "Model aligned with outcome"}

        score = {
            "away": {"total": 24},
            "home": {"total": 17},
        }
        snapshot = _snapshot()
        result = grading.grade_frozen_capture(
            snapshot=snapshot,
            final_score=score,
            outcome_builder=outcome_builder,
            trust_builder=trust_builder,
        )

        payload = snapshot["response_payload"]
        self.assertIs(calls["outcome"]["matchup_lean"], payload["matchup_lean"])
        self.assertIs(calls["trust"]["game_profile"], payload["game_profile"])
        self.assertIs(
            calls["trust"]["team_comparison"],
            payload["team_comparison"],
        )
        self.assertEqual(result["capture_id"], "capture_dal_sea")
        self.assertEqual(result["source_payload_sha256"], snapshot["payload_sha256"])
        self.assertEqual(result["final_score_sha256"], payload_sha256(score))
        self.assertEqual(result["grade_version"], "frozen_capture_outcome_v1")

    def test_rejects_payload_hash_mismatch(self):
        with self.assertRaisesRegex(grading.PostgameGradeError, "payload_hash_mismatch"):
            grading.grade_frozen_capture(
                snapshot=_snapshot(payload_sha256="wrong"),
                final_score={"away": {"total": 1}, "home": {"total": 0}},
                outcome_builder=lambda **_: {},
                trust_builder=lambda **_: {},
            )

    def test_rejects_snapshot_game_identity_mismatch(self):
        snapshot = _snapshot(game_id="another_game")
        with self.assertRaisesRegex(
            grading.PostgameGradeError,
            "snapshot_game_identity_mismatch",
        ):
            grading.grade_frozen_capture(
                snapshot=snapshot,
                final_score={"away": {"total": 1}, "home": {"total": 0}},
                outcome_builder=lambda **_: {},
                trust_builder=lambda **_: {},
            )

    def test_rejects_postgame_data_in_frozen_payload(self):
        payload = _snapshot()["response_payload"] | {
            "model_outcome": {"result": "Correct"}
        }
        with self.assertRaisesRegex(
            grading.PostgameGradeError,
            "postgame_fields_populated",
        ):
            grading.grade_frozen_capture(
                snapshot=_snapshot(payload),
                final_score={"away": {"total": 1}, "home": {"total": 0}},
                outcome_builder=lambda **_: {},
                trust_builder=lambda **_: {},
            )

    def test_rejects_non_dev_snapshot(self):
        with self.assertRaisesRegex(grading.PostgameGradeError, "development_only"):
            grading.grade_frozen_capture(
                snapshot=_snapshot(environment="prod"),
                final_score={"away": {"total": 1}, "home": {"total": 0}},
                outcome_builder=lambda **_: {},
                trust_builder=lambda **_: {},
            )

    def test_rejects_incomplete_final_score(self):
        with self.assertRaisesRegex(grading.PostgameGradeError, "final_score_incomplete"):
            grading.grade_frozen_capture(
                snapshot=_snapshot(),
                final_score={"away": {"total": None}, "home": {"total": None}},
                outcome_builder=lambda **_: None,
                trust_builder=lambda **_: {},
            )

    def test_rejects_non_object_matchup_lean(self):
        payload = _snapshot()["response_payload"] | {"matchup_lean": ["DAL"]}
        with self.assertRaisesRegex(
            grading.PostgameGradeError,
            "matchup_lean_not_object",
        ):
            grading.grade_frozen_capture(
                snapshot=_snapshot(payload),
                final_score={"away": {"total": 1}, "home": {"total": 0}},
                outcome_builder=lambda **_: {},
                trust_builder=lambda **_: {},
            )


if __name__ == "__main__":
    unittest.main()
