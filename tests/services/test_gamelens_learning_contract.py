import unittest
from datetime import datetime, timedelta, timezone

from services.gamelens_learning_contract import (
    DEFAULT_LOOKAHEAD_DAYS,
    build_capture_id,
    build_capture_manifest,
    build_claim_key,
    build_learning_run_id,
    decide_canonical_capture,
    evaluate_capture_candidate,
    evaluate_postgame_readiness,
    summarize_capture_candidates,
    validate_pregame_payload,
)


NOW = datetime(2026, 9, 9, 20, 0, tzinfo=timezone.utc)
KICKOFF = NOW + timedelta(hours=1)
GAME = {
    "game_id": "20260909_SEA@NE",
    "season": "2026",
    "season_type": "Regular Season",
    "game_week": "Week 1",
    "game_status": "Scheduled",
}


class TestPregameCaptureContract(unittest.TestCase):
    def test_default_lookahead_reuses_today_plus_next_two_dates(self):
        self.assertEqual(DEFAULT_LOOKAHEAD_DAYS, 2)

    def test_scheduled_regular_season_game_before_kickoff_is_eligible(self):
        self.assertEqual(
            evaluate_capture_candidate(
                GAME, captured_at=NOW, scheduled_kickoff=KICKOFF
            ),
            {"eligible": True, "reason": None},
        )

    def test_in_progress_and_final_games_are_rejected(self):
        for status in ("In Progress", "Final", "Final/OT"):
            with self.subTest(status=status):
                game = {**GAME, "game_status": status}
                result = evaluate_capture_candidate(
                    game, captured_at=NOW, scheduled_kickoff=KICKOFF
                )
                self.assertFalse(result["eligible"])
                self.assertEqual(result["reason"], "game_not_scheduled")

    def test_exactly_at_kickoff_is_rejected(self):
        result = evaluate_capture_candidate(
            GAME, captured_at=KICKOFF, scheduled_kickoff=KICKOFF
        )
        self.assertEqual(result, {"eligible": False, "reason": "kickoff_reached"})

    def test_preseason_is_shadow_only_in_production(self):
        game = {**GAME, "season_type": "Preseason"}
        result = evaluate_capture_candidate(
            game, captured_at=NOW, scheduled_kickoff=KICKOFF
        )
        self.assertEqual(
            result, {"eligible": False, "reason": "preseason_shadow_only"}
        )
        self.assertTrue(
            evaluate_capture_candidate(
                game,
                captured_at=NOW,
                scheduled_kickoff=KICKOFF,
                production=False,
            )["eligible"]
        )

    def test_postgame_fields_are_rejected_when_populated(self):
        payload = {
            "final_score": {"away": {"total": 20}, "home": {"total": 17}},
            "model_outcome": {"result": "Correct"},
            "audit": {"actual_winner": "SEA", "model_result": "Correct"},
        }
        result = validate_pregame_payload(payload)
        self.assertFalse(result["valid"])
        self.assertEqual(result["reason"], "postgame_fields_populated")
        self.assertEqual(
            result["violations"],
            [
                "audit.actual_winner",
                "audit.model_result",
                "final_score",
                "model_outcome",
            ],
        )

    def test_empty_outcome_fields_and_missing_rankings_are_accepted(self):
        payload = {
            "final_score": None,
            "model_outcome": None,
            "model_trust": {"learning_label": "Outcome not available yet"},
            "ranking_context": {
                "available": False,
                "reason": "no_ranking_rows_found",
            },
        }
        result = validate_pregame_payload(payload)
        self.assertTrue(result["valid"])
        self.assertFalse(result["ranking_context_available"])
        self.assertEqual(result["ranking_context_reason"], "no_ranking_rows_found")

    def test_retry_produces_the_same_capture_and_claim_identity(self):
        learning_run_id = build_learning_run_id(
            season="2026",
            season_type="Regular Season",
            ruleset_version="v1",
        )
        first = build_capture_id(
            learning_run_id=learning_run_id,
            game_id=GAME["game_id"],
            scheduled_kickoff=KICKOFF,
        )
        second = build_capture_id(
            learning_run_id=learning_run_id,
            game_id=GAME["game_id"],
            scheduled_kickoff=KICKOFF,
        )
        self.assertEqual(first, second)
        self.assertEqual(
            build_claim_key(
                capture_id=first,
                source_field_path="team_comparison[0]",
                claim_type="team_comparison_metric",
                claim_rank=1,
            ),
            build_claim_key(
                capture_id=second,
                source_field_path="team_comparison[0]",
                claim_type="team_comparison_metric",
                claim_rank=1,
            ),
        )

    def test_manifest_records_missing_rankings_and_disables_outcome_writes(self):
        payload = {
            "final_score": None,
            "model_outcome": None,
            "ranking_context": {
                "available": False,
                "reason": "no_ranking_rows_found",
            },
        }
        manifest = build_capture_manifest(
            payload=payload,
            game=GAME,
            pipeline_run_id="pipeline_20260909",
            learning_run_id="gamelens_2026_regular_season_v1",
            captured_at=NOW,
            scheduled_kickoff=KICKOFF,
            model_version="game_service_v1",
            ruleset_version="v1",
            source_dates={"rankings": None},
        )
        self.assertEqual(manifest["capture_status"], "captured")
        self.assertFalse(manifest["ranking_context_available"])
        self.assertFalse(manifest["postgame_fields_present"])
        self.assertFalse(manifest["outcome_write_allowed"])

    def test_first_capture_is_canonical_and_conflicting_retry_is_quarantined(self):
        base = {"capture_id": "capture_1", "payload_sha256": "hash_a"}
        self.assertEqual(
            decide_canonical_capture(
                existing_manifest=None, candidate_manifest=base
            ),
            {"action": "create", "reason": None},
        )
        self.assertEqual(
            decide_canonical_capture(
                existing_manifest=base, candidate_manifest=dict(base)
            ),
            {"action": "no_op", "reason": "canonical_capture_exists"},
        )
        self.assertEqual(
            decide_canonical_capture(
                existing_manifest=base,
                candidate_manifest={
                    "capture_id": "capture_1",
                    "payload_sha256": "hash_b",
                },
            ),
            {"action": "quarantine", "reason": "canonical_payload_conflict"},
        )

    def test_no_games_is_a_clean_no_op(self):
        self.assertEqual(
            summarize_capture_candidates([]),
            {
                "status": "no_op",
                "games_checked": 0,
                "games_eligible": 0,
                "games_skipped": 0,
            },
        )


class TestPostgameReadinessContract(unittest.TestCase):
    def test_capture_and_final_score_allow_grade_but_not_levels_without_facts(self):
        result = evaluate_postgame_readiness(
            has_valid_capture=True,
            has_final_score=True,
            has_accepted_facts=False,
        )
        self.assertTrue(result["outcome_grade"]["ready"])
        self.assertFalse(result["levels_2_3"]["ready"])
        self.assertEqual(
            result["levels_2_3"]["reason"], "accepted_facts_missing"
        )

    def test_capture_final_score_and_facts_allow_levels_2_3(self):
        result = evaluate_postgame_readiness(
            has_valid_capture=True,
            has_final_score=True,
            has_accepted_facts=True,
        )
        self.assertTrue(result["outcome_grade"]["ready"])
        self.assertTrue(result["levels_2_3"]["ready"])

    def test_missing_capture_blocks_both_gates(self):
        result = evaluate_postgame_readiness(
            has_valid_capture=False,
            has_final_score=True,
            has_accepted_facts=True,
        )
        self.assertEqual(result["outcome_grade"]["reason"], "capture_missing")
        self.assertEqual(result["levels_2_3"]["reason"], "capture_missing")


if __name__ == "__main__":
    unittest.main()
