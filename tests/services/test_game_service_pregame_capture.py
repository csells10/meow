import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from services import game_service


HEADER = {
    "game_id": "20260909_SEA@NE",
    "game_date": "2026-09-09",
    "game_time": "8:20p",
    "game_status": "Scheduled",
    "season": "2026",
    "game_week": "Week 1",
    "season_type": "Regular Season",
    "away_team": {
        "id": "1",
        "name": "SEA",
        "abbreviation": "SEA",
        "logo": None,
    },
    "home_team": {
        "id": "2",
        "name": "NE",
        "abbreviation": "NE",
        "logo": None,
    },
    "espn_link": None,
}

KICKOFF = datetime(2026, 9, 10, 0, 20, tzinfo=timezone.utc)

PREGAME_PRODUCT_SECTIONS = (
    "header",
    "game_profile",
    "matchup_lean",
    "model_trust",
    "team_comparison",
    "core_area_comparison",
    "ranking_context",
    "claim_language_context",
    "matchup_breakdown",
)


def evidence(*, header=None, final_score=None):
    return game_service.GameDetailsEvidence(
        header=dict(header or HEADER),
        away_metrics={},
        home_metrics={},
        ranking_context={
            "available": False,
            "reason": "no_ranking_rows_found",
        },
        away_rankings={},
        home_rankings={},
        final_score=final_score,
    )


class TestPregameResponseBuilder(unittest.TestCase):
    def test_live_and_pregame_builders_match_product_sections(self):
        fixed = evidence()

        def annotate(**kwargs):
            # language_boost_allowed is the backend support the frontend renders
            # as "Fits matchup".  The shared builder must preserve it.
            return {
                "team_comparison": [
                    {
                        "metric": "points_per_play",
                        "language_support": {
                            "language_boost_allowed": True,
                        },
                    }
                ],
                "matchup_breakdown": kwargs["matchup_breakdown"],
            }

        with (
            patch.object(
                game_service,
                "load_game_details_evidence",
                return_value=fixed,
            ),
            patch.object(
                game_service,
                "apply_claim_language_support_to_response_sections",
                side_effect=annotate,
            ),
        ):
            live = game_service.get_game_details(HEADER["game_id"])
            pregame = game_service.get_pregame_game_details(
                HEADER["game_id"],
                evidence=fixed,
            )

        for section in PREGAME_PRODUCT_SECTIONS:
            with self.subTest(section=section):
                self.assertEqual(pregame[section], live[section])

        self.assertTrue(
            pregame["team_comparison"][0]["language_support"][
                "language_boost_allowed"
            ]
        )

    def test_pregame_loader_never_queries_final_score_or_writes_outcome(self):
        with (
            patch.object(game_service, "get_game_header", return_value=HEADER),
            patch.object(game_service, "get_team_metrics", return_value=({}, {})),
            patch.object(
                game_service,
                "get_ranking_context_for_game_safe",
                return_value=(
                    {"available": False, "reason": "no_ranking_rows_found"},
                    {},
                    {},
                ),
            ),
            patch.object(
                game_service,
                "get_final_score",
                side_effect=AssertionError("final score queried"),
            ) as final_score,
            patch.object(game_service, "save_model_results") as outcome_writer,
        ):
            payload = game_service.get_pregame_game_details(HEADER["game_id"])

        final_score.assert_not_called()
        outcome_writer.assert_not_called()
        self.assertIsNone(payload["final_score"])
        self.assertIsNone(payload["model_outcome"])

    def test_pregame_mode_ignores_supplied_postgame_evidence(self):
        final_header = {**HEADER, "game_status": "Final"}
        fixed = evidence(
            header=final_header,
            final_score={
                "away": {"total": 17},
                "home": {"total": 20},
            },
        )

        with patch.object(game_service, "save_model_results") as outcome_writer:
            payload = game_service.get_pregame_game_details(
                HEADER["game_id"],
                evidence=fixed,
            )

        outcome_writer.assert_not_called()
        self.assertIsNone(payload["final_score"])
        self.assertIsNone(payload["model_outcome"])

    def test_pregame_entry_rejects_postgame_shaped_builder_output(self):
        postgame_payload = {
            "final_score": {"away": {"total": 17}},
            "model_outcome": None,
        }
        with patch.object(
            game_service,
            "build_game_details_from_evidence",
            return_value=postgame_payload,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "postgame_fields_populated",
            ):
                game_service.get_pregame_game_details(
                    HEADER["game_id"],
                    evidence=evidence(),
                )

    def test_eligible_fixture_produces_stable_hashable_contract(self):
        fixed = evidence()
        first = game_service.get_pregame_game_contract(
            HEADER["game_id"],
            learning_run_id="gamelens_2026_regular_season_v1",
            scheduled_kickoff=KICKOFF,
            evidence=fixed,
        )
        second = game_service.get_pregame_game_contract(
            HEADER["game_id"],
            learning_run_id="gamelens_2026_regular_season_v1",
            scheduled_kickoff=KICKOFF,
            evidence=fixed,
        )

        self.assertEqual(first, second)
        self.assertRegex(first["capture_id"], r"^capture_[0-9a-f]{24}$")
        self.assertRegex(first["payload_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(first["payload"]["header"]["game_id"], HEADER["game_id"])

    def test_live_entry_uses_shared_builder_with_existing_defaults(self):
        fixed = evidence()
        with (
            patch.object(
                game_service,
                "load_game_details_evidence",
                return_value=fixed,
            ) as loader,
            patch.object(
                game_service,
                "build_game_details_from_evidence",
                return_value={"same_builder": True},
            ) as builder,
        ):
            response = game_service.get_game_details(HEADER["game_id"])

        loader.assert_called_once_with(
            HEADER["game_id"],
            include_final_score=True,
        )
        builder.assert_called_once_with(fixed, pregame_only=False)
        self.assertEqual(response, {"same_builder": True})


if __name__ == "__main__":
    unittest.main()
