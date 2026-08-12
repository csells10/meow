import unittest
from unittest.mock import patch

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from services import game_service


HEADER = {
    "game_id": "20260813_AWY@HME",
    "game_date": "2026-08-13",
    "game_time": "8:00p",
    "game_status": "Scheduled",
    "season": "2026",
    "game_week": "Preseason Week 2",
    "season_type": "Preseason",
    "away_team": {
        "id": "1",
        "name": "AWY",
        "abbreviation": "AWY",
        "logo": None,
    },
    "home_team": {
        "id": "2",
        "name": "HME",
        "abbreviation": "HME",
        "logo": None,
    },
    "espn_link": None,
}


def evidence(header=None, final_score=None):
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
    def test_fixed_evidence_live_and_capture_paths_match_exactly(self):
        fixed = evidence()
        with patch.object(
            game_service,
            "load_game_details_evidence",
            return_value=fixed,
        ):
            live = game_service.get_game_details(HEADER["game_id"])
        capture = game_service.get_pregame_game_details(
            HEADER["game_id"],
            evidence=fixed,
        )
        self.assertEqual(capture, live)

    def test_capture_loader_never_queries_final_score(self):
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

    def test_pregame_mode_ignores_supplied_outcome_data_and_writer_is_unreachable(self):
        final_header = {**HEADER, "game_status": "Final"}
        fixed = evidence(
            final_header,
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

    def test_live_entry_path_uses_the_same_builder(self):
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
        loader.assert_called_once_with(HEADER["game_id"], include_final_score=True)
        builder.assert_called_once_with(fixed, pregame_only=False)
        self.assertEqual(response, {"same_builder": True})


if __name__ == "__main__":
    unittest.main()
