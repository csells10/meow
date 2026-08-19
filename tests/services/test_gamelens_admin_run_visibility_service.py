from __future__ import annotations

import unittest
import subprocess
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import qa_gamelens_packet5_admin_service as preview
from services.gamelens_admin_run_visibility_service import (
    DevelopmentRunVisibilityUnavailable,
    GameVisibilityNotFound,
    build_admin_run_visibility_response,
    get_admin_run_visibility,
)


def _stage(
    name, status="complete", attention="none", reason="present", count=None
):
    return {
        "stage": name,
        "status": status,
        "attention": attention,
        "reason": reason,
        "source": f"source.{name}",
        "count": count,
        "details": {"attempt_id": f"attempt_{name}"},
    }


def _game(game_id, *, issue=None, capture_id="capture_1"):
    return {
        "game_id": game_id,
        "matchup": "DAL @ SEA" if "DAL@SEA" in game_id else "CAR @ BUF",
        "game_date": "2026-08-15",
        "scheduled_kickoff": "2026-08-16T00:00:00+00:00",
        "game_status": "Final",
        "season": "2026",
        "season_type": "Preseason",
        "learning_run_id": "gamelens_2026_preseason_v1",
        "capture_id": capture_id,
        "first_issue": issue,
        "data_load": [_stage("schedule"), _stage("target_facts", count=130)],
        "pregame": [_stage("snapshot"), _stage("level1", count=0)],
        "postgame": [
            _stage("game_grade", count=1),
            _stage("level2", count=0),
            _stage("level3", count=0),
            _stage(
                "level4",
                status="not_applicable",
                reason="weekly_contract_not_implemented",
            ),
        ],
        "traceability": {"failed_boundary": None, "retryable": None},
    }


def _report():
    known_gap = _game(
        "20260815_CAR@BUF",
        capture_id=None,
        issue={
            "stage": "snapshot",
            "status": "warning",
            "attention": "known_gap",
            "reason": "kickoff_reached",
        },
    )
    known_gap["pregame"][0] = _stage(
        "snapshot",
        status="warning",
        attention="known_gap",
        reason="kickoff_reached",
    )
    healthy = _game("20260815_DAL@SEA")
    return {
        "access_mode": "read_only",
        "write_performed": False,
        "audited_at": "2026-08-19T12:00:00+00:00",
        "filters": {
            "season": "2026",
            "season_type": "Preseason",
            "learning_run_id": "gamelens_2026_preseason_v1",
            "start_date": "2026-08-15",
            "end_date": "2026-08-15",
        },
        "inventory_summary": {
            "table_count": 6,
            "available_count": 6,
            "duplicate_key_count": 0,
            "missing_key_row_count": 0,
        },
        "tables": [
            {
                "table": "nfl-stream-406420.GameLens_dev.pregame_snapshots",
                "status": "available",
                "row_count": 7,
                "logical_key_count": 7,
                "duplicate_key_count": 0,
                "missing_key_row_count": 0,
            }
        ],
        "game_summary": {
            "scheduled_game_count": 2,
            "captured_game_count": 1,
            "action_required_game_count": 0,
            "known_gap_game_count": 1,
        },
        "run_summary": [
            {
                "source": "stage_runs",
                "attempt_id": "snapshot_attempt",
                "stage_name": "snapshot_capture",
                "status": "success",
                "reason": None,
                "input_count": 2,
                "output_count": 1,
                "duration_ms": 100,
                "finished_at": "2026-08-15T12:00:00+00:00",
            }
        ],
        "games": [known_gap, healthy],
    }


class GameLensAdminRunVisibilityServiceTests(unittest.TestCase):
    def test_preview_cli_entrypoint_is_executable(self):
        result = subprocess.run(
            [sys.executable, str(Path(preview.__file__)), "--help"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("hierarchical Packet 5 Admin", result.stdout)

    def test_preview_opens_stage_evidence_only_for_a_selected_game(self):
        response = build_admin_run_visibility_response(
            _report(), game_id="20260815_DAL@SEA"
        )
        rendered = preview.render_admin_service_preview(response)

        self.assertIn("Overview > Game > Clock > Stage evidence", rendered)
        self.assertIn("GAME JOURNEY", rendered)
        self.assertIn("SELECTED GAME / 20260815_DAL@SEA", rendered)
        self.assertIn("STAGE EVIDENCE", rendered)
        self.assertIn("source.level2", rendered)

    def test_default_response_is_overview_first_and_compact(self):
        response = build_admin_run_visibility_response(_report(), game_limit=1)

        self.assertEqual(response["source_profile"], "development_learning")
        self.assertEqual(
            response["navigation"]["levels"],
            ["overview", "game", "clock", "stage_evidence"],
        )
        self.assertEqual(
            response["overview"]["games"],
            {
                "scheduled": 2,
                "captured": 1,
                "need_attention": 0,
                "known_gaps": 1,
                "returned": 1,
                "truncated": True,
            },
        )
        self.assertIsNone(response["selected_game"])
        self.assertEqual(len(response["games"]), 1)
        compact_stage = response["games"][0]["clocks"]["pregame"]["stages"][0]
        self.assertEqual(compact_stage["attention"], "known_gap")
        self.assertNotIn("reason", compact_stage)
        self.assertNotIn("source", compact_stage)
        self.assertNotIn("details", compact_stage)
        self.assertEqual(response["attention"]["needs_attention"], [])
        self.assertEqual(
            response["attention"]["known_gaps"][0]["reason"],
            "kickoff_reached",
        )

    def test_selected_game_returns_full_stage_evidence_for_only_that_game(self):
        response = build_admin_run_visibility_response(
            _report(), game_id="20260815_DAL@SEA"
        )

        selected = response["selected_game"]
        self.assertEqual(selected["game_id"], "20260815_DAL@SEA")
        self.assertEqual(
            selected["lineage"],
            {
                "learning_run_id": "gamelens_2026_preseason_v1",
                "capture_id": "capture_1",
            },
        )
        level2 = selected["clocks"]["postgame"]["stages"][1]
        self.assertEqual(level2["count"], 0)
        self.assertEqual(level2["reason"], "present")
        self.assertEqual(level2["source"], "source.level2")
        self.assertEqual(level2["details"]["attempt_id"], "attempt_level2")
        compact_level2 = response["games"][1]["clocks"]["postgame"]["stages"][1]
        self.assertNotIn("source", compact_level2)

    def test_service_is_dev_only_and_does_not_call_loader_in_production(self):
        calls = []

        def loader(**kwargs):
            calls.append(kwargs)
            return _report()

        with self.assertRaisesRegex(
            DevelopmentRunVisibilityUnavailable, "dev-only"
        ):
            get_admin_run_visibility(
                client=object(),
                bigquery=object(),
                runtime_config=SimpleNamespace(
                    is_dev=False, active_season="2026"
                ),
                season="2026",
                season_type="Preseason",
                learning_run_id="gamelens_2026_preseason_v1",
                start_date=date(2026, 8, 15),
                end_date=date(2026, 8, 15),
                report_loader=loader,
            )
        self.assertEqual(calls, [])

    def test_service_passes_bounded_filters_to_the_proven_reader(self):
        calls = []

        def loader(**kwargs):
            calls.append(kwargs)
            return _report()

        response = get_admin_run_visibility(
            client="client",
            bigquery="bigquery",
            runtime_config=SimpleNamespace(is_dev=True, active_season="2026"),
            season="2026",
            season_type="Preseason",
            learning_run_id="gamelens_2026_preseason_v1",
            start_date=date(2026, 8, 15),
            end_date=date(2026, 8, 15),
            game_id="20260815_DAL@SEA",
            report_loader=loader,
        )

        self.assertEqual(response["selected_game"]["game_id"], "20260815_DAL@SEA")
        self.assertEqual(
            calls,
            [
                {
                    "client": "client",
                    "bigquery": "bigquery",
                    "season": "2026",
                    "season_type": "Preseason",
                    "learning_run_id": "gamelens_2026_preseason_v1",
                    "start_date": date(2026, 8, 15),
                    "end_date": date(2026, 8, 15),
                    "now": None,
                }
            ],
        )

    def test_response_rejects_unbounded_or_unknown_selection(self):
        cases = (
            ({"game_limit": 0}, ValueError, "game_limit"),
            ({"game_id": "DAL-SEA"}, ValueError, "canonical"),
            (
                {"game_id": "20260815_BUF@CAR"},
                GameVisibilityNotFound,
                "not in the requested slate",
            ),
        )
        for kwargs, error_type, message in cases:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(error_type, message):
                    build_admin_run_visibility_response(_report(), **kwargs)

    def test_service_rejects_a_date_range_over_31_days(self):
        with self.assertRaisesRegex(ValueError, "31 inclusive days"):
            get_admin_run_visibility(
                client=object(),
                bigquery=object(),
                runtime_config=SimpleNamespace(is_dev=True, active_season="2026"),
                season="2026",
                season_type="Preseason",
                learning_run_id="gamelens_2026_preseason_v1",
                start_date=date(2026, 8, 1),
                end_date=date(2026, 9, 1),
                report_loader=lambda **_: _report(),
            )

    def test_service_refuses_any_report_that_claims_a_write(self):
        report = _report()
        report["write_performed"] = True
        with self.assertRaisesRegex(ValueError, "read-only reports only"):
            build_admin_run_visibility_response(report)


if __name__ == "__main__":
    unittest.main()
