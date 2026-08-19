from __future__ import annotations

import unittest
from datetime import datetime, timezone

import qa_gamelens_packet5_admin_inventory as inventory


class _Field:
    def __init__(self, name):
        self.name = name


class _Table:
    def __init__(self, fields, rows=3):
        self.schema = [_Field(name) for name in fields]
        self.num_rows = rows


class _Job:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return list(self._rows)


class _Client:
    def __init__(self, table, rows):
        self.table = table
        self.rows = rows
        self.queries = []

    def get_table(self, table_id):
        self.table_id = table_id
        return self.table

    def query(self, query, job_config=None):
        self.queries.append((query, job_config))
        return _Job(self.rows)


def _stage(game, clock, name):
    return next(item for item in game[clock] if item["stage"] == name)


def _captured_row(**overrides):
    row = {
        "game_id": "20260815_DAL@SEA",
        "game_date": "2026-08-15",
        "scheduled_kickoff": "2026-08-16T00:00:00+00:00",
        "game_status": "Final",
        "season": "2026",
        "season_type": "Preseason",
        "away": "DAL",
        "home": "SEA",
        "learning_run_id": "gamelens_2026_preseason_v1",
        "capture_id": "capture_dal_sea",
        "captured_at": "2026-08-15T14:00:00+00:00",
        "metric_source_date": "2026-08-14",
        "metric_pipeline_run_id": "metric_pipeline_20260814",
        "ranking_as_of_date": "2026-08-14",
        "ranking_context_available": True,
        "ranking_context_reason": None,
        "snapshot_receipt_status": "success",
        "snapshot_receipt_reason": None,
        "snapshot_attempt_id": "snapshot_attempt",
        "level1_receipt_status": "success",
        "level1_receipt_reason": None,
        "level1_attempt_id": "level1_attempt",
        "claim_count": 0,
        "level2_count": 0,
        "level3_count": 0,
        "grade_count": 1,
        "grade_version": "packet4_v1",
        "graded_at": "2026-08-16T14:00:00+00:00",
        "grade_status": "success",
        "grade_reason": None,
        "grade_attempt_id": "grade_attempt",
        "level2_status": "no_op",
        "level2_reason": "zero_claims",
        "level3_status": "no_op",
        "level3_reason": "zero_claims",
        "score_row_count": 2,
        "fact_row_count": 110,
        "failed_boundary": None,
        "retryable": None,
        "failure_message": None,
        "log_reference": None,
        "run_alias_disagreement_count": 0,
    }
    row.update(overrides)
    return row


class Packet5AdminInventoryTests(unittest.TestCase):
    def test_inventory_is_bounded_to_the_six_canonical_tables(self):
        self.assertEqual(
            [spec.table_name for spec in inventory.TABLE_SPECS],
            [
                "pregame_snapshots",
                "stage_runs",
                "stage_game_results",
                "claim_training_examples",
                "game_model_outcomes",
                "postgame_learning_stage_receipts",
            ],
        )
        self.assertEqual(
            inventory.TABLE_SPECS[3].logical_key,
            ("learning_run_id", "claim_key"),
        )
        self.assertEqual(
            inventory.TABLE_SPECS[5].logical_key,
            ("attempt_id", "receipt_scope", "game_id", "stage_name"),
        )

    def test_grain_query_is_read_only_and_uses_the_approved_key(self):
        query = inventory.build_grain_query(inventory.TABLE_SPECS[2])
        self.assertIn("COUNT(DISTINCT TO_JSON_STRING(STRUCT", query)
        self.assertIn("`attempt_id` AS `attempt_id`", query)
        self.assertIn("`stage_name` AS `stage_name`", query)
        self.assertIn("`game_id` AS `game_id`", query)
        inventory.assert_read_only_sql(query)

    def test_attempt_scope_receipts_allow_intentionally_null_game_and_stage(self):
        query = inventory.build_grain_query(inventory.TABLE_SPECS[5])
        self.assertIn("receipt_scope = 'attempt'", query)
        self.assertIn("game_id IS NOT NULL", query)
        self.assertIn("receipt_scope = 'game_stage'", query)
        self.assertNotIn(
            "COUNTIF(`attempt_id` IS NULL OR TRIM(CAST(`attempt_id` AS STRING)) = '' OR "
            "`receipt_scope` IS NULL OR TRIM(CAST(`receipt_scope` AS STRING)) = '' OR "
            "`game_id` IS NULL",
            query,
        )

    def test_inspect_table_reports_counts_and_missing_fields(self):
        spec = inventory.TABLE_SPECS[0]
        fields = list(spec.required_fields)
        client = _Client(
            _Table(fields),
            [{
                "row_count": 3,
                "logical_key_count": 3,
                "duplicate_key_count": 0,
                "missing_key_row_count": 0,
            }],
        )
        result = inventory.inspect_table(client=client, spec=spec)
        self.assertEqual(result["status"], "available")
        self.assertEqual(result["duplicate_key_count"], 0)
        self.assertEqual(result["missing_key_row_count"], 0)
        self.assertEqual(len(client.queries), 1)

    def test_schedule_query_uses_explicit_left_joins_and_lineage(self):
        query = inventory.build_game_journey_query("2026")
        self.assertIn("FROM `nfl-stream-406420.League.schedule` AS schedule", query)
        self.assertGreaterEqual(query.count("LEFT JOIN"), 7)
        self.assertIn("snapshots.learning_run_id = claims.learning_run_id", query)
        self.assertIn("snapshots.capture_id = claims.capture_id", query)
        self.assertIn("snapshots.capture_id = grades.capture_id", query)
        self.assertIn("@learning_run_id", query)
        self.assertIn("@season_type", query)
        self.assertIn("@start_date", query)
        self.assertIn("@end_date", query)
        inventory.assert_read_only_sql(query)

    def test_zero_claim_game_is_successful_and_needs_no_level2_or_level3_work(self):
        game = inventory.build_game_journey(
            _captured_row(),
            now=datetime(2026, 8, 19, tzinfo=timezone.utc),
        )
        self.assertEqual(_stage(game, "pregame", "snapshot")["status"], "complete")
        self.assertEqual(_stage(game, "pregame", "level1")["status"], "complete")
        self.assertEqual(_stage(game, "pregame", "level1")["count"], 0)
        self.assertEqual(_stage(game, "postgame", "game_grade")["status"], "complete")
        self.assertEqual(_stage(game, "postgame", "level2")["status"], "no_work_needed")
        self.assertEqual(_stage(game, "postgame", "level3")["status"], "no_work_needed")
        self.assertEqual(_stage(game, "postgame", "level4")["status"], "not_applicable")

    def test_pipeline_lineage_supports_pregame_context_when_date_columns_are_blank(self):
        game = inventory.build_game_journey(
            _captured_row(
                metric_source_date=None,
                ranking_as_of_date=None,
                ranking_context_available=False,
                ranking_context_reason="no_ranking_rows_found",
                level1_receipt_status=None,
                level1_receipt_reason=None,
            ),
            now=datetime(2026, 8, 19, tzinfo=timezone.utc),
        )
        self.assertEqual(_stage(game, "pregame", "prior_facts")["status"], "complete")
        self.assertEqual(_stage(game, "pregame", "windowed")["status"], "complete")
        self.assertEqual(
            _stage(game, "pregame", "rankings")["status"],
            "no_work_needed",
        )
        self.assertEqual(_stage(game, "pregame", "level1")["status"], "warning")
        self.assertEqual(_stage(game, "postgame", "level2")["status"], "no_work_needed")
        self.assertEqual(_stage(game, "postgame", "level3")["status"], "no_work_needed")

    def test_kickoff_skip_is_warning_not_worker_failure(self):
        game = inventory.build_game_journey(
            _captured_row(
                capture_id=None,
                captured_at=None,
                metric_source_date=None,
                metric_pipeline_run_id=None,
                ranking_as_of_date=None,
                ranking_context_available=None,
                snapshot_receipt_status="skipped",
                snapshot_receipt_reason="kickoff_reached",
                level1_receipt_status=None,
                grade_count=0,
                grade_status=None,
            ),
            now=datetime(2026, 8, 19, tzinfo=timezone.utc),
        )
        self.assertEqual(_stage(game, "pregame", "prior_facts")["status"], "not_applicable")
        self.assertEqual(_stage(game, "pregame", "snapshot")["status"], "warning")
        self.assertEqual(_stage(game, "postgame", "game_grade")["status"], "not_applicable")
        self.assertEqual(game["first_issue"]["stage"], "snapshot")
        self.assertEqual(game["first_issue"]["reason"], "kickoff_reached")

    def test_missing_capture_and_failed_grade_are_visible_without_hiding_game(self):
        game = inventory.build_game_journey(
            _captured_row(
                game_id="20260806_CAR@ARI",
                away="CAR",
                home="ARI",
                capture_id=None,
                captured_at=None,
                metric_source_date=None,
                ranking_as_of_date=None,
                ranking_context_available=None,
                snapshot_receipt_status="failure",
                snapshot_receipt_reason="capture_missing",
                level1_receipt_status="skipped",
                level1_receipt_reason="capture_required",
                grade_count=0,
                grade_status="failed",
                grade_reason="canonical_capture_missing",
                failed_boundary="game_grade",
                retryable=False,
                score_row_count=2,
                fact_row_count=110,
            ),
            now=datetime(2026, 8, 19, tzinfo=timezone.utc),
        )
        self.assertEqual(game["game_id"], "20260806_CAR@ARI")
        self.assertEqual(_stage(game, "pregame", "snapshot")["status"], "failed")
        self.assertEqual(_stage(game, "postgame", "game_grade")["status"], "failed")
        self.assertEqual(_stage(game, "postgame", "level2")["status"], "not_applicable")
        self.assertEqual(_stage(game, "postgame", "level3")["status"], "not_applicable")
        self.assertEqual(game["first_issue"]["status"], "failed")
        self.assertEqual(game["first_issue"]["stage"], "snapshot")

    def test_visual_report_has_separate_pregame_and_postgame_clocks(self):
        game = inventory.build_game_journey(
            _captured_row(),
            now=datetime(2026, 8, 19, tzinfo=timezone.utc),
        )
        report = {
            "inventory_summary": {
                "available_count": 6,
                "table_count": 6,
                "duplicate_key_count": 0,
                "missing_key_row_count": 0,
            },
            "game_summary": {
                "scheduled_game_count": 1,
                "captured_game_count": 1,
                "game_with_issue_count": 0,
            },
            "games": [game],
            "tables": [
                {
                    "table": "nfl-stream-406420.GameLens_dev.pregame_snapshots",
                    "row_count": 7,
                    "logical_key_count": 7,
                    "duplicate_key_count": 0,
                    "missing_key_row_count": 0,
                    "status": "available",
                }
            ],
        }
        rendered = inventory.render_visual_report(report)
        self.assertIn("SOURCE GRAINS", rendered)
        self.assertIn("PREGAME CLOCK", rendered)
        self.assertIn("POSTGAME CLOCK", rendered)
        self.assertIn("pregame_snapshots", rendered)
        self.assertIn("20260815_DAL@SEA", rendered)
        self.assertIn("NO WORK", rendered)

    def test_main_requires_explicit_read_only_confirmation_before_cloud_import(self):
        args = [
            "--season-type", "Preseason",
            "--learning-run-id", "gamelens_2026_preseason_v1",
            "--start-date", "2026-08-15",
            "--end-date", "2026-08-15",
        ]
        with self.assertRaisesRegex(ValueError, "--dev-read-only"):
            inventory.main(args)


if __name__ == "__main__":
    unittest.main()
