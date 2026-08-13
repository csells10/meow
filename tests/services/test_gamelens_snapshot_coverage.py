import unittest
from datetime import date, datetime, timezone

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from runtime_config import RuntimeConfig
from services.gamelens_snapshot_coverage import (
    BigQuerySnapshotCoverageAuditor,
    classify_snapshot_coverage,
)


NOW = datetime(2026, 8, 13, 18, 0, tzinfo=timezone.utc)


def config(environment="dev"):
    return RuntimeConfig(
        project_id="nfl-stream-406420",
        environment=environment,
        run_mode="daily",
        active_season="2026",
        league_dataset="League_dev" if environment == "dev" else "League",
        scores_dataset="Scores_dev" if environment == "dev" else "Scores",
        analytics_dataset=(
            "Analytics_dev" if environment == "dev" else "Analytics"
        ),
        raw_response_bucket=(
            "xtra-point-dev" if environment == "dev" else "xtra_point"
        ),
    )


def schedule_row(**overrides):
    row = {
        "game_id": "20260813_DET@CIN",
        "game_date": date(2026, 8, 13),
        "scheduled_kickoff": datetime(
            2026,
            8,
            13,
            23,
            0,
            tzinfo=timezone.utc,
        ),
        "game_status": "Scheduled",
        "season": "2026",
        "season_type": "Preseason",
        "away": "DET",
        "home": "CIN",
        "capture_id": None,
        "learning_run_id": None,
        "captured_at": None,
        "latest_attempt_id": None,
        "latest_attempt_status": None,
        "latest_attempt_reason": None,
    }
    row.update(overrides)
    return row


class QueryResult:
    def __init__(self, rows):
        self.rows = rows

    def result(self):
        return list(self.rows)


class QueryClient:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def query(self, query, job_config=None):
        self.calls.append((query, job_config))
        return QueryResult(self.rows)


class TestSnapshotCoverageClassification(unittest.TestCase):
    def test_canonical_snapshot_is_captured(self):
        result = classify_snapshot_coverage(
            schedule_row(
                capture_id="capture_1",
                learning_run_id="learning_1",
                captured_at="2026-08-13T14:00:00Z",
                scheduled_kickoff="2026-08-13T17:00:00Z",
            ),
            now=NOW,
        )

        self.assertEqual(result["coverage_status"], "captured")
        self.assertIsNone(result["coverage_reason"])
        self.assertEqual(result["capture_id"], "capture_1")

    def test_future_game_without_snapshot_is_upcoming(self):
        result = classify_snapshot_coverage(
            schedule_row(),
            now=NOW,
        )

        self.assertEqual(result["coverage_status"], "upcoming")
        self.assertEqual(
            result["coverage_reason"],
            "capture_window_open",
        )
        self.assertEqual(
            result["latest_attempt_status"],
            "not_attempted",
        )

    def test_august_six_gap_is_not_reconstructed(self):
        result = classify_snapshot_coverage(
            schedule_row(
                game_id="20260806_A@B",
                game_date=date(2026, 8, 6),
                scheduled_kickoff="2026-08-06T23:00:00Z",
            ),
            now=NOW,
        )

        self.assertEqual(result["coverage_status"], "capture_missing")
        self.assertEqual(
            result["coverage_reason"],
            "before_packet_2_capture_program",
        )
        self.assertEqual(
            result["latest_attempt_status"],
            "not_attempted",
        )

    def test_post_start_failed_attempt_stays_capture_missing(self):
        result = classify_snapshot_coverage(
            schedule_row(
                scheduled_kickoff="2026-08-13T17:00:00Z",
                latest_attempt_id="attempt_1",
                latest_attempt_status="failure",
                latest_attempt_reason="save failed",
            ),
            now=NOW,
        )

        self.assertEqual(result["coverage_status"], "capture_missing")
        self.assertEqual(
            result["coverage_reason"],
            "latest_attempt_failed",
        )
        self.assertEqual(result["latest_attempt_id"], "attempt_1")


class TestBigQuerySnapshotCoverageAuditor(unittest.TestCase):
    def test_audit_is_read_only_and_uses_production_schedule(self):
        client = QueryClient([
            schedule_row(
                game_id="20260806_A@B",
                game_date=date(2026, 8, 6),
                scheduled_kickoff="2026-08-06T23:00:00Z",
            ),
            schedule_row(capture_id="capture_1"),
        ])
        auditor = BigQuerySnapshotCoverageAuditor(
            client=client,
            runtime_config=config(),
        )

        result = auditor.audit(
            start_date=date(2026, 8, 6),
            end_date=date(2026, 8, 13),
            season="2026",
            now=NOW,
        )

        self.assertEqual(result["access_mode"], "read_only")
        self.assertEqual(result["games_checked"], 2)
        self.assertEqual(
            result["coverage_counts"],
            {"capture_missing": 1, "captured": 1},
        )
        query = client.calls[0][0]
        self.assertIn(
            "nfl-stream-406420.League.schedule",
            query,
        )
        self.assertIn(
            "nfl-stream-406420.GameLens_dev.pregame_snapshots",
            query,
        )
        self.assertFalse(hasattr(client, "insert_rows_json"))

    def test_auditor_refuses_production_runtime(self):
        with self.assertRaisesRegex(ValueError, "dev-only"):
            BigQuerySnapshotCoverageAuditor(
                client=QueryClient([]),
                runtime_config=config("production"),
            )


if __name__ == "__main__":
    unittest.main()

