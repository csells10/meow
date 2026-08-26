import copy
import unittest
from datetime import date, datetime, timezone

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from queries.gamelens_snapshot_queries import SnapshotGameEvidence
from runtime_config import RuntimeConfig
from services.game_service import GameDetailsEvidence
from services.gamelens_pregame_contract import payload_sha256
from services.gamelens_snapshot_capture import (
    SnapshotCaptureService,
    collect_lens_tags,
)


NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
KICKOFF = datetime(2026, 8, 28, 0, 0, tzinfo=timezone.utc)
GAME_ID = "20260827_PIT@BUF"


def runtime_config():
    return RuntimeConfig(
        project_id="nfl-stream-406420",
        environment="dev",
        run_mode="controlled_replay",
        active_season="2026",
        league_dataset="League_dev",
        scores_dataset="Scores_dev",
        analytics_dataset="Analytics_dev",
        raw_response_bucket="xtra-point-dev",
        replay_date="2026-08-27",
    )


def evidence():
    return GameDetailsEvidence(
        header={
            "game_id": GAME_ID,
            "game_date": "2026-08-27",
            "game_time": "8:00p",
            "game_status": "Scheduled",
            "season": "2026",
            "game_week": "Preseason Week 4",
            "season_type": "Preseason",
            "away_team": {
                "id": "23",
                "name": "PIT",
                "abbreviation": "PIT",
                "logo": None,
            },
            "home_team": {
                "id": "2",
                "name": "BUF",
                "abbreviation": "BUF",
                "logo": None,
            },
            "espn_link": None,
        },
        away_metrics={},
        home_metrics={},
        ranking_context={
            "available": False,
            "reason": "no_ranking_rows_found",
        },
        away_rankings={},
        home_rankings={},
        final_score=None,
    )


class Loader:
    def __init__(self):
        self.loaded = SnapshotGameEvidence(
            game_id=GAME_ID,
            scheduled_kickoff=KICKOFF,
            evidence=evidence(),
            source_lineage={"access_mode": "read_only"},
            metric_source_date=date(2026, 8, 23),
            ranking_as_of_date=date(2026, 8, 23),
        )
        self.refreshed_kickoff = KICKOFF

    def load(self, game_id):
        self.requested_game_id = game_id
        return self.loaded

    def recheck_schedule(self, game_id):
        return {
            "game_id": game_id,
            "scheduled_kickoff": self.refreshed_kickoff,
            "header": self.loaded.evidence.header,
        }


class Storage:
    table_id = "nfl-stream-406420.GameLens_dev.pregame_snapshots"

    def __init__(self):
        self.rows = {}
        self.reconcile_calls = 0
        self.verify_calls = 0

    def verify_table_contract(self):
        self.verify_calls += 1
        return {"status": "verified"}

    def reconcile_snapshot(self, row):
        self.reconcile_calls += 1
        status = "identical_no_op" if row["capture_id"] in self.rows else "inserted"
        if status == "inserted":
            self.rows[row["capture_id"]] = copy.deepcopy(row)
        return {
            "status": status,
            "material_change": status == "inserted",
        }

    def read_snapshot(self, capture_id):
        return copy.deepcopy(self.rows.get(capture_id))


def contract_builder(
    game_id,
    *,
    learning_run_id,
    captured_at,
    scheduled_kickoff,
    evidence,
):
    payload = {
        "header": evidence.header,
        "final_score": None,
        "model_outcome": None,
        "ranking_context": evidence.ranking_context,
    }
    return {
        "capture_id": "capture_controlled_ll3",
        "payload_sha256": payload_sha256(payload),
        "payload": payload,
    }


class TestSnapshotCaptureService(unittest.TestCase):
    def setUp(self):
        self.loader = Loader()
        self.storage = Storage()
        self.service = SnapshotCaptureService(
            runtime_config=runtime_config(),
            evidence_loader=self.loader,
            storage=self.storage,
            contract_builder=contract_builder,
            now=lambda: NOW,
        )

    def capture(self, *, write=False):
        return self.service.capture_one(
            game_id=GAME_ID,
            learning_run_id="gamelens_2026_preseason_ll3_v1",
            model_version="game_service_v1",
            ruleset_version="ll3_v1",
            metric_pipeline_run_id="observed_manual_read_20260826",
            write=write,
        )

    def test_dry_run_verifies_table_without_reconciliation(self):
        result = self.capture()

        self.assertEqual(result["status"], "dry_run")
        self.assertFalse(result["material_change"])
        self.assertEqual(self.storage.verify_calls, 1)
        self.assertEqual(self.storage.reconcile_calls, 0)
        self.assertFalse(result["ranking_context_available"])
        self.assertEqual(
            result["ranking_context_reason"],
            "no_ranking_rows_found",
        )

    def test_write_inserts_and_reads_exactly_one_snapshot(self):
        result = self.capture(write=True)

        self.assertEqual(result["status"], "inserted")
        self.assertEqual(result["readback"], "verified")
        self.assertEqual(len(self.storage.rows), 1)
        saved = next(iter(self.storage.rows.values()))
        self.assertEqual(saved["capture_status"], "captured")
        self.assertEqual(saved["environment"], "dev")
        self.assertEqual(saved["model_version"], "game_service_v1")
        self.assertEqual(saved["ruleset_version"], "ll3_v1")
        self.assertEqual(
            saved["evidence_context"]["source_lineage"]["access_mode"],
            "read_only",
        )

    def test_identical_service_retry_is_no_op(self):
        self.capture(write=True)
        result = self.capture(write=True)

        self.assertEqual(result["status"], "identical_no_op")
        self.assertFalse(result["material_change"])
        self.assertEqual(len(self.storage.rows), 1)

    def test_changed_schedule_fails_before_storage(self):
        self.loader.refreshed_kickoff = datetime(
            2026, 8, 28, 1, 0, tzinfo=timezone.utc
        )

        with self.assertRaisesRegex(
            ValueError,
            "schedule_identity_changed_before_save",
        ):
            self.capture(write=True)

        self.assertEqual(self.storage.reconcile_calls, 0)

    def test_malformed_lens_tags_fail_closed(self):
        broken = evidence()
        broken.away_rankings["metric"] = {"lens_tags": "bad"}

        with self.assertRaisesRegex(ValueError, "malformed_lens_tags"):
            collect_lens_tags(broken)


if __name__ == "__main__":
    unittest.main()
