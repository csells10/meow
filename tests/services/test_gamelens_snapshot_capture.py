import copy
import unittest
from datetime import date, datetime, timedelta, timezone

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from runtime_config import RuntimeConfig
from services.game_service import GameDetailsEvidence
from services.gamelens_learning_contract import (
    build_capture_id,
    build_learning_run_id,
    payload_sha256,
)
from services.gamelens_snapshot_capture import (
    SnapshotCaptureCoordinator,
    collect_lens_tags,
    readiness_from_metric_pipeline_summary,
    semantic_json_diff,
)
from services.gamelens_snapshot_storage import BigQueryBufferPending


NOW = datetime(2026, 8, 13, 16, 0, tzinfo=timezone.utc)


def runtime_config():
    return RuntimeConfig(
        project_id="nfl-stream-406420",
        environment="dev",
        run_mode="daily",
        active_season="2026",
        league_dataset="League_dev",
        scores_dataset="Scores_dev",
        analytics_dataset="Analytics_dev",
        raw_response_bucket="xtra-point-dev",
    )


def candidate(game_id, away_id, home_id, kickoff_hours=4):
    kickoff = NOW + timedelta(hours=kickoff_hours)
    away = f"A{away_id}"
    home = f"H{home_id}"
    return {
        "game_id": game_id,
        "scheduled_kickoff": kickoff,
        "header": {
            "game_id": game_id,
            "game_date": kickoff.date().isoformat(),
            "game_time": "8:00p",
            "game_status": "Scheduled",
            "season": "2026",
            "game_week": "Preseason Week 2",
            "season_type": "Preseason",
            "away_team": {
                "id": away_id,
                "name": away,
                "abbreviation": away,
                "logo": None,
            },
            "home_team": {
                "id": home_id,
                "name": home,
                "abbreviation": home,
                "logo": None,
            },
            "espn_link": None,
        },
    }


def evidence_for(item, tags=None):
    ranking = {
        "metric_one": {
            "metric": "metric_one",
            "lens_tags": list(tags or ["efficiency", "strong-signal"]),
        }
    }
    return GameDetailsEvidence(
        header=item["header"],
        away_metrics={},
        home_metrics={},
        ranking_context={
            "available": True,
            "as_of_date": "2026-08-12",
        },
        away_rankings=ranking,
        home_rankings=copy.deepcopy(ranking),
        final_score=None,
    )


def payload_for(item):
    return {
        "header": item["header"],
        "final_score": None,
        "model_outcome": None,
        "ranking_context": {"available": True},
    }


class FakeLoader:
    def __init__(self, candidates, events=None):
        self.candidates = list(candidates)
        self.events = events if events is not None else []
        self.load_calls = 0

    def fetch_candidates(self, **kwargs):
        self.events.append("fetch_candidates")
        return self.candidates

    def load_slate_evidence(self, candidates):
        self.load_calls += 1
        self.events.append("load_slate")
        return {
            item["game_id"]: evidence_for(item)
            for item in candidates
        }

    def recheck_candidate(self, game_id):
        self.events.append(f"recheck:{game_id}")
        return next(item for item in self.candidates if item["game_id"] == game_id)


class FakeStorage:
    def __init__(self, events=None):
        self.rows = {}
        self.receipts = []
        self.events = events if events is not None else []
        self.buffer_on_save = False
        self.mutate_readback = False

    def find_capture(self, capture_id):
        self.events.append(f"find:{capture_id}")
        return self.rows.get(capture_id)

    def save_snapshot(self, row):
        self.events.append(f"save:{row['game_id']}")
        if self.buffer_on_save:
            raise BigQueryBufferPending("rows are in the streaming buffer")
        self.rows[row["capture_id"]] = copy.deepcopy(dict(row))

    def read_snapshot(self, capture_id):
        row = self.rows.get(capture_id)
        self.events.append(f"read:{row['game_id'] if row else capture_id}")
        result = copy.deepcopy(row)
        if result and self.mutate_readback:
            result["response_payload"]["ranking_context"] = {"available": False}
        return result

    def write_stage_receipt(self, receipt):
        self.receipts.append(dict(receipt))


class TestSnapshotCaptureCoordinator(unittest.TestCase):
    def _coordinator(self, loader, storage, builder, readiness=None):
        return SnapshotCaptureCoordinator(
            runtime_config=runtime_config(),
            evidence_loader=loader,
            storage=storage,
            upstream_readiness=lambda games: readiness or {
                "ready": True,
                "status": "ready",
                "upstream_run_id": "metric_20260813",
            },
            response_builder=builder,
            now=lambda: NOW,
        )

    @staticmethod
    def _run(coordinator):
        return coordinator.run(
            start_date=date(2026, 8, 13),
            end_date=date(2026, 8, 15),
            ruleset_version="v1",
            model_version="game_service_v1",
        )

    def test_identical_retry_skips_before_batch_load_or_rebuild(self):
        item = candidate("20260813_A1@H2", "1", "2")
        loader = FakeLoader([item])
        storage = FakeStorage()
        learning_run_id = build_learning_run_id(
            season="2026",
            season_type="Preseason",
            ruleset_version="v1",
        )
        capture_id = build_capture_id(
            learning_run_id=learning_run_id,
            game_id=item["game_id"],
            scheduled_kickoff=item["scheduled_kickoff"],
        )
        existing_payload = payload_for(item)
        storage.rows[capture_id] = {
            "capture_id": capture_id,
            "learning_run_id": learning_run_id,
            "response_payload": existing_payload,
            "payload_sha256": payload_sha256(existing_payload),
            "metric_pipeline_run_id": "metric_20260813",
        }
        builds = []
        result = self._coordinator(
            loader,
            storage,
            lambda *args, **kwargs: builds.append(args) or {},
        ).run(
            start_date=date(2026, 8, 13),
            end_date=date(2026, 8, 15),
            ruleset_version="v1",
            model_version="game_service_v1",
            game_id=item["game_id"],
        )
        self.assertEqual(result["status"], "no_op")
        self.assertEqual(result["reason"], "canonical_capture_exists")
        self.assertEqual(result["games_skipped"], 1)
        self.assertEqual(loader.load_calls, 0)
        self.assertEqual(builds, [])
        self.assertEqual(len(storage.rows), 1)
        self.assertEqual(storage.receipts[0]["game_id"], item["game_id"])
        self.assertEqual(storage.receipts[0]["season_type"], "Preseason")
        self.assertEqual(
            storage.receipts[0]["upstream_run_id"],
            "metric_20260813",
        )
        self.assertEqual(
            storage.receipts[0]["reason"],
            "canonical_capture_exists",
        )

    def test_corrupt_existing_capture_fails_without_rebuilding(self):
        item = candidate("20260813_A1@H2", "1", "2")
        loader = FakeLoader([item])
        storage = FakeStorage()
        learning_run_id = build_learning_run_id(
            season="2026",
            season_type="Preseason",
            ruleset_version="v1",
        )
        capture_id = build_capture_id(
            learning_run_id=learning_run_id,
            game_id=item["game_id"],
            scheduled_kickoff=item["scheduled_kickoff"],
        )
        storage.rows[capture_id] = {
            "capture_id": capture_id,
            "learning_run_id": learning_run_id,
            "response_payload": payload_for(item),
            "payload_sha256": "not-the-payload-hash",
        }
        builds = []

        result = self._run(
            self._coordinator(
                loader,
                storage,
                lambda *args, **kwargs: builds.append(args) or {},
            )
        )

        self.assertEqual(result["status"], "failure")
        self.assertIn("payload_hash_mismatch", result["game_results"][0]["reason"])
        self.assertEqual(loader.load_calls, 0)
        self.assertEqual(builds, [])

    def test_existing_capture_without_learning_lineage_fails_without_rebuilding(self):
        item = candidate("20260813_A1@H2", "1", "2")
        loader = FakeLoader([item])
        storage = FakeStorage()
        learning_run_id = build_learning_run_id(
            season="2026",
            season_type="Preseason",
            ruleset_version="v1",
        )
        capture_id = build_capture_id(
            learning_run_id=learning_run_id,
            game_id=item["game_id"],
            scheduled_kickoff=item["scheduled_kickoff"],
        )
        existing_payload = payload_for(item)
        storage.rows[capture_id] = {
            "capture_id": capture_id,
            "response_payload": existing_payload,
            "payload_sha256": payload_sha256(existing_payload),
        }
        builds = []

        result = self._run(
            self._coordinator(
                loader,
                storage,
                lambda *args, **kwargs: builds.append(args) or {},
            )
        )

        self.assertEqual(result["status"], "failure")
        self.assertIn(
            "learning_run_identity_mismatch",
            result["game_results"][0]["reason"],
        )
        self.assertEqual(loader.load_calls, 0)
        self.assertEqual(builds, [])

    def test_no_games_is_successful_no_op_with_readable_receipt(self):
        loader = FakeLoader([])
        storage = FakeStorage()
        result = self._run(
            self._coordinator(
                loader,
                storage,
                lambda *args, **kwargs: self.fail("builder should not run"),
            )
        )
        self.assertEqual(result["status"], "no_op")
        self.assertEqual(result["reason"], "no_games_scheduled")
        self.assertEqual(result["games_checked"], 0)
        self.assertEqual(storage.receipts[0]["status"], "no_op")

    def test_one_game_proof_restricts_capture_before_evidence_load(self):
        items = [
            candidate("20260813_A1@H2", "1", "2"),
            candidate("20260813_A3@H4", "3", "4"),
        ]
        loader = FakeLoader(items)
        loader.evidence_lineage = lambda: {
            "source_environment": "production",
            "schedule_dataset": "League",
            "analytics_dataset": "Analytics",
            "access_mode": "read_only",
        }
        storage = FakeStorage()
        builds = []
        coordinator = self._coordinator(
            loader,
            storage,
            lambda game_id, *, evidence: (
                builds.append(game_id)
                or payload_for(next(i for i in items if i["game_id"] == game_id))
            ),
        )

        result = coordinator.run(
            start_date=date(2026, 8, 13),
            end_date=date(2026, 8, 15),
            ruleset_version="v1",
            model_version="game_service_v1",
            game_id=items[1]["game_id"],
        )

        self.assertEqual(result["games_discovered"], 2)
        self.assertEqual(result["games_checked"], 1)
        self.assertEqual(result["games_captured"], 1)
        self.assertEqual(builds, [items[1]["game_id"]])
        self.assertEqual(len(storage.rows), 1)
        self.assertEqual(storage.receipts[0]["game_id"], items[1]["game_id"])
        self.assertEqual(storage.receipts[0]["season_type"], "Preseason")
        saved_row = next(iter(storage.rows.values()))
        self.assertEqual(
            saved_row["evidence_context"]["source_lineage"],
            loader.evidence_lineage(),
        )

    def test_one_game_proof_missing_game_is_safe_no_op(self):
        loader = FakeLoader([candidate("20260813_A1@H2", "1", "2")])
        storage = FakeStorage()
        coordinator = self._coordinator(
            loader,
            storage,
            lambda *args, **kwargs: self.fail("builder should not run"),
        )

        result = coordinator.run(
            start_date=date(2026, 8, 13),
            end_date=date(2026, 8, 15),
            ruleset_version="v1",
            model_version="game_service_v1",
            game_id="20260813_MISSING",
        )

        self.assertEqual(result["status"], "no_op")
        self.assertEqual(result["reason"], "requested_game_not_found")
        self.assertEqual(result["games_checked"], 0)
        self.assertEqual(loader.load_calls, 0)
        self.assertEqual(storage.rows, {})
        self.assertEqual(storage.receipts[0]["game_id"], "20260813_MISSING")
        self.assertIsNone(storage.receipts[0]["season_type"])

    def test_slate_builds_saves_reads_and_releases_sequentially(self):
        events = []
        items = [
            candidate("20260813_A1@H2", "1", "2"),
            candidate("20260813_A3@H4", "3", "4"),
        ]
        loader = FakeLoader(items, events)
        storage = FakeStorage(events)

        def builder(game_id, *, evidence):
            events.append(f"build:{game_id}")
            return payload_for(next(i for i in items if i["game_id"] == game_id))

        result = self._run(self._coordinator(loader, storage, builder))
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["games_captured"], 2)
        self.assertEqual(result["internal_http_game_calls"], 0)
        lifecycle = [
            event for event in events
            if event.startswith(("build:", "save:", "read:"))
        ]
        self.assertEqual(
            lifecycle,
            [
                f"build:{items[0]['game_id']}",
                f"save:{items[0]['game_id']}",
                f"read:{items[0]['game_id']}",
                f"build:{items[1]['game_id']}",
                f"save:{items[1]['game_id']}",
                f"read:{items[1]['game_id']}",
            ],
        )
        self.assertEqual(loader.load_calls, 1)
        self.assertEqual(storage.receipts[0]["status"], "success")
        self.assertIsNone(storage.receipts[0]["game_id"])
        self.assertEqual(storage.receipts[0]["season_type"], "Preseason")
        for row in storage.rows.values():
            self.assertEqual(
                row["learning_run_id"],
                "gamelens_2026_preseason_v1",
            )
            self.assertEqual(
                row["lens_tags"],
                ["efficiency", "strong-signal"],
            )
            self.assertNotIn("away_rankings", row["response_payload"])
            self.assertIn("away_rankings", row["evidence_context"])

    def test_mixed_season_slate_does_not_invent_receipt_season_type(self):
        items = [
            candidate("20260813_A1@H2", "1", "2"),
            candidate("20260813_A3@H4", "3", "4"),
        ]
        items[1]["header"]["season_type"] = "Regular Season"
        items[1]["header"]["game_week"] = "Week 1"
        loader = FakeLoader(items)
        storage = FakeStorage()
        coordinator = self._coordinator(
            loader,
            storage,
            lambda game_id, *, evidence: payload_for(
                next(item for item in items if item["game_id"] == game_id)
            ),
        )

        result = self._run(coordinator)

        self.assertEqual(result["status"], "success")
        self.assertIsNone(storage.receipts[0]["game_id"])
        self.assertIsNone(storage.receipts[0]["season_type"])

    def test_one_game_failure_does_not_discard_the_other(self):
        items = [
            candidate("20260813_A1@H2", "1", "2"),
            candidate("20260813_A3@H4", "3", "4"),
        ]
        loader = FakeLoader(items)
        storage = FakeStorage()

        def builder(game_id, *, evidence):
            if game_id == items[0]["game_id"]:
                raise ValueError("bad evidence")
            return payload_for(items[1])

        result = self._run(self._coordinator(loader, storage, builder))
        self.assertEqual(result["status"], "partial_failure")
        self.assertEqual(result["games_failed"], 1)
        self.assertEqual(result["games_captured"], 1)
        self.assertEqual(len(storage.rows), 1)

    def test_streaming_buffer_is_waiting_and_does_not_rerun_upstream(self):
        item = candidate("20260813_A1@H2", "1", "2")
        loader = FakeLoader([item])
        storage = FakeStorage()
        storage.buffer_on_save = True
        readiness_calls = []
        coordinator = SnapshotCaptureCoordinator(
            runtime_config=runtime_config(),
            evidence_loader=loader,
            storage=storage,
            upstream_readiness=lambda games: readiness_calls.append(games) or {
                "ready": True,
                "status": "ready",
                "upstream_run_id": "metric_20260813",
            },
            response_builder=lambda game_id, *, evidence: payload_for(item),
            now=lambda: NOW,
        )
        result = self._run(coordinator)
        self.assertEqual(result["status"], "waiting")
        self.assertEqual(result["games_waiting"], 1)
        self.assertEqual(len(readiness_calls), 1)
        self.assertEqual(len(storage.rows), 0)

    def test_saved_payload_mismatch_reports_field_level_path_and_blocks_game(self):
        item = candidate("20260813_A1@H2", "1", "2")
        loader = FakeLoader([item])
        storage = FakeStorage()
        storage.mutate_readback = True
        result = self._run(
            self._coordinator(
                loader,
                storage,
                lambda game_id, *, evidence: payload_for(item),
            )
        )
        self.assertEqual(result["status"], "failure")
        self.assertEqual(result["games_failed"], 1)
        self.assertIn(
            "$.ranking_context.available",
            result["game_results"][0]["reason"],
        )

    def test_changed_kickoff_is_rejected_immediately_before_save(self):
        item = candidate("20260813_A1@H2", "1", "2")
        loader = FakeLoader([item])
        changed = copy.deepcopy(item)
        changed["scheduled_kickoff"] += timedelta(hours=1)
        loader.recheck_candidate = lambda game_id: changed
        storage = FakeStorage()
        result = self._run(
            self._coordinator(
                loader,
                storage,
                lambda game_id, *, evidence: payload_for(item),
            )
        )
        self.assertEqual(result["status"], "failure")
        self.assertEqual(
            result["game_results"][0]["reason"],
            "schedule_identity_changed_before_save",
        )
        self.assertEqual(storage.rows, {})

    def test_failed_upstream_blocks_capture_before_evidence_load(self):
        item = candidate("20260813_A1@H2", "1", "2")
        loader = FakeLoader([item])
        result = self._run(
            self._coordinator(
                loader,
                FakeStorage(),
                lambda *args, **kwargs: self.fail("builder should not run"),
                readiness={
                    "ready": False,
                    "status": "blocked",
                    "reason": "metric_pipeline_partial_failure",
                },
            )
        )
        self.assertEqual(result["status"], "failure")
        self.assertEqual(result["reason"], "metric_pipeline_partial_failure")
        self.assertEqual(loader.load_calls, 0)

    def test_unknown_season_type_skips_without_loading_evidence(self):
        item = candidate("20260813_A1@H2", "1", "2")
        item["header"]["season_type"] = "Kickoff Classic"
        loader = FakeLoader([item])
        result = self._run(
            self._coordinator(
                loader,
                FakeStorage(),
                lambda *args, **kwargs: self.fail("builder should not run"),
            )
        )
        self.assertEqual(result["status"], "no_op")
        self.assertEqual(
            result["game_results"][0]["reason"],
            "season_type_unknown",
        )
        self.assertEqual(loader.load_calls, 0)


class TestSnapshotHelpers(unittest.TestCase):
    def test_readiness_requires_successful_complete_metric_pipeline_summary(self):
        completed = {
            stage: {"status": "completed", "row_count": 1}
            for stage in ("facts", "windowed_metrics", "rankings")
        }
        ready = readiness_from_metric_pipeline_summary(
            {"status": "success", "stages": completed},
            upstream_run_id="metric_20260813",
        )
        self.assertTrue(ready["ready"])

        failed = readiness_from_metric_pipeline_summary(
            {"status": "failed", "stages": completed},
            upstream_run_id="metric_20260813",
        )
        self.assertFalse(failed["ready"])
        self.assertEqual(failed["status"], "blocked")

        incomplete = readiness_from_metric_pipeline_summary(
            {
                "status": "success",
                "stages": {
                    **completed,
                    "rankings": {"status": "skipped", "row_count": None},
                },
            },
            upstream_run_id="metric_20260813",
        )
        self.assertFalse(incomplete["ready"])
        self.assertIn("rankings", incomplete["reason"])

    def test_semantic_diff_ignores_key_order_but_not_product_values(self):
        self.assertEqual(
            semantic_json_diff({"b": [1, 2], "a": None}, {"a": None, "b": [1, 2]}),
            [],
        )
        diff = semantic_json_diff(
            {"matchup": {"confidence": 0.8}},
            {"matchup": {"confidence": 0.81}},
        )
        self.assertEqual(diff[0]["path"], "$.matchup.confidence")
        self.assertEqual(diff[0]["reason"], "value_mismatch")

    def test_lens_tags_must_remain_arrays_of_nonblank_strings(self):
        item = candidate("20260813_A1@H2", "1", "2")
        self.assertEqual(
            collect_lens_tags(evidence_for(item)),
            ["efficiency", "strong-signal"],
        )
        malformed = evidence_for(item)
        malformed.away_rankings["metric_one"]["lens_tags"] = "efficiency"
        with self.assertRaisesRegex(ValueError, "malformed_lens_tags"):
            collect_lens_tags(malformed)


if __name__ == "__main__":
    unittest.main()
