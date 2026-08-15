import copy
import json
import unittest
from datetime import date, datetime, timedelta, timezone

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from analytics.metric_registry import METRIC_REGISTRY
from runtime_config import RuntimeConfig
from services.core_area_analysis import ACTIVE_CORE_AREAS
from services.game_service import GameDetailsEvidence
from services.gamelens_level1_service import prepare_level1_claims
from services.gamelens_snapshot_capture import SnapshotCaptureCoordinator


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


def candidate():
    kickoff = NOW + timedelta(hours=4)
    return {
        "game_id": "20260813_AWY@HME",
        "scheduled_kickoff": kickoff,
        "header": {
            "game_id": "20260813_AWY@HME",
            "game_date": kickoff.date().isoformat(),
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
        },
    }


def populated_evidence(item):
    away_metrics = {}
    home_metrics = {}
    away_rankings = {}
    home_rankings = {}

    for metric, metadata in METRIC_REGISTRY.items():
        category = metadata.get("category")
        core_area = metadata.get("core_area")
        if not category or not core_area:
            continue

        direction = metadata.get("comparison_direction")
        away_value, home_value = (
            (0.8, 1.0) if direction == "lower" else (1.0, 0.8)
        )
        metric_key = f"{category}::{metric}"
        metric_base = {
            "metric": metric,
            "category": category,
            "core_area": core_area,
            "data_date": "2026-08-12",
            "window_type": "preseason_to_date",
        }
        away_metrics[metric_key] = {
            **metric_base,
            "value": away_value,
            "team_id": "1",
            "team_abv": "AWY",
        }
        home_metrics[metric_key] = {
            **metric_base,
            "value": home_value,
            "team_id": "2",
            "team_abv": "HME",
        }

        ranking_usage = metadata.get("ranking_usage")
        ranking_base = {
            **metadata,
            "metric": metric,
            "category": category,
            "core_area": core_area,
            "as_of_date": "2026-08-12",
            "source_data_date": "2026-08-12",
            "data_lag_days": 1,
            "window_type": "preseason_to_date",
            "teams_ranked": 32,
            "lens_tags": list(metadata.get("lens_tags") or []),
            "ranking_kind": (
                "context" if ranking_usage == "context_only" else "edge"
            ),
            "rank_direction": (
                "lower_is_better"
                if direction == "lower"
                else "higher_is_better"
            ),
            "rank_interpretation": "representative_test_ranking",
            "rank_tie_method": "competition_min_rank",
        }
        away_rankings[metric] = {
            **ranking_base,
            "value": away_value,
            "team_id": "1",
            "team_abv": "AWY",
            "league_rank": 5,
            "league_percentile": 85.0,
            "tier": 1,
            "tier_label": "Top",
        }
        home_rankings[metric] = {
            **ranking_base,
            "value": home_value,
            "team_id": "2",
            "team_abv": "HME",
            "league_rank": 27,
            "league_percentile": 15.0,
            "tier": 4,
            "tier_label": "Bottom",
        }

    return GameDetailsEvidence(
        header=item["header"],
        away_metrics=away_metrics,
        home_metrics=home_metrics,
        ranking_context={
            "available": True,
            "game_id": item["game_id"],
            "game_date": item["header"]["game_date"],
            "season": "2026",
            "window_type": "preseason_to_date",
            "as_of_date": "2026-08-12",
            "source_data_dates": ["2026-08-12"],
            "max_data_lag_days": 1,
        },
        away_rankings=away_rankings,
        home_rankings=home_rankings,
        final_score=None,
    )


class HandoffLoader:
    def __init__(self, item):
        self.item = item
        self.evidence = populated_evidence(item)

    def fetch_candidates(self, **kwargs):
        return [self.item]

    def load_slate_evidence(self, candidates):
        return {self.item["game_id"]: self.evidence}

    def recheck_candidate(self, game_id):
        return self.item


class JsonRoundTripStorage:
    """Emulate BigQuery JSON serialization for the local handoff contract."""

    def __init__(self):
        self.rows = {}
        self.receipts = []
        self.game_results = []

    def find_capture(self, capture_id):
        return copy.deepcopy(self.rows.get(capture_id))

    def save_snapshot(self, row):
        self.rows[row["capture_id"]] = json.loads(
            json.dumps(dict(row), default=str)
        )

    def read_snapshot(self, capture_id):
        return copy.deepcopy(self.rows.get(capture_id))

    def write_stage_receipt(self, receipt):
        self.receipts.append(dict(receipt))

    def write_stage_game_results(self, rows):
        copied = [copy.deepcopy(dict(row)) for row in rows]
        self.game_results.extend(copied)
        return {
            "input_count": len(copied),
            "inserted_count": len(copied),
            "existing_count": 0,
        }


class TestPopulatedSnapshotHandoff(unittest.TestCase):
    def test_saved_response_preserves_learning_and_level1_handoff(self):
        item = candidate()
        storage = JsonRoundTripStorage()
        coordinator = SnapshotCaptureCoordinator(
            runtime_config=runtime_config(),
            evidence_loader=HandoffLoader(item),
            storage=storage,
            upstream_readiness=lambda games: {
                "ready": True,
                "status": "ready",
                "upstream_run_id": "metric_20260813",
            },
            now=lambda: NOW,
        )

        result = coordinator.run(
            start_date=date(2026, 8, 13),
            end_date=date(2026, 8, 15),
            ruleset_version="v1",
            model_version="game_service_v1",
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["games_captured"], 1)
        self.assertEqual(len(storage.game_results), 1)
        self.assertEqual(
            storage.game_results[0]["game_id"],
            item["game_id"],
        )
        self.assertEqual(
            storage.game_results[0]["status"],
            "success",
        )
        saved = next(iter(storage.rows.values()))
        self.assertEqual(
            saved["learning_run_id"],
            "gamelens_2026_preseason_v1",
        )

        payload = saved["response_payload"]
        self.assertIsNone(payload["final_score"])
        self.assertIsNone(payload["model_outcome"])
        self.assertTrue(payload["core_area_comparison"])
        self.assertTrue(payload["matchup_breakdown"]["core_area_summaries"])
        self.assertTrue(payload["matchup_breakdown"]["category_summaries"])
        self.assertTrue(payload["matchup_breakdown"]["metric_highlights"])

        self.assertTrue(saved["lens_tags"])
        for rankings in (
            saved["evidence_context"]["away_rankings"],
            saved["evidence_context"]["home_rankings"],
        ):
            self.assertTrue(rankings)
            self.assertTrue(
                all(
                    isinstance(row.get("lens_tags"), list)
                    for row in rankings.values()
                )
            )

        level1 = prepare_level1_claims(
            saved,
            headline_top_metrics=8,
            extracted_at=NOW,
        )
        claims = level1["rows"]
        claim_types = {row["claim_type"] for row in claims}
        self.assertEqual(
            claim_types,
            {
                "game_profile",
                "core_area_comparison",
                "core_area_summary",
                "category_summary",
                "metric_highlight",
                "team_comparison_metric",
            },
        )
        self.assertEqual(level1["claim_count"], len(claims))
        self.assertEqual(level1["unique_claim_key_count"], len(claims))
        self.assertTrue(
            all(row["capture_id"] == saved["capture_id"] for row in claims)
        )
        self.assertTrue(
            all(
                row["learning_run_id"] == saved["learning_run_id"]
                for row in claims
            )
        )
        self.assertTrue(
            all(
                row["source_payload_sha256"] == saved["payload_sha256"]
                for row in claims
            )
        )
        self.assertNotIn("lens_tag", claim_types)
        self.assertGreaterEqual(len(claims), 20)
        self.assertLessEqual(len(claims), 45)
        self.assertTrue(
            ACTIVE_CORE_AREAS.issubset(
                {row["core_area"] for row in claims if row.get("core_area")}
            )
        )
        self.assertGreaterEqual(
            len({row["category"] for row in claims if row.get("category")}),
            8,
        )


if __name__ == "__main__":
    unittest.main()
