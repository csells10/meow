import unittest
from datetime import date, datetime, timezone

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from queries.gamelens_snapshot_queries import BigQuerySingleGameEvidenceLoader
from runtime_config import RuntimeConfig


KICKOFF = datetime(2026, 8, 28, 0, 0, tzinfo=timezone.utc)


def runtime_config(environment="dev"):
    return RuntimeConfig(
        project_id="nfl-stream-406420",
        environment=environment,
        run_mode="controlled_replay" if environment == "dev" else "daily",
        active_season="2026",
        league_dataset="League_dev" if environment == "dev" else "League",
        scores_dataset="Scores_dev" if environment == "dev" else "Scores",
        analytics_dataset=(
            "Analytics_dev" if environment == "dev" else "Analytics"
        ),
        raw_response_bucket=(
            "xtra-point-dev" if environment == "dev" else "xtra_point"
        ),
        replay_date="2026-08-27" if environment == "dev" else None,
    )


class Job:
    def __init__(self, rows):
        self.rows = rows

    def result(self):
        return self.rows


class Client:
    def __init__(self, *, rankings=True, kickoff=KICKOFF):
        self.queries = []
        self.rankings = rankings
        self.kickoff = kickoff

    def query(self, query, job_config):
        self.queries.append(query)
        if "FROM `nfl-stream-406420.League" in query:
            return Job([{
                "gameID": "20260827_PIT@BUF",
                "gameDate": date(2026, 8, 27),
                "gameTime": "8:00p",
                "scheduled_kickoff": self.kickoff,
                "gameStatus": "Scheduled",
                "season": "2026",
                "gameWeek": "Preseason Week 4",
                "seasonType": "Preseason",
                "teamIDAway": "23",
                "away": "PIT",
                "away_logo": None,
                "teamIDHome": "2",
                "home": "BUF",
                "home_logo": None,
                "espnLink": None,
            }])
        if "team_metrics_windowed_2026" in query:
            return Job([
                {
                    "team_id": "23",
                    "team_abv": "PIT",
                    "metric": "points_per_play",
                    "category": "Offense",
                    "core_area": "Scoring Efficiency",
                    "value": 0.2,
                    "data_date": date(2026, 8, 21),
                },
                {
                    "team_id": "2",
                    "team_abv": "BUF",
                    "metric": "points_per_play",
                    "category": "Offense",
                    "core_area": "Scoring Efficiency",
                    "value": 0.3,
                    "data_date": date(2026, 8, 22),
                },
            ])
        if "team_metric_rankings_2026" in query:
            if not self.rankings:
                return Job([])
            return Job([
                {
                    "season": "2026",
                    "as_of_date": date(2026, 8, 23),
                    "source_data_date": date(2026, 8, 22),
                    "data_lag_days": 1,
                    "window_type": "preseason_to_date",
                    "team_id": "23",
                    "team_abv": "PIT",
                    "metric": "points_per_play",
                    "value": 0.2,
                    "label": "Points per play",
                    "lens_tags": ["scoring-efficiency"],
                }
            ])
        raise AssertionError(query)


class TestSingleGameEvidenceLoader(unittest.TestCase):
    def test_production_evidence_is_read_only_and_product_shaped(self):
        client = Client()
        loader = BigQuerySingleGameEvidenceLoader(
            client=client,
            runtime_config=runtime_config(),
            evidence_source="production",
        )

        loaded = loader.load("20260827_PIT@BUF")

        self.assertEqual(loaded.scheduled_kickoff, KICKOFF)
        self.assertEqual(loaded.metric_source_date, date(2026, 8, 22))
        self.assertEqual(loaded.ranking_as_of_date, "2026-08-23")
        self.assertIsNone(loaded.evidence.final_score)
        self.assertTrue(loaded.evidence.ranking_context["available"])
        self.assertEqual(
            loaded.evidence.away_rankings["points_per_play"]["lens_tags"],
            ["scoring-efficiency"],
        )
        self.assertEqual(
            loaded.source_lineage["schedule_table"],
            "nfl-stream-406420.League.schedule",
        )
        self.assertEqual(
            loaded.source_lineage["metric_table"],
            "nfl-stream-406420.Analytics.team_metrics_windowed_2026",
        )
        self.assertEqual(loaded.source_lineage["access_mode"], "read_only")
        self.assertTrue(all("final_score" not in query for query in client.queries))
        self.assertTrue(all("INSERT" not in query for query in client.queries))

    def test_configured_source_uses_only_approved_dev_datasets(self):
        client = Client(rankings=False)
        loader = BigQuerySingleGameEvidenceLoader(
            client=client,
            runtime_config=runtime_config(),
            evidence_source="configured",
        )

        loaded = loader.load("20260827_PIT@BUF")

        self.assertFalse(loaded.evidence.ranking_context["available"])
        self.assertEqual(
            loaded.evidence.ranking_context["reason"],
            "no_ranking_rows_found",
        )
        joined = "\n".join(client.queries)
        self.assertIn("nfl-stream-406420.League_dev.schedule", joined)
        self.assertIn(
            "nfl-stream-406420.Analytics_dev.team_metrics_windowed_2026",
            joined,
        )

    def test_loader_refuses_production_runtime(self):
        with self.assertRaisesRegex(ValueError, "dev-only"):
            BigQuerySingleGameEvidenceLoader(
                client=Client(),
                runtime_config=runtime_config("production"),
                evidence_source="production",
            )

    def test_missing_kickoff_fails_closed(self):
        loader = BigQuerySingleGameEvidenceLoader(
            client=Client(kickoff=None),
            runtime_config=runtime_config(),
            evidence_source="production",
        )

        with self.assertRaisesRegex(ValueError, "scheduled_kickoff_missing"):
            loader.load("20260827_PIT@BUF")


if __name__ == "__main__":
    unittest.main()
