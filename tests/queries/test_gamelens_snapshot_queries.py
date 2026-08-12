import unittest
from datetime import date, datetime, timezone

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from queries.gamelens_snapshot_queries import BigQuerySlateEvidenceLoader
from runtime_config import RuntimeConfig


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


def candidate(game_id, away_id, home_id):
    return {
        "game_id": game_id,
        "scheduled_kickoff": datetime(
            2026, 8, 13, 23, 0, tzinfo=timezone.utc
        ),
        "header": {
            "game_id": game_id,
            "game_date": "2026-08-13",
            "game_time": "7:00p",
            "game_status": "Scheduled",
            "season": "2026",
            "game_week": "Hall of Fame Weekend",
            "season_type": "Preseason",
            "away_team": {
                "id": away_id,
                "name": f"A{away_id}",
                "abbreviation": f"A{away_id}",
                "logo": None,
            },
            "home_team": {
                "id": home_id,
                "name": f"H{home_id}",
                "abbreviation": f"H{home_id}",
                "logo": None,
            },
            "espn_link": None,
        },
    }


class QueryResult:
    def __init__(self, rows):
        self.rows = rows

    def result(self):
        return list(self.rows)


class FakeClient:
    def __init__(self, result_sets):
        self.result_sets = list(result_sets)
        self.queries = []

    def query(self, query, job_config=None):
        self.queries.append((query, job_config))
        return QueryResult(self.result_sets.pop(0))


class TestSlateEvidenceLoader(unittest.TestCase):
    def test_production_evidence_is_explicit_read_only_and_dev_only(self):
        loader = BigQuerySlateEvidenceLoader(
            client=FakeClient([[]]),
            runtime_config=runtime_config(),
            evidence_source="production",
        )

        loader.fetch_candidates(
            start_date=date(2026, 8, 13),
            end_date=date(2026, 8, 13),
        )

        self.assertIn(
            "`nfl-stream-406420.League.schedule`",
            loader.client.queries[0][0],
        )
        self.assertEqual(
            loader.evidence_lineage(),
            {
                "source_environment": "production",
                "schedule_dataset": "League",
                "analytics_dataset": "Analytics",
                "access_mode": "read_only",
            },
        )

        production_config = RuntimeConfig(
            project_id="nfl-stream-406420",
            environment="production",
            run_mode="daily",
            active_season="2026",
            league_dataset="League",
            scores_dataset="Scores",
            analytics_dataset="Analytics",
            raw_response_bucket="xtra_point",
        )
        with self.assertRaisesRegex(ValueError, "dev shadow capture"):
            BigQuerySlateEvidenceLoader(
                client=FakeClient([]),
                runtime_config=production_config,
                evidence_source="production",
            )

    def test_production_evidence_uses_production_analytics_tables(self):
        item = candidate("20260813_A1@H2", "1", "2")
        client = FakeClient([[], []])
        loader = BigQuerySlateEvidenceLoader(
            client=client,
            runtime_config=runtime_config(),
            evidence_source="production",
        )

        loader.load_slate_evidence([item])

        self.assertIn(
            "`nfl-stream-406420.Analytics.team_metrics_windowed_2026`",
            client.queries[0][0],
        )
        self.assertIn(
            "`nfl-stream-406420.Analytics.team_metric_rankings_2026`",
            client.queries[1][0],
        )

    def test_two_game_slate_uses_one_metric_and_one_ranking_query(self):
        items = [
            candidate("20260813_A1@H2", "1", "2"),
            candidate("20260813_A3@H4", "3", "4"),
        ]
        metric_rows = [
            {
                "team_id": team_id,
                "team_abv": f"T{team_id}",
                "metric": "yards_per_play",
                "category": "Offense",
                "core_area": "Efficiency",
                "value": float(team_id),
                "data_date": date(2026, 8, 12),
            }
            for team_id in ("1", "2", "3", "4")
        ]
        ranking_rows = [
            {
                "season": "2026",
                "as_of_date": date(2026, 8, 12),
                "source_data_date": date(2026, 8, 12),
                "data_lag_days": 1,
                "window_type": "preseason_to_date",
                "team_id": team_id,
                "team_abv": f"T{team_id}",
                "metric": "yards_per_play",
                "category": "Offense",
                "core_area": "Efficiency",
                "value": float(team_id),
                "lens_tags": ["efficiency", "explosiveness"],
                "league_rank": int(team_id),
                "league_percentile": 0.5,
            }
            for team_id in ("1", "2", "3", "4")
        ]
        client = FakeClient([metric_rows, ranking_rows])
        loader = BigQuerySlateEvidenceLoader(
            client=client,
            runtime_config=runtime_config(),
        )

        evidence = loader.load_slate_evidence(items)

        self.assertEqual(len(client.queries), 2)
        self.assertEqual(set(evidence), {item["game_id"] for item in items})
        for item in items:
            game_evidence = evidence[item["game_id"]]
            self.assertIsNone(game_evidence.final_score)
            self.assertTrue(game_evidence.ranking_context["available"])
            self.assertEqual(
                game_evidence.away_rankings["yards_per_play"]["lens_tags"],
                ["efficiency", "explosiveness"],
            )
        metric_sql, ranking_sql = (call[0] for call in client.queries)
        self.assertIn("team_id IN UNNEST(@team_ids)", metric_sql)
        self.assertNotIn("team_id IN UNNEST(@team_ids)", ranking_sql)

    def test_per_game_cutoff_selects_latest_strictly_prior_rows(self):
        item = candidate("20260813_A1@H2", "1", "2")
        metric_rows = [
            {
                "team_id": team_id,
                "team_abv": f"T{team_id}",
                "metric": "yards_per_play",
                "category": "Offense",
                "core_area": "Efficiency",
                "value": value,
                "data_date": row_date,
            }
            for team_id in ("1", "2")
            for row_date, value in (
                (date(2026, 8, 11), 1.0),
                (date(2026, 8, 12), 2.0),
                (date(2026, 8, 13), 999.0),
            )
        ]
        client = FakeClient([metric_rows, []])
        loader = BigQuerySlateEvidenceLoader(
            client=client,
            runtime_config=runtime_config(),
        )
        evidence = loader.load_slate_evidence([item])[item["game_id"]]
        self.assertEqual(
            evidence.away_metrics["Offense::yards_per_play"]["value"],
            2.0,
        )
        self.assertFalse(evidence.ranking_context["available"])

    def test_ranking_cutoff_matches_live_global_as_of_date_selection(self):
        item = candidate("20260813_A1@H2", "1", "2")
        ranking_rows = [
            {
                "season": "2026",
                "as_of_date": date(2026, 8, 11),
                "window_type": "preseason_to_date",
                "team_id": "1",
                "metric": "yards_per_play",
                "category": "Offense",
                "core_area": "Efficiency",
                "value": 5.2,
                "lens_tags": ["efficiency"],
            },
            {
                "season": "2026",
                "as_of_date": date(2026, 8, 12),
                "window_type": "preseason_to_date",
                "team_id": "31",
                "metric": "yards_per_play",
                "category": "Offense",
                "core_area": "Efficiency",
                "value": 6.1,
                "lens_tags": ["efficiency"],
            },
        ]
        loader = BigQuerySlateEvidenceLoader(
            client=FakeClient([[], ranking_rows]),
            runtime_config=runtime_config(),
        )

        evidence = loader.load_slate_evidence([item])[item["game_id"]]

        self.assertFalse(evidence.ranking_context["available"])
        self.assertEqual(evidence.away_rankings, {})


if __name__ == "__main__":
    unittest.main()
