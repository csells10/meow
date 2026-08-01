import unittest

from runtime_config import load_runtime_config


class RuntimeConfigTests(unittest.TestCase):
    @staticmethod
    def _dev_env(**overrides):
        values = {
            "GAMELENS_ENVIRONMENT": "dev",
            "GAMELENS_RUN_MODE": "controlled_replay",
            "GAMELENS_ACTIVE_SEASON": "2025",
            "GAMELENS_LEAGUE_DATASET": "League_dev",
            "GAMELENS_SCORES_DATASET": "Scores_dev",
            "GAMELENS_ANALYTICS_DATASET": "Analytics_dev",
            "GCS_BUCKET_NAME": "gamelens-dev-raw-nfl-stream-406420",
        }
        values.update(overrides)
        return values

    def test_dev_mode_resolves_only_approved_dev_targets(self):
        config = load_runtime_config(self._dev_env())

        self.assertEqual(config.active_season, "2025")
        self.assertTrue(config.is_controlled_replay)
        self.assertEqual(
            config.league_table("schedule"),
            "nfl-stream-406420.League_dev.schedule",
        )
        self.assertEqual(
            config.scores_table("scores"),
            "nfl-stream-406420.Scores_dev.scores",
        )
        self.assertEqual(
            config.analytics_table("game_metrics_flat"),
            "nfl-stream-406420.Analytics_dev.game_metrics_flat",
        )

    def test_missing_dev_target_raises_before_any_client_exists(self):
        env = self._dev_env()
        del env["GAMELENS_ANALYTICS_DATASET"]

        with self.assertRaisesRegex(
            ValueError,
            "GAMELENS_ANALYTICS_DATASET is required",
        ):
            load_runtime_config(env)

    def test_production_dataset_is_rejected_in_dev_mode(self):
        for variable, production_dataset in (
            ("GAMELENS_LEAGUE_DATASET", "League"),
            ("GAMELENS_SCORES_DATASET", "Scores"),
            ("GAMELENS_ANALYTICS_DATASET", "Analytics"),
        ):
            with self.subTest(variable=variable):
                with self.assertRaisesRegex(ValueError, "production dataset"):
                    load_runtime_config(
                        self._dev_env(**{variable: production_dataset})
                    )

    def test_unapproved_dev_dataset_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "approved target Analytics_dev"):
            load_runtime_config(
                self._dev_env(GAMELENS_ANALYTICS_DATASET="Analytics_sandbox")
            )

    def test_raw_bucket_must_be_development_only(self):
        for bucket in ("xtra_point", "gamelens-raw-replay"):
            with self.subTest(bucket=bucket):
                with self.assertRaisesRegex(ValueError, "raw-response|dev segment"):
                    load_runtime_config(self._dev_env(GCS_BUCKET_NAME=bucket))

    def test_blank_or_invalid_season_is_rejected(self):
        for season in ("", "25", "2025-2026"):
            with self.subTest(season=season):
                with self.assertRaisesRegex(
                    ValueError,
                    "GAMELENS_ACTIVE_SEASON is required|four-digit NFL season",
                ):
                    load_runtime_config(
                        self._dev_env(GAMELENS_ACTIVE_SEASON=season)
                    )

    def test_production_defaults_remain_unchanged(self):
        config = load_runtime_config({})

        self.assertEqual(config.environment, "production")
        self.assertEqual(config.run_mode, "daily")
        self.assertEqual(config.active_season, "2026")
        self.assertEqual(config.league_dataset, "League")
        self.assertEqual(config.scores_dataset, "Scores")
        self.assertEqual(config.analytics_dataset, "Analytics")
        self.assertEqual(config.raw_response_bucket, "xtra_point")


if __name__ == "__main__":
    unittest.main()
