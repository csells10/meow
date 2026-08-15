import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import Mock, patch

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from runtime_config import RuntimeConfig
from run_gamelens_level1_capture import _display_result, main


def _config(environment="dev"):
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


class TestRunGameLensLevel1Capture(unittest.TestCase):
    def test_display_keeps_counts_and_limits_claim_sample(self):
        result = {
            "claim_count": 7,
            "claims_out": 7,
            "rows": [
                {
                    "claim_key": f"claim_{index}",
                    "claim_type": "game_profile",
                    "claim_layer": "headline",
                    "claimed_team": "ARI",
                    "source_field_path": f"game_profile[{index}]",
                    "claim_text": "not repeated in the compact display",
                }
                for index in range(7)
            ],
        }

        visible = _display_result(result)

        self.assertNotIn("rows", visible)
        self.assertEqual(visible["claim_count"], visible["claims_out"])
        self.assertEqual(len(visible["claim_sample"]), 5)
        self.assertNotIn("claim_text", visible["claim_sample"][0])

    @patch("run_gamelens_level1_capture.extract_level1_from_capture")
    @patch("run_gamelens_level1_capture.BigQueryClaimStorage")
    @patch("run_gamelens_level1_capture.BigQuerySnapshotStorage")
    @patch("run_gamelens_level1_capture.bigquery.Client")
    @patch("run_gamelens_level1_capture.load_runtime_config")
    def test_main_uses_same_service_for_dry_and_write_modes(
        self,
        load_config,
        client_factory,
        snapshot_storage,
        claim_storage,
        extract,
    ):
        load_config.return_value = _config()
        client_factory.return_value = Mock(name="bigquery_client")
        extract.return_value = {
            "status": "ready",
            "claim_count": 1,
            "claims_out": 1,
            "rows": [{"claim_key": "claim_one"}],
        }

        for write_flag in (False, True):
            with self.subTest(write=write_flag):
                args = [
                    "--capture-id",
                    "capture_one",
                    "--attempt-id",
                    f"attempt_{write_flag}",
                ]
                if write_flag:
                    args.append("--write")
                output = io.StringIO()
                with redirect_stdout(output):
                    self.assertEqual(main(args), 0)
                self.assertEqual(json.loads(output.getvalue())["claim_count"], 1)
                self.assertEqual(extract.call_args.kwargs["write"], write_flag)

        self.assertEqual(snapshot_storage.call_count, 2)
        self.assertEqual(claim_storage.call_count, 2)

    @patch("run_gamelens_level1_capture.load_runtime_config")
    def test_main_refuses_production_before_constructing_storage(self, load_config):
        load_config.return_value = _config(environment="production")

        with self.assertRaisesRegex(ValueError, "only in dev"):
            main(
                [
                    "--capture-id",
                    "capture_one",
                    "--attempt-id",
                    "attempt_one",
                ]
            )


if __name__ == "__main__":
    unittest.main()
