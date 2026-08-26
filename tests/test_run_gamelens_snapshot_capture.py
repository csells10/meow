import unittest

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from run_gamelens_snapshot_capture import build_parser


class TestSnapshotCaptureRunner(unittest.TestCase):
    def test_runner_is_dry_run_by_default_and_one_game_only(self):
        args = build_parser().parse_args([
            "--game-id", "20260827_PIT@BUF",
            "--learning-run-id", "gamelens_2026_preseason_ll3_v1",
            "--model-version", "game_service_v1",
            "--ruleset-version", "ll3_v1",
        ])

        self.assertFalse(args.write)
        self.assertEqual(args.game_id, "20260827_PIT@BUF")
        self.assertEqual(args.evidence_source, "production")

    def test_write_requires_explicit_flag(self):
        args = build_parser().parse_args([
            "--game-id", "20260827_PIT@BUF",
            "--learning-run-id", "gamelens_2026_preseason_ll3_v1",
            "--model-version", "game_service_v1",
            "--ruleset-version", "ll3_v1",
            "--write",
        ])

        self.assertTrue(args.write)


if __name__ == "__main__":
    unittest.main()
