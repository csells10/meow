import unittest
from unittest.mock import patch

import qa_gamelens_packet4_level3 as preview


def _level2():
    return {
        "learning_run_id": "cohort-1",
        "capture_id": "capture-1",
        "game_id": "game-1",
        "final_score": {"away": {"total": 17}, "home": {"total": 7}},
        "source_counts": {
            "claims": 0,
            "accepted_fact_rows_total": 130,
            "eligible_actual_fact_rows": 94,
        },
        "status": "no_op",
        "reason": "zero_claims",
        "reconciliation": {
            "claims_in": 0,
            "validations_out": 0,
            "write_performed": False,
        },
    }


class Packet4Level3PreviewTests(unittest.TestCase):
    @patch.object(preview, "load_bounded_claim_rows", return_value=[])
    @patch.object(preview, "build_packet4_level2_preview", return_value=_level2())
    def test_zero_claim_preview_preserves_level2_gate(self, level2, claims):
        result = preview.build_packet4_level3_preview(
            client=object(),
            bigquery=object(),
            project_id="project",
            game_id="game-1",
        )
        self.assertEqual(result["status"], "no_op")
        self.assertEqual(result["reason"], "zero_claims")
        self.assertEqual(result["level2_gate"]["status"], "no_op")
        self.assertEqual(result["facts_counts"]["accepted_fact_rows_total"], 130)
        self.assertEqual(
            result["reconciliation"]["postgame_fields_admitted"], 0
        )
        self.assertFalse(result["write_performed"])
        level2.assert_called_once()
        claims.assert_called_once()

    def test_cli_requires_explicit_read_only_confirmation(self):
        with self.assertRaisesRegex(ValueError, "--dev-read-only"):
            preview.main(["--game-id", "game-1"])


if __name__ == "__main__":
    unittest.main()
