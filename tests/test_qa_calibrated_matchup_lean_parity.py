import unittest

from qa_calibrated_matchup_lean_parity import (
    build_parity_summary,
    render_visual,
)


def row(
    game_id: str,
    confidence: str,
    core_gap,
    *,
    label_variants: int = 1,
    gap_variants: int = 1,
) -> dict:
    return {
        "game_id": game_id,
        "raw_confidence_label": confidence,
        "core_gap": core_gap,
        "label_variant_count": label_variants,
        "core_gap_variant_count": gap_variants,
    }


class TestCalibratedMatchupLeanParityQa(unittest.TestCase):
    def test_2025_benchmark_renders_as_a_visual_pass(self):
        rows = []
        rows.extend(row(f"low_{i}", "Low", 0.10) for i in range(154))
        rows.extend(row(f"medium_{i}", "Medium", 0.30) for i in range(93))
        rows.extend(row(f"softened_{i}", "High", 0.44) for i in range(10))
        rows.extend(row(f"retained_{i}", "High", 0.45) for i in range(12))

        summary = build_parity_summary(rows)
        output = render_visual(
            summary,
            run_id="full_2025_reg_post_claim_matrix_pilot",
            source_table="test.table",
        )

        self.assertEqual(
            summary["raw"],
            {"Low": 154, "Medium": 93, "High": 22},
        )
        self.assertEqual(
            summary["calibrated"],
            {"Low": 154, "Medium": 103, "High": 12},
        )
        self.assertEqual(summary["changed_count"], 10)
        self.assertIn("RESULT: PASS", output)

    def test_conflicting_game_rows_fail_visibly(self):
        summary = build_parity_summary(
            [
                row(
                    "conflict",
                    "High",
                    0.44,
                    label_variants=2,
                )
            ]
        )
        output = render_visual(
            summary,
            run_id="test",
            source_table="test.table",
        )

        self.assertEqual(summary["conflict_count"], 1)
        self.assertIn("RESULT: FAIL", output)


if __name__ == "__main__":
    unittest.main()
