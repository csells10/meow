import unittest

from services.confidence_calibration import (
    apply_core_area_durability_confidence_calibration,
)


class TestConfidenceCalibration(unittest.TestCase):
    def test_only_high_below_floor_softens_to_medium(self):
        cases = (
            ("High", 0.449, "Medium", True),
            ("High", 0.45, "High", False),
            ("High", 0.451, "High", False),
            ("High", None, "High", False),
            ("Medium", 0.10, "Medium", False),
            ("Low", 0.10, "Low", False),
        )

        for raw, gap, expected, applied in cases:
            with self.subTest(raw=raw, gap=gap):
                result = apply_core_area_durability_confidence_calibration(
                    confidence_label=raw,
                    core_gap=gap,
                )
                self.assertEqual(result["raw_confidence_label"], raw)
                self.assertEqual(result["calibrated_confidence_label"], expected)
                self.assertEqual(result["confidence_calibrated"], applied)

    def test_invalid_gap_preserves_the_original_label(self):
        result = apply_core_area_durability_confidence_calibration(
            confidence_label="High",
            core_gap="not-a-number",
        )

        self.assertEqual(result["calibrated_confidence_label"], "High")
        self.assertFalse(result["confidence_calibrated"])
        self.assertIsNone(result["core_gap"])


if __name__ == "__main__":
    unittest.main()
