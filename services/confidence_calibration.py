from __future__ import annotations

from typing import Any, Optional, TypedDict


CORE_AREA_DURABILITY_CONFIDENCE_VERSION = "core_area_durability_context_v0"
CORE_GAP_HIGH_FLOOR_RULE = "core_gap_high_floor_v1"
DEFAULT_CORE_GAP_FLOOR = 0.45
INSUFFICIENT_DURABILITY_REASON = "insufficient_core_area_durability_for_high"


class ConfidenceCalibrationResult(TypedDict):
    raw_confidence_label: Optional[str]
    calibrated_confidence_label: Optional[str]
    confidence_calibrated: bool
    calibration_reason: Optional[str]
    calibration_rule: str
    calibration_feature: str
    core_gap: Optional[float]
    core_gap_floor: float


def _optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def apply_core_area_durability_confidence_calibration(
    *,
    confidence_label: Optional[str],
    core_gap: Any,
    core_gap_floor: float = DEFAULT_CORE_GAP_FLOOR,
) -> ConfidenceCalibrationResult:
    """Soften fragile High confidence without changing the matchup lean."""
    raw_label = (
        confidence_label.strip()
        if isinstance(confidence_label, str)
        else confidence_label
    )
    numeric_core_gap = _optional_float(core_gap)
    calibrated_label = raw_label
    calibrated = (
        raw_label == "High"
        and numeric_core_gap is not None
        and numeric_core_gap < core_gap_floor
    )

    if calibrated:
        calibrated_label = "Medium"

    return {
        "raw_confidence_label": raw_label,
        "calibrated_confidence_label": calibrated_label,
        "confidence_calibrated": calibrated,
        "calibration_reason": INSUFFICIENT_DURABILITY_REASON if calibrated else None,
        "calibration_rule": CORE_GAP_HIGH_FLOOR_RULE,
        "calibration_feature": CORE_AREA_DURABILITY_CONFIDENCE_VERSION,
        "core_gap": numeric_core_gap,
        "core_gap_floor": core_gap_floor,
    }
