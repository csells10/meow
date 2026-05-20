"""
Attach claim-language support metadata to /game response sections.

Purpose
-------
This module annotates existing /game response rows with language_support
objects when two_way_context and the allowlist agree.

Important boundary:
- This does NOT change winner prediction.
- This does NOT change matchup_lean confidence.
- This does NOT change model_trust.
- This does NOT rewrite summaries yet.
- This only adds structured metadata that the frontend/backend can later use.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Optional

from services.claim_language_support_registry import build_language_support



# Runtime claim-strength metadata thresholds.
# These mirror the Level 3 claim_strength_context idea, but stay response-only.
CLAIM_STRENGTH_REAL_EDGE_MIN = 30.0
CLAIM_STRENGTH_USABLE_EDGE_MIN = 15.0
CLAIM_STRENGTH_THIN_EDGE_MIN = 7.0

TRUSTED_CLAIM_STRENGTH_CATEGORIES = {
    "Passing Game",
    "Rushing Game",
    "Scoring Suppression",
}

TRUSTED_CLAIM_STRENGTH_CORE_AREAS = {
    "Defensive Control",
}

WATCH_CLAIM_STRENGTH_CATEGORIES = {
    "Drive Conversion",
    "Offensive Rhythm",
}

CAUTION_CLAIM_STRENGTH_CATEGORIES = {
    "Turnovers",
    "Red Zone Finish",
    "Scoring Production",
    "Pressure",
    "Turnover Risk",
}

CAUTION_CLAIM_STRENGTH_CORE_AREAS = {
    "Disruption and Turnovers",
}

def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _claim_strength_bucket(percentile_gap: Any) -> str:
    gap = _safe_float(percentile_gap)

    if gap is None:
        return "missing"

    gap = abs(gap)

    if gap >= CLAIM_STRENGTH_REAL_EDGE_MIN:
        return "real_edge"

    if gap >= CLAIM_STRENGTH_USABLE_EDGE_MIN:
        return "usable_edge"

    if gap >= CLAIM_STRENGTH_THIN_EDGE_MIN:
        return "thin_edge"

    return "near_even"


def _build_claim_strength_metadata(
    *,
    percentile_gap: Any,
    category: str | None = None,
    core_area: str | None = None,
) -> dict:
    gap = _safe_float(percentile_gap)
    bucket = _claim_strength_bucket(gap)
    score = None if gap is None else round(min(abs(gap) / 100.0, 1.0), 4)

    if bucket == "missing":
        return {
            "claim_strength_score": score,
            "claim_strength_bucket": bucket,
            "claim_strength_context": "missing_gap_context",
            "claim_strength_language_signal": "no_boost",
        }

    if bucket == "near_even":
        return {
            "claim_strength_score": score,
            "claim_strength_bucket": bucket,
            "claim_strength_context": "near_even_gap",
            "claim_strength_language_signal": "soften",
        }

    if bucket == "thin_edge":
        return {
            "claim_strength_score": score,
            "claim_strength_bucket": bucket,
            "claim_strength_context": "thin_gap",
            "claim_strength_language_signal": "soften",
        }

    if (
        category in CAUTION_CLAIM_STRENGTH_CATEGORIES
        or core_area in CAUTION_CLAIM_STRENGTH_CORE_AREAS
    ):
        return {
            "claim_strength_score": score,
            "claim_strength_bucket": bucket,
            "claim_strength_context": "caution_area_edge",
            "claim_strength_language_signal": "caution_only",
        }

    if (
        category in TRUSTED_CLAIM_STRENGTH_CATEGORIES
        or core_area in TRUSTED_CLAIM_STRENGTH_CORE_AREAS
    ):
        if bucket == "real_edge":
            return {
                "claim_strength_score": score,
                "claim_strength_bucket": bucket,
                "claim_strength_context": "trusted_real_edge",
                "claim_strength_language_signal": "boost_candidate",
            }

        return {
            "claim_strength_score": score,
            "claim_strength_bucket": bucket,
            "claim_strength_context": "trusted_measured_edge",
            "claim_strength_language_signal": "measured",
        }

    if category in WATCH_CLAIM_STRENGTH_CATEGORIES:
        return {
            "claim_strength_score": score,
            "claim_strength_bucket": bucket,
            "claim_strength_context": "watch_area_edge",
            "claim_strength_language_signal": "measured",
        }

    return {
        "claim_strength_score": score,
        "claim_strength_bucket": bucket,
        "claim_strength_context": "unclassified_edge",
        "claim_strength_language_signal": "normal",
    }

def _side_two_way_values(
    *,
    claim_language_context: dict,
    side: str | None,
) -> tuple[Optional[str], Optional[float]]:
    """
    Return side-level two_way_context and two_way_edge_score.

    side must be away/home. Neutral or missing sides cannot receive support.
    """
    if side not in {"away", "home"}:
        return None, None

    side_payload = (
        claim_language_context
        .get("two_way_context_by_side", {})
        .get(side, {})
    )

    return (
        side_payload.get("two_way_context"),
        side_payload.get("two_way_edge_score"),
    )


def _build_row_language_support(
    *,
    claim_language_context: dict,
    claim_type: str,
    claim_layer: str,
    metric: str | None,
    side: str | None,
    claim_strength_bucket: str | None = None,
    claim_strength_context: str | None = None,
    claim_strength_language_signal: str | None = None,
) -> dict:
    """
    Build language_support for one claim-like row.
    """
    two_way_context, two_way_edge_score = _side_two_way_values(
        claim_language_context=claim_language_context,
        side=side,
    )

    language_support = build_language_support(
        claim_type=claim_type,
        claim_layer=claim_layer,
        metric=metric,
        two_way_context=two_way_context,
        two_way_edge_score=two_way_edge_score,
        claim_strength_bucket=claim_strength_bucket,
        claim_strength_language_signal=claim_strength_language_signal,
        claim_strength_context=claim_strength_context,
    )

    return language_support

def annotate_team_comparison_with_language_support(
    *,
    team_comparison: list[dict],
    claim_language_context: dict,
) -> list[dict]:
    """
    Annotate visible Team Comparison rows.

    Claim mapping:
        team_comparison row -> team_comparison_metric + supporting
    """
    annotated = []

    for row in team_comparison or []:
        item = deepcopy(row)

        side = item.get("better")
        if side not in {"away", "home"}:
            side = item.get("technical_better")

        claim_strength = _build_claim_strength_metadata(
            percentile_gap=item.get("percentile_gap"),
            category=item.get("category"),
            core_area=item.get("core_area"),
        )

        item["language_support"] = _build_row_language_support(
            claim_language_context=claim_language_context,
            claim_type="team_comparison_metric",
            claim_layer="supporting",
            metric=item.get("metric"),
            side=side,
            claim_strength_bucket=claim_strength.get("claim_strength_bucket"),
            claim_strength_context=claim_strength.get("claim_strength_context"),
            claim_strength_language_signal=claim_strength.get("claim_strength_language_signal"),
        )

        annotated.append(item)

    return annotated


def annotate_metric_highlights_with_language_support(
    *,
    metric_highlights: list[dict],
    claim_language_context: dict,
) -> list[dict]:
    """
    Annotate matchup_breakdown.metric_highlights.

    Claim mapping:
        metric_highlight row -> metric_highlight + headline
    """
    annotated = []

    for row in metric_highlights or []:
        item = deepcopy(row)

        claim_strength = _build_claim_strength_metadata(
            percentile_gap=item.get("percentile_gap"),
            category=item.get("category"),
            core_area=item.get("core_area"),
        )

        item["language_support"] = _build_row_language_support(
            claim_language_context=claim_language_context,
            claim_type="metric_highlight",
            claim_layer="headline",
            metric=item.get("metric"),
            side=item.get("leader"),
            claim_strength_bucket=claim_strength.get("claim_strength_bucket"),
            claim_strength_context=claim_strength.get("claim_strength_context"),
            claim_strength_language_signal=claim_strength.get("claim_strength_language_signal"),
        )

        annotated.append(item)

    return annotated


def annotate_category_summaries_with_language_support(
    *,
    category_summaries: list[dict],
    claim_language_context: dict,
) -> list[dict]:
    """
    Annotate matchup_breakdown.category_summaries and their drivers.

    Claim mapping:
        category summary driver -> category_summary + supporting

    The summary receives a small aggregate language_support object when one or
    more drivers are allowed to receive support.
    """
    annotated = []

    for summary in category_summaries or []:
        item = deepcopy(summary)
        side = item.get("leader")
        category = item.get("name") or item.get("category")
        core_area = item.get("core_area")

        supported_driver_count = 0
        updated_drivers = []

        for driver in item.get("drivers", []) or []:
            driver_item = deepcopy(driver)

            claim_strength = _build_claim_strength_metadata(
                percentile_gap=driver_item.get("percentile_gap"),
                category=driver_item.get("category") or category,
                core_area=driver_item.get("core_area") or core_area,
            )

            driver_support = _build_row_language_support(
                claim_language_context=claim_language_context,
                claim_type="category_summary",
                claim_layer="supporting",
                metric=driver_item.get("metric"),
                side=side,
                claim_strength_bucket=claim_strength.get("claim_strength_bucket"),
                claim_strength_context=claim_strength.get("claim_strength_context"),
                claim_strength_language_signal=claim_strength.get("claim_strength_language_signal"),
            )

            if driver_support.get("language_boost_allowed"):
                supported_driver_count += 1

            driver_item["language_support"] = driver_support
            updated_drivers.append(driver_item)

        primary_driver_support = (
            updated_drivers[0].get("language_support")
            if updated_drivers
            else {}
        )

        item["drivers"] = updated_drivers
        item["language_support"] = {
            "language_boost_allowed": supported_driver_count > 0,
            "supported_driver_count": supported_driver_count,
            "scope": "claim_language_support",
            "reason": (
                "one_or_more_category_drivers_have_two_way_support"
                if supported_driver_count > 0
                else None
            ),
            "claim_strength_bucket": primary_driver_support.get("claim_strength_bucket"),
            "claim_strength_context": primary_driver_support.get("claim_strength_context"),
            "claim_strength_language_signal": primary_driver_support.get("claim_strength_language_signal"),
        }

        annotated.append(item)

    return annotated


def annotate_matchup_breakdown_with_language_support(
    *,
    matchup_breakdown: dict,
    claim_language_context: dict,
) -> dict:
    """
    Annotate supported sections inside matchup_breakdown.

    This intentionally does not annotate:
    - context_notes
    - core_area_summaries

    Those can be evaluated later.
    """
    breakdown = deepcopy(matchup_breakdown or {})

    breakdown["metric_highlights"] = annotate_metric_highlights_with_language_support(
        metric_highlights=breakdown.get("metric_highlights", []),
        claim_language_context=claim_language_context,
    )

    breakdown["category_summaries"] = annotate_category_summaries_with_language_support(
        category_summaries=breakdown.get("category_summaries", []),
        claim_language_context=claim_language_context,
    )

    return breakdown


def apply_claim_language_support_to_response_sections(
    *,
    team_comparison: list[dict],
    matchup_breakdown: dict,
    claim_language_context: dict,
) -> Dict[str, Any]:
    """
    Return annotated response sections.

    This helper lets game_service.py stay readable.
    """
    return {
        "team_comparison": annotate_team_comparison_with_language_support(
            team_comparison=team_comparison,
            claim_language_context=claim_language_context,
        ),
        "matchup_breakdown": annotate_matchup_breakdown_with_language_support(
            matchup_breakdown=matchup_breakdown,
            claim_language_context=claim_language_context,
        ),
    }