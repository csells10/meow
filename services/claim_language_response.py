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

from analytics.claim_language_support_registry import build_language_support


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
) -> dict:
    """
    Build language_support for one claim-like row.
    """
    two_way_context, two_way_edge_score = _side_two_way_values(
        claim_language_context=claim_language_context,
        side=side,
    )

    return build_language_support(
        claim_type=claim_type,
        claim_layer=claim_layer,
        metric=metric,
        two_way_context=two_way_context,
        two_way_edge_score=two_way_edge_score,
    )


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

        item["language_support"] = _build_row_language_support(
            claim_language_context=claim_language_context,
            claim_type="team_comparison_metric",
            claim_layer="supporting",
            metric=item.get("metric"),
            side=side,
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

        item["language_support"] = _build_row_language_support(
            claim_language_context=claim_language_context,
            claim_type="metric_highlight",
            claim_layer="headline",
            metric=item.get("metric"),
            side=item.get("leader"),
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

        supported_driver_count = 0
        updated_drivers = []

        for driver in item.get("drivers", []) or []:
            driver_item = deepcopy(driver)

            driver_support = _build_row_language_support(
                claim_language_context=claim_language_context,
                claim_type="category_summary",
                claim_layer="supporting",
                metric=driver_item.get("metric"),
                side=side,
            )

            if driver_support.get("language_boost_allowed"):
                supported_driver_count += 1

            driver_item["language_support"] = driver_support
            updated_drivers.append(driver_item)

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