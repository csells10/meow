"""
GameLens claim language support registry.

Purpose
-------
This registry controls where two_way_context is allowed to strengthen
claim-level language.

Important boundary:
- This is NOT winner prediction.
- This is NOT model confidence.
- This is NOT a pick boost.
- This is claim-language support only.

A claim may receive stronger wording only when:
1. two_way_context == "supportive"
2. the claim surface is allowlisted here
3. the metric is allowlisted for that surface
"""


LANGUAGE_SUPPORT_REASON = (
    "supported_by_offensive_finish_and_defensive_suppression_context"
)


# Strong allowlist from May 15, 2026 QA.
# Shape:
#   (claim_type, claim_layer, metric)
TWO_WAY_LANGUAGE_SUPPORT_ALLOWLIST = {
    # Category Summary — Supporting
    ("category_summary", "supporting", "1st_down_rate"),
    ("category_summary", "supporting", "points_allowed_per_play"),
    ("category_summary", "supporting", "yards_per_rush"),
    ("category_summary", "supporting", "red_zone_efficiency"),

    # Metric Highlight
    ("metric_highlight", "supporting", "points_allowed_per_play"),
    ("metric_highlight", "supporting", "1st_down_rate"),
    ("metric_highlight", "headline", "points_allowed_per_play"),

    # Team Comparison Metric — Supporting
    ("team_comparison_metric", "supporting", "points_allowed_per_play"),
    ("team_comparison_metric", "supporting", "red_zone_efficiency"),
    ("team_comparison_metric", "supporting", "points_per_play"),
}


def is_two_way_language_support_allowed(
    *,
    claim_type: str | None,
    claim_layer: str | None,
    metric: str | None,
    two_way_context: str | None,
) -> bool:
    """
    Return whether two_way_context may strengthen claim-level language.

    This intentionally requires two_way_context == "supportive".
    available_mixed and unavailable should not strengthen language.
    """
    if two_way_context != "supportive":
        return False

    key = (
        str(claim_type or "").strip(),
        str(claim_layer or "").strip(),
        str(metric or "").strip(),
    )

    return key in TWO_WAY_LANGUAGE_SUPPORT_ALLOWLIST


def build_language_support(
    *,
    claim_type: str | None,
    claim_layer: str | None,
    metric: str | None,
    two_way_context: str | None,
    two_way_edge_score: float | None = None,
) -> dict:
    """
    Build a small structured language-support payload.

    This object is safe to expose through /game because it describes
    claim-language support only, not outcome confidence.
    """
    allowed = is_two_way_language_support_allowed(
        claim_type=claim_type,
        claim_layer=claim_layer,
        metric=metric,
        two_way_context=two_way_context,
    )

    return {
        "two_way_context": two_way_context,
        "two_way_edge_score": two_way_edge_score,
        "language_boost_allowed": allowed,
        "reason": LANGUAGE_SUPPORT_REASON if allowed else None,
        "scope": "claim_language_support",
    }