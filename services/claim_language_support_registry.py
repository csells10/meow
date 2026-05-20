"""
Runtime claim-language support registry for GameLens.

Recommended repo location:
    services/claim_language_support_registry.py

Purpose
-------
This module is the small runtime rulebook that decides whether a specific
claim-like row may receive claim-language support metadata.

Important boundary:
- This does NOT pick winners.
- This does NOT change matchup_lean.
- This does NOT change outcome_confidence.
- This does NOT change Model Trust.
- This does NOT rewrite user-facing summaries.
- This only returns structured metadata that downstream response code can attach.

v0.2 change
-----------
Level 4 claim-strength calibration found that real edges in:
    - yards_per_pass
    - yards_per_rush
    - points_allowed_per_play

are worth treating differently from noisier areas.

This registry keeps points_allowed_per_play as strong support and moves
yards_per_pass / yards_per_rush from blocked to measured/watch support.

That is intentionally conservative:
- passing/rushing efficiency can receive measured support language
- they do NOT receive strong support language yet
- volatile scoring/turnover/red-zone metrics remain blocked
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


# ---------------------------------------------------------------------------
# Public registry sets
# ---------------------------------------------------------------------------
# These sets are imported by Level 4 calibration so keep the names stable.

STRONG_ALLOWLIST_METRICS = {
    "1st_down_rate",
    "points_allowed_per_play",
}

WATCH_ALLOWLIST_METRICS = {
    "points_per_play",

    # v0.2: moved from blocked -> measured/watch after claim-strength review.
    # These are not strong-language metrics yet.
    "yards_per_pass",
    "yards_per_rush",
}

CONDITIONAL_DISABLED_METRICS = {
    "third_down_pct",
}

BLOCKED_METRICS = {
    "red_zone_efficiency",
    "td_rate",
    "turnover_margin_per_game",
    "yards_per_play",
}


# ---------------------------------------------------------------------------
# Supported claim surfaces
# ---------------------------------------------------------------------------
# Surface = where the metric is appearing in the /game response / training row.
# Keep these explicit so a metric does not accidentally receive support
# everywhere just because it is on an allowlist.

STRONG_ALLOWED_SURFACES = {
    # Defensive suppression showed the cleanest repeatable support.
    ("category_summary", "supporting", "points_allowed_per_play"),
    ("core_area_summary", "supporting", "points_allowed_per_play"),
    ("metric_highlight", "headline", "points_allowed_per_play"),
    ("metric_highlight", "supporting", "points_allowed_per_play"),
    ("team_comparison_metric", "supporting", "points_allowed_per_play"),

    # First down rate remains a strong-allowed support candidate from v0.1.
    ("category_summary", "supporting", "1st_down_rate"),
    ("core_area_summary", "supporting", "1st_down_rate"),
    ("metric_highlight", "headline", "1st_down_rate"),
    ("metric_highlight", "supporting", "1st_down_rate"),
    ("team_comparison_metric", "supporting", "1st_down_rate"),
}

WATCH_ALLOWED_SURFACES = {
    # v0.1 measured/watch support.
    ("metric_highlight", "headline", "points_per_play"),
    ("metric_highlight", "supporting", "points_per_play"),

    # v0.2 measured/watch support.
    # Level 4 found the clearest support in category_summary rows for real
    # passing/rushing efficiency edges. Metric-highlight headline rows are
    # allowed measured support but not strong support.
    ("category_summary", "supporting", "yards_per_pass"),
    ("metric_highlight", "headline", "yards_per_pass"),

    ("category_summary", "supporting", "yards_per_rush"),
    ("metric_highlight", "headline", "yards_per_rush"),
}


# ---------------------------------------------------------------------------
# Result object
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ClaimLanguageDecision:
    """Structured decision returned by the registry."""

    language_boost_allowed: bool
    support_level: str
    rule_status: str
    reason: str
    language_modifier: str = "normal_language"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm(value: Any) -> str:
    return str(value or "").strip()


def _surface_key(
    *,
    claim_type: str,
    claim_layer: str,
    metric: Optional[str],
) -> Tuple[str, str, str]:
    return (
        _norm(claim_type),
        _norm(claim_layer),
        _norm(metric),
    )


def _blocked_decision(metric: str) -> ClaimLanguageDecision:
    return ClaimLanguageDecision(
        language_boost_allowed=False,
        support_level="blocked",
        rule_status="blocked_metric",
        reason=f"{metric} is blocked from automatic stronger language.",
        language_modifier="block_stronger_language",
    )


def _conditional_disabled_decision(metric: str) -> ClaimLanguageDecision:
    return ClaimLanguageDecision(
        language_boost_allowed=False,
        support_level="conditional_disabled",
        rule_status="conditional_disabled_metric",
        reason=f"{metric} is a conditional candidate but remains disabled.",
        language_modifier="normal_language",
    )


def _context_not_supportive_decision(two_way_context: str) -> ClaimLanguageDecision:
    return ClaimLanguageDecision(
        language_boost_allowed=False,
        support_level="none",
        rule_status="context_not_supportive",
        reason=(
            "two_way_context_not_supportive"
            if two_way_context
            else "two_way_context_missing"
        ),
        language_modifier="normal_or_cautious_language",
    )


def _no_rule_decision() -> ClaimLanguageDecision:
    return ClaimLanguageDecision(
        language_boost_allowed=False,
        support_level="none",
        rule_status="no_rule",
        reason="no_allowlist_rule_for_claim_surface",
        language_modifier="normal_language",
    )


def _strong_decision(metric: str, claim_type: str, claim_layer: str) -> ClaimLanguageDecision:
    if metric == "points_allowed_per_play":
        if claim_type == "category_summary":
            reason = "points_allowed_per_play category summaries are allowed when two_way_context is supportive."
        elif claim_type == "core_area_summary":
            reason = "points_allowed_per_play core-area summaries are allowed when two_way_context is supportive."
        elif claim_type == "metric_highlight" and claim_layer == "headline":
            reason = "points_allowed_per_play headline highlights showed strong support when two_way_context was supportive."
        elif claim_type == "metric_highlight":
            reason = "points_allowed_per_play supporting highlights are allowed when two_way_context is supportive."
        else:
            reason = "points_allowed_per_play was the cleanest stable support metric in the 240-game QA run."
    elif metric == "1st_down_rate":
        reason = "1st_down_rate is strong-allowed as a drive-sustainability support metric when two_way_context is supportive."
    else:
        reason = f"{metric} is strong-allowed when two_way_context is supportive."

    return ClaimLanguageDecision(
        language_boost_allowed=True,
        support_level="strong",
        rule_status="allowed",
        reason=reason,
        language_modifier="stronger_language_allowed",
    )


def _watch_decision(metric: str, claim_type: str, claim_layer: str) -> ClaimLanguageDecision:
    if metric == "points_per_play":
        reason = f"points_per_play {claim_layer} {claim_type} rows may receive measured support language."
    elif metric == "yards_per_pass":
        reason = (
            "yards_per_pass may receive measured support language for real passing-efficiency edges; "
            "kept below strong language in v0.2."
        )
    elif metric == "yards_per_rush":
        reason = (
            "yards_per_rush may receive measured support language for real rushing-efficiency edges; "
            "kept below strong language in v0.2."
        )
    else:
        reason = f"{metric} may receive measured support language."

    return ClaimLanguageDecision(
        language_boost_allowed=True,
        support_level="watch",
        rule_status="allowed",
        reason=reason,
        language_modifier="measured_support_language",
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_claim_language_decision(
    *,
    claim_type: str,
    claim_layer: str,
    metric: Optional[str],
    two_way_context: Optional[str],
    two_way_edge_score: Optional[float] = None,
    claim_strength_language_signal: Optional[str] = None,
    claim_strength_context: Optional[str] = None,
) -> ClaimLanguageDecision:
    """
    Decide whether a claim-like row may receive claim-language support.

    Parameters
    ----------
    claim_type, claim_layer, metric:
        Identify the row/surface being evaluated.

    two_way_context:
        Runtime v0.1 support gate. Must be "supportive" before any stronger
        or measured language is allowed.

    two_way_edge_score:
        Accepted for API compatibility / future use. Not used as a hard rule
        in v0.2.

    claim_strength_language_signal, claim_strength_context:
        Accepted for v0.2+ compatibility. The current response layer does not
        pass these yet, so this registry keeps the runtime behavior conservative
        by using explicit surfaces and measured/watch support for new metrics.

    Returns
    -------
    ClaimLanguageDecision
    """
    del two_way_edge_score  # Future-ready argument; intentionally unused today.
    del claim_strength_language_signal
    del claim_strength_context

    metric_norm = _norm(metric)
    claim_type_norm = _norm(claim_type)
    claim_layer_norm = _norm(claim_layer)
    two_way_norm = _norm(two_way_context)

    if not metric_norm or metric_norm == "missing_metric":
        return _no_rule_decision()

    if metric_norm in BLOCKED_METRICS:
        return _blocked_decision(metric_norm)

    if metric_norm in CONDITIONAL_DISABLED_METRICS:
        return _conditional_disabled_decision(metric_norm)

    if two_way_norm != "supportive":
        return _context_not_supportive_decision(two_way_norm)

    surface = _surface_key(
        claim_type=claim_type_norm,
        claim_layer=claim_layer_norm,
        metric=metric_norm,
    )

    if surface in STRONG_ALLOWED_SURFACES:
        return _strong_decision(metric_norm, claim_type_norm, claim_layer_norm)

    if surface in WATCH_ALLOWED_SURFACES:
        return _watch_decision(metric_norm, claim_type_norm, claim_layer_norm)

    return _no_rule_decision()


def build_language_support(
    *,
    claim_type: str,
    claim_layer: str,
    metric: Optional[str],
    two_way_context: Optional[str],
    two_way_edge_score: Optional[float] = None,
    claim_strength_bucket: Optional[str] = None,
    claim_strength_language_signal: Optional[str] = None,
    claim_strength_context: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build the JSON-safe language_support payload attached to /game rows.

    This is metadata only. It should not be interpreted as winner confidence.
    """
    decision = get_claim_language_decision(
        claim_type=claim_type,
        claim_layer=claim_layer,
        metric=metric,
        two_way_context=two_way_context,
        two_way_edge_score=two_way_edge_score,
        claim_strength_language_signal=claim_strength_language_signal,
        claim_strength_context=claim_strength_context,
    )

    return {
        "language_boost_allowed": decision.language_boost_allowed,
        "support_level": decision.support_level,
        "rule_status": decision.rule_status,
        "reason": decision.reason,
        "language_modifier": decision.language_modifier,
        "scope": "claim_language_support",
        "claim_type": claim_type,
        "claim_layer": claim_layer,
        "metric": metric,
        "two_way_context": two_way_context,
        "two_way_edge_score": two_way_edge_score,
        "claim_strength_bucket": claim_strength_bucket,
        "claim_strength_language_signal": claim_strength_language_signal,
        "claim_strength_context": claim_strength_context,
    }