"""
GameLens Level 4 v0.1 claim-language support registry.

Purpose:
    Centralize the first approved Level 4 language-calibration rules.

This registry does NOT:
    - pick winners
    - change matchup lean
    - change outcome confidence
    - change Model Trust
    - create frontend UI by itself

It only answers:
    "Is this claim/metric/surface allowed to receive firmer explanatory language?"

Current tested basis:
    Larger 240-game QA run: larger_240_level3_qa_20260517

Main finding:
    two_way_context = supportive is useful as claim-language support,
    but only for approved metrics/surfaces.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# Registry models
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ClaimLanguageRule:
    claim_type: str
    claim_layer: str
    metric: str
    support_level: str
    enabled: bool
    reason: str


@dataclass(frozen=True)
class ClaimLanguageDecision:
    language_boost_allowed: bool
    support_level: str
    reason: str
    rule_status: str


# ---------------------------------------------------------------------------
# Metric groups from 240-game QA
# ---------------------------------------------------------------------------

STRONG_ALLOWLIST_METRICS = {
    "points_allowed_per_play",
    "1st_down_rate",
}

WATCH_ALLOWLIST_METRICS = {
    "points_per_play",
}

CONDITIONAL_DISABLED_METRICS = {
    "third_down_pct",
}

BLOCKED_METRICS = {
    "red_zone_efficiency",
    "turnover_margin_per_game",
    "td_rate",
    "yards_per_play",
    "yards_per_pass",
    "yards_per_rush",
}


# ---------------------------------------------------------------------------
# Surface rules
# ---------------------------------------------------------------------------
# v0.1 deliberately starts conservative.
#
# Allowed surfaces are the row types where two_way_context performed best
# and where the product meaning is easiest to keep honest.
#
# Game Profile is intentionally excluded for now.

CLAIM_LANGUAGE_RULES = [
    # points_allowed_per_play — strongest current candidate
    ClaimLanguageRule(
        claim_type="team_comparison_metric",
        claim_layer="supporting",
        metric="points_allowed_per_play",
        support_level="strong",
        enabled=True,
        reason="points_allowed_per_play was the cleanest stable support metric in the 240-game QA run.",
    ),
    ClaimLanguageRule(
        claim_type="metric_highlight",
        claim_layer="headline",
        metric="points_allowed_per_play",
        support_level="strong",
        enabled=True,
        reason="points_allowed_per_play headline highlights showed strong support when two_way_context was supportive.",
    ),
    ClaimLanguageRule(
        claim_type="metric_highlight",
        claim_layer="supporting",
        metric="points_allowed_per_play",
        support_level="strong",
        enabled=True,
        reason="points_allowed_per_play supporting highlights are allowed when two_way_context is supportive.",
    ),
    ClaimLanguageRule(
        claim_type="category_summary",
        claim_layer="supporting",
        metric="points_allowed_per_play",
        support_level="strong",
        enabled=True,
        reason="points_allowed_per_play category summaries are allowed when two_way_context is supportive.",
    ),
    ClaimLanguageRule(
        claim_type="core_area_summary",
        claim_layer="supporting",
        metric="points_allowed_per_play",
        support_level="strong",
        enabled=True,
        reason="points_allowed_per_play core-area summaries are allowed when two_way_context is supportive.",
    ),

    # 1st_down_rate — strong allowlist
    ClaimLanguageRule(
        claim_type="team_comparison_metric",
        claim_layer="supporting",
        metric="1st_down_rate",
        support_level="strong",
        enabled=True,
        reason="1st_down_rate was a strong drive-sustain support metric in the 240-game QA run.",
    ),
    ClaimLanguageRule(
        claim_type="metric_highlight",
        claim_layer="headline",
        metric="1st_down_rate",
        support_level="strong",
        enabled=True,
        reason="1st_down_rate headline highlights are allowed when two_way_context is supportive.",
    ),
    ClaimLanguageRule(
        claim_type="metric_highlight",
        claim_layer="supporting",
        metric="1st_down_rate",
        support_level="strong",
        enabled=True,
        reason="1st_down_rate supporting highlights are allowed when two_way_context is supportive.",
    ),
    ClaimLanguageRule(
        claim_type="category_summary",
        claim_layer="supporting",
        metric="1st_down_rate",
        support_level="strong",
        enabled=True,
        reason="1st_down_rate category summaries are allowed when two_way_context is supportive.",
    ),
    ClaimLanguageRule(
        claim_type="core_area_summary",
        claim_layer="supporting",
        metric="1st_down_rate",
        support_level="strong",
        enabled=True,
        reason="1st_down_rate core-area summaries are allowed when two_way_context is supportive.",
    ),

    # points_per_play — useful, but slightly softer/watch
    ClaimLanguageRule(
        claim_type="team_comparison_metric",
        claim_layer="supporting",
        metric="points_per_play",
        support_level="watch",
        enabled=True,
        reason="points_per_play was useful, but less clean than points_allowed_per_play and 1st_down_rate.",
    ),
    ClaimLanguageRule(
        claim_type="metric_highlight",
        claim_layer="headline",
        metric="points_per_play",
        support_level="watch",
        enabled=True,
        reason="points_per_play headline highlights may receive measured support language.",
    ),
    ClaimLanguageRule(
        claim_type="metric_highlight",
        claim_layer="supporting",
        metric="points_per_play",
        support_level="watch",
        enabled=True,
        reason="points_per_play supporting highlights may receive measured support language.",
    ),
]


_RULE_INDEX = {
    (rule.claim_type, rule.claim_layer, rule.metric): rule
    for rule in CLAIM_LANGUAGE_RULES
}


# ---------------------------------------------------------------------------
# Decision helper
# ---------------------------------------------------------------------------

def get_claim_language_decision(
    *,
    claim_type: Optional[str],
    claim_layer: Optional[str],
    metric: Optional[str],
    two_way_context: Optional[str],
) -> ClaimLanguageDecision:
    """
    Decide whether a claim row may receive stronger explanatory language.

    Required for boost:
        - two_way_context == "supportive"
        - metric is not blocked
        - exact claim_type + claim_layer + metric rule exists
        - rule.enabled is True
    """

    if not metric:
        return ClaimLanguageDecision(
            language_boost_allowed=False,
            support_level="none",
            reason="missing_metric",
            rule_status="not_applicable",
        )

    if metric in BLOCKED_METRICS:
        return ClaimLanguageDecision(
            language_boost_allowed=False,
            support_level="blocked",
            reason="metric_blocked_from_automatic_stronger_language",
            rule_status="blocked_metric",
        )

    if metric in CONDITIONAL_DISABLED_METRICS:
        return ClaimLanguageDecision(
            language_boost_allowed=False,
            support_level="conditional",
            reason="metric_is_conditional_candidate_but_disabled_in_v0_1",
            rule_status="conditional_disabled",
        )

    if two_way_context != "supportive":
        return ClaimLanguageDecision(
            language_boost_allowed=False,
            support_level="none",
            reason="two_way_context_not_supportive",
            rule_status="context_not_supportive",
        )

    key = (claim_type or "", claim_layer or "", metric)
    rule = _RULE_INDEX.get(key)

    if not rule:
        return ClaimLanguageDecision(
            language_boost_allowed=False,
            support_level="none",
            reason="no_allowlist_rule_for_claim_surface",
            rule_status="no_rule",
        )

    if not rule.enabled:
        return ClaimLanguageDecision(
            language_boost_allowed=False,
            support_level=rule.support_level,
            reason="allowlist_rule_disabled",
            rule_status="disabled",
        )

    return ClaimLanguageDecision(
        language_boost_allowed=True,
        support_level=rule.support_level,
        reason=rule.reason,
        rule_status="allowed",
    )
    
def build_language_support(
    *,
    claim_type: Optional[str],
    claim_layer: Optional[str],
    metric: Optional[str],
    two_way_context: Optional[str],
    two_way_edge_score: Optional[float] = None,
) -> dict:
    """
    Build the row-level language_support object used by /game response sections.

    This is response metadata only. It must not change winner logic,
    matchup lean, outcome confidence, or Model Trust.
    """
    decision = get_claim_language_decision(
        claim_type=claim_type,
        claim_layer=claim_layer,
        metric=metric,
        two_way_context=two_way_context,
    )

    return {
        "language_boost_allowed": decision.language_boost_allowed,
        "support_level": decision.support_level,
        "reason": decision.reason,
        "rule_status": decision.rule_status,
        "scope": "claim_language_support",
        "two_way_context": two_way_context,
        "two_way_edge_score": two_way_edge_score,
        "metric": metric,
        "claim_type": claim_type,
        "claim_layer": claim_layer,
    }