"""
Runtime claim-language feature helpers for GameLens.

Purpose
-------
Compute small, pregame-safe claim-support features from live /game payload
objects.

Important boundary:
- This is NOT winner prediction.
- This is NOT model confidence.
- This is NOT pick logic.
- This exists only to support claim-level language calibration.

The training pipeline computes richer Level 3 fields in:
    agg/gamelens_training/update_claim_training_features.py

This runtime helper stays aligned with that idea:
offensive finish + defensive suppression can create claim-language support,
but only downstream allowlisted claims should use it.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def clamp(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def calculate_two_way_edge_score(
    *,
    offense_finish_score: Optional[float],
    defensive_suppression_score: Optional[float],
) -> Optional[float]:
    """
    Compute simple two-way support score.

    The two-way score is only as strong as the weaker side.
    """
    if offense_finish_score is None or defensive_suppression_score is None:
        return None

    return round(min(offense_finish_score, defensive_suppression_score), 4)


def calculate_two_way_context(
    *,
    offense_finish_score: Optional[float],
    defensive_suppression_score: Optional[float],
) -> str:
    """
    Categorize side-level two-way context.

    Buckets:
    - supportive: both scores >= 0.15
    - available_mixed: both scores exist, but not both supportive
    - unavailable: either score is missing
    """
    if offense_finish_score is None or defensive_suppression_score is None:
        return "unavailable"

    if offense_finish_score >= 0.15 and defensive_suppression_score >= 0.15:
        return "supportive"

    return "available_mixed"


def build_side_two_way_context(
    *,
    offense_finish_score: Optional[float],
    defensive_suppression_score: Optional[float],
) -> Dict[str, Any]:
    """
    Build a small side-level two-way context payload.
    """
    two_way_edge_score = calculate_two_way_edge_score(
        offense_finish_score=offense_finish_score,
        defensive_suppression_score=defensive_suppression_score,
    )

    two_way_context = calculate_two_way_context(
        offense_finish_score=offense_finish_score,
        defensive_suppression_score=defensive_suppression_score,
    )

    return {
        "offense_finish_score": offense_finish_score,
        "defensive_suppression_score": defensive_suppression_score,
        "two_way_edge_score": two_way_edge_score,
        "two_way_context": two_way_context,
    }


def build_core_area_edges(core_area_comparison: list[dict]) -> Dict[str, float]:
    """
    Build away-signed Core Area edges from /game core_area_comparison.

    Return:
        {
            "Offensive Output": positive if away led, negative if home led,
            "Scoring Efficiency": positive if away led, negative if home led,
            "Defensive Control": positive if away led, negative if home led,
        }

    Uses away_score - home_score so the result is naturally away-signed.
    """
    edges: Dict[str, float] = {}

    for row in core_area_comparison or []:
        core_area = row.get("core_area")
        away_score = safe_float(row.get("away_score"))
        home_score = safe_float(row.get("home_score"))

        if not core_area or away_score is None or home_score is None:
            continue

        edges[str(core_area)] = round(away_score - home_score, 4)

    return edges


def metric_edge_from_team_comparison(
    team_comparison: list[dict],
    metric: str,
) -> Optional[float]:
    """
    Build an away-signed metric edge from /game team_comparison.

    Positive = away had the edge.
    Negative = home had the edge.
    Zero = neutral / near-even.

    Prefer percentile_gap because it is more comparable across metrics.
    Fallback to raw_gap only if percentile_gap is unavailable.
    """
    for row in team_comparison or []:
        if row.get("metric") != metric:
            continue

        better = row.get("better")
        technical_better = row.get("technical_better")
        side = better if better in {"away", "home", "neutral"} else technical_better

        if side == "neutral":
            return 0.0

        percentile_gap = safe_float(row.get("percentile_gap"))
        raw_gap = safe_float(row.get("raw_gap"))

        if percentile_gap is not None:
            magnitude = clamp(abs(percentile_gap) / 100.0)
        elif raw_gap is not None:
            # Conservative fallback: raw gaps are not cross-metric comparable,
            # so cap them tightly.
            magnitude = clamp(abs(raw_gap))
        else:
            return None

        if side == "away":
            return round(magnitude, 4)

        if side == "home":
            return round(-magnitude, 4)

        return None

    return None


def calculate_offense_finish_score(
    *,
    offensive_output_edge: Optional[float],
    scoring_efficiency_edge: Optional[float],
    side: str,
) -> Optional[float]:
    """
    Runtime version of offense finish support.

    Inputs are away-signed Core Area edges.
    If side is home, signs are flipped so the score is team-perspective.
    """
    if offensive_output_edge is None or scoring_efficiency_edge is None:
        return None

    if side == "away":
        offense = offensive_output_edge
        scoring = scoring_efficiency_edge
    elif side == "home":
        offense = -offensive_output_edge
        scoring = -scoring_efficiency_edge
    else:
        return None

    base = (0.45 * offense) + (0.55 * scoring)

    synergy_bonus = 0.0
    conflict_penalty = 0.0

    if offense > 0 and scoring > 0:
        synergy_bonus = 0.15 * min(offense, scoring)
    elif (offense > 0 > scoring) or (scoring > 0 > offense):
        conflict_penalty = 0.10 * min(abs(offense), abs(scoring))

    return round(clamp(base + synergy_bonus - conflict_penalty), 4)


def calculate_defensive_suppression_score(
    *,
    defensive_control_edge: Optional[float],
    scoring_suppression_edge: Optional[float],
    side: str,
) -> Optional[float]:
    """
    Runtime version of defensive suppression support.

    Requires Defensive Control.
    points_allowed_per_play can sharpen the score but should not stand alone.
    """
    if defensive_control_edge is None:
        return None

    if side == "away":
        defense = defensive_control_edge
        suppression = scoring_suppression_edge
    elif side == "home":
        defense = -defensive_control_edge
        suppression = (
            -scoring_suppression_edge
            if scoring_suppression_edge is not None
            else None
        )
    else:
        return None

    if suppression is None:
        return round(clamp(0.75 * defense), 4)

    base = (0.65 * defense) + (0.35 * suppression)

    synergy_bonus = 0.0
    conflict_penalty = 0.0

    if defense > 0 and suppression > 0:
        synergy_bonus = 0.10 * min(defense, suppression)
    elif (defense > 0 > suppression) or (suppression > 0 > defense):
        conflict_penalty = 0.10 * min(abs(defense), abs(suppression))

    return round(clamp(base + synergy_bonus - conflict_penalty), 4)


def build_runtime_two_way_context_by_side(
    *,
    core_area_comparison: list[dict],
    team_comparison: list[dict],
) -> Dict[str, Dict[str, Any]]:
    """
    Build side-level two_way_context for away and home.

    This is intended for /game runtime use.
    """
    core_edges = build_core_area_edges(core_area_comparison)

    offensive_output_edge = core_edges.get("Offensive Output")
    scoring_efficiency_edge = core_edges.get("Scoring Efficiency")
    defensive_control_edge = core_edges.get("Defensive Control")

    scoring_suppression_edge = metric_edge_from_team_comparison(
        team_comparison,
        "points_allowed_per_play",
    )

    output: Dict[str, Dict[str, Any]] = {}

    for side in ["away", "home"]:
        offense_finish_score = calculate_offense_finish_score(
            offensive_output_edge=offensive_output_edge,
            scoring_efficiency_edge=scoring_efficiency_edge,
            side=side,
        )

        defensive_suppression_score = calculate_defensive_suppression_score(
            defensive_control_edge=defensive_control_edge,
            scoring_suppression_edge=scoring_suppression_edge,
            side=side,
        )

        output[side] = build_side_two_way_context(
            offense_finish_score=offense_finish_score,
            defensive_suppression_score=defensive_suppression_score,
        )

    return output