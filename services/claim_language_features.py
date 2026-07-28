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
and offensive_efficiency_support_v1 can expose claim-language metadata,
but only downstream allowlisted claims should use it for stronger wording.
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


# ---------------------------------------------------------------------------
# offensive_efficiency_support_v1 runtime metadata
# ---------------------------------------------------------------------------

OFFENSIVE_EFFICIENCY_SUPPORT_VERSION = "offensive_efficiency_support_v1"

# Evidence-backed support set from Level 3 validation review.
OFFENSIVE_EFFICIENCY_SUPPORT_METRICS = {
    "points_per_play",
    "yards_per_play",
    "yards_per_pass",
    "yards_per_rush",
}

OFFENSIVE_EFFICIENCY_CAUTION_ONLY_METRICS = {
    "td_rate",
    "red_zone_efficiency",
    "turnover_margin_per_game",
}

OFFENSIVE_EFFICIENCY_CONTEXT_ONLY_METRICS = {
    "third_down_pct",
    "1st_down_rate",
}


def _side_signed_edge(edge: Optional[float], side: str | None) -> Optional[float]:
    """Convert an away-signed edge into the requested side's perspective."""
    if edge is None:
        return None
    if side == "away":
        return edge
    if side == "home":
        return -edge
    return None


def metric_edge_from_rankings(
    away_rankings: dict,
    home_rankings: dict,
    metric: str,
) -> Optional[float]:
    """
    Build an away-signed metric edge from ranking percentiles.

    Positive = away has the better ranked profile.
    Negative = home has the better ranked profile.

    Ranking percentiles are preferred because they already normalize metric
    direction and make cross-metric support scores more comparable.
    """
    away_row = (away_rankings or {}).get(metric) or {}
    home_row = (home_rankings or {}).get(metric) or {}

    away_pct = safe_float(away_row.get("league_percentile"))
    home_pct = safe_float(home_row.get("league_percentile"))

    if away_pct is not None and home_pct is not None:
        return round(clamp((away_pct - home_pct) / 100.0), 4)

    # Conservative fallback to value only when ranking percentiles are absent.
    # This should be rare for /game after rankings are wired.
    away_value = safe_float(away_row.get("value"))
    home_value = safe_float(home_row.get("value"))
    if away_value is None or home_value is None:
        return None

    direction = str(
        away_row.get("comparison_direction")
        or home_row.get("comparison_direction")
        or "higher"
    ).lower()

    raw_gap = away_value - home_value
    if direction == "lower":
        raw_gap = -raw_gap

    return round(clamp(raw_gap), 4)


def metric_edge_from_payload_rows(
    rows: list[dict],
    metric: str,
) -> Optional[float]:
    """
    Build an away-signed metric edge from already-built /game payload rows.

    Positive = away had the edge.
    Negative = home had the edge.
    Zero = neutral / near-even.

    This is a fallback for runtime API exposure when the rankings dictionaries
    are unavailable or do not include a support metric. It uses the same
    convention as Team Comparison and Matchup Breakdown rows: `leader` plus
    `percentile_gap`.
    """
    for row in rows or []:
        if row.get("metric") != metric:
            continue

        leader = row.get("leader")
        if leader == "neutral":
            return 0.0

        percentile_gap = safe_float(row.get("percentile_gap"))
        raw_gap = safe_float(row.get("raw_gap"))

        if percentile_gap is not None:
            magnitude = clamp(abs(percentile_gap) / 100.0)
        elif raw_gap is not None:
            magnitude = clamp(abs(raw_gap))
        else:
            return None

        if leader == "away":
            return round(magnitude, 4)
        if leader == "home":
            return round(-magnitude, 4)

    return None


def collect_matchup_breakdown_metric_rows(matchup_breakdown: dict) -> list[dict]:
    """
    Flatten metric-bearing rows from matchup_breakdown for runtime fallbacks.

    This intentionally reads only existing /game pregame payload rows. It does
    not query postgame or validation fields, so it stays metadata-only and
    pregame-safe.
    """
    rows: list[dict] = []
    breakdown = matchup_breakdown or {}

    rows.extend(breakdown.get("metric_highlights") or [])

    for summary in breakdown.get("category_summaries") or []:
        rows.extend(summary.get("drivers") or [])

    for summary in breakdown.get("core_area_summaries") or []:
        rows.extend(summary.get("drivers") or [])

    return rows


def _first_available_metric_edge(
    *,
    metric: str,
    away_rankings: dict,
    home_rankings: dict,
    team_comparison: list[dict],
    matchup_metric_rows: list[dict],
) -> Optional[float]:
    """
    Prefer normalized rankings, then fall back to response rows.

    The fallback is important because local/API smoke tests may have enough
    `/game` row context to expose metadata even when the ranking dictionaries
    are empty or not threaded into this helper yet.
    """
    edge = metric_edge_from_rankings(away_rankings, home_rankings, metric)
    if edge is not None:
        return edge

    edge = metric_edge_from_team_comparison(team_comparison, metric)
    if edge is not None:
        return edge

    return metric_edge_from_payload_rows(matchup_metric_rows, metric)


def calculate_offensive_efficiency_support_score(
    *,
    points_per_play_edge: Optional[float],
    yards_per_play_edge: Optional[float],
    yards_per_pass_edge: Optional[float],
    yards_per_rush_edge: Optional[float],
    side: str,
    metric: str | None = None,
) -> Optional[float]:
    """
    Runtime version of offensive_efficiency_support_v1.

    points_per_play is the required anchor. yards_per_play and yards_per_pass
    are measured/watch inputs. yards_per_rush only receives extra weight when
    the claim itself is rushing-scoped.
    """
    anchor = _side_signed_edge(points_per_play_edge, side)
    if anchor is None:
        return None

    components: list[tuple[float, float]] = [(0.55, anchor)]

    ypp = _side_signed_edge(yards_per_play_edge, side)
    if ypp is not None:
        components.append((0.20, ypp))

    ypa = _side_signed_edge(yards_per_pass_edge, side)
    if ypa is not None:
        components.append((0.15, ypa))

    ypr = _side_signed_edge(yards_per_rush_edge, side)
    if ypr is not None and metric == "yards_per_rush":
        components.append((0.10, ypr))

    total_weight = sum(weight for weight, _ in components)
    if total_weight <= 0:
        return None

    weighted = sum(weight * value for weight, value in components) / total_weight

    # Reward broad agreement very lightly and penalize conflicting support.
    values = [value for _, value in components]
    positive_values = [value for value in values if value > 0]
    negative_values = [value for value in values if value < 0]

    adjustment = 0.0
    if positive_values and not negative_values:
        adjustment = 0.05 * min(positive_values)
    elif positive_values and negative_values:
        adjustment = -0.05 * min(abs(value) for value in values)

    return round(clamp(weighted + adjustment), 4)


def bucket_offensive_efficiency_support_score(score: Optional[float]) -> tuple[str, str]:
    """Return bucket + strength for score-bearing offensive efficiency claims."""
    if score is None:
        return "anchor_unavailable", "unavailable"

    if score >= 0.45:
        return "repeat_positive_strong", "strong_support"
    if score >= 0.15:
        return "repeat_positive_supportive", "measured_support"
    if score >= -0.10:
        return "mixed_near_even", "mixed"
    if score >= -0.35:
        return "negative_caution", "caution"
    return "opposing_efficiency_signal", "caution"


def build_runtime_offensive_efficiency_support_by_side(
    *,
    away_rankings: dict | None = None,
    home_rankings: dict | None = None,
    team_comparison: list[dict] | None = None,
    matchup_breakdown: dict | None = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Build side-level offensive efficiency components for /game metadata.

    Source priority:
    1. ranking dictionaries keyed by metric
    2. visible Team Comparison rows
    3. Matchup Breakdown rows/drivers

    This keeps API smoke tests useful even when one upstream source is missing.
    Row-level bucket/strength decisions still happen per claim because some
    metrics are support-worthy while others are context-only or caution-only.
    """
    away_rankings = away_rankings or {}
    home_rankings = home_rankings or {}
    team_comparison = team_comparison or []
    matchup_metric_rows = collect_matchup_breakdown_metric_rows(matchup_breakdown or {})

    metric_edges = {
        metric: _first_available_metric_edge(
            metric=metric,
            away_rankings=away_rankings,
            home_rankings=home_rankings,
            team_comparison=team_comparison,
            matchup_metric_rows=matchup_metric_rows,
        )
        for metric in sorted(OFFENSIVE_EFFICIENCY_SUPPORT_METRICS)
    }

    output: Dict[str, Dict[str, Any]] = {}
    for side in ["away", "home"]:
        components = {
            metric: _side_signed_edge(edge, side)
            for metric, edge in metric_edges.items()
        }

        score = calculate_offensive_efficiency_support_score(
            points_per_play_edge=components.get("points_per_play"),
            yards_per_play_edge=components.get("yards_per_play"),
            yards_per_pass_edge=components.get("yards_per_pass"),
            yards_per_rush_edge=components.get("yards_per_rush"),
            # Components are already side-perspective, so reuse away orientation.
            side="away",
            metric=None,
        )
        bucket, strength = bucket_offensive_efficiency_support_score(score)

        output[side] = {
            "feature_version": OFFENSIVE_EFFICIENCY_SUPPORT_VERSION,
            "score": score,
            "bucket": bucket,
            "strength": strength,
            "components": components,
            "anchor_metric": "points_per_play",
            "anchor_available": components.get("points_per_play") is not None,
            "metadata_only": True,
            "language_boost_allowed": False,
        }

    return output

def build_offensive_efficiency_support_for_claim(
    *,
    claim_language_context: dict,
    side: str | None,
    metric: str | None,
) -> Dict[str, Any]:
    """
    Build row-level offensive_efficiency_support_v1 metadata.

    Metadata-only boundary: this object does not decide winner, matchup lean,
    confidence, Model Trust, or language_boost_allowed.
    """
    metric = str(metric or "").strip() or None

    base = {
        "feature_version": OFFENSIVE_EFFICIENCY_SUPPORT_VERSION,
        "score": None,
        "bucket": "not_relevant",
        "strength": "not_applicable",
        "reason": "offensive_efficiency_support_v1 not relevant to this claim family",
        "metrics": None,
        "language_boost_allowed": False,
        "metadata_only": True,
    }

    if metric in OFFENSIVE_EFFICIENCY_CAUTION_ONLY_METRICS:
        return {
            **base,
            "bucket": "caution_only",
            "strength": "caution_only",
            "reason": f"metric:{metric}:caution_only; excluded from support scoring by revalidation evidence",
            "metrics": metric,
        }

    if metric in OFFENSIVE_EFFICIENCY_CONTEXT_ONLY_METRICS:
        return {
            **base,
            "bucket": "context_only",
            "strength": "context_only",
            "reason": f"metric:{metric}:context_only; retained as context only, not stronger-language support",
            "metrics": metric,
        }

    if metric not in OFFENSIVE_EFFICIENCY_SUPPORT_METRICS:
        return base

    if side not in {"away", "home"}:
        return {
            **base,
            "bucket": "anchor_unavailable",
            "strength": "unavailable",
            "reason": f"metric:{metric}; side unavailable for offensive efficiency support",
            "metrics": metric,
        }

    side_payload = (
        claim_language_context
        .get("offensive_efficiency_support_by_side", {})
        .get(side, {})
    )
    components = side_payload.get("components") or {}

    points_per_play_edge = components.get("points_per_play")
    if points_per_play_edge is None:
        return {
            **base,
            "bucket": "anchor_unavailable",
            "strength": "unavailable",
            "reason": f"metric:{metric}; points_per_play anchor missing",
            "metrics": metric,
        }

    score = calculate_offensive_efficiency_support_score(
        points_per_play_edge=points_per_play_edge,
        yards_per_play_edge=components.get("yards_per_play"),
        yards_per_pass_edge=components.get("yards_per_pass"),
        yards_per_rush_edge=components.get("yards_per_rush"),
        side="away",  # components are already side-perspective.
        metric=metric,
    )
    bucket, strength = bucket_offensive_efficiency_support_score(score)

    return {
        **base,
        "score": score,
        "bucket": bucket,
        "strength": strength,
        "reason": (
            f"metric:{metric}; score anchored by repeat-positive points_per_play, "
            "with yards_per_play/yards_per_pass as watch inputs and "
            "yards_per_rush only when rushing-scoped"
        ),
        "metrics": ",".join(
            metric_name
            for metric_name in [
                "points_per_play",
                "yards_per_play",
                "yards_per_pass",
                "yards_per_rush" if metric == "yards_per_rush" else None,
            ]
            if metric_name and components.get(metric_name) is not None
        ),
        # Exposure only. Existing two_way_context/registry rules still decide boost.
        "language_boost_allowed": False,
        "metadata_only": True,
    }

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