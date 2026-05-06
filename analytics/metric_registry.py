"""
GameLens metric registry.

This module is a descriptive metadata registry for metrics.

Its job is to provide consistent color and meaning to existing metric data:
- display label
- plain-English definition
- category
- core_area
- comparison direction
- raw/derived status
- aggregation method/formula metadata
- display formatting hints
- ranking / edge / confidence usage guardrails
- lens tags for product/UI grouping

Important boundary:
This registry does NOT decide which metrics the model uses.
Model input selection should remain in the model/query/scoring layer after a separate audit.

Key design idea:
comparison_direction describes how values compare.
ranking_usage describes how the product is allowed to talk about the metric.
signal_strength describes how loudly the metric should speak when it is allowed to speak.

Default steering rule:
Only strong, good-quality edge metrics automatically contribute to Core Area Advantage
or confidence. Supporting metrics remain useful for explanation, but should not steer
the car unless manually opted in.

That prevents context/supporting metrics from accidentally becoming fake winner/confidence signals.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


CORE_AREAS = {
    "Disruption and Turnovers",
    "Field Control (Special Teams)",
    "Offensive Output",
    "Scoring Efficiency",
    "Defensive Control",
}

COMPARISON_DIRECTIONS = {"higher", "lower", "context"}
RAW_OR_DERIVED_VALUES = {"raw", "derived", "contextual"}
AGGREGATION_METHODS = {
    "sum",
    "ratio_from_sums",
    "mean_contextual",
    "latest",
}

# How rankings / explanations are allowed to use a metric.
#
# edge:
#   Metric can be ranked and can support better/worse language.
#
# context_only:
#   Metric can be ranked/displayed as style, volume, opportunity, pace, or identity,
#   but should not support "Team A has the edge" language.
#
# exclude:
#   Metric remains in the registry, but should be excluded from rankings and edge logic
#   until the definition/data quality improves.
RANKING_USAGE_VALUES = {"edge", "context_only", "exclude"}

# How strongly a metric should speak after it is deemed eligible.
#
# strong:
#   Cleaner efficiency/rate-style metric that can carry primary edge language and,
#   by default, contribute to Core Area Advantage / confidence if data quality is good.
#
# supporting:
#   Directional and football-relevant, but volume-sensitive, noisy, or context-dependent.
#   By default, supporting metrics can explain/support but do not contribute to
#   Core Area Advantage or confidence unless manually opted in.
#
# context:
#   Descriptive only. Useful for lenses, style, opportunity, pace, script, or environment.
#
# exclude:
#   Registered for completeness/formulas, but should not be used in rankings or edge logic.
SIGNAL_STRENGTH_VALUES = {"strong", "supporting", "context", "exclude"}

# Data-quality status lets us keep a metric registered without pretending it is equally trusted.
#
# good:
#   Usable as configured.
#
# watch:
#   Usable with caution. Generally okay for display/core-area context, but not confidence.
#
# exclude:
#   Do not use in rankings, edge language, core-area advantage, or confidence.
DATA_QUALITY_STATUS_VALUES = {"good", "watch", "exclude"}


EXPECTED_METRICS = {
    "defensive_snap_load",
    "total_defensive_snaps",
    "fourth_down_attempts",
    "fourth_down_conversions",
    "fourth_down_pct",
    "third_down_attempts",
    "third_down_conversions",
    "third_down_pct",
    "1st_down_rate",
    "first_downs",
    "offensive_snap_load",
    "pass_play_pct",
    "pass_run_ratio",
    "pass_td_share",
    "passing_tds_rushing_tds_sum",
    "run_play_pct",
    "rush_td_share",
    "time_of_possession",
    "total_drives",
    "total_offensive_snaps",
    "total_plays",
    "total_yards",
    "yards_per_play",
    "pass_attempts",
    "pass_completions",
    "passing_tds",
    "passing_yards",
    "yards_per_pass",
    "pressure_rate",
    "sack_to_turnover_ratio",
    "sack_yards_lost",
    "sacks",
    "sacks_plus_sacks_taken",
    "sacks_taken",
    "red_zone_attempts",
    "red_zone_efficiency",
    "red_zone_tds",
    "rushing_attempts",
    "rushing_tds",
    "rushing_yards",
    "yards_per_rush",
    "actual_points",
    "points_per_play",
    "td_rate",
    "points_allowed",
    "points_allowed_per_play",
    "special_teams_snap_pct",
    "total_snaps",
    "total_special_teams_snaps",
    "defensive_interceptions",
    "fumbles_lost",
    "fumbles_recovered",
    "interceptions_thrown",
    "turnover_margin",
    "defensive_success_rate",
    "opponent_total_plays",
    "points_allowed_per_yard",
    "yards_allowed",
}


def _default_ranking_usage(comparison_direction: str) -> str:
    """Default usage policy based on comparison direction."""
    if comparison_direction in {"higher", "lower"}:
        return "edge"
    return "context_only"


def _default_signal_strength(ranking_usage: str) -> str:
    """Default signal strength based on ranking usage."""
    if ranking_usage == "edge":
        return "supporting"
    if ranking_usage == "context_only":
        return "context"
    return "exclude"


def _metric(
    *,
    label: str,
    definition: str,
    category: str,
    core_area: str,
    comparison_direction: str,
    raw_or_derived: str,
    aggregation_method: str,
    numerator: Optional[str] = None,
    denominator: Optional[str] = None,
    format: str = "decimal",
    decimals: int = 3,
    notes: str,
    ranking_usage: Optional[str] = None,
    signal_strength: Optional[str] = None,
    edge_language_allowed: Optional[bool] = None,
    include_in_core_area_advantage: Optional[bool] = None,
    confidence_eligible: Optional[bool] = None,
    data_quality_status: str = "good",
    lens_tags: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Create a standardized descriptive metric config object.

    comparison_direction values:
    - "higher": higher values are generally better
    - "lower": lower values are generally better
    - "context": not inherently good or bad without more context

    ranking_usage values:
    - "edge": rankable and can support better/worse language
    - "context_only": rankable as style/context/opportunity only
    - "exclude": do not rank until the metric definition/data improves

    signal_strength values:
    - "strong": cleaner primary signal
    - "supporting": useful but should speak more softly
    - "context": descriptive only
    - "exclude": no ranking/edge usage
    """
    higher_is_better: Optional[bool]
    if comparison_direction == "higher":
        higher_is_better = True
    elif comparison_direction == "lower":
        higher_is_better = False
    else:
        higher_is_better = None

    resolved_ranking_usage = ranking_usage or _default_ranking_usage(comparison_direction)
    resolved_signal_strength = signal_strength or _default_signal_strength(
        resolved_ranking_usage
    )

    if edge_language_allowed is None:
        edge_language_allowed = (
            resolved_ranking_usage == "edge"
            and comparison_direction in {"higher", "lower"}
            and data_quality_status != "exclude"
        )

    if include_in_core_area_advantage is None:
        include_in_core_area_advantage = bool(
            edge_language_allowed
            and data_quality_status == "good"
            and resolved_signal_strength == "strong"
        )

    if confidence_eligible is None:
        confidence_eligible = bool(
            edge_language_allowed
            and data_quality_status == "good"
            and resolved_signal_strength == "strong"
        )

    return {
        "label": label,
        "definition": definition,
        "category": category,
        "core_area": core_area,
        "comparison_direction": comparison_direction,
        "higher_is_better": higher_is_better,
        "raw_or_derived": raw_or_derived,
        "aggregation_method": aggregation_method,
        "numerator": numerator,
        "denominator": denominator,
        "format": format,
        "decimals": decimals,
        "notes": notes,
        "ranking_usage": resolved_ranking_usage,
        "signal_strength": resolved_signal_strength,
        "edge_language_allowed": edge_language_allowed,
        "include_in_core_area_advantage": include_in_core_area_advantage,
        "confidence_eligible": confidence_eligible,
        "data_quality_status": data_quality_status,
        "lens_tags": lens_tags or [],
    }


METRIC_REGISTRY: Dict[str, Dict[str, Any]] = {
    # ------------------------------------------------------------------
    # Offensive Output — Passing Game
    # ------------------------------------------------------------------
    "passing_yards": _metric(
        label="Passing Yards",
        definition="Total yards gained through completed passes.",
        category="Passing Game",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Passing production total. Higher is generally useful, but volume-sensitive; "
            "prefer pairing with yards_per_pass, attempts, and scoring context."
        ),
        signal_strength="supporting",
        lens_tags=["passing-production", "offensive-output", "volume-sensitive"],
    ),
    "pass_completions": _metric(
        label="Pass Completions",
        definition="Total completed passes by the offense.",
        category="Passing Game",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Passing-volume context. More completions do not automatically mean better "
            "offense without attempts and efficiency."
        ),
        lens_tags=["passing-volume", "game-script", "offensive-style"],
    ),
    "pass_attempts": _metric(
        label="Pass Attempts",
        definition="Total pass attempts by the offense.",
        category="Passing Game",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Passing-volume/style context. High attempts can reflect pass-heavy identity, "
            "trailing script, or matchup plan."
        ),
        lens_tags=["passing-volume", "game-script", "offensive-style"],
    ),
    "passing_tds": _metric(
        label="Passing TDs",
        definition="Total touchdowns scored by passing.",
        category="Passing Game",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Passing scoring production. Higher is generally better, but touchdowns are "
            "game-script and red-zone dependent."
        ),
        signal_strength="supporting",
        lens_tags=["passing-production", "touchdowns", "scoring"],
    ),
    "yards_per_pass": _metric(
        label="Yards Per Pass",
        definition="Passing yards divided by pass attempts.",
        category="Passing Game",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="passing_yards",
        denominator="pass_attempts",
        format="decimal",
        decimals=2,
        notes=(
            "Passing efficiency metric. Higher is generally better and cleaner than raw "
            "passing volume, with small-sample caution."
        ),
        signal_strength="strong",
        lens_tags=["passing-efficiency", "explosiveness", "strong-signal"],
    ),

    # ------------------------------------------------------------------
    # Offensive Output — Rushing Game
    # ------------------------------------------------------------------
    "rushing_yards": _metric(
        label="Rushing Yards",
        definition="Total yards gained on rushing plays.",
        category="Rushing Game",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Rushing production total. Higher is generally useful, but volume-sensitive; "
            "prefer pairing with yards_per_rush and game script."
        ),
        signal_strength="supporting",
        lens_tags=["rushing-production", "offensive-output", "volume-sensitive"],
    ),
    "rushing_attempts": _metric(
        label="Rushing Attempts",
        definition="Total rushing attempts by the offense.",
        category="Rushing Game",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Rushing-volume/style context. High attempts can reflect control, run-heavy "
            "identity, or leading script."
        ),
        lens_tags=["rushing-volume", "game-script", "control-profile"],
    ),
    "rushing_tds": _metric(
        label="Rushing TDs",
        definition="Total touchdowns scored by rushing.",
        category="Rushing Game",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Rushing scoring production. Higher is generally better, but touchdowns depend "
            "on red-zone usage, short fields, and game script."
        ),
        signal_strength="supporting",
        lens_tags=["rushing-production", "touchdowns", "scoring"],
    ),
    "yards_per_rush": _metric(
        label="Yards Per Rush",
        definition="Rushing yards divided by rushing attempts.",
        category="Rushing Game",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="rushing_yards",
        denominator="rushing_attempts",
        format="decimal",
        decimals=2,
        notes=(
            "Rushing efficiency metric. Higher is generally better and helps separate "
            "rushing quality from volume."
        ),
        signal_strength="strong",
        lens_tags=["rushing-efficiency", "control-profile", "strong-signal"],
    ),

    # ------------------------------------------------------------------
    # Offensive Output — Offensive Rhythm
    # ------------------------------------------------------------------
    "total_yards": _metric(
        label="Total Yards",
        definition="Total offensive yards gained, including passing and rushing.",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Broad production total. Higher is generally useful, but yards alone can "
            "overstate offense if drives do not finish."
        ),
        signal_strength="supporting",
        lens_tags=["offensive-output", "yardage", "volume-sensitive"],
    ),
    "total_plays": _metric(
        label="Total Plays",
        definition="Total offensive plays run.",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Opportunity/pace context. More plays can create chances, but does not "
            "automatically mean better offense."
        ),
        lens_tags=["pace", "opportunity", "play-volume"],
    ),
    "yards_per_play": _metric(
        label="Yards Per Play",
        definition="Total offensive yards divided by total offensive plays.",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="total_yards",
        denominator="total_plays",
        format="decimal",
        decimals=2,
        notes=(
            "Core offensive efficiency metric. Higher is generally better and stronger "
            "than raw yardage totals."
        ),
        signal_strength="strong",
        lens_tags=["offensive-efficiency", "explosiveness", "strong-signal"],
    ),
    "first_downs": _metric(
        label="First Downs",
        definition="Total first downs earned by the offense.",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Drive-sustainability production. Higher is generally useful, but "
            "volume-sensitive; prefer pairing with 1st_down_rate."
        ),
        signal_strength="supporting",
        lens_tags=["drive-sustainability", "offensive-output", "volume-sensitive"],
    ),
    "1st_down_rate": _metric(
        label="First Down Rate",
        definition="First downs divided by total offensive plays.",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="first_downs",
        denominator="total_plays",
        format="percent",
        decimals=1,
        notes=(
            "Drive-efficiency metric. Higher is generally better because it shows how "
            "often plays help sustain drives."
        ),
        signal_strength="strong",
        lens_tags=["drive-sustainability", "offensive-efficiency", "strong-signal"],
    ),
    "total_drives": _metric(
        label="Total Drives",
        definition="Total offensive possessions or drives.",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Opportunity/pace context. More drives can mean more chances, faster game "
            "flow, or volatility."
        ),
        lens_tags=["pace", "opportunity", "drive-volume"],
    ),
    "time_of_possession": _metric(
        label="Time of Possession",
        definition="Total time the team possessed the ball.",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="duration_minutes",
        decimals=2,
        notes=(
            "Possession/control context. Useful with efficiency, scoring, and defensive "
            "workload; not a standalone strength metric."
        ),
        lens_tags=["possession", "control-profile", "pace"],
    ),
    "run_play_pct": _metric(
        label="Run Play %",
        definition="Percentage of offensive plays that were rushing attempts.",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="rushing_attempts",
        denominator="total_plays",
        format="percent",
        decimals=1,
        notes="Offensive style context. Higher means more run-heavy, not automatically better.",
        lens_tags=["run-heavy", "offensive-style", "game-script"],
    ),
    "pass_play_pct": _metric(
        label="Pass Play %",
        definition="Percentage of offensive plays that were pass attempts.",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="pass_attempts",
        denominator="total_plays",
        format="percent",
        decimals=1,
        notes="Offensive style context. Higher means more pass-heavy, not automatically better.",
        lens_tags=["pass-heavy", "offensive-style", "game-script"],
    ),
    "pass_run_ratio": _metric(
        label="Pass/Run Ratio",
        definition="Pass attempts divided by rushing attempts.",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="pass_attempts",
        denominator="rushing_attempts",
        format="decimal",
        decimals=2,
        notes="Offensive identity context. Higher means more pass-heavy, not automatically better.",
        lens_tags=["pass-heavy", "offensive-style", "game-script"],
    ),
    "passing_tds_rushing_tds_sum": _metric(
        label="Passing + Rushing TDs",
        definition="Passing touchdowns plus rushing touchdowns.",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Offensive touchdown production. Useful, but avoid double-counting with "
            "td_rate, actual_points, or separate pass/rush TD metrics."
        ),
        signal_strength="supporting",
        lens_tags=["touchdowns", "offensive-output", "scoring"],
    ),
    "pass_td_share": _metric(
        label="Passing TD Share",
        definition="Passing TDs divided by total passing plus rushing TDs.",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="passing_tds",
        denominator="passing_tds_rushing_tds_sum",
        format="percent",
        decimals=1,
        notes="Touchdown-environment context. Useful for pass-TD/player lenses, not team edge language.",
        lens_tags=["passing-td-environment", "td-share", "offensive-style"],
    ),
    "rush_td_share": _metric(
        label="Rushing TD Share",
        definition="Rushing TDs divided by total passing plus rushing TDs.",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="rushing_tds",
        denominator="passing_tds_rushing_tds_sum",
        format="percent",
        decimals=1,
        notes="Touchdown-environment context. Useful for rush-TD/control lenses, not team edge language.",
        lens_tags=["rushing-td-environment", "td-share", "offensive-style"],
    ),
    "total_offensive_snaps": _metric(
        label="Total Offensive Snaps",
        definition="Total offensive snaps played.",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Offensive workload/opportunity context. More snaps may show pace or rhythm, "
            "but not quality by itself."
        ),
        lens_tags=["snap-volume", "offensive-opportunity", "pace"],
    ),
    "offensive_snap_load": _metric(
        label="Offensive Snap Load",
        definition="Offensive snaps divided by total team snaps.",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="total_offensive_snaps",
        denominator="total_snaps",
        format="percent",
        decimals=1,
        notes=(
            "Snap-share context. Useful for workload/pace profile; should not drive edge "
            "or confidence language by itself."
        ),
        lens_tags=["snap-share", "offensive-opportunity", "pace"],
    ),

    # ------------------------------------------------------------------
    # Scoring Efficiency — Scoring Production
    # ------------------------------------------------------------------
    "actual_points": _metric(
        label="Actual Points",
        definition="Total points scored by the team.",
        category="Scoring Production",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Direct scoring production. Higher is better, but it is an outcome total; "
            "for pregame analysis, only prior games should be used."
        ),
        signal_strength="supporting",
        lens_tags=["scoring", "production", "outcome-total"],
    ),
    "points_per_play": _metric(
        label="Points Per Play",
        definition="Points scored divided by offensive plays.",
        category="Scoring Production",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="actual_points",
        denominator="total_plays",
        format="decimal",
        decimals=3,
        notes=(
            "Strong scoring-efficiency metric. Higher is generally better because it "
            "measures scoring output per opportunity."
        ),
        signal_strength="strong",
        lens_tags=["scoring-efficiency", "efficiency", "strong-signal"],
    ),
    "td_rate": _metric(
        label="TD Rate",
        definition="Passing plus rushing touchdowns divided by offensive plays.",
        category="Scoring Production",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="passing_tds_rushing_tds_sum",
        denominator="total_plays",
        format="percent",
        decimals=1,
        notes=(
            "Touchdown-efficiency metric. Higher is generally better, but it can overlap "
            "with other scoring metrics."
        ),
        signal_strength="strong",
        lens_tags=["touchdown-efficiency", "scoring-efficiency", "strong-signal"],
    ),

    # ------------------------------------------------------------------
    # Scoring Efficiency — Red Zone Finish
    # ------------------------------------------------------------------
    "red_zone_tds": _metric(
        label="Red Zone TDs",
        definition="Total touchdowns scored from red-zone opportunities.",
        category="Red Zone Finish",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Red-zone scoring production. Higher is useful, but volume-sensitive; pair with attempts and efficiency.",
        signal_strength="supporting",
        lens_tags=["red-zone", "touchdowns", "volume-sensitive"],
    ),
    "red_zone_attempts": _metric(
        label="Red Zone Attempts",
        definition="Total offensive red-zone opportunities.",
        category="Red Zone Finish",
        core_area="Scoring Efficiency",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Scoring-opportunity context. More attempts show chances, not finishing quality.",
        lens_tags=["red-zone", "opportunity", "scoring-chances"],
    ),
    "red_zone_efficiency": _metric(
        label="Red Zone Efficiency",
        definition="Red-zone touchdowns divided by red-zone attempts.",
        category="Red Zone Finish",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="red_zone_tds",
        denominator="red_zone_attempts",
        format="percent",
        decimals=1,
        notes="Red-zone finishing metric. Higher is generally better, with small-sample caution.",
        signal_strength="strong",
        lens_tags=["red-zone", "scoring-efficiency", "strong-signal"],
    ),

    # ------------------------------------------------------------------
    # Scoring Efficiency — Drive Conversion
    # ------------------------------------------------------------------
    "third_down_conversions": _metric(
        label="Third Down Conversions",
        definition="Total successful third-down conversions.",
        category="Drive Conversion",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Conversion production. Higher is useful, but attempt-sensitive; prefer pairing with third_down_pct.",
        signal_strength="supporting",
        lens_tags=["third-down", "drive-conversion", "volume-sensitive"],
    ),
    "third_down_attempts": _metric(
        label="Third Down Attempts",
        definition="Total third-down attempts.",
        category="Drive Conversion",
        core_area="Scoring Efficiency",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Drive-context metric. More attempts can mean sustained drives or poor early-down efficiency.",
        lens_tags=["third-down", "opportunity", "drive-context"],
    ),
    "third_down_pct": _metric(
        label="Third Down %",
        definition="Third-down conversions divided by third-down attempts.",
        category="Drive Conversion",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="third_down_conversions",
        denominator="third_down_attempts",
        format="percent",
        decimals=1,
        notes="Drive-conversion efficiency. Higher is generally better, with distance-to-go and sample-size context.",
        signal_strength="strong",
        lens_tags=["third-down", "drive-efficiency", "strong-signal"],
    ),
    "fourth_down_conversions": _metric(
        label="Fourth Down Conversions",
        definition="Total successful fourth-down conversions.",
        category="Drive Conversion",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Conversion production. Higher can show execution, but is attempt-sensitive and coaching/script dependent.",
        signal_strength="supporting",
        lens_tags=["fourth-down", "drive-conversion", "volume-sensitive"],
    ),
    "fourth_down_attempts": _metric(
        label="Fourth Down Attempts",
        definition="Total fourth-down attempts.",
        category="Drive Conversion",
        core_area="Scoring Efficiency",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Aggression/desperation context. Attempts show situation and coaching tendency, not strength by themselves.",
        lens_tags=["fourth-down", "aggression", "coaching-tendency"],
    ),
    "fourth_down_pct": _metric(
        label="Fourth Down %",
        definition="Fourth-down conversions divided by fourth-down attempts.",
        category="Drive Conversion",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="fourth_down_conversions",
        denominator="fourth_down_attempts",
        format="percent",
        decimals=1,
        notes="Fourth-down execution metric. Higher is generally better, but sample sizes are often small.",
        signal_strength="supporting",
        lens_tags=["fourth-down", "drive-efficiency", "small-sample"],
    ),

    # ------------------------------------------------------------------
    # Defensive Control — Yardage Suppression
    # ------------------------------------------------------------------
    "yards_allowed": _metric(
        label="Yards Allowed",
        definition="Total yards allowed by the defense.",
        category="Yardage Suppression",
        core_area="Defensive Control",
        comparison_direction="lower",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Defensive yardage-allowed total. Lower is generally better, but "
            "volume-sensitive; prefer pairing with opponent plays and per-play metrics."
        ),
        signal_strength="supporting",
        lens_tags=["defense", "yardage-suppression", "volume-sensitive"],
    ),
    "opponent_total_plays": _metric(
        label="Opponent Total Plays",
        definition="Total plays run by opponents against the team.",
        category="Yardage Suppression",
        core_area="Defensive Control",
        comparison_direction="lower",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Defensive exposure metric. Lower is generally preferable, but pace, opponent "
            "style, and game script matter."
        ),
        signal_strength="supporting",
        lens_tags=["defensive-exposure", "opponent-volume", "pace"],
    ),
    "defensive_success_rate": _metric(
        label="Defensive Success Rate",
        definition="Yards allowed divided by opponent total plays.",
        category="Yardage Suppression",
        core_area="Defensive Control",
        comparison_direction="lower",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="yards_allowed",
        denominator="opponent_total_plays",
        format="decimal",
        decimals=2,
        notes=(
            "Current formula behaves like yards allowed per opponent play, not true "
            "football success rate. Lower is better until renamed/redefined."
        ),
        signal_strength="supporting",
        data_quality_status="watch",
        confidence_eligible=False,
        lens_tags=["defense", "yardage-efficiency", "naming-review", "data-quality-watch"],
    ),
    "points_allowed_per_yard": _metric(
        label="Points Allowed Per Yard",
        definition="Points allowed divided by yards allowed.",
        category="Yardage Suppression",
        core_area="Defensive Control",
        comparison_direction="lower",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="points_allowed",
        denominator="yards_allowed",
        format="decimal",
        decimals=3,
        notes=(
            "Defensive scoring-allowed context. Lower is generally better, but can behave "
            "strangely with short fields or low yardage totals."
        ),
        signal_strength="supporting",
        data_quality_status="watch",
        confidence_eligible=False,
        lens_tags=["defense", "scoring-suppression", "data-quality-watch"],
    ),

    # ------------------------------------------------------------------
    # Defensive Control — Scoring Suppression
    # ------------------------------------------------------------------
    "points_allowed": _metric(
        label="Points Allowed",
        definition="Total points allowed by the team.",
        category="Scoring Suppression",
        core_area="Defensive Control",
        comparison_direction="lower",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Defensive scoring-allowed total. Lower is generally better, but can be "
            "influenced by turnovers, field position, pace, and opponent strength."
        ),
        signal_strength="supporting",
        lens_tags=["defense", "scoring-suppression", "volume-sensitive"],
    ),
    "points_allowed_per_play": _metric(
        label="Points Allowed Per Play",
        definition="Points allowed divided by opponent total plays.",
        category="Scoring Suppression",
        core_area="Defensive Control",
        comparison_direction="lower",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="points_allowed",
        denominator="opponent_total_plays",
        format="decimal",
        decimals=3,
        notes=(
            "Defensive scoring-efficiency-allowed metric. Lower is generally better and "
            "cleaner than raw points allowed."
        ),
        signal_strength="strong",
        lens_tags=["defense", "scoring-efficiency-allowed", "strong-signal"],
    ),

    # ------------------------------------------------------------------
    # Defensive Control — Defensive Workload
    # ------------------------------------------------------------------
    "total_defensive_snaps": _metric(
        label="Total Defensive Snaps",
        definition="Total defensive snaps played.",
        category="Defensive Workload",
        core_area="Defensive Control",
        comparison_direction="lower",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Defensive workload/exposure metric. Lower is generally preferable, but pace, "
            "offensive performance, and game script matter."
        ),
        signal_strength="supporting",
        lens_tags=["defensive-workload", "fatigue", "snap-volume"],
    ),
    "defensive_snap_load": _metric(
        label="Defensive Snap Load",
        definition="Defensive snaps divided by total team snaps.",
        category="Defensive Workload",
        core_area="Defensive Control",
        comparison_direction="lower",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="total_defensive_snaps",
        denominator="total_snaps",
        format="percent",
        decimals=1,
        notes=(
            "Defensive workload share. Lower is probably preferable, but keep confidence "
            "disabled until snap data quality is trusted."
        ),
        signal_strength="supporting",
        data_quality_status="watch",
        confidence_eligible=False,
        lens_tags=["defensive-workload", "fatigue", "snap-share", "data-quality-watch"],
    ),

    # ------------------------------------------------------------------
    # Disruption and Turnovers — Pressure
    # ------------------------------------------------------------------
    "sacks": _metric(
        label="Sacks",
        definition="Total sacks recorded by the team defense.",
        category="Pressure",
        core_area="Disruption and Turnovers",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Defensive disruption metric. Higher is generally better if this field is confirmed as defensive sacks created.",
        signal_strength="supporting",
        lens_tags=["pressure", "disruption", "negative-plays"],
    ),
    "pressure_rate": _metric(
        label="Pressure Rate",
        definition="Sacks plus sacks taken divided by pass attempts.",
        category="Pressure",
        core_area="Disruption and Turnovers",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="sacks_plus_sacks_taken",
        denominator="pass_attempts",
        format="percent",
        decimals=1,
        notes="Exclude from rankings for now. Current definition mixes pressure created and pressure allowed.",
        ranking_usage="exclude",
        data_quality_status="exclude",
        lens_tags=["pressure", "data-quality-review", "excluded"],
    ),
    "sacks_taken": _metric(
        label="Sacks Taken",
        definition="Total times the team’s quarterback was sacked.",
        category="Pressure",
        core_area="Disruption and Turnovers",
        comparison_direction="lower",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Offensive protection/negative-play metric. Lower is generally better.",
        signal_strength="supporting",
        lens_tags=["protection", "negative-plays", "pressure-allowed"],
    ),
    "sack_yards_lost": _metric(
        label="Sack Yards Lost",
        definition="Total yards lost by the offense on sacks.",
        category="Pressure",
        core_area="Disruption and Turnovers",
        comparison_direction="lower",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Offensive negative-yardage metric. Lower is generally better.",
        signal_strength="supporting",
        lens_tags=["protection", "negative-plays", "pressure-allowed"],
    ),
    "sacks_plus_sacks_taken": _metric(
        label="Sacks + Sacks Taken",
        definition="Sacks recorded plus sacks taken.",
        category="Pressure",
        core_area="Disruption and Turnovers",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Formula ingredient only. Mixed good/bad components make it unsafe for rankings or edge language.",
        ranking_usage="exclude",
        data_quality_status="exclude",
        lens_tags=["pressure", "formula-ingredient", "excluded"],
    ),
    "sack_to_turnover_ratio": _metric(
        label="Sack to Turnover Ratio",
        definition="Sacks divided by defensive interceptions.",
        category="Pressure",
        core_area="Disruption and Turnovers",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="sacks",
        denominator="defensive_interceptions",
        format="decimal",
        decimals=2,
        notes="Exclude from rankings for now. Not a clean higher/lower signal and easy to misinterpret.",
        ranking_usage="exclude",
        data_quality_status="exclude",
        lens_tags=["pressure", "turnovers", "data-quality-review", "excluded"],
    ),

    # ------------------------------------------------------------------
    # Disruption and Turnovers — Turnovers
    # ------------------------------------------------------------------
    "defensive_interceptions": _metric(
        label="Defensive Interceptions",
        definition="Total interceptions recorded by the team defense.",
        category="Turnovers",
        core_area="Disruption and Turnovers",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Takeaway production. Higher is generally better, but interceptions are "
            "volatile and impact depends on field position/timing."
        ),
        signal_strength="supporting",
        lens_tags=["turnovers", "takeaways", "volatility"],
    ),
    "fumbles_recovered": _metric(
        label="Fumbles Recovered",
        definition="Total opponent fumbles recovered by the team.",
        category="Turnovers",
        core_area="Disruption and Turnovers",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Takeaway production. Higher is generally better, but fumble recovery is especially volatile.",
        signal_strength="supporting",
        lens_tags=["turnovers", "takeaways", "volatility"],
    ),
    "interceptions_thrown": _metric(
        label="Interceptions Thrown",
        definition="Total interceptions thrown by the team offense.",
        category="Turnovers",
        core_area="Disruption and Turnovers",
        comparison_direction="lower",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Giveaway-risk metric. Lower is generally better.",
        signal_strength="supporting",
        lens_tags=["turnovers", "giveaways", "risk"],
    ),
    "fumbles_lost": _metric(
        label="Fumbles Lost",
        definition="Total fumbles lost by the team offense.",
        category="Turnovers",
        core_area="Disruption and Turnovers",
        comparison_direction="lower",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Giveaway-risk metric. Lower is generally better, but fumbles can be noisy.",
        signal_strength="supporting",
        lens_tags=["turnovers", "giveaways", "risk", "volatility"],
    ),
    "turnover_margin": _metric(
        label="Turnover Margin",
        definition="Takeaways minus giveaways.",
        category="Turnovers",
        core_area="Disruption and Turnovers",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes=(
            "Turnover-balance metric. Higher is generally better, but turnover signals "
            "are volatile and should not dominate confidence alone."
        ),
        signal_strength="strong",
        lens_tags=["turnovers", "takeaway-margin", "strong-signal", "volatility"],
    ),

    # ------------------------------------------------------------------
    # Field Control (Special Teams) — Special Teams Usage
    # ------------------------------------------------------------------
    "total_special_teams_snaps": _metric(
        label="Total Special Teams Snaps",
        definition="Total special teams snaps played.",
        category="Special Teams Usage",
        core_area="Field Control (Special Teams)",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Special teams usage context. Does not measure field-position quality or special teams performance by itself.",
        lens_tags=["special-teams", "snap-volume", "usage-context"],
    ),
    "special_teams_snap_pct": _metric(
        label="Special Teams Snap %",
        definition="Special teams snaps divided by total team snaps.",
        category="Special Teams Usage",
        core_area="Field Control (Special Teams)",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="total_special_teams_snaps",
        denominator="total_snaps",
        format="percent",
        decimals=1,
        notes="Special teams usage share. Useful for profile context, not field-control edge language by itself.",
        lens_tags=["special-teams", "snap-share", "usage-context"],
    ),
    "total_snaps": _metric(
        label="Total Snaps",
        definition="Total team snaps across offense, defense, and special teams.",
        category="Special Teams Usage",
        core_area="Field Control (Special Teams)",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Snap-volume denominator/context. Useful for snap-share calculations, not a direct strength metric.",
        lens_tags=["snap-volume", "formula-ingredient", "pace"],
    ),
}


REQUIRED_FIELDS = {
    "label",
    "definition",
    "category",
    "core_area",
    "comparison_direction",
    "higher_is_better",
    "raw_or_derived",
    "aggregation_method",
    "numerator",
    "denominator",
    "format",
    "decimals",
    "notes",
    "ranking_usage",
    "signal_strength",
    "edge_language_allowed",
    "include_in_core_area_advantage",
    "confidence_eligible",
    "data_quality_status",
    "lens_tags",
}


def get_metric_config(metric: str) -> Dict[str, Any]:
    """Return the full registry config for one metric."""
    if metric not in METRIC_REGISTRY:
        raise KeyError(f"Metric not found in METRIC_REGISTRY: {metric}")
    return METRIC_REGISTRY[metric]


def get_metric_meta(metric: str) -> Dict[str, Any]:
    """Return descriptive metadata fields commonly attached to output rows."""
    cfg = get_metric_config(metric)
    return {
        "label": cfg["label"],
        "definition": cfg["definition"],
        "category": cfg["category"],
        "core_area": cfg["core_area"],
        "comparison_direction": cfg["comparison_direction"],
        "higher_is_better": cfg["higher_is_better"],
        "raw_or_derived": cfg["raw_or_derived"],
        "aggregation_method": cfg["aggregation_method"],
        "numerator": cfg["numerator"],
        "denominator": cfg["denominator"],
        "format": cfg["format"],
        "decimals": cfg["decimals"],
        "notes": cfg["notes"],
        "ranking_usage": cfg["ranking_usage"],
        "signal_strength": cfg["signal_strength"],
        "edge_language_allowed": cfg["edge_language_allowed"],
        "include_in_core_area_advantage": cfg["include_in_core_area_advantage"],
        "confidence_eligible": cfg["confidence_eligible"],
        "data_quality_status": cfg["data_quality_status"],
        "lens_tags": cfg["lens_tags"],
    }


def get_derived_metric_formulas() -> Dict[str, Dict[str, Optional[str]]]:
    """Return formula metadata for derived metrics.

    This mirrors the old DERIVED_METRICS concept without deciding model usage.
    """
    return {
        metric: {
            "numerator": cfg["numerator"],
            "denominator": cfg["denominator"],
            "aggregation_method": cfg["aggregation_method"],
        }
        for metric, cfg in METRIC_REGISTRY.items()
        if cfg["raw_or_derived"] == "derived"
    }


def get_raw_metrics() -> List[str]:
    """Return raw metrics currently described by the registry."""
    return [
        metric
        for metric, cfg in METRIC_REGISTRY.items()
        if cfg["raw_or_derived"] == "raw"
    ]


def get_metrics_by_core_area(core_area: str) -> List[str]:
    """Return all registered metrics in a core area, including context/excluded metrics."""
    if core_area not in CORE_AREAS:
        raise ValueError(f"Invalid core_area: {core_area}")
    return [
        metric
        for metric, cfg in METRIC_REGISTRY.items()
        if cfg["core_area"] == core_area
    ]


def is_rankable_metric(metric: str) -> bool:
    """Return True if the metric may appear in any ranking view."""
    cfg = get_metric_config(metric)
    return cfg["ranking_usage"] in {"edge", "context_only"}


def is_edge_metric(metric: str) -> bool:
    """Return True if the metric can support better/worse edge language."""
    cfg = get_metric_config(metric)
    return (
        cfg["ranking_usage"] == "edge"
        and cfg["edge_language_allowed"] is True
        and cfg["comparison_direction"] in {"higher", "lower"}
        and cfg["data_quality_status"] != "exclude"
    )


def is_context_only_metric(metric: str) -> bool:
    """Return True if the metric can be ranked/displayed only as context."""
    cfg = get_metric_config(metric)
    return cfg["ranking_usage"] == "context_only"


def is_excluded_metric(metric: str) -> bool:
    """Return True if the metric is registered but intentionally excluded from rankings."""
    cfg = get_metric_config(metric)
    return cfg["ranking_usage"] == "exclude" or cfg["data_quality_status"] == "exclude"


def include_in_core_area_advantage(metric: str) -> bool:
    """Return True if the metric can help decide a Core Area Advantage winner."""
    cfg = get_metric_config(metric)
    return (
        cfg["include_in_core_area_advantage"] is True
        and cfg["data_quality_status"] != "exclude"
    )


def is_confidence_eligible_metric(metric: str) -> bool:
    """Return True if the metric can influence confidence/model-trust language."""
    cfg = get_metric_config(metric)
    return (
        cfg["confidence_eligible"] is True
        and cfg["edge_language_allowed"] is True
        and cfg["comparison_direction"] in {"higher", "lower"}
        and cfg["data_quality_status"] == "good"
    )


def get_edge_metrics() -> List[str]:
    """Return metrics that can support better/worse edge language."""
    return [metric for metric in METRIC_REGISTRY if is_edge_metric(metric)]


def get_context_only_metrics() -> List[str]:
    """Return metrics that can be ranked/displayed only as context."""
    return [metric for metric in METRIC_REGISTRY if is_context_only_metric(metric)]


def get_excluded_metrics() -> List[str]:
    """Return metrics intentionally excluded from rankings/edge logic."""
    return [metric for metric in METRIC_REGISTRY if is_excluded_metric(metric)]


def get_metrics_by_signal_strength(signal_strength: str) -> List[str]:
    """Return metrics for one signal strength bucket."""
    if signal_strength not in SIGNAL_STRENGTH_VALUES:
        raise ValueError(f"Invalid signal_strength: {signal_strength}")
    return [
        metric
        for metric, cfg in METRIC_REGISTRY.items()
        if cfg["signal_strength"] == signal_strength
    ]


def get_core_area_advantage_metrics(core_area: str) -> List[str]:
    """Return metrics allowed to contribute to Core Area Advantage for a core area."""
    if core_area not in CORE_AREAS:
        raise ValueError(f"Invalid core_area: {core_area}")
    return [
        metric
        for metric, cfg in METRIC_REGISTRY.items()
        if cfg["core_area"] == core_area and include_in_core_area_advantage(metric)
    ]


def validate_metric_registry() -> None:
    """Fail loudly if the descriptive registry is incomplete or inconsistent."""
    registry_metrics = set(METRIC_REGISTRY.keys())

    missing_from_registry = sorted(EXPECTED_METRICS - registry_metrics)
    extra_in_registry = sorted(registry_metrics - EXPECTED_METRICS)

    if missing_from_registry or extra_in_registry:
        raise ValueError(
            "Metric registry coverage mismatch. "
            f"Missing from registry: {missing_from_registry}. "
            f"Extra in registry: {extra_in_registry}."
        )

    for metric, cfg in METRIC_REGISTRY.items():
        missing_fields = sorted(REQUIRED_FIELDS - set(cfg.keys()))
        if missing_fields:
            raise ValueError(f"{metric} missing required fields: {missing_fields}")

        if not isinstance(cfg["label"], str) or not cfg["label"].strip():
            raise ValueError(f"{metric} label must be a non-empty string")

        if not isinstance(cfg["definition"], str) or not cfg["definition"].strip():
            raise ValueError(f"{metric} definition must be a non-empty string")

        if not isinstance(cfg["notes"], str) or not cfg["notes"].strip():
            raise ValueError(f"{metric} notes must be a non-empty string")

        if cfg["core_area"] not in CORE_AREAS:
            raise ValueError(f"{metric} has invalid core_area: {cfg['core_area']}")

        if cfg["comparison_direction"] not in COMPARISON_DIRECTIONS:
            raise ValueError(
                f"{metric} has invalid comparison_direction: "
                f"{cfg['comparison_direction']}"
            )

        expected_higher_is_better = {
            "higher": True,
            "lower": False,
            "context": None,
        }[cfg["comparison_direction"]]

        if cfg["higher_is_better"] is not expected_higher_is_better:
            raise ValueError(
                f"{metric} has inconsistent higher_is_better. "
                f"comparison_direction={cfg['comparison_direction']} should imply "
                f"higher_is_better={expected_higher_is_better}."
            )

        if cfg["raw_or_derived"] not in RAW_OR_DERIVED_VALUES:
            raise ValueError(
                f"{metric} has invalid raw_or_derived: {cfg['raw_or_derived']}"
            )

        if cfg["aggregation_method"] not in AGGREGATION_METHODS:
            raise ValueError(
                f"{metric} has invalid aggregation_method: "
                f"{cfg['aggregation_method']}"
            )

        if cfg["ranking_usage"] not in RANKING_USAGE_VALUES:
            raise ValueError(
                f"{metric} has invalid ranking_usage: {cfg['ranking_usage']}"
            )

        if cfg["signal_strength"] not in SIGNAL_STRENGTH_VALUES:
            raise ValueError(
                f"{metric} has invalid signal_strength: {cfg['signal_strength']}"
            )

        if cfg["data_quality_status"] not in DATA_QUALITY_STATUS_VALUES:
            raise ValueError(
                f"{metric} has invalid data_quality_status: "
                f"{cfg['data_quality_status']}"
            )

        if not isinstance(cfg["edge_language_allowed"], bool):
            raise ValueError(f"{metric} edge_language_allowed must be bool")

        if not isinstance(cfg["include_in_core_area_advantage"], bool):
            raise ValueError(f"{metric} include_in_core_area_advantage must be bool")

        if not isinstance(cfg["confidence_eligible"], bool):
            raise ValueError(f"{metric} confidence_eligible must be bool")

        if not isinstance(cfg["lens_tags"], list):
            raise ValueError(f"{metric} lens_tags must be a list")

        if cfg["comparison_direction"] == "context" and cfg["ranking_usage"] == "edge":
            raise ValueError(
                f"{metric} cannot have comparison_direction='context' "
                "and ranking_usage='edge'."
            )

        if cfg["ranking_usage"] == "edge":
            if cfg["signal_strength"] not in {"strong", "supporting"}:
                raise ValueError(
                    f"{metric} has ranking_usage='edge' but signal_strength="
                    f"{cfg['signal_strength']}"
                )

        if cfg["ranking_usage"] == "context_only":
            if cfg["signal_strength"] != "context":
                raise ValueError(
                    f"{metric} is context_only but signal_strength="
                    f"{cfg['signal_strength']}"
                )
            if cfg["edge_language_allowed"]:
                raise ValueError(
                    f"{metric} is context_only but edge_language_allowed=True"
                )
            if cfg["include_in_core_area_advantage"]:
                raise ValueError(
                    f"{metric} is context_only but include_in_core_area_advantage=True"
                )
            if cfg["confidence_eligible"]:
                raise ValueError(f"{metric} is context_only but confidence_eligible=True")

        if cfg["ranking_usage"] == "exclude":
            if cfg["signal_strength"] != "exclude":
                raise ValueError(
                    f"{metric} is excluded but signal_strength="
                    f"{cfg['signal_strength']}"
                )
            if cfg["edge_language_allowed"]:
                raise ValueError(f"{metric} is excluded but edge_language_allowed=True")
            if cfg["include_in_core_area_advantage"]:
                raise ValueError(
                    f"{metric} is excluded but include_in_core_area_advantage=True"
                )
            if cfg["confidence_eligible"]:
                raise ValueError(f"{metric} is excluded but confidence_eligible=True")

        if cfg["data_quality_status"] == "exclude" and cfg["ranking_usage"] != "exclude":
            raise ValueError(
                f"{metric} has data_quality_status='exclude' but ranking_usage is not exclude"
            )

        if cfg["edge_language_allowed"] and cfg["comparison_direction"] not in {"higher", "lower"}:
            raise ValueError(
                f"{metric} allows edge language but has comparison_direction="
                f"{cfg['comparison_direction']}"
            )

        if cfg["confidence_eligible"]:
            if not cfg["edge_language_allowed"]:
                raise ValueError(
                    f"{metric} is confidence_eligible but edge_language_allowed=False"
                )
            if cfg["data_quality_status"] != "good":
                raise ValueError(
                    f"{metric} is confidence_eligible but data_quality_status is not good"
                )

        if cfg["aggregation_method"] == "ratio_from_sums":
            if not cfg.get("numerator") or not cfg.get("denominator"):
                raise ValueError(
                    f"{metric} uses ratio_from_sums but is missing numerator/denominator"
                )

            numerator = cfg["numerator"]
            denominator = cfg["denominator"]
            if numerator not in METRIC_REGISTRY:
                raise ValueError(f"{metric} numerator not found in registry: {numerator}")
            if denominator not in METRIC_REGISTRY:
                raise ValueError(f"{metric} denominator not found in registry: {denominator}")


# Validate at import time so bad metadata fails fast in ETL/API jobs.
validate_metric_registry()
