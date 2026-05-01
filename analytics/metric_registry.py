"""
GameLens metric registry.

This module is a descriptive metadata registry for metrics.

Its job is to provide consistent color and meaning to existing metric data:
- display label
- category
- core_area
- comparison direction
- raw/derived status
- aggregation method/formula metadata
- display formatting hints

Important boundary:
This registry does NOT decide which metrics the model uses.
Model input selection should remain in the model/query/scoring layer after a separate audit.
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


def _metric(
    *,
    label: str,
    category: str,
    core_area: str,
    comparison_direction: str,
    raw_or_derived: str,
    aggregation_method: str,
    numerator: Optional[str] = None,
    denominator: Optional[str] = None,
    format: str = "decimal",
    decimals: int = 3,
    notes: str = "",
) -> Dict[str, Any]:
    """Create a standardized descriptive metric config object.

    comparison_direction values:
    - "higher": higher values are generally better
    - "lower": lower values are generally better
    - "context": not inherently good or bad without more context
    """
    higher_is_better: Optional[bool]
    if comparison_direction == "higher":
        higher_is_better = True
    elif comparison_direction == "lower":
        higher_is_better = False
    else:
        higher_is_better = None

    return {
        "label": label,
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
    }


METRIC_REGISTRY: Dict[str, Dict[str, Any]] = {
    # ------------------------------------------------------------------
    # Offensive Output — Passing Game
    # ------------------------------------------------------------------
    "passing_yards": _metric(
        label="Passing Yards",
        category="Passing Game",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "pass_completions": _metric(
        label="Pass Completions",
        category="Passing Game",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Volume metric; useful context but not automatically better without attempts/efficiency.",
    ),
    "pass_attempts": _metric(
        label="Pass Attempts",
        category="Passing Game",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Volume/style metric; often reflects game script.",
    ),
    "passing_tds": _metric(
        label="Passing TDs",
        category="Passing Game",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "yards_per_pass": _metric(
        label="Yards Per Pass",
        category="Passing Game",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="passing_yards",
        denominator="pass_attempts",
        format="decimal",
        decimals=2,
    ),

    # ------------------------------------------------------------------
    # Offensive Output — Rushing Game
    # ------------------------------------------------------------------
    "rushing_yards": _metric(
        label="Rushing Yards",
        category="Rushing Game",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "rushing_attempts": _metric(
        label="Rushing Attempts",
        category="Rushing Game",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Volume/style metric; often reflects game script and control.",
    ),
    "rushing_tds": _metric(
        label="Rushing TDs",
        category="Rushing Game",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "yards_per_rush": _metric(
        label="Yards Per Rush",
        category="Rushing Game",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="rushing_yards",
        denominator="rushing_attempts",
        format="decimal",
        decimals=2,
    ),

    # ------------------------------------------------------------------
    # Offensive Output — Offensive Rhythm
    # ------------------------------------------------------------------
    "total_yards": _metric(
        label="Total Yards",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "total_plays": _metric(
        label="Total Plays",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Important denominator for multiple efficiency metrics.",
    ),
    "yards_per_play": _metric(
        label="Yards Per Play",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="total_yards",
        denominator="total_plays",
        format="decimal",
        decimals=2,
    ),
    "first_downs": _metric(
        label="First Downs",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "1st_down_rate": _metric(
        label="First Down Rate",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="first_downs",
        denominator="total_plays",
        format="percent",
        decimals=1,
    ),
    "total_drives": _metric(
        label="Total Drives",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "time_of_possession": _metric(
        label="Time of Possession",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="duration_minutes",
        decimals=2,
    ),
    "run_play_pct": _metric(
        label="Run Play %",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="rushing_attempts",
        denominator="total_plays",
        format="percent",
        decimals=1,
        notes="Style/game-script metric; not inherently good or bad.",
    ),
    "pass_play_pct": _metric(
        label="Pass Play %",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="pass_attempts",
        denominator="total_plays",
        format="percent",
        decimals=1,
        notes="Style/game-script metric; not inherently good or bad.",
    ),
    "pass_run_ratio": _metric(
        label="Pass/Run Ratio",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="pass_attempts",
        denominator="rushing_attempts",
        format="decimal",
        decimals=2,
        notes="Style/game-script metric; not inherently good or bad.",
    ),
    "passing_tds_rushing_tds_sum": _metric(
        label="Passing + Rushing TDs",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Formula ingredient for TD rate and TD share metrics.",
    ),
    "pass_td_share": _metric(
        label="Passing TD Share",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="passing_tds",
        denominator="passing_tds_rushing_tds_sum",
        format="percent",
        decimals=1,
        notes="Identity/style metric; not inherently good or bad.",
    ),
    "rush_td_share": _metric(
        label="Rushing TD Share",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="rushing_tds",
        denominator="passing_tds_rushing_tds_sum",
        format="percent",
        decimals=1,
        notes="Identity/style metric; not inherently good or bad.",
    ),
    "total_offensive_snaps": _metric(
        label="Total Offensive Snaps",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "offensive_snap_load": _metric(
        label="Offensive Snap Load",
        category="Offensive Rhythm",
        core_area="Offensive Output",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="total_offensive_snaps",
        denominator="total_snaps",
        format="percent",
        decimals=1,
    ),

    # ------------------------------------------------------------------
    # Scoring Efficiency — Scoring Production
    # ------------------------------------------------------------------
    "actual_points": _metric(
        label="Actual Points",
        category="Scoring Production",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "points_per_play": _metric(
        label="Points Per Play",
        category="Scoring Production",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="actual_points",
        denominator="total_plays",
        format="decimal",
        decimals=3,
    ),
    "td_rate": _metric(
        label="TD Rate",
        category="Scoring Production",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="passing_tds_rushing_tds_sum",
        denominator="total_plays",
        format="percent",
        decimals=1,
    ),

    # ------------------------------------------------------------------
    # Scoring Efficiency — Red Zone Finish
    # ------------------------------------------------------------------
    "red_zone_tds": _metric(
        label="Red Zone TDs",
        category="Red Zone Finish",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "red_zone_attempts": _metric(
        label="Red Zone Attempts",
        category="Red Zone Finish",
        core_area="Scoring Efficiency",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Opportunity/volume metric; efficiency should come from red_zone_efficiency.",
    ),
    "red_zone_efficiency": _metric(
        label="Red Zone Efficiency",
        category="Red Zone Finish",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="red_zone_tds",
        denominator="red_zone_attempts",
        format="percent",
        decimals=1,
    ),

    # ------------------------------------------------------------------
    # Scoring Efficiency — Drive Conversion
    # ------------------------------------------------------------------
    "third_down_conversions": _metric(
        label="Third Down Conversions",
        category="Drive Conversion",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "third_down_attempts": _metric(
        label="Third Down Attempts",
        category="Drive Conversion",
        core_area="Scoring Efficiency",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "third_down_pct": _metric(
        label="Third Down %",
        category="Drive Conversion",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="third_down_conversions",
        denominator="third_down_attempts",
        format="percent",
        decimals=1,
    ),
    "fourth_down_conversions": _metric(
        label="Fourth Down Conversions",
        category="Drive Conversion",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "fourth_down_attempts": _metric(
        label="Fourth Down Attempts",
        category="Drive Conversion",
        core_area="Scoring Efficiency",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "fourth_down_pct": _metric(
        label="Fourth Down %",
        category="Drive Conversion",
        core_area="Scoring Efficiency",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="fourth_down_conversions",
        denominator="fourth_down_attempts",
        format="percent",
        decimals=1,
    ),

    # ------------------------------------------------------------------
    # Defensive Control — Yardage Suppression
    # ------------------------------------------------------------------
    "yards_allowed": _metric(
        label="Yards Allowed",
        category="Yardage Suppression",
        core_area="Defensive Control",
        comparison_direction="lower",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "opponent_total_plays": _metric(
        label="Opponent Total Plays",
        category="Yardage Suppression",
        core_area="Defensive Control",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Important denominator for defensive efficiency metrics.",
    ),
    "defensive_success_rate": _metric(
        label="Defensive Success Rate",
        category="Yardage Suppression",
        core_area="Defensive Control",
        comparison_direction="lower",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="yards_allowed",
        denominator="opponent_total_plays",
        format="decimal",
        decimals=2,
        notes="Currently behaves like yards allowed per opponent play; lower is better.",
    ),
    "points_allowed_per_yard": _metric(
        label="Points Allowed Per Yard",
        category="Yardage Suppression",
        core_area="Defensive Control",
        comparison_direction="lower",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="points_allowed",
        denominator="yards_allowed",
        format="decimal",
        decimals=3,
    ),

    # ------------------------------------------------------------------
    # Defensive Control — Scoring Suppression
    # ------------------------------------------------------------------
    "points_allowed": _metric(
        label="Points Allowed",
        category="Scoring Suppression",
        core_area="Defensive Control",
        comparison_direction="lower",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "points_allowed_per_play": _metric(
        label="Points Allowed Per Play",
        category="Scoring Suppression",
        core_area="Defensive Control",
        comparison_direction="lower",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="points_allowed",
        denominator="opponent_total_plays",
        format="decimal",
        decimals=3,
    ),

    # ------------------------------------------------------------------
    # Defensive Control — Defensive Workload
    # ------------------------------------------------------------------
    "total_defensive_snaps": _metric(
        label="Total Defensive Snaps",
        category="Defensive Workload",
        core_area="Defensive Control",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "defensive_snap_load": _metric(
        label="Defensive Snap Load",
        category="Defensive Workload",
        core_area="Defensive Control",
        comparison_direction="lower",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="total_defensive_snaps",
        denominator="total_snaps",
        format="percent",
        decimals=1,
    ),

    # ------------------------------------------------------------------
    # Disruption and Turnovers — Pressure
    # ------------------------------------------------------------------
    "sacks": _metric(
        label="Sacks",
        category="Pressure",
        core_area="Disruption and Turnovers",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "pressure_rate": _metric(
        label="Pressure Rate",
        category="Pressure",
        core_area="Disruption and Turnovers",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="sacks_plus_sacks_taken",
        denominator="pass_attempts",
        format="percent",
        decimals=1,
        notes="Mixed pressure-environment metric because numerator combines sacks and sacks taken.",
    ),
    "sacks_taken": _metric(
        label="Sacks Taken",
        category="Pressure",
        core_area="Disruption and Turnovers",
        comparison_direction="lower",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "sack_yards_lost": _metric(
        label="Sack Yards Lost",
        category="Pressure",
        core_area="Disruption and Turnovers",
        comparison_direction="lower",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "sacks_plus_sacks_taken": _metric(
        label="Sacks + Sacks Taken",
        category="Pressure",
        core_area="Disruption and Turnovers",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Formula ingredient for pressure_rate; mixed good/bad components.",
    ),
    "sack_to_turnover_ratio": _metric(
        label="Sack to Turnover Ratio",
        category="Pressure",
        core_area="Disruption and Turnovers",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="sacks",
        denominator="defensive_interceptions",
        format="decimal",
        decimals=2,
        notes="Context metric; not a simple higher/lower signal.",
    ),

    # ------------------------------------------------------------------
    # Disruption and Turnovers — Turnovers
    # ------------------------------------------------------------------
    "defensive_interceptions": _metric(
        label="Defensive Interceptions",
        category="Turnovers",
        core_area="Disruption and Turnovers",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "fumbles_recovered": _metric(
        label="Fumbles Recovered",
        category="Turnovers",
        core_area="Disruption and Turnovers",
        comparison_direction="higher",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "interceptions_thrown": _metric(
        label="Interceptions Thrown",
        category="Turnovers",
        core_area="Disruption and Turnovers",
        comparison_direction="lower",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "fumbles_lost": _metric(
        label="Fumbles Lost",
        category="Turnovers",
        core_area="Disruption and Turnovers",
        comparison_direction="lower",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "turnover_margin": _metric(
        label="Turnover Margin",
        category="Turnovers",
        core_area="Disruption and Turnovers",
        comparison_direction="higher",
        raw_or_derived="derived",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Additive derived metric; sum across games instead of averaging.",
    ),

    # ------------------------------------------------------------------
    # Field Control (Special Teams) — Special Teams Usage
    # ------------------------------------------------------------------
    "total_special_teams_snaps": _metric(
        label="Total Special Teams Snaps",
        category="Special Teams Usage",
        core_area="Field Control (Special Teams)",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
    ),
    "special_teams_snap_pct": _metric(
        label="Special Teams Snap %",
        category="Special Teams Usage",
        core_area="Field Control (Special Teams)",
        comparison_direction="context",
        raw_or_derived="derived",
        aggregation_method="ratio_from_sums",
        numerator="total_special_teams_snaps",
        denominator="total_snaps",
        format="percent",
        decimals=1,
        notes="Usage/context metric; not inherently good or bad.",
    ),
    "total_snaps": _metric(
        label="Total Snaps",
        category="Special Teams Usage",
        core_area="Field Control (Special Teams)",
        comparison_direction="context",
        raw_or_derived="raw",
        aggregation_method="sum",
        format="integer",
        decimals=0,
        notes="Denominator for offensive/defensive/special teams snap share metrics.",
    ),
}


REQUIRED_FIELDS = {
    "label",
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
        "category": cfg["category"],
        "core_area": cfg["core_area"],
        "comparison_direction": cfg["comparison_direction"],
        "higher_is_better": cfg["higher_is_better"],
        "raw_or_derived": cfg["raw_or_derived"],
        "aggregation_method": cfg["aggregation_method"],
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
    if core_area not in CORE_AREAS:
        raise ValueError(f"Invalid core_area: {core_area}")
    return [
        metric
        for metric, cfg in METRIC_REGISTRY.items()
        if cfg["core_area"] == core_area
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
