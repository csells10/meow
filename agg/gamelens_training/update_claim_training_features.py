"""
Update GameLens claim training examples with Level 3 engineered feature scores.

Adds clean_hierarchy_context_v1 metadata while preserving the existing
offense_finish_score, defensive_suppression_score, two_way_edge_score,
and two_way_context behavior.

Recommended repo location:
    agg/gamelens_training/update_claim_training_features.py

Purpose:
    Level 3 job for GameLens feature engineering.

    Level 1 built one row per pregame GameLens claim.
    Level 2 attached postgame validation labels.
    Level 3 begins adding pregame-only engineered features that can later help
    predict claim quality and calibrate language.

This worker computes / attaches:
    clean_hierarchy_context_v1 registry-backed hierarchy metadata
    offensive_efficiency_support_v1 metadata
    offense_finish_score
    defensive_suppression_score
    two_way_edge_score
    two_way_context

Football idea:
    Can the claimed team both move the ball and finish drives?

Important:
    offense_finish_score is pregame-only. It uses the pregame Core Area
    Comparison rows already stored in gamelens_claim_training_examples:
        - Offensive Output
        - Scoring Efficiency

    It does NOT use validation_result, actual_gap, final_score, or any other
    postgame field. That prevents leakage.

Score meaning:
    Range is roughly -1.0 to +1.0.
        Positive = claimed team had combined Offensive Output + Scoring Efficiency support.
        Near 0   = mixed/near-even offensive finish profile.
        Negative = opponent had the stronger offensive finish profile.

Example dry run:
    python -m agg.gamelens_training.update_claim_training_features \
      --run-id baseline_96_stage1_v2 \
      --dry-run

Example BigQuery update:
    python -m agg.gamelens_training.update_claim_training_features \
      --run-id baseline_96_stage1_v2 \
      --write-bigquery
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from analytics.metric_registry import get_metric_meta  # type: ignore


PROJECT_ID = "nfl-stream-406420"
DATASET_ID = "Analytics"
TRAINING_TABLE = "gamelens_claim_training_examples"
DEFAULT_OUTPUT_ROOT = Path("qa/gamelens_feature_update_runs")
DEFAULT_FORMULA_VERSION = "clean_hierarchy_context_v1__offensive_efficiency_support_v1__offense_finish_v2__defensive_suppression_v3__two_way_context_v1"

REQUIRED_CORE_AREAS = ("Offensive Output", "Scoring Efficiency", "Defensive Control")

# V2 finding:
# offense_finish_score helped most for finishing-context claims, not as a
# universal offensive feature. Keep the score scoped so defensive/pressure/
# turnover rows do not accidentally inherit an offensive context feature.
OFFENSE_FINISH_RELEVANT_CORE_AREAS = {
    "Scoring Efficiency",
}

OFFENSE_FINISH_RELEVANT_CATEGORIES = {
    "Drive Conversion",
    "Red Zone Finish",
    "Rushing Game",
}

OFFENSE_FINISH_RELEVANT_METRICS = {
    "third_down_pct",
    "red_zone_efficiency",
    "yards_per_rush",
}

# V3 feature:
# defensive_suppression_score asks whether the claimed team had pregame support
# for limiting opponent movement/scoring. It is intentionally scoped to
# defensive/suppression rows only. Turnovers and Pressure are not included here
# because they are more volatile and deserve separate features later.
DEFENSIVE_SUPPRESSION_RELEVANT_CORE_AREAS = {
    "Defensive Control",
}

DEFENSIVE_SUPPRESSION_RELEVANT_CATEGORIES = {
    "Scoring Suppression",
    "Defensive Efficiency",
}

DEFENSIVE_SUPPRESSION_RELEVANT_METRICS = {
    "points_allowed_per_play",
    "points_allowed_per_yard",
    "yards_allowed",
    "points_allowed",
    "defensive_success_rate",
}

DEFENSIVE_SUPPRESSION_CONFIRMING_METRICS = (
    "points_allowed_per_play",
    "points_allowed_per_yard",
)


# V1 metadata feature:
# offensive_efficiency_support_v1 tests whether repeat-positive offensive
# efficiency metrics identify claims that deserve stronger claim-language
# support. This is metadata only; it does not change prediction, confidence,
# Level 4 rules, or frontend behavior.
OFFENSIVE_EFFICIENCY_PRIMARY_METRICS = (
    "points_per_play",
)

OFFENSIVE_EFFICIENCY_SCOPED_METRICS = (
    "yards_per_rush",
)

OFFENSIVE_EFFICIENCY_WATCH_METRICS = (
    "yards_per_play",
    "yards_per_pass",
)

OFFENSIVE_EFFICIENCY_CONTEXT_ONLY_METRICS = {
    "third_down_pct",
    "1st_down_rate",
}

OFFENSIVE_EFFICIENCY_EXCLUDED_METRICS = {
    "td_rate",
    "red_zone_efficiency",
    "turnover_margin_per_game",
}

OFFENSIVE_EFFICIENCY_SUPPORT_METRICS = (
    *OFFENSIVE_EFFICIENCY_PRIMARY_METRICS,
    *OFFENSIVE_EFFICIENCY_SCOPED_METRICS,
    *OFFENSIVE_EFFICIENCY_WATCH_METRICS,
)

OFFENSIVE_EFFICIENCY_RELEVANT_CORE_AREAS = {
    "Offensive Output",
    "Scoring Efficiency",
}

OFFENSIVE_EFFICIENCY_RELEVANT_CATEGORIES = {
    "Passing Game",
    "Rushing Game",
    "Offensive Rhythm",
    "Scoring Production",
}

OFFENSIVE_EFFICIENCY_CONTEXT_ONLY_CATEGORIES = {
    "Drive Conversion",
}

OFFENSIVE_EFFICIENCY_CAUTION_CATEGORIES = {
    "Red Zone Finish",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def clean_string(value: Any) -> Optional[str]:
    """Normalize blank hierarchy fields to None without changing labels."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def build_clean_hierarchy_context(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Attach canonical registry-backed hierarchy metadata to a claim row.

    This is metadata only. It does not change any Level 3 score formula,
    validation label, winner logic, matchup lean, confidence, Model Trust,
    Level 4 rule, or frontend behavior.
    """
    metric = clean_string(row.get("metric"))
    original_core_area = clean_string(row.get("core_area"))
    original_category = clean_string(row.get("category"))

    if metric:
        try:
            meta = get_metric_meta(metric)
        except KeyError:
            return {
                "registry_core_area": None,
                "registry_category": None,
                "registry_metric_label": None,
                "registry_signal_strength": None,
                "registry_ranking_usage": None,
                "clean_hierarchy_path": f"unregistered_metric > {metric}",
                "clean_hierarchy_status": "metric_not_in_registry",
                "clean_hierarchy_path_flag": False,
                "missing_hierarchy_parent_flag": True,
            }

        registry_core_area = clean_string(meta.get("core_area"))
        registry_category = clean_string(meta.get("category"))
        registry_metric_label = clean_string(meta.get("label"))
        registry_signal_strength = clean_string(meta.get("signal_strength"))
        registry_ranking_usage = clean_string(meta.get("ranking_usage"))
        missing_parent = not original_core_area or not original_category

        return {
            "registry_core_area": registry_core_area,
            "registry_category": registry_category,
            "registry_metric_label": registry_metric_label,
            "registry_signal_strength": registry_signal_strength,
            "registry_ranking_usage": registry_ranking_usage,
            "clean_hierarchy_path": f"{registry_core_area} > {registry_category} > {metric}",
            "clean_hierarchy_status": (
                "registry_metric_parent_recovered"
                if missing_parent
                else "registry_metric_clean"
            ),
            "clean_hierarchy_path_flag": True,
            "missing_hierarchy_parent_flag": missing_parent,
        }

    if original_core_area and original_category:
        return {
            "registry_core_area": None,
            "registry_category": None,
            "registry_metric_label": None,
            "registry_signal_strength": None,
            "registry_ranking_usage": None,
            "clean_hierarchy_path": f"{original_core_area} > {original_category}",
            "clean_hierarchy_status": "row_metadata_category",
            "clean_hierarchy_path_flag": True,
            "missing_hierarchy_parent_flag": False,
        }

    if original_core_area:
        return {
            "registry_core_area": None,
            "registry_category": None,
            "registry_metric_label": None,
            "registry_signal_strength": None,
            "registry_ranking_usage": None,
            "clean_hierarchy_path": original_core_area,
            "clean_hierarchy_status": "row_metadata_core_area",
            "clean_hierarchy_path_flag": True,
            "missing_hierarchy_parent_flag": False,
        }

    if original_category:
        return {
            "registry_core_area": None,
            "registry_category": None,
            "registry_metric_label": None,
            "registry_signal_strength": None,
            "registry_ranking_usage": None,
            "clean_hierarchy_path": f"missing_core_area > {original_category}",
            "clean_hierarchy_status": "row_metadata_missing_core_area",
            "clean_hierarchy_path_flag": False,
            "missing_hierarchy_parent_flag": True,
        }

    return {
        "registry_core_area": None,
        "registry_category": None,
        "registry_metric_label": None,
        "registry_signal_strength": None,
        "registry_ranking_usage": None,
        "clean_hierarchy_path": None,
        "clean_hierarchy_status": "no_hierarchy_context",
        "clean_hierarchy_path_flag": False,
        "missing_hierarchy_parent_flag": True,
    }


def clamp(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def write_json(data: Any, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def write_csv(rows: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return

    preferred = [
        "run_id",
        "claim_key",
        "game_id",
        "claimed_side",
        "claimed_team",
        "claim_type",
        "claim_layer",
        "claim_name",
        "core_area",
        "category",
        "metric",
        "registry_core_area",
        "registry_category",
        "registry_metric_label",
        "registry_signal_strength",
        "registry_ranking_usage",
        "clean_hierarchy_path",
        "clean_hierarchy_status",
        "clean_hierarchy_path_flag",
        "missing_hierarchy_parent_flag",
        "offensive_efficiency_support_score",
        "offensive_efficiency_support_bucket",
        "offensive_efficiency_support_strength",
        "offensive_efficiency_support_reason",
        "offensive_efficiency_support_metrics",
        "offense_finish_score",
        "offense_finish_relevance_reason",
        "away_offense_finish_score",
        "home_offense_finish_score",
        "offensive_output_away_edge",
        "scoring_efficiency_away_edge",
        "defensive_suppression_score",
        "defensive_suppression_relevance_reason",
        "two_way_edge_score",
        "two_way_context",
        "away_defensive_suppression_score",
        "home_defensive_suppression_score",
        "defensive_control_away_edge",
        "scoring_suppression_away_edge",
        "scoring_suppression_metric",
        "feature_formula_version",
        "feature_status",
        "feature_notes",
        "updated_at",
    ]
    all_keys = sorted({key for row in rows for key in row.keys()})
    fieldnames = [key for key in preferred if key in all_keys] + [key for key in all_keys if key not in preferred]

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def import_bigquery():
    from google.cloud import bigquery
    return bigquery


# ---------------------------------------------------------------------------
# BigQuery loading
# ---------------------------------------------------------------------------

def load_training_rows(
    *,
    client: Any,
    bigquery: Any,
    table_ref: str,
    run_id: str,
    limit: Optional[int],
) -> List[Dict[str, Any]]:
    limit_clause = "LIMIT @limit" if limit else ""

    query = f"""
        SELECT
            claim_key,
            run_id,
            game_id,
            season,
            claimed_team,
            claimed_side,
            opponent_team,
            opponent_side,
            claim_type,
            claim_layer,
            claim_name,
            core_area,
            category,
            metric,
            pregame_raw_gap,
            pregame_percentile_gap,
            pregame_abs_percentile_gap,
            claimed_team_value,
            opponent_team_value,
            core_area_agreement_rate,
            feature_status,
            feature_notes,
            feature_formula_version
        FROM `{table_ref}`
        WHERE run_id = @run_id
        ORDER BY season, game_id, claim_type, claim_layer, claim_rank
        {limit_clause}
    """

    params = [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
    if limit:
        params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return [dict(row) for row in client.query(query, job_config=job_config).result()]


# ---------------------------------------------------------------------------
# Feature formula
# ---------------------------------------------------------------------------

def build_core_area_edges(training_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    """
    Returns:
        {
            game_id: {
                "Offensive Output": away_signed_edge,
                "Scoring Efficiency": away_signed_edge,
            }
        }

    away_signed_edge:
        Positive if away led that core area.
        Negative if home led that core area.

    Source rows:
        claim_type = core_area_comparison
        core_area in REQUIRED_CORE_AREAS

    Why core_area_comparison?
        This is the cleanest high-level pregame source for "which side led the
        Core Area" and avoids double-counting summary/detail rows.
    """
    edges: Dict[str, Dict[str, float]] = defaultdict(dict)

    for row in training_rows:
        if row.get("claim_type") != "core_area_comparison":
            continue

        core_area = row.get("core_area")
        if core_area not in REQUIRED_CORE_AREAS:
            continue

        game_id = row.get("game_id")
        claimed_side = row.get("claimed_side")
        raw_gap = as_float(row.get("pregame_raw_gap"))

        if not game_id or claimed_side not in {"away", "home"} or raw_gap is None:
            continue

        # pregame_raw_gap is leader score - opponent score from the builder.
        away_signed = raw_gap if claimed_side == "away" else -raw_gap
        edges[str(game_id)][str(core_area)] = away_signed

    return edges


def build_metric_edges(
    training_rows: List[Dict[str, Any]],
    *,
    metrics: tuple[str, ...],
) -> Dict[str, Dict[str, float]]:
    """
    Build game-level away-signed metric edges from pregame percentile gaps.

    Returns:
        {
            game_id: {
                metric: away_signed_edge,
            }
        }

    away_signed_edge:
        Positive if away had the edge.
        Negative if home had the edge.

    Notes:
        - Uses pregame_percentile_gap because it is already oriented toward
          the claimed/better team and is more comparable across metrics.
        - Normalizes percentile gap from 0-100 into roughly 0-1.
        - If duplicate rows exist for the same game/metric, keep the strongest
          absolute edge.
    """
    wanted = set(metrics)
    edges: Dict[str, Dict[str, float]] = defaultdict(dict)

    for row in training_rows:
        metric = row.get("metric")
        if metric not in wanted:
            continue

        game_id = row.get("game_id")
        claimed_side = row.get("claimed_side")
        pct_gap = as_float(row.get("pregame_percentile_gap"))

        if not game_id or claimed_side not in {"away", "home"} or pct_gap is None:
            continue

        normalized_gap = clamp(abs(pct_gap) / 100.0, 0.0, 1.0)
        away_signed = normalized_gap if claimed_side == "away" else -normalized_gap

        current = edges[str(game_id)].get(str(metric))
        if current is None or abs(away_signed) > abs(current):
            edges[str(game_id)][str(metric)] = away_signed

    return edges



def calculate_offense_finish_score(
    *,
    offensive_output_edge: Optional[float],
    scoring_efficiency_edge: Optional[float],
    side: str,
) -> Optional[float]:
    """
    Compute a side-specific offense_finish_score.

    Inputs are away-signed core area edges.
    If side is home, flip signs so the score is from that team's perspective.

    Formula v3:
        side_offense_edge = signed Offensive Output edge
        side_scoring_edge = signed Scoring Efficiency edge

        base = 0.45 * side_offense_edge + 0.55 * side_scoring_edge

        if both are positive:
            synergy bonus = 0.15 * min(side_offense_edge, side_scoring_edge)

        if one positive and one negative:
            conflict penalty = 0.10 * min(abs(side_offense_edge), abs(side_scoring_edge))

        score = clamp(base + synergy - conflict_penalty, -1, 1)

    Why heavier scoring efficiency?
        The 96-game QA showed scoring-related claims validated better than some
        noisier areas, and football-wise finishing drives matters more than
        empty yardage.

    Return:
        None if either required core area is missing.
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


def offense_finish_relevance_reason(row: Dict[str, Any]) -> Optional[str]:
    """
    V2 scoping rule.

    Only apply offense_finish_score to claim families where the first QA pass
    showed useful signal and where the football meaning is sensible.

    Included:
        - Scoring Efficiency core-area level claims
        - Drive Conversion
        - Red Zone Finish
        - Rushing Game
        - Direct metrics: third_down_pct, red_zone_efficiency, yards_per_rush

    Excluded for now:
        - Generic Offensive Output
        - Offensive Rhythm
        - Passing Game
        - Scoring Production
        - Defensive Control
        - Pressure / Turnovers

    This keeps the feature from looking noisy when evaluated against unrelated
    claim families.
    """
    core_area = row.get("core_area")
    category = row.get("category")
    metric = row.get("metric")

    # Avoid applying broad Scoring Efficiency to the weaker Scoring Production
    # family unless the direct metric is specifically relevant.
    if metric in OFFENSE_FINISH_RELEVANT_METRICS:
        return f"metric:{metric}"

    if category in OFFENSE_FINISH_RELEVANT_CATEGORIES:
        return f"category:{category}"

    if core_area in OFFENSE_FINISH_RELEVANT_CORE_AREAS and not category:
        return f"core_area:{core_area}"

    return None


def is_offense_finish_relevant(row: Dict[str, Any]) -> bool:
    return offense_finish_relevance_reason(row) is not None


def offensive_efficiency_support_relevance_reason(row: Dict[str, Any]) -> Optional[str]:
    """
    V1 scoping rule for offensive_efficiency_support_v1.

    This feature is intentionally conservative:
    - points_per_play is the repeat-positive anchor.
    - yards_per_rush is allowed only in rushing-scoped rows.
    - yards_per_play and yards_per_pass are measured/watch support only.
    - td_rate, red_zone_efficiency, and turnover_margin_per_game are excluded.
    - third_down_pct and 1st_down_rate remain context-only.
    """
    metric = row.get("metric")
    category = row.get("category")
    core_area = row.get("core_area")

    if metric in OFFENSIVE_EFFICIENCY_EXCLUDED_METRICS:
        return f"metric:{metric}:caution_only"

    if metric in OFFENSIVE_EFFICIENCY_CONTEXT_ONLY_METRICS:
        return f"metric:{metric}:context_only"

    if metric in OFFENSIVE_EFFICIENCY_SUPPORT_METRICS:
        return f"metric:{metric}"

    if category in OFFENSIVE_EFFICIENCY_CAUTION_CATEGORIES:
        return f"category:{category}:caution_only"

    if category in OFFENSIVE_EFFICIENCY_CONTEXT_ONLY_CATEGORIES:
        return f"category:{category}:context_only"

    if category in OFFENSIVE_EFFICIENCY_RELEVANT_CATEGORIES:
        return f"category:{category}"

    if core_area in OFFENSIVE_EFFICIENCY_RELEVANT_CORE_AREAS:
        return f"core_area:{core_area}"

    return None


def _side_oriented_edge(edge: Optional[float], side: str) -> Optional[float]:
    """Convert an away-signed edge into the claimed team's perspective."""
    if edge is None:
        return None
    if side == "away":
        return edge
    if side == "home":
        return -edge
    return None


def _is_rushing_scoped_offensive_efficiency_row(row: Dict[str, Any]) -> bool:
    return (
        row.get("metric") == "yards_per_rush"
        or row.get("category") == "Rushing Game"
    )


def calculate_offensive_efficiency_support(
    *,
    row: Dict[str, Any],
    game_metric_edges: Dict[str, float],
    side: str,
) -> Dict[str, Any]:
    """
    Build offensive_efficiency_support_v1 metadata for one claim row.

    Output contract:
        offensive_efficiency_support_score FLOAT
        offensive_efficiency_support_bucket STRING
        offensive_efficiency_support_strength STRING
        offensive_efficiency_support_reason STRING
        offensive_efficiency_support_metrics STRING

    Important boundary:
        This does not change any existing score, prediction, confidence, Model
        Trust, Level 4 rule, or frontend behavior. It only attaches metadata so
        the run can be analyzed after Level 3.
    """
    relevance_reason = offensive_efficiency_support_relevance_reason(row)

    if relevance_reason is None:
        return {
            "score": None,
            "bucket": "not_relevant",
            "strength": "not_applicable",
            "reason": "offensive_efficiency_support_v1 not relevant to this claim family",
            "metrics": None,
        }

    if side not in {"away", "home"}:
        return {
            "score": None,
            "bucket": "invalid_side",
            "strength": "not_applicable",
            "reason": "claimed_side missing or invalid",
            "metrics": None,
        }

    if relevance_reason.endswith(":caution_only"):
        return {
            "score": None,
            "bucket": "caution_only",
            "strength": "caution_only",
            "reason": f"{relevance_reason}; excluded from support scoring by revalidation evidence",
            "metrics": None,
        }

    if relevance_reason.endswith(":context_only"):
        return {
            "score": None,
            "bucket": "context_only",
            "strength": "context_only",
            "reason": f"{relevance_reason}; retained as context only, not stronger-language support",
            "metrics": None,
        }

    oriented_edges = {
        metric: _side_oriented_edge(game_metric_edges.get(metric), side)
        for metric in OFFENSIVE_EFFICIENCY_SUPPORT_METRICS
    }

    points_per_play_edge = oriented_edges.get("points_per_play")
    if points_per_play_edge is None:
        return {
            "score": None,
            "bucket": "anchor_unavailable",
            "strength": "unavailable",
            "reason": f"{relevance_reason}; points_per_play anchor missing",
            "metrics": None,
        }

    weighted_parts = [("points_per_play", points_per_play_edge, 0.70)]

    yards_per_play_edge = oriented_edges.get("yards_per_play")
    if yards_per_play_edge is not None:
        weighted_parts.append(("yards_per_play", yards_per_play_edge, 0.15))

    yards_per_pass_edge = oriented_edges.get("yards_per_pass")
    if yards_per_pass_edge is not None:
        weighted_parts.append(("yards_per_pass", yards_per_pass_edge, 0.05))

    yards_per_rush_edge = oriented_edges.get("yards_per_rush")
    if yards_per_rush_edge is not None and _is_rushing_scoped_offensive_efficiency_row(row):
        weighted_parts.append(("yards_per_rush", yards_per_rush_edge, 0.20))

    total_weight = sum(weight for _, _, weight in weighted_parts)
    score = sum(edge * weight for _, edge, weight in weighted_parts) / total_weight
    score = round(clamp(score), 4)

    metrics_text = ";".join(
        f"{metric}:{round(edge, 4)}@{weight}"
        for metric, edge, weight in weighted_parts
    )

    if score >= 0.40:
        bucket = "repeat_positive_strong"
        strength = "strong_support"
    elif score >= 0.15:
        bucket = "repeat_positive_supportive"
        strength = "measured_support"
    elif score > -0.15:
        bucket = "mixed_near_even"
        strength = "mixed"
    elif score > -0.40:
        bucket = "negative_caution"
        strength = "caution"
    else:
        bucket = "opposing_efficiency_signal"
        strength = "caution"

    return {
        "score": score,
        "bucket": bucket,
        "strength": strength,
        "reason": (
            f"{relevance_reason}; score anchored by repeat-positive points_per_play, "
            "with yards_per_play/yards_per_pass as watch inputs and yards_per_rush only when rushing-scoped"
        ),
        "metrics": metrics_text,
    }


def calculate_defensive_suppression_score(
    *,
    defensive_control_edge: Optional[float],
    scoring_suppression_edge: Optional[float],
    side: str,
) -> Optional[float]:
    """
    Compute a side-specific defensive_suppression_score.

    Inputs are away-signed pregame edges:
        defensive_control_edge:
            from Core Area Comparison / Defensive Control
        scoring_suppression_edge:
            from a confirming scoring-suppression metric, currently
            points_allowed_per_play or points_allowed_per_yard when available

    Formula v3 / control-required:
        Best:
            Defensive Control + scoring suppression metric
            base = 0.65 * defensive_control + 0.35 * scoring_suppression
            + small bonus if both agree
            - small penalty if they conflict

        Partial allowed:
            Defensive Control only:
                score = 0.75 * defensive_control

        Missing:
            No Defensive Control -> NULL

    Why require Defensive Control?
        QA showed that metric-only fallback rows, especially
        points_allowed_per_play without Defensive Control, validated poorly.
        So direct suppression metrics can sharpen the score, but they should
        not stand alone in v5.
    """
    if defensive_control_edge is None:
        return None

    if side == "away":
        defense = defensive_control_edge
        suppression = scoring_suppression_edge
    elif side == "home":
        defense = -defensive_control_edge
        suppression = -scoring_suppression_edge if scoring_suppression_edge is not None else None
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


def defensive_suppression_relevance_reason(row: Dict[str, Any]) -> Optional[str]:
    """
    V3 scoping rule for defensive_suppression_score.

    Included:
        - Defensive Control core-area claims
        - Scoring Suppression / Defensive Efficiency category claims
        - Direct defensive suppression metrics:
            points_allowed_per_play
            points_allowed_per_yard
            yards_allowed
            points_allowed
            defensive_success_rate

    Excluded for now:
        - Pressure
        - Turnovers
        - Generic Disruption and Turnovers

    Those should become separate volatility/disruption features later.
    """
    core_area = row.get("core_area")
    category = row.get("category")
    metric = row.get("metric")

    if metric in DEFENSIVE_SUPPRESSION_RELEVANT_METRICS:
        return f"metric:{metric}"

    if category in DEFENSIVE_SUPPRESSION_RELEVANT_CATEGORIES:
        return f"category:{category}"

    if core_area in DEFENSIVE_SUPPRESSION_RELEVANT_CORE_AREAS:
        return f"core_area:{core_area}"

    return None


def is_defensive_suppression_relevant(row: Dict[str, Any]) -> bool:
    return defensive_suppression_relevance_reason(row) is not None


def strip_prior_level3_notes(notes: Optional[str]) -> str:
    """
    Prevent repeated local reruns from appending Level 3 notes forever.
    Keeps Level 1/2 notes, removes any previous ' | Level 3 ...' suffix.
    """
    if not notes:
        return ""
    return notes.split(" | Level 3 ")[0]



def calculate_two_way_edge_score(
    *,
    offense_finish_score: Optional[float],
    defensive_suppression_score: Optional[float],
) -> Optional[float]:
    """
    Compute a simple two-way edge score from side-level feature context.

    v1 rule:
        If both offense_finish_score and defensive_suppression_score exist:
            two_way_edge_score = min(offense_finish_score, defensive_suppression_score)

        Else:
            NULL

    Why min()?
        A two-way profile is only as strong as its weaker side. This prevents
        a great offense score from hiding missing/weak defensive suppression,
        and vice versa.
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

    QA finding:
        The reliable signal was not "strong + strong always wins."
        The reliable signal was that claims, especially metric_highlight rows,
        validated better when both offense and defensive context were available
        and supportive.

    Buckets:
        supportive:
            offense_finish_score >= 0.15 and defensive_suppression_score >= 0.15

        available_mixed:
            both scores exist, but they are not both supportive

        unavailable:
            either score is missing
    """
    if offense_finish_score is None or defensive_suppression_score is None:
        return "unavailable"

    if offense_finish_score >= 0.15 and defensive_suppression_score >= 0.15:
        return "supportive"

    return "available_mixed"


def build_feature_updates(
    training_rows: List[Dict[str, Any]],
    *,
    formula_version: str,
) -> List[Dict[str, Any]]:
    core_edges = build_core_area_edges(training_rows)
    metric_edges = build_metric_edges(
        training_rows,
        metrics=DEFENSIVE_SUPPRESSION_CONFIRMING_METRICS,
    )
    offensive_efficiency_metric_edges = build_metric_edges(
        training_rows,
        metrics=OFFENSIVE_EFFICIENCY_SUPPORT_METRICS,
    )
    now = utc_now_iso()
    updates: List[Dict[str, Any]] = []

    for row in training_rows:
        game_id = str(row.get("game_id") or "")
        claimed_side = row.get("claimed_side")
        game_edges = core_edges.get(game_id, {})
        hierarchy_context = build_clean_hierarchy_context(row)

        # -------------------------
        # Offense finish feature
        # -------------------------
        offensive_output_away_edge = game_edges.get("Offensive Output")
        scoring_efficiency_away_edge = game_edges.get("Scoring Efficiency")

        away_offense_score = calculate_offense_finish_score(
            offensive_output_edge=offensive_output_away_edge,
            scoring_efficiency_edge=scoring_efficiency_away_edge,
            side="away",
        )
        home_offense_score = calculate_offense_finish_score(
            offensive_output_edge=offensive_output_away_edge,
            scoring_efficiency_edge=scoring_efficiency_away_edge,
            side="home",
        )

        offense_relevance_reason = offense_finish_relevance_reason(row)

        if offense_relevance_reason is None:
            offense_score = None
            offense_note = (
                " | Level 3 v5: offense_finish_score intentionally left NULL because "
                "this claim family is not offense-finish relevant."
            )
        elif claimed_side == "away":
            offense_score = away_offense_score
            offense_note = (
                " | Level 3 v5: offense_finish_score applied to relevant claim family "
                f"({offense_relevance_reason}); computed pregame-only from Core Area Comparison "
                "(Offensive Output + Scoring Efficiency)."
            )
        elif claimed_side == "home":
            offense_score = home_offense_score
            offense_note = (
                " | Level 3 v5: offense_finish_score applied to relevant claim family "
                f"({offense_relevance_reason}); computed pregame-only from Core Area Comparison "
                "(Offensive Output + Scoring Efficiency)."
            )
        else:
            offense_score = None
            offense_note = (
                " | Level 3 v5: offense_finish_score not calculated because claimed_side was missing/invalid."
            )

        if offense_relevance_reason is not None and offense_score is None:
            offense_note = (
                " | Level 3 v5: offense_finish_score not calculated for relevant claim because "
                "required Core Area comparison rows were missing."
            )

        # -------------------------
        # Defensive suppression feature
        # -------------------------
        defensive_control_away_edge = game_edges.get("Defensive Control")
        game_metric_edges = metric_edges.get(game_id, {})

        scoring_suppression_away_edge = None
        scoring_suppression_metric = None
        for metric_name in DEFENSIVE_SUPPRESSION_CONFIRMING_METRICS:
            if metric_name in game_metric_edges:
                scoring_suppression_away_edge = game_metric_edges[metric_name]
                scoring_suppression_metric = metric_name
                break

        away_defense_score = calculate_defensive_suppression_score(
            defensive_control_edge=defensive_control_away_edge,
            scoring_suppression_edge=scoring_suppression_away_edge,
            side="away",
        )
        home_defense_score = calculate_defensive_suppression_score(
            defensive_control_edge=defensive_control_away_edge,
            scoring_suppression_edge=scoring_suppression_away_edge,
            side="home",
        )

        # -------------------------
        # Two-way context feature
        # -------------------------
        if claimed_side == "away":
            side_offense_score = away_offense_score
            side_defense_score = away_defense_score
        elif claimed_side == "home":
            side_offense_score = home_offense_score
            side_defense_score = home_defense_score
        else:
            side_offense_score = None
            side_defense_score = None

        two_way_edge_score = calculate_two_way_edge_score(
            offense_finish_score=side_offense_score,
            defensive_suppression_score=side_defense_score,
        )
        two_way_context = calculate_two_way_context(
            offense_finish_score=side_offense_score,
            defensive_suppression_score=side_defense_score,
        )

        # -------------------------
        # Offensive efficiency support metadata
        # -------------------------
        offensive_efficiency_support = calculate_offensive_efficiency_support(
            row=row,
            game_metric_edges=offensive_efficiency_metric_edges.get(game_id, {}),
            side=claimed_side,
        )

        defense_relevance_reason = defensive_suppression_relevance_reason(row)

        if defense_relevance_reason is None:
            defensive_score = None
            defensive_note = (
                " | Level 3 v5: defensive_suppression_score intentionally left NULL because "
                "this claim family is not defensive-suppression relevant."
            )
        elif claimed_side == "away":
            defensive_score = away_defense_score
            defensive_note = (
                " | Level 3 v5: defensive_suppression_score applied to relevant claim family "
                f"({defense_relevance_reason}); computed pregame-only "
                + (
                    f"from Defensive Control plus {scoring_suppression_metric}."
                    if defensive_control_away_edge is not None and scoring_suppression_metric
                    else "using partial Defensive Control only."
                    if defensive_control_away_edge is not None
                    else "but required Defensive Control input was missing; metric-only fallback intentionally disabled."
                )
            )
        elif claimed_side == "home":
            defensive_score = home_defense_score
            defensive_note = (
                " | Level 3 v5: defensive_suppression_score applied to relevant claim family "
                f"({defense_relevance_reason}); computed pregame-only "
                + (
                    f"from Defensive Control plus {scoring_suppression_metric}."
                    if defensive_control_away_edge is not None and scoring_suppression_metric
                    else "using partial Defensive Control only."
                    if defensive_control_away_edge is not None
                    else "but required Defensive Control input was missing; metric-only fallback intentionally disabled."
                )
            )
        else:
            defensive_score = None
            defensive_note = (
                " | Level 3 v5: defensive_suppression_score not calculated because claimed_side was missing/invalid."
            )

        if defense_relevance_reason is not None and defensive_score is None:
            defensive_note = (
                " | Level 3 v5: defensive_suppression_score not calculated for relevant claim because "
                "required Defensive Control comparison row was missing; metric-only fallback intentionally disabled."
            )

        two_way_note = (
            " | Level 3 v6: two_way_context computed from side-level offense_finish_score "
            "and defensive_suppression_score; intended as reasoning support, not a standalone prediction score."
        )
        offensive_efficiency_note = (
            " | Level 3 offensive_efficiency_support_v1: metadata-only support score added; "
            "anchored by points_per_play repeat-positive evidence and does not alter existing formulas or confidence."
        )

        feature_notes = (
            strip_prior_level3_notes(row.get("feature_notes"))
            + offense_note
            + defensive_note
            + two_way_note
            + offensive_efficiency_note
        )

        updates.append({
            "run_id": row.get("run_id"),
            "claim_key": row.get("claim_key"),
            "game_id": game_id,
            "claimed_team": row.get("claimed_team"),
            "claimed_side": claimed_side,
            "claim_type": row.get("claim_type"),
            "claim_layer": row.get("claim_layer"),
            "claim_name": row.get("claim_name"),
            "core_area": row.get("core_area"),
            "category": row.get("category"),
            "metric": row.get("metric"),

            "registry_core_area": hierarchy_context["registry_core_area"],
            "registry_category": hierarchy_context["registry_category"],
            "registry_metric_label": hierarchy_context["registry_metric_label"],
            "registry_signal_strength": hierarchy_context["registry_signal_strength"],
            "registry_ranking_usage": hierarchy_context["registry_ranking_usage"],
            "clean_hierarchy_path": hierarchy_context["clean_hierarchy_path"],
            "clean_hierarchy_status": hierarchy_context["clean_hierarchy_status"],
            "clean_hierarchy_path_flag": hierarchy_context["clean_hierarchy_path_flag"],
            "missing_hierarchy_parent_flag": hierarchy_context["missing_hierarchy_parent_flag"],

            "offensive_efficiency_support_score": offensive_efficiency_support["score"],
            "offensive_efficiency_support_bucket": offensive_efficiency_support["bucket"],
            "offensive_efficiency_support_strength": offensive_efficiency_support["strength"],
            "offensive_efficiency_support_reason": offensive_efficiency_support["reason"],
            "offensive_efficiency_support_metrics": offensive_efficiency_support["metrics"],

            "offense_finish_score": offense_score,
            "offense_finish_relevance_reason": offense_relevance_reason,
            "away_offense_finish_score": away_offense_score,
            "home_offense_finish_score": home_offense_score,
            "offensive_output_away_edge": offensive_output_away_edge,
            "scoring_efficiency_away_edge": scoring_efficiency_away_edge,

            "defensive_suppression_score": defensive_score,
            "defensive_suppression_relevance_reason": defense_relevance_reason,
            "two_way_edge_score": two_way_edge_score,
            "two_way_context": two_way_context,
            "away_defensive_suppression_score": away_defense_score,
            "home_defensive_suppression_score": home_defense_score,
            "defensive_control_away_edge": defensive_control_away_edge,
            "scoring_suppression_away_edge": scoring_suppression_away_edge,
            "scoring_suppression_metric": scoring_suppression_metric,

            "feature_formula_version": formula_version,
            "feature_status": row.get("feature_status"),
            "feature_notes": feature_notes,
            "updated_at": now,
        })

    return updates


# ---------------------------------------------------------------------------
# BigQuery update
# ---------------------------------------------------------------------------

def feature_update_schema(bigquery: Any) -> List[Any]:
    return [
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("claim_key", "STRING"),
        bigquery.SchemaField("registry_core_area", "STRING"),
        bigquery.SchemaField("registry_category", "STRING"),
        bigquery.SchemaField("registry_metric_label", "STRING"),
        bigquery.SchemaField("registry_signal_strength", "STRING"),
        bigquery.SchemaField("registry_ranking_usage", "STRING"),
        bigquery.SchemaField("clean_hierarchy_path", "STRING"),
        bigquery.SchemaField("clean_hierarchy_status", "STRING"),
        bigquery.SchemaField("clean_hierarchy_path_flag", "BOOL"),
        bigquery.SchemaField("missing_hierarchy_parent_flag", "BOOL"),
        bigquery.SchemaField("offensive_efficiency_support_score", "FLOAT"),
        bigquery.SchemaField("offensive_efficiency_support_bucket", "STRING"),
        bigquery.SchemaField("offensive_efficiency_support_strength", "STRING"),
        bigquery.SchemaField("offensive_efficiency_support_reason", "STRING"),
        bigquery.SchemaField("offensive_efficiency_support_metrics", "STRING"),
        bigquery.SchemaField("offense_finish_score", "FLOAT"),
        bigquery.SchemaField("defensive_suppression_score", "FLOAT"),
        bigquery.SchemaField("two_way_edge_score", "FLOAT"),
        bigquery.SchemaField("two_way_context", "STRING"),
        bigquery.SchemaField("feature_formula_version", "STRING"),
        bigquery.SchemaField("feature_notes", "STRING"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]


def ensure_target_feature_columns(
    *,
    client: Any,
    target_table: str,
) -> None:
    """Ensure additive Level 3 output columns exist before MERGE."""
    columns = [
        ("registry_core_area", "STRING"),
        ("registry_category", "STRING"),
        ("registry_metric_label", "STRING"),
        ("registry_signal_strength", "STRING"),
        ("registry_ranking_usage", "STRING"),
        ("clean_hierarchy_path", "STRING"),
        ("clean_hierarchy_status", "STRING"),
        ("clean_hierarchy_path_flag", "BOOL"),
        ("missing_hierarchy_parent_flag", "BOOL"),
        ("offensive_efficiency_support_score", "FLOAT64"),
        ("offensive_efficiency_support_bucket", "STRING"),
        ("offensive_efficiency_support_strength", "STRING"),
        ("offensive_efficiency_support_reason", "STRING"),
        ("offensive_efficiency_support_metrics", "STRING"),
    ]

    for column_name, column_type in columns:
        alter_sql = f"""
            ALTER TABLE `{target_table}`
            ADD COLUMN IF NOT EXISTS {column_name} {column_type}
        """
        client.query(alter_sql).result()


def update_bigquery_rows(
    *,
    client: Any,
    bigquery: Any,
    project_id: str,
    target_table: str,
    update_rows: List[Dict[str, Any]],
    run_id: str,
) -> None:
    if not update_rows:
        print("No feature rows to update.")
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_run = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in run_id)[:80]
    temp_table = f"{project_id}.{DATASET_ID}._tmp_gamelens_claim_features_{safe_run}_{timestamp}"

    bq_rows = [
        {
            "run_id": row["run_id"],
            "claim_key": row["claim_key"],
            "registry_core_area": row["registry_core_area"],
            "registry_category": row["registry_category"],
            "registry_metric_label": row["registry_metric_label"],
            "registry_signal_strength": row["registry_signal_strength"],
            "registry_ranking_usage": row["registry_ranking_usage"],
            "clean_hierarchy_path": row["clean_hierarchy_path"],
            "clean_hierarchy_status": row["clean_hierarchy_status"],
            "clean_hierarchy_path_flag": row["clean_hierarchy_path_flag"],
            "missing_hierarchy_parent_flag": row["missing_hierarchy_parent_flag"],
            "offensive_efficiency_support_score": row["offensive_efficiency_support_score"],
            "offensive_efficiency_support_bucket": row["offensive_efficiency_support_bucket"],
            "offensive_efficiency_support_strength": row["offensive_efficiency_support_strength"],
            "offensive_efficiency_support_reason": row["offensive_efficiency_support_reason"],
            "offensive_efficiency_support_metrics": row["offensive_efficiency_support_metrics"],
            "offense_finish_score": row["offense_finish_score"],
            "defensive_suppression_score": row["defensive_suppression_score"],
            "two_way_edge_score": row["two_way_edge_score"],
            "two_way_context": row["two_way_context"],
            "feature_formula_version": row["feature_formula_version"],
            "feature_notes": row["feature_notes"],
            "updated_at": row["updated_at"],
        }
        for row in update_rows
    ]

    ensure_target_feature_columns(client=client, target_table=target_table)

    job_config = bigquery.LoadJobConfig(
        schema=feature_update_schema(bigquery),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = client.load_table_from_json(bq_rows, temp_table, job_config=job_config)
    load_job.result()
    print(f"Loaded {len(bq_rows)} feature rows into temp table {temp_table}")

    merge_sql = f"""
        MERGE `{target_table}` AS T
        USING `{temp_table}` AS S
        ON T.run_id = S.run_id
           AND T.claim_key = S.claim_key
        WHEN MATCHED THEN UPDATE SET
            registry_core_area = S.registry_core_area,
            registry_category = S.registry_category,
            registry_metric_label = S.registry_metric_label,
            registry_signal_strength = S.registry_signal_strength,
            registry_ranking_usage = S.registry_ranking_usage,
            clean_hierarchy_path = S.clean_hierarchy_path,
            clean_hierarchy_status = S.clean_hierarchy_status,
            clean_hierarchy_path_flag = S.clean_hierarchy_path_flag,
            missing_hierarchy_parent_flag = S.missing_hierarchy_parent_flag,
            offensive_efficiency_support_score = S.offensive_efficiency_support_score,
            offensive_efficiency_support_bucket = S.offensive_efficiency_support_bucket,
            offensive_efficiency_support_strength = S.offensive_efficiency_support_strength,
            offensive_efficiency_support_reason = S.offensive_efficiency_support_reason,
            offensive_efficiency_support_metrics = S.offensive_efficiency_support_metrics,
            offense_finish_score = S.offense_finish_score,
            defensive_suppression_score = S.defensive_suppression_score,
            two_way_edge_score = S.two_way_edge_score,
            two_way_context = S.two_way_context,
            feature_formula_version = S.feature_formula_version,
            feature_notes = S.feature_notes,
            updated_at = S.updated_at
    """
    client.query(merge_sql).result()
    print(f"✅ Updated Level 3 feature scores in {target_table} for run_id={run_id}")

    client.delete_table(temp_table, not_found_ok=True)
    print(f"Deleted temp table {temp_table}")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def bucket_score(score: Optional[float]) -> str:
    if score is None:
        return "missing"
    if score >= 0.40:
        return "strong_positive"
    if score >= 0.15:
        return "positive"
    if score > -0.15:
        return "mixed_near_even"
    if score > -0.40:
        return "negative"
    return "strong_negative"


def build_summary(update_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    offense_bucket_counts: Dict[str, int] = {}
    offense_relevance_counts: Dict[str, int] = {}
    offense_non_null_scores = []

    defense_bucket_counts: Dict[str, int] = {}
    defense_relevance_counts: Dict[str, int] = {}
    defense_non_null_scores = []

    two_way_context_counts: Dict[str, int] = {}
    two_way_non_null_scores = []

    offensive_efficiency_bucket_counts: Dict[str, int] = {}
    offensive_efficiency_strength_counts: Dict[str, int] = {}
    offensive_efficiency_reason_counts: Dict[str, int] = {}
    offensive_efficiency_non_null_scores = []

    clean_hierarchy_status_counts: Dict[str, int] = {}
    clean_hierarchy_path_flag_counts: Dict[str, int] = {}
    missing_hierarchy_parent_flag_counts: Dict[str, int] = {}

    for row in update_rows:
        offense_bucket = bucket_score(row.get("offense_finish_score"))
        offense_bucket_counts[offense_bucket] = offense_bucket_counts.get(offense_bucket, 0) + 1

        offense_relevance_reason = row.get("offense_finish_relevance_reason")
        offense_relevance_key = offense_relevance_reason or "not_relevant"
        offense_relevance_counts[offense_relevance_key] = offense_relevance_counts.get(offense_relevance_key, 0) + 1

        if row.get("offense_finish_score") is not None:
            offense_non_null_scores.append(row["offense_finish_score"])

        defense_bucket = bucket_score(row.get("defensive_suppression_score"))
        defense_bucket_counts[defense_bucket] = defense_bucket_counts.get(defense_bucket, 0) + 1

        defense_relevance_reason = row.get("defensive_suppression_relevance_reason")
        defense_relevance_key = defense_relevance_reason or "not_relevant"
        defense_relevance_counts[defense_relevance_key] = defense_relevance_counts.get(defense_relevance_key, 0) + 1

        if row.get("defensive_suppression_score") is not None:
            defense_non_null_scores.append(row["defensive_suppression_score"])

        two_way_context_value = row.get("two_way_context") or "NULL"
        two_way_context_counts[two_way_context_value] = two_way_context_counts.get(two_way_context_value, 0) + 1

        if row.get("two_way_edge_score") is not None:
            two_way_non_null_scores.append(row["two_way_edge_score"])

        offensive_efficiency_bucket = row.get("offensive_efficiency_support_bucket") or "NULL"
        offensive_efficiency_bucket_counts[offensive_efficiency_bucket] = (
            offensive_efficiency_bucket_counts.get(offensive_efficiency_bucket, 0) + 1
        )

        offensive_efficiency_strength = row.get("offensive_efficiency_support_strength") or "NULL"
        offensive_efficiency_strength_counts[offensive_efficiency_strength] = (
            offensive_efficiency_strength_counts.get(offensive_efficiency_strength, 0) + 1
        )

        offensive_efficiency_reason = row.get("offensive_efficiency_support_reason") or "NULL"
        offensive_efficiency_reason_counts[offensive_efficiency_reason] = (
            offensive_efficiency_reason_counts.get(offensive_efficiency_reason, 0) + 1
        )

        if row.get("offensive_efficiency_support_score") is not None:
            offensive_efficiency_non_null_scores.append(row["offensive_efficiency_support_score"])

        clean_status = row.get("clean_hierarchy_status") or "NULL"
        clean_hierarchy_status_counts[clean_status] = clean_hierarchy_status_counts.get(clean_status, 0) + 1

        clean_path_flag = "true" if row.get("clean_hierarchy_path_flag") is True else "false"
        clean_hierarchy_path_flag_counts[clean_path_flag] = clean_hierarchy_path_flag_counts.get(clean_path_flag, 0) + 1

        missing_parent_flag = "true" if row.get("missing_hierarchy_parent_flag") is True else "false"
        missing_hierarchy_parent_flag_counts[missing_parent_flag] = missing_hierarchy_parent_flag_counts.get(missing_parent_flag, 0) + 1

    offense_relevant_rows = [r for r in update_rows if r.get("offense_finish_relevance_reason")]
    offense_relevant_missing = [
        r for r in offense_relevant_rows
        if r.get("offense_finish_score") is None
    ]

    defense_relevant_rows = [r for r in update_rows if r.get("defensive_suppression_relevance_reason")]
    defense_relevant_missing = [
        r for r in defense_relevant_rows
        if r.get("defensive_suppression_score") is None
    ]

    return {
        "formula_version": DEFAULT_FORMULA_VERSION,
        "rows_updated": len(update_rows),

        "clean_hierarchy_status_distribution": dict(sorted(clean_hierarchy_status_counts.items())),
        "clean_hierarchy_path_flag_distribution": dict(sorted(clean_hierarchy_path_flag_counts.items())),
        "missing_hierarchy_parent_flag_distribution": dict(sorted(missing_hierarchy_parent_flag_counts.items())),

        "rows_relevant_for_offense_finish": len(offense_relevant_rows),
        "rows_not_relevant_for_offense_finish": len(update_rows) - len(offense_relevant_rows),
        "rows_with_offense_finish_score": len(offense_non_null_scores),
        "rows_missing_offense_finish_score": len(update_rows) - len(offense_non_null_scores),
        "relevant_rows_missing_offense_finish_score": len(offense_relevant_missing),
        "min_offense_finish_score": min(offense_non_null_scores) if offense_non_null_scores else None,
        "max_offense_finish_score": max(offense_non_null_scores) if offense_non_null_scores else None,
        "avg_offense_finish_score": round(sum(offense_non_null_scores) / len(offense_non_null_scores), 4) if offense_non_null_scores else None,
        "offense_score_bucket_distribution": dict(sorted(offense_bucket_counts.items())),
        "offense_relevance_reason_distribution": dict(sorted(offense_relevance_counts.items())),

        "rows_relevant_for_defensive_suppression": len(defense_relevant_rows),
        "rows_not_relevant_for_defensive_suppression": len(update_rows) - len(defense_relevant_rows),
        "rows_with_defensive_suppression_score": len(defense_non_null_scores),
        "rows_missing_defensive_suppression_score": len(update_rows) - len(defense_non_null_scores),
        "relevant_rows_missing_defensive_suppression_score": len(defense_relevant_missing),
        "min_defensive_suppression_score": min(defense_non_null_scores) if defense_non_null_scores else None,
        "max_defensive_suppression_score": max(defense_non_null_scores) if defense_non_null_scores else None,
        "avg_defensive_suppression_score": round(sum(defense_non_null_scores) / len(defense_non_null_scores), 4) if defense_non_null_scores else None,
        "defensive_score_bucket_distribution": dict(sorted(defense_bucket_counts.items())),
        "defensive_relevance_reason_distribution": dict(sorted(defense_relevance_counts.items())),

        "rows_with_two_way_edge_score": len(two_way_non_null_scores),
        "rows_missing_two_way_edge_score": len(update_rows) - len(two_way_non_null_scores),
        "min_two_way_edge_score": min(two_way_non_null_scores) if two_way_non_null_scores else None,
        "max_two_way_edge_score": max(two_way_non_null_scores) if two_way_non_null_scores else None,
        "avg_two_way_edge_score": round(sum(two_way_non_null_scores) / len(two_way_non_null_scores), 4) if two_way_non_null_scores else None,
        "two_way_context_distribution": dict(sorted(two_way_context_counts.items())),

        "rows_with_offensive_efficiency_support_score": len(offensive_efficiency_non_null_scores),
        "rows_missing_offensive_efficiency_support_score": len(update_rows) - len(offensive_efficiency_non_null_scores),
        "min_offensive_efficiency_support_score": min(offensive_efficiency_non_null_scores) if offensive_efficiency_non_null_scores else None,
        "max_offensive_efficiency_support_score": max(offensive_efficiency_non_null_scores) if offensive_efficiency_non_null_scores else None,
        "avg_offensive_efficiency_support_score": (
            round(sum(offensive_efficiency_non_null_scores) / len(offensive_efficiency_non_null_scores), 4)
            if offensive_efficiency_non_null_scores
            else None
        ),
        "offensive_efficiency_support_bucket_distribution": dict(sorted(offensive_efficiency_bucket_counts.items())),
        "offensive_efficiency_support_strength_distribution": dict(sorted(offensive_efficiency_strength_counts.items())),
        "offensive_efficiency_support_reason_distribution": dict(sorted(offensive_efficiency_reason_counts.items())),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update GameLens claim training examples with Level 3 engineered feature scores and clean hierarchy context.")
    parser.add_argument("--run-id", required=True, help="run_id in Analytics.gamelens_claim_training_examples.")
    parser.add_argument("--project-id", default=PROJECT_ID)
    parser.add_argument("--training-table", default=f"{PROJECT_ID}.{DATASET_ID}.{TRAINING_TABLE}")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--formula-version", default=DEFAULT_FORMULA_VERSION)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true", help="Write local preview CSV/JSON only. Recommended first.")
    parser.add_argument("--write-bigquery", action="store_true", help="MERGE feature fields back into BigQuery.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    bigquery = import_bigquery()
    client = bigquery.Client(project=args.project_id)

    output_dir = Path(args.output_root) / args.run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading training rows for run_id={args.run_id}")
    training_rows = load_training_rows(
        client=client,
        bigquery=bigquery,
        table_ref=args.training_table,
        run_id=args.run_id,
        limit=args.limit,
    )
    print(f"Training rows loaded: {len(training_rows)}")

    update_rows = build_feature_updates(
        training_rows=training_rows,
        formula_version=args.formula_version,
    )
    summary = build_summary(update_rows)

    write_csv(update_rows, output_dir / "feature_update_preview.csv")
    write_json(summary, output_dir / "summary.json")

    print("\nFeature build complete")
    print("----------------------")
    print(f"Feature rows built: {len(update_rows)}")
    print(f"Clean hierarchy status distribution: {summary['clean_hierarchy_status_distribution']}")
    print(f"Clean hierarchy path flag distribution: {summary['clean_hierarchy_path_flag_distribution']}")
    print(f"Missing hierarchy parent flag distribution: {summary['missing_hierarchy_parent_flag_distribution']}")
    print(f"Rows with offense_finish_score: {summary['rows_with_offense_finish_score']}")
    print(f"Rows missing offense_finish_score: {summary['rows_missing_offense_finish_score']}")
    print(f"Rows with defensive_suppression_score: {summary['rows_with_defensive_suppression_score']}")
    print(f"Rows missing defensive_suppression_score: {summary['rows_missing_defensive_suppression_score']}")
    print(f"Rows with two_way_edge_score: {summary['rows_with_two_way_edge_score']}")
    print(f"Rows missing two_way_edge_score: {summary['rows_missing_two_way_edge_score']}")
    print(f"Rows with offensive_efficiency_support_score: {summary['rows_with_offensive_efficiency_support_score']}")
    print(f"Rows missing offensive_efficiency_support_score: {summary['rows_missing_offensive_efficiency_support_score']}")
    print(f"Offensive efficiency support bucket distribution: {summary['offensive_efficiency_support_bucket_distribution']}")
    print(f"Preview CSV: {output_dir / 'feature_update_preview.csv'}")
    print(f"Summary JSON: {output_dir / 'summary.json'}")

    if args.write_bigquery:
        update_bigquery_rows(
            client=client,
            bigquery=bigquery,
            project_id=args.project_id,
            target_table=args.training_table,
            update_rows=update_rows,
            run_id=args.run_id,
        )
    else:
        print("BigQuery update skipped. Use --write-bigquery to update the training table.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
