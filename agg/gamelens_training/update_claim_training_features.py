"""
Update GameLens claim training examples with Level 3 engineered feature scores.

V7 keeps scoped offense_finish_score, requires Defensive Control for defensive_suppression_score, adds two_way_context, and adds claim_strength_context.

Recommended repo location:
    agg/gamelens_training/update_claim_training_features.py

Purpose:
    Level 3 job for GameLens feature engineering.

    Level 1 built one row per pregame GameLens claim.
    Level 2 attached postgame validation labels.
    Level 3 begins adding pregame-only engineered features that can later help
    predict claim quality and calibrate language.

This worker computes:
    offense_finish_score
    defensive_suppression_score
    two_way_context
    claim_strength_score
    claim_strength_bucket
    claim_strength_context
    claim_strength_language_signal

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


PROJECT_ID = "nfl-stream-406420"
DATASET_ID = "Analytics"
TRAINING_TABLE = "gamelens_claim_training_examples"
DEFAULT_OUTPUT_ROOT = Path("qa/gamelens_feature_update_runs")
DEFAULT_FORMULA_VERSION = "offense_finish_v2__defensive_suppression_v3__two_way_context_v1__claim_strength_context_v1"

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


# V7 feature:
# claim_strength_context asks whether a pregame claim had enough matchup
# separation to deserve stronger, softer, or caution-only language.
#
# Football/product idea:
# - A "real edge" in trusted areas like Passing Game, Rushing Game, or
#   Scoring Suppression can support stronger claim language.
# - A "real edge" in volatile/noisy areas like Turnovers should stay
#   caution-only.
# - Thin or near-even edges should soften language.
#
# Important boundary:
# This is not winner prediction, not matchup_lean confidence, and not a pick
# override. It is only claim-language support context.
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
        "claim_strength_score",
        "claim_strength_bucket",
        "claim_strength_context",
        "claim_strength_language_signal",
        "claim_strength_notes",
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




def calculate_claim_strength_bucket(abs_percentile_gap: Optional[float]) -> str:
    """
    Bucket pregame separation using the same thresholds we tested in SQL.

    Buckets:
    - real_edge: meaningful separation
    - usable_edge: some separation, but not enough for strongest language
    - thin_edge: skinny edge; language should be softened
    - near_even: not enough separation
    - missing: no percentile-gap evidence available for this claim row
    """
    if abs_percentile_gap is None:
        return "missing"

    if abs_percentile_gap >= CLAIM_STRENGTH_REAL_EDGE_MIN:
        return "real_edge"

    if abs_percentile_gap >= CLAIM_STRENGTH_USABLE_EDGE_MIN:
        return "usable_edge"

    if abs_percentile_gap >= CLAIM_STRENGTH_THIN_EDGE_MIN:
        return "thin_edge"

    return "near_even"


def calculate_claim_strength_context(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build claim-strength language context for one pregame claim row.

    This feature answers a product/football question:
        Is this edge real enough, and in a football area trustworthy enough,
        to let GameLens speak more clearly?

    It intentionally does not change winner logic or confidence.
    It produces structured metadata for Level 4 language calibration.
    """
    abs_percentile_gap = as_float(row.get("pregame_abs_percentile_gap"))
    bucket = calculate_claim_strength_bucket(abs_percentile_gap)
    score = None if abs_percentile_gap is None else round(clamp(abs_percentile_gap / 100.0, 0.0, 1.0), 4)

    claim_type = row.get("claim_type")
    claim_layer = row.get("claim_layer")
    core_area = row.get("core_area")
    category = row.get("category")

    # Rows without percentile gap evidence cannot receive a boost from this feature.
    if bucket == "missing":
        return {
            "claim_strength_score": score,
            "claim_strength_bucket": bucket,
            "claim_strength_context": "missing_gap_context",
            "claim_strength_language_signal": "no_boost",
            "claim_strength_notes": (
                "claim_strength_context not available because pregame_abs_percentile_gap is missing."
            ),
        }

    # Small edges should not talk loudly, regardless of football area.
    if bucket == "near_even":
        return {
            "claim_strength_score": score,
            "claim_strength_bucket": bucket,
            "claim_strength_context": "near_even_gap",
            "claim_strength_language_signal": "soften",
            "claim_strength_notes": "Near-even pregame gap; use softer language.",
        }

    if bucket == "thin_edge":
        return {
            "claim_strength_score": score,
            "claim_strength_bucket": bucket,
            "claim_strength_context": "thin_gap",
            "claim_strength_language_signal": "soften",
            "claim_strength_notes": "Thin pregame gap; avoid strong edge language.",
        }

    # Volatile/noisy areas should stay caution-only even when the gap is large.
    if category in CAUTION_CLAIM_STRENGTH_CATEGORIES or core_area in CAUTION_CLAIM_STRENGTH_CORE_AREAS:
        return {
            "claim_strength_score": score,
            "claim_strength_bucket": bucket,
            "claim_strength_context": "caution_area_edge",
            "claim_strength_language_signal": "caution_only",
            "claim_strength_notes": (
                "Pregame gap exists, but this football area is volatile/noisy; "
                "treat as a swing factor rather than a strong edge."
            ),
        }

    # Trusted areas earned the first-pass language support in SQL testing.
    if category in TRUSTED_CLAIM_STRENGTH_CATEGORIES or (
        category is None and core_area in TRUSTED_CLAIM_STRENGTH_CORE_AREAS
    ):
        if bucket == "real_edge":
            return {
                "claim_strength_score": score,
                "claim_strength_bucket": bucket,
                "claim_strength_context": "trusted_real_edge",
                "claim_strength_language_signal": "boost_candidate",
                "claim_strength_notes": (
                    "Real pregame separation in a trusted football area; candidate for clearer claim language."
                ),
            }

        return {
            "claim_strength_score": score,
            "claim_strength_bucket": bucket,
            "claim_strength_context": "trusted_measured_edge",
            "claim_strength_language_signal": "measured",
            "claim_strength_notes": (
                "Pregame separation exists in a trusted football area, but not enough for strongest language."
            ),
        }

    # Watch areas may be useful, but should be measured until Level 4 confirms.
    if category in WATCH_CLAIM_STRENGTH_CATEGORIES:
        return {
            "claim_strength_score": score,
            "claim_strength_bucket": bucket,
            "claim_strength_context": "watch_area_edge",
            "claim_strength_language_signal": "measured",
            "claim_strength_notes": (
                "Pregame separation exists in a watch area; keep language measured until further calibration."
            ),
        }

    # Default: useful as context, but do not boost yet.
    return {
        "claim_strength_score": score,
        "claim_strength_bucket": bucket,
        "claim_strength_context": "unclassified_edge",
        "claim_strength_language_signal": "normal",
        "claim_strength_notes": (
            f"Pregame gap bucket={bucket}, but claim surface is not yet allowlisted for stronger language "
            f"(claim_type={claim_type}, claim_layer={claim_layer}, core_area={core_area}, category={category})."
        ),
    }

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
    now = utc_now_iso()
    updates: List[Dict[str, Any]] = []

    for row in training_rows:
        game_id = str(row.get("game_id") or "")
        claimed_side = row.get("claimed_side")
        game_edges = core_edges.get(game_id, {})

        # -------------------------
        # Claim strength context feature
        # -------------------------
        claim_strength = calculate_claim_strength_context(row)
        claim_strength_note = (
            " | Level 3 v7: claim_strength_context computed from pregame percentile-gap "
            "separation and trusted/caution football area grouping; intended for claim-language "
            "calibration only, not winner prediction."
        )

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

        feature_notes = strip_prior_level3_notes(row.get("feature_notes")) + claim_strength_note + offense_note + defensive_note + two_way_note

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

            "claim_strength_score": claim_strength["claim_strength_score"],
            "claim_strength_bucket": claim_strength["claim_strength_bucket"],
            "claim_strength_context": claim_strength["claim_strength_context"],
            "claim_strength_language_signal": claim_strength["claim_strength_language_signal"],
            "claim_strength_notes": claim_strength["claim_strength_notes"],

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
        bigquery.SchemaField("claim_strength_score", "FLOAT"),
        bigquery.SchemaField("claim_strength_bucket", "STRING"),
        bigquery.SchemaField("claim_strength_context", "STRING"),
        bigquery.SchemaField("claim_strength_language_signal", "STRING"),
        bigquery.SchemaField("claim_strength_notes", "STRING"),
        bigquery.SchemaField("offense_finish_score", "FLOAT"),
        bigquery.SchemaField("defensive_suppression_score", "FLOAT"),
        bigquery.SchemaField("two_way_edge_score", "FLOAT"),
        bigquery.SchemaField("two_way_context", "STRING"),
        bigquery.SchemaField("feature_formula_version", "STRING"),
        bigquery.SchemaField("feature_notes", "STRING"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]


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
            "claim_strength_score": row["claim_strength_score"],
            "claim_strength_bucket": row["claim_strength_bucket"],
            "claim_strength_context": row["claim_strength_context"],
            "claim_strength_language_signal": row["claim_strength_language_signal"],
            "claim_strength_notes": row["claim_strength_notes"],
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
            claim_strength_score = S.claim_strength_score,
            claim_strength_bucket = S.claim_strength_bucket,
            claim_strength_context = S.claim_strength_context,
            claim_strength_language_signal = S.claim_strength_language_signal,
            claim_strength_notes = S.claim_strength_notes,
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

    claim_strength_bucket_counts: Dict[str, int] = {}
    claim_strength_context_counts: Dict[str, int] = {}
    claim_strength_language_signal_counts: Dict[str, int] = {}
    claim_strength_non_null_scores = []

    for row in update_rows:
        claim_strength_bucket = row.get("claim_strength_bucket") or "NULL"
        claim_strength_bucket_counts[claim_strength_bucket] = claim_strength_bucket_counts.get(claim_strength_bucket, 0) + 1

        claim_strength_context = row.get("claim_strength_context") or "NULL"
        claim_strength_context_counts[claim_strength_context] = claim_strength_context_counts.get(claim_strength_context, 0) + 1

        claim_strength_signal = row.get("claim_strength_language_signal") or "NULL"
        claim_strength_language_signal_counts[claim_strength_signal] = claim_strength_language_signal_counts.get(claim_strength_signal, 0) + 1

        if row.get("claim_strength_score") is not None:
            claim_strength_non_null_scores.append(row["claim_strength_score"])

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

        "rows_with_claim_strength_score": len(claim_strength_non_null_scores),
        "rows_missing_claim_strength_score": len(update_rows) - len(claim_strength_non_null_scores),
        "min_claim_strength_score": min(claim_strength_non_null_scores) if claim_strength_non_null_scores else None,
        "max_claim_strength_score": max(claim_strength_non_null_scores) if claim_strength_non_null_scores else None,
        "avg_claim_strength_score": round(sum(claim_strength_non_null_scores) / len(claim_strength_non_null_scores), 4) if claim_strength_non_null_scores else None,
        "claim_strength_bucket_distribution": dict(sorted(claim_strength_bucket_counts.items())),
        "claim_strength_context_distribution": dict(sorted(claim_strength_context_counts.items())),
        "claim_strength_language_signal_distribution": dict(sorted(claim_strength_language_signal_counts.items())),

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
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update GameLens claim training examples with Level 3 engineered feature scores. Adds two_way_context and claim_strength_context.")
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
    print(f"Rows with claim_strength_score: {summary['rows_with_claim_strength_score']}")
    print(f"Rows missing claim_strength_score: {summary['rows_missing_claim_strength_score']}")
    print(f"Rows with offense_finish_score: {summary['rows_with_offense_finish_score']}")
    print(f"Rows missing offense_finish_score: {summary['rows_missing_offense_finish_score']}")
    print(f"Rows with defensive_suppression_score: {summary['rows_with_defensive_suppression_score']}")
    print(f"Rows missing defensive_suppression_score: {summary['rows_missing_defensive_suppression_score']}")
    print(f"Rows with two_way_edge_score: {summary['rows_with_two_way_edge_score']}")
    print(f"Rows missing two_way_edge_score: {summary['rows_missing_two_way_edge_score']}")
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
