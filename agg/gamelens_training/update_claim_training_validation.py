"""
Update GameLens claim training examples with postgame validation labels.

Recommended repo location:
    agg/update_gamelens_claim_training_validation_v2.py

Purpose:
    Level 2 job for GameLens claim-quality learning.

    Level 1 built one row per pregame GameLens claim in:
        Analytics.gamelens_claim_training_examples

    Level 2 fills validation fields on those rows:
        actual_team
        actual_side
        validation_result
        validated_flag
        actual_gap
        actual_gap_bucket
        elevated_deserved_flag
        qa_read_v2
        headline_claim_validation_rate
        unique_claim_validation_rate

Source of truth:
    Postgame actuals are pulled from:
        Analytics.game_team_metric_facts_{season}

Important v1 limitation:
    game_team_metric_facts_{season} contains actual postgame metric values and
    comparison_direction, but not postgame rank/percentile context. Therefore:
        actual_rank_gap = NULL
        actual_percentile_gap = NULL
    in v1.

Example dry run:
    python -m agg.update_gamelens_claim_training_validation_v2 \
      --run-id baseline_96_stage1_v2 \
      --dry-run

Example BigQuery update:
    python -m agg.update_gamelens_claim_training_validation_v2 \
      --run-id baseline_96_stage1_v2 \
      --write-bigquery
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


PROJECT_ID = "nfl-stream-406420"
DATASET_ID = "Analytics"
TRAINING_TABLE = "gamelens_claim_training_examples"
FACTS_TABLE_TEMPLATE = "Analytics.game_team_metric_facts_{season}"
DEFAULT_OUTPUT_ROOT = Path("qa/gamelens_validation_update_runs")

SIDE_VALUES = {"away", "home", "neutral"}
DIRECTIONAL_CLAIM_TYPES = {
    "game_profile",
    "core_area_comparison",
    "core_area_summary",
    "category_summary",
    "metric_highlight",
    "team_comparison_metric",
}

# Pregame/windowed metric names sometimes differ from single-game postgame fact names.
# Example:
#   Pregame payload uses: turnover_margin_per_game
#   Postgame facts use:   turnover_margin
#
# For one completed game, turnover_margin is the correct actual-game validation
# fallback for turnover_margin_per_game.
METRIC_ALIAS_MAP = {
    "turnover_margin_per_game": ["turnover_margin_per_game", "turnover_margin"],
}


def metric_candidates(metric: str) -> List[str]:
    """Return the primary metric plus any postgame fact fallback aliases."""
    candidates = METRIC_ALIAS_MAP.get(metric, [metric])
    out: List[str] = []
    for candidate in candidates:
        if candidate and candidate not in out:
            out.append(candidate)
    return out


GAME_PROFILE_GROUP_MAP = {
    # claim_name / group_name -> candidates in order
    "pressure": [
        ("category", "Pressure"),
        ("core_area", "Disruption and Turnovers"),
    ],
    "turnover risk": [
        ("category", "Turnovers"),
        ("core_area", "Disruption and Turnovers"),
    ],
    "turnovers": [
        ("category", "Turnovers"),
        ("core_area", "Disruption and Turnovers"),
    ],
    "scoring efficiency": [
        ("core_area", "Scoring Efficiency"),
        ("category", "Scoring Production"),
    ],
    "defensive control": [
        ("core_area", "Defensive Control"),
    ],
    "offensive output": [
        ("core_area", "Offensive Output"),
    ],
}


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_identifier(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_")[:80] or "run"


def as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        number = float(value)
        if math.isnan(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def as_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def json_safe(value: Any) -> Any:
    if isinstance(value, (datetime,)):
        return value.isoformat()
    return str(value)


def write_json(data: Any, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2, default=json_safe), encoding="utf-8")


def write_csv(rows: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return

    preferred = [
        "run_id",
        "claim_key",
        "game_id",
        "claim_type",
        "claim_layer",
        "claim_name",
        "claimed_side",
        "claimed_team",
        "metric",
        "core_area",
        "category",
        "actual_side",
        "actual_team",
        "validation_result",
        "validated_flag",
        "actual_gap",
        "actual_gap_bucket",
        "elevated_deserved_flag",
        "qa_read_v2",
        "headline_claim_validation_rate",
        "unique_claim_validation_rate",
        "feature_status",
        "feature_notes",
        "updated_at",
    ]
    all_keys = sorted({k for row in rows for k in row.keys()})
    fieldnames = [k for k in preferred if k in all_keys] + [k for k in all_keys if k not in preferred]

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# BigQuery loading
# ---------------------------------------------------------------------------

def import_bigquery():
    from google.cloud import bigquery
    return bigquery


def load_training_rows(
    *,
    client: Any,
    bigquery: Any,
    project_id: str,
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
            game_date,
            game_week,
            away_team,
            home_team,
            claimed_team,
            claimed_side,
            opponent_team,
            opponent_side,
            claim_type,
            claim_layer,
            claim_name,
            claim_text,
            group_name,
            core_area,
            category,
            metric,
            metric_label,
            summary_label,
            model_result,
            is_tie,
            final_margin_abs
        FROM `{table_ref}`
        WHERE run_id = @run_id
        ORDER BY season, game_date, game_id, claim_type, claim_layer, claim_rank
        {limit_clause}
    """

    params = [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
    if limit:
        params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return [dict(row) for row in client.query(query, job_config=job_config).result()]


def group_game_ids_by_season(training_rows: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    out: Dict[str, set[str]] = defaultdict(set)
    for row in training_rows:
        season = str(row.get("season") or "")
        game_id = str(row.get("game_id") or "")
        if season and game_id:
            out[season].add(game_id)
    return {season: sorted(game_ids) for season, game_ids in out.items()}


def load_actual_metrics_from_facts(
    *,
    client: Any,
    bigquery: Any,
    project_id: str,
    facts_table_template: str,
    game_ids_by_season: Dict[str, List[str]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for season, game_ids in sorted(game_ids_by_season.items()):
        if not game_ids:
            continue

        table = f"{project_id}.{facts_table_template.format(season=season)}"
        query = f"""
            SELECT
                CAST(season AS STRING) AS season,
                game_id,
                game_date,
                game_week,
                team_id,
                team_abv,
                team_type,
                metric,
                SAFE_CAST(value AS FLOAT64) AS value,
                label,
                category,
                core_area,
                comparison_direction
            FROM `{table}`
            WHERE game_id IN UNNEST(@game_ids)
              AND value IS NOT NULL
              AND comparison_direction IN ('higher', 'lower')
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("game_ids", "STRING", list(game_ids))]
        )

        try:
            season_rows = [dict(row) for row in client.query(query, job_config=job_config).result()]
            rows.extend(season_rows)
        except Exception as exc:
            errors.append({
                "season": season,
                "source": "facts",
                "table": table,
                "error": str(exc),
            })

    return rows, errors


# ---------------------------------------------------------------------------
# Actual metric comparison
# ---------------------------------------------------------------------------

def build_actual_index(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    game_id -> {
        away: {metric: row},
        home: {metric: row},
        rows: [row, ...]
    }
    """
    index: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        game_id = str(row.get("game_id") or "")
        side = str(row.get("team_type") or "")
        metric = str(row.get("metric") or "")
        if not game_id or side not in {"away", "home"} or not metric:
            continue

        game = index.setdefault(game_id, {"away": {}, "home": {}, "rows": []})
        game[side][metric] = row
        game["rows"].append(row)

    return index


def is_near_even(a: float, b: float, *, abs_tol: float, pct_tol: float) -> bool:
    diff = abs(a - b)
    if diff <= abs_tol:
        return True
    scale = max(abs(a), abs(b), 1.0)
    return (diff / scale) <= pct_tol


def actual_gap_bucket_from_values(
    *,
    actual_gap_for_claimed: Optional[float],
    away_value: Optional[float],
    home_value: Optional[float],
    actual_side: str,
) -> str:
    if actual_side == "unavailable":
        return "unknown"
    if actual_side == "neutral":
        return "near_even"
    if actual_gap_for_claimed is None or away_value is None or home_value is None:
        return "unknown"

    # Use relative magnitude for rough cross-metric bucket.
    diff = abs(away_value - home_value)
    scale = max(abs(away_value), abs(home_value), 1.0)
    ratio = diff / scale

    if ratio <= 0.05:
        return "small_edge"
    if ratio <= 0.20:
        return "clear_edge"
    return "dominant_edge"


def actual_gap_bucket_from_group(
    *,
    claimed_wins: Optional[int],
    opponent_wins: Optional[int],
    neutral_count: Optional[int],
    metrics_checked: Optional[int],
    actual_side: str,
) -> str:
    if actual_side == "unavailable":
        return "unknown"
    if actual_side == "neutral":
        return "near_even"
    if claimed_wins is None or opponent_wins is None or not metrics_checked:
        return "unknown"

    gap_share = abs(claimed_wins - opponent_wins) / max(metrics_checked, 1)
    if gap_share <= 0.25:
        return "small_edge"
    if gap_share <= 0.60:
        return "clear_edge"
    return "dominant_edge"


def compare_metric(
    game_actuals: Dict[str, Any],
    metric: str,
    claimed_side: str,
    *,
    abs_tol: float,
    pct_tol: float,
) -> Dict[str, Any]:
    selected_metric = None
    away_row = None
    home_row = None

    # Try the requested metric first, then any known postgame fact aliases.
    for candidate_metric in metric_candidates(metric):
        candidate_away = game_actuals.get("away", {}).get(candidate_metric)
        candidate_home = game_actuals.get("home", {}).get(candidate_metric)

        if candidate_away and candidate_home:
            selected_metric = candidate_metric
            away_row = candidate_away
            home_row = candidate_home
            break

    if not away_row or not home_row or not selected_metric:
        return {
            "actual_side": "unavailable",
            "actual_team": None,
            "actual_gap": None,
            "actual_gap_bucket": "unknown",
            "actual_note": f"missing_metric_actuals:{metric}",
        }

    direction = str(away_row.get("comparison_direction") or home_row.get("comparison_direction") or "").lower()
    if direction not in {"higher", "lower"}:
        return {
            "actual_side": "unavailable",
            "actual_team": None,
            "actual_gap": None,
            "actual_gap_bucket": "unknown",
            "actual_note": f"metric_not_directional:{selected_metric}",
        }

    away_val = as_float(away_row.get("value"))
    home_val = as_float(home_row.get("value"))
    if away_val is None or home_val is None:
        return {
            "actual_side": "unavailable",
            "actual_team": None,
            "actual_gap": None,
            "actual_gap_bucket": "unknown",
            "actual_note": f"actual_value_not_numeric:{selected_metric}",
        }

    if is_near_even(away_val, home_val, abs_tol=abs_tol, pct_tol=pct_tol):
        actual_side = "neutral"
    elif direction == "higher":
        actual_side = "away" if away_val > home_val else "home"
    else:
        actual_side = "away" if away_val < home_val else "home"

    actual_team = None
    if actual_side == "away":
        actual_team = away_row.get("team_abv")
    elif actual_side == "home":
        actual_team = home_row.get("team_abv")
    elif actual_side == "neutral":
        actual_team = "neutral"

    # Direction-adjusted gap from claimed team's perspective.
    if claimed_side == "away":
        claimed_val, opponent_val = away_val, home_val
    else:
        claimed_val, opponent_val = home_val, away_val

    if direction == "higher":
        actual_gap_for_claimed = claimed_val - opponent_val
    else:
        actual_gap_for_claimed = opponent_val - claimed_val

    bucket = actual_gap_bucket_from_values(
        actual_gap_for_claimed=actual_gap_for_claimed,
        away_value=away_val,
        home_value=home_val,
        actual_side=actual_side,
    )

    alias_note = (
        f"metric_compare:{metric}->alias:{selected_metric}"
        if selected_metric != metric
        else f"metric_compare:{metric}"
    )

    return {
        "actual_side": actual_side,
        "actual_team": actual_team,
        "actual_gap": actual_gap_for_claimed,
        "actual_gap_bucket": bucket,
        "actual_note": alias_note,
    }


def compare_group(
    game_actuals: Dict[str, Any],
    *,
    group_kind: str,
    group_name: str,
    claimed_side: str,
    abs_tol: float,
    pct_tol: float,
) -> Dict[str, Any]:
    rows = game_actuals.get("rows", []) or []

    if group_kind == "core_area":
        metrics = sorted({row.get("metric") for row in rows if row.get("core_area") == group_name and row.get("metric")})
    elif group_kind == "category":
        metrics = sorted({row.get("metric") for row in rows if row.get("category") == group_name and row.get("metric")})
    else:
        metrics = []

    if not metrics:
        return {
            "actual_side": "unavailable",
            "actual_team": None,
            "actual_gap": None,
            "actual_gap_bucket": "unknown",
            "actual_note": f"missing_group_actuals:{group_kind}:{group_name}",
            "actual_metrics_checked": 0,
        }

    away_wins = 0
    home_wins = 0
    neutral_count = 0
    unavailable_count = 0
    metric_results = []

    for metric in metrics:
        result = compare_metric(
            game_actuals=game_actuals,
            metric=metric,
            claimed_side=claimed_side,
            abs_tol=abs_tol,
            pct_tol=pct_tol,
        )
        side = result.get("actual_side")
        if side == "away":
            away_wins += 1
        elif side == "home":
            home_wins += 1
        elif side == "neutral":
            neutral_count += 1
        else:
            unavailable_count += 1

        metric_results.append({
            "metric": metric,
            "actual_side": side,
            "actual_gap": result.get("actual_gap"),
            "actual_gap_bucket": result.get("actual_gap_bucket"),
        })

    if away_wins > home_wins:
        actual_side = "away"
    elif home_wins > away_wins:
        actual_side = "home"
    else:
        actual_side = "neutral"

    away_team = None
    home_team = None
    for row in rows:
        if row.get("team_type") == "away" and not away_team:
            away_team = row.get("team_abv")
        elif row.get("team_type") == "home" and not home_team:
            home_team = row.get("team_abv")

    if actual_side == "away":
        actual_team = away_team
    elif actual_side == "home":
        actual_team = home_team
    else:
        actual_team = "neutral"

    if claimed_side == "away":
        claimed_wins, opponent_wins = away_wins, home_wins
    else:
        claimed_wins, opponent_wins = home_wins, away_wins

    actual_gap = claimed_wins - opponent_wins
    bucket = actual_gap_bucket_from_group(
        claimed_wins=claimed_wins,
        opponent_wins=opponent_wins,
        neutral_count=neutral_count,
        metrics_checked=len(metrics),
        actual_side=actual_side,
    )

    return {
        "actual_side": actual_side,
        "actual_team": actual_team,
        "actual_gap": float(actual_gap),
        "actual_gap_bucket": bucket,
        "actual_note": f"group_compare:{group_kind}:{group_name};metrics={len(metrics)}",
        "actual_metrics_checked": len(metrics),
        "actual_away_metric_wins": away_wins,
        "actual_home_metric_wins": home_wins,
        "actual_neutral_metrics": neutral_count,
        "actual_unavailable_metrics": unavailable_count,
        "actual_metrics_json": json.dumps(metric_results, sort_keys=True),
    }


def compare_game_profile(
    row: Dict[str, Any],
    game_actuals: Dict[str, Any],
    *,
    abs_tol: float,
    pct_tol: float,
) -> Dict[str, Any]:
    name = str(row.get("claim_name") or row.get("group_name") or "").strip().lower()
    claimed_side = str(row.get("claimed_side") or "")

    candidates = GAME_PROFILE_GROUP_MAP.get(name, [])
    for group_kind, group_name in candidates:
        result = compare_group(
            game_actuals=game_actuals,
            group_kind=group_kind,
            group_name=group_name,
            claimed_side=claimed_side,
            abs_tol=abs_tol,
            pct_tol=pct_tol,
        )
        if result.get("actual_side") != "unavailable":
            result["actual_note"] = f"game_profile_proxy:{group_kind}:{group_name}"
            return result

    return {
        "actual_side": "unavailable",
        "actual_team": None,
        "actual_gap": None,
        "actual_gap_bucket": "unknown",
        "actual_note": f"no_game_profile_mapping:{name}",
    }


def validation_result_for_claim(claimed_side: Optional[str], actual_side: Optional[str]) -> str:
    claimed = claimed_side if claimed_side in SIDE_VALUES else None
    actual = actual_side if actual_side in SIDE_VALUES else actual_side

    if actual in {None, "unavailable"}:
        return "unavailable"
    if claimed is None:
        return "unavailable"

    if claimed == "neutral":
        return "validated" if actual == "neutral" else "neutral_claim_but_actual_edge"

    if actual == "neutral":
        return "actual_neutral_or_mixed"

    if claimed == actual:
        return "validated"

    return "not_validated"


def validate_training_row(
    row: Dict[str, Any],
    actual_index: Dict[str, Dict[str, Any]],
    *,
    abs_tol: float,
    pct_tol: float,
) -> Dict[str, Any]:
    game_id = str(row.get("game_id") or "")
    game_actuals = actual_index.get(game_id)
    now = utc_now_iso()

    if not game_actuals:
        actual = {
            "actual_side": "unavailable",
            "actual_team": None,
            "actual_gap": None,
            "actual_gap_bucket": "unknown",
            "actual_note": "no_actual_metrics_for_game",
        }
    else:
        claim_type = row.get("claim_type")
        metric = row.get("metric")
        claimed_side = str(row.get("claimed_side") or "")

        if metric:
            actual = compare_metric(
                game_actuals=game_actuals,
                metric=str(metric),
                claimed_side=claimed_side,
                abs_tol=abs_tol,
                pct_tol=pct_tol,
            )
        elif claim_type in {"core_area_comparison", "core_area_summary"} and row.get("core_area"):
            actual = compare_group(
                game_actuals=game_actuals,
                group_kind="core_area",
                group_name=str(row.get("core_area")),
                claimed_side=claimed_side,
                abs_tol=abs_tol,
                pct_tol=pct_tol,
            )
        elif claim_type == "category_summary" and row.get("category"):
            actual = compare_group(
                game_actuals=game_actuals,
                group_kind="category",
                group_name=str(row.get("category")),
                claimed_side=claimed_side,
                abs_tol=abs_tol,
                pct_tol=pct_tol,
            )
        elif claim_type == "game_profile":
            actual = compare_game_profile(
                row=row,
                game_actuals=game_actuals,
                abs_tol=abs_tol,
                pct_tol=pct_tol,
            )
        else:
            actual = {
                "actual_side": "unavailable",
                "actual_team": None,
                "actual_gap": None,
                "actual_gap_bucket": "unknown",
                "actual_note": "no_metric_or_group_mapping",
            }

    validation_result = validation_result_for_claim(row.get("claimed_side"), actual.get("actual_side"))

    if validation_result == "validated":
        validated_flag: Optional[bool] = True
    elif validation_result == "not_validated":
        validated_flag = False
    else:
        validated_flag = None

    # Conservative v1 label: only validated claims with clear/dominant actual edge
    # get elevated_deserved=True. Not-validated claims get False. Neutral/unavailable stays NULL.
    if validation_result == "validated":
        elevated_deserved_flag = actual.get("actual_gap_bucket") in {"clear_edge", "dominant_edge"}
    elif validation_result == "not_validated":
        elevated_deserved_flag = False
    else:
        elevated_deserved_flag = None

    feature_status = "validated" if validation_result != "unavailable" else "missing_postgame_actual"
    notes = (
        f"Level 2 v2 validation from game_team_metric_facts. {actual.get('actual_note')}. "
        "actual_rank_gap and actual_percentile_gap left NULL in v1 because facts table has no postgame ranking context."
    )

    return {
        "run_id": row.get("run_id"),
        "claim_key": row.get("claim_key"),
        "feature_build_stage": "validated",
        "feature_status": feature_status,
        "feature_notes": notes,
        "actual_team": actual.get("actual_team"),
        "actual_side": actual.get("actual_side"),
        "validation_result": validation_result,
        "validated_flag": validated_flag,
        "elevated_deserved_flag": elevated_deserved_flag,
        "actual_gap": actual.get("actual_gap"),
        "actual_rank_gap": None,
        "actual_percentile_gap": None,
        "actual_gap_bucket": actual.get("actual_gap_bucket"),
        "updated_at": now,
    }


# ---------------------------------------------------------------------------
# Game-level scoring
# ---------------------------------------------------------------------------

def rows_for_score(rows: List[Dict[str, Any]], *, mode: str) -> List[Dict[str, Any]]:
    directional = [r for r in rows if r.get("claim_type") in DIRECTIONAL_CLAIM_TYPES]

    if mode == "headline":
        return [r for r in directional if r.get("claim_layer") == "headline"]
    if mode == "supporting":
        return [r for r in directional if r.get("claim_layer") == "supporting"]
    if mode == "full":
        return directional

    if mode == "unique":
        seen = set()
        unique_rows = []
        for row in directional:
            key = (
                row.get("claim_type"),
                row.get("claim_layer"),
                row.get("group_name"),
                row.get("core_area"),
                row.get("category"),
                row.get("metric"),
                row.get("claimed_side"),
            )
            if key in seen:
                continue
            seen.add(key)
            unique_rows.append(row)
        return unique_rows

    raise ValueError(f"Unsupported score mode: {mode}")


def score_validation_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    eligible = [r for r in rows if r.get("validation_result") != "unavailable"]
    validated = [r for r in eligible if r.get("validation_result") == "validated"]
    not_validated = [r for r in eligible if r.get("validation_result") == "not_validated"]
    neutralish = [
        r for r in eligible
        if r.get("validation_result") in {"actual_neutral_or_mixed", "neutral_claim_but_actual_edge"}
    ]

    return {
        "eligible": len(eligible),
        "validated": len(validated),
        "not_validated": len(not_validated),
        "neutralish": len(neutralish),
        "rate": round(len(validated) / len(eligible), 4) if eligible else None,
    }


def qa_read_for_game(
    *,
    model_result: Optional[str],
    headline_rate: Optional[float],
    valid_threshold: float,
    weak_threshold: float,
) -> str:
    result = str(model_result or "").strip().lower()

    if result in {"no decision", "tie", "push"}:
        return "tie_no_decision"

    if result == "no pick":
        return "no_pick_claim_review"

    if headline_rate is None:
        return "mixed_review"

    if result == "correct":
        if headline_rate >= valid_threshold:
            return "good_reasoning_correct_outcome"
        if headline_rate <= weak_threshold:
            return "outcome_correct_reasoning_mixed"
        return "mixed_review"

    if result == "incorrect":
        if headline_rate >= valid_threshold:
            return "good_reasoning_bad_outcome"
        if headline_rate <= weak_threshold:
            return "bad_reasoning_bad_outcome"
        return "mixed_review"

    return "mixed_review"


def add_game_level_scores(
    validation_rows: List[Dict[str, Any]],
    training_rows: List[Dict[str, Any]],
    *,
    valid_threshold: float,
    weak_threshold: float,
) -> None:
    training_by_key = {row["claim_key"]: row for row in training_rows}
    by_game: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for row in validation_rows:
        original = training_by_key.get(row["claim_key"], {})
        merged = {**original, **row}
        by_game[str(original.get("game_id") or "")].append(merged)

    game_scores: Dict[str, Dict[str, Any]] = {}

    for game_id, rows in by_game.items():
        if not game_id:
            continue

        headline = score_validation_rows(rows_for_score(rows, mode="headline"))
        unique = score_validation_rows(rows_for_score(rows, mode="unique"))

        model_result = None
        for row in rows:
            if row.get("model_result"):
                model_result = row.get("model_result")
                break

        qa_read = qa_read_for_game(
            model_result=model_result,
            headline_rate=headline["rate"],
            valid_threshold=valid_threshold,
            weak_threshold=weak_threshold,
        )

        game_scores[game_id] = {
            "headline_claim_validation_rate": headline["rate"],
            "unique_claim_validation_rate": unique["rate"],
            "qa_read_v2": qa_read,
        }

    for row in validation_rows:
        original = training_by_key.get(row["claim_key"], {})
        game_id = str(original.get("game_id") or "")
        score = game_scores.get(game_id, {})
        row["headline_claim_validation_rate"] = score.get("headline_claim_validation_rate")
        row["unique_claim_validation_rate"] = score.get("unique_claim_validation_rate")
        row["qa_read_v2"] = score.get("qa_read_v2")


# ---------------------------------------------------------------------------
# BigQuery update
# ---------------------------------------------------------------------------

def validation_update_schema(bigquery: Any) -> List[Any]:
    return [
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("claim_key", "STRING"),
        bigquery.SchemaField("feature_build_stage", "STRING"),
        bigquery.SchemaField("feature_status", "STRING"),
        bigquery.SchemaField("feature_notes", "STRING"),
        bigquery.SchemaField("actual_team", "STRING"),
        bigquery.SchemaField("actual_side", "STRING"),
        bigquery.SchemaField("validation_result", "STRING"),
        bigquery.SchemaField("validated_flag", "BOOLEAN"),
        bigquery.SchemaField("elevated_deserved_flag", "BOOLEAN"),
        bigquery.SchemaField("actual_gap", "FLOAT"),
        bigquery.SchemaField("actual_rank_gap", "INTEGER"),
        bigquery.SchemaField("actual_percentile_gap", "FLOAT"),
        bigquery.SchemaField("actual_gap_bucket", "STRING"),
        bigquery.SchemaField("qa_read_v2", "STRING"),
        bigquery.SchemaField("headline_claim_validation_rate", "FLOAT"),
        bigquery.SchemaField("unique_claim_validation_rate", "FLOAT"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]


def update_bigquery_rows(
    *,
    client: Any,
    bigquery: Any,
    project_id: str,
    target_table: str,
    validation_rows: List[Dict[str, Any]],
    run_id: str,
) -> None:
    if not validation_rows:
        print("No validation rows to update.")
        return

    safe_run = sanitize_identifier(run_id)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    temp_table = f"{project_id}.{DATASET_ID}._tmp_gamelens_claim_validation_{safe_run}_{timestamp}"

    job_config = bigquery.LoadJobConfig(
        schema=validation_update_schema(bigquery),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = client.load_table_from_json(validation_rows, temp_table, job_config=job_config)
    load_job.result()
    print(f"Loaded {len(validation_rows)} validation rows into temp table {temp_table}")

    merge_sql = f"""
        MERGE `{target_table}` AS T
        USING `{temp_table}` AS S
        ON T.run_id = S.run_id
           AND T.claim_key = S.claim_key
        WHEN MATCHED THEN UPDATE SET
            feature_build_stage = S.feature_build_stage,
            feature_status = S.feature_status,
            feature_notes = S.feature_notes,
            actual_team = S.actual_team,
            actual_side = S.actual_side,
            validation_result = S.validation_result,
            validated_flag = S.validated_flag,
            elevated_deserved_flag = S.elevated_deserved_flag,
            actual_gap = S.actual_gap,
            actual_rank_gap = S.actual_rank_gap,
            actual_percentile_gap = S.actual_percentile_gap,
            actual_gap_bucket = S.actual_gap_bucket,
            qa_read_v2 = S.qa_read_v2,
            headline_claim_validation_rate = S.headline_claim_validation_rate,
            unique_claim_validation_rate = S.unique_claim_validation_rate,
            updated_at = S.updated_at
    """
    client.query(merge_sql).result()
    print(f"✅ Updated {target_table} for run_id={run_id}")

    client.delete_table(temp_table, not_found_ok=True)
    print(f"Deleted temp table {temp_table}")


# ---------------------------------------------------------------------------
# Summaries
# ---------------------------------------------------------------------------

def count_by(rows: List[Dict[str, Any]], field: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in rows:
        key = str(row.get(field) if row.get(field) is not None else "NULL")
        out[key] = out.get(key, 0) + 1
    return dict(sorted(out.items()))


def build_summary(
    *,
    training_rows: List[Dict[str, Any]],
    actual_rows: List[Dict[str, Any]],
    validation_rows: List[Dict[str, Any]],
    actual_errors: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "training_rows_loaded": len(training_rows),
        "actual_metric_rows_loaded": len(actual_rows),
        "validation_rows_built": len(validation_rows),
        "actual_load_errors": len(actual_errors),
        "by_validation_result": count_by(validation_rows, "validation_result"),
        "by_actual_gap_bucket": count_by(validation_rows, "actual_gap_bucket"),
        "by_qa_read_v2": count_by(validation_rows, "qa_read_v2"),
        "by_feature_status": count_by(validation_rows, "feature_status"),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update GameLens claim training examples with postgame validation labels.")
    parser.add_argument("--run-id", required=True, help="run_id in Analytics.gamelens_claim_training_examples to validate.")
    parser.add_argument("--project-id", default=PROJECT_ID)
    parser.add_argument("--training-table", default=f"{PROJECT_ID}.{DATASET_ID}.{TRAINING_TABLE}")
    parser.add_argument("--facts-table-template", default=FACTS_TABLE_TEMPLATE)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--abs-tol", type=float, default=0.0001, help="Absolute near-even tolerance for actual metric comparison.")
    parser.add_argument("--pct-tol", type=float, default=0.02, help="Percent/relative near-even tolerance for actual metric comparison.")
    parser.add_argument("--reasoning-valid-threshold", type=float, default=0.60)
    parser.add_argument("--reasoning-weak-threshold", type=float, default=0.40)
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit for testing.")
    parser.add_argument("--dry-run", action="store_true", help="Write local preview CSV/JSON only. Recommended first.")
    parser.add_argument("--write-bigquery", action="store_true", help="MERGE validation fields back into BigQuery.")
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
        project_id=args.project_id,
        table_ref=args.training_table,
        run_id=args.run_id,
        limit=args.limit,
    )
    print(f"Training rows loaded: {len(training_rows)}")

    game_ids_by_season = group_game_ids_by_season(training_rows)
    print(f"Seasons: {', '.join(sorted(game_ids_by_season))}")

    actual_rows, actual_errors = load_actual_metrics_from_facts(
        client=client,
        bigquery=bigquery,
        project_id=args.project_id,
        facts_table_template=args.facts_table_template,
        game_ids_by_season=game_ids_by_season,
    )
    print(f"Actual metric rows loaded: {len(actual_rows)}")
    if actual_errors:
        print(f"Actual load errors: {len(actual_errors)}")

    actual_index = build_actual_index(actual_rows)

    validation_rows = [
        validate_training_row(
            row=row,
            actual_index=actual_index,
            abs_tol=args.abs_tol,
            pct_tol=args.pct_tol,
        )
        for row in training_rows
    ]

    add_game_level_scores(
        validation_rows=validation_rows,
        training_rows=training_rows,
        valid_threshold=args.reasoning_valid_threshold,
        weak_threshold=args.reasoning_weak_threshold,
    )

    summary = build_summary(
        training_rows=training_rows,
        actual_rows=actual_rows,
        validation_rows=validation_rows,
        actual_errors=actual_errors,
    )

    write_csv(validation_rows, output_dir / "validation_update_preview.csv")
    write_json(summary, output_dir / "summary.json")
    write_json(actual_errors, output_dir / "actual_load_errors.json")

    print("\nValidation build complete")
    print("-------------------------")
    print(f"Validation rows built: {len(validation_rows)}")
    print(f"Preview CSV: {output_dir / 'validation_update_preview.csv'}")
    print(f"Summary JSON: {output_dir / 'summary.json'}")

    if args.write_bigquery:
        update_bigquery_rows(
            client=client,
            bigquery=bigquery,
            project_id=args.project_id,
            target_table=args.training_table,
            validation_rows=validation_rows,
            run_id=args.run_id,
        )
    else:
        print("BigQuery update skipped. Use --write-bigquery to update the training table.")

    return 0 if not actual_errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
