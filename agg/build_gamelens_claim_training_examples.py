"""
Build GameLens claim training examples from saved GameLens payload JSON files.

V2 adds richer numeric context for core area and summary claims.

Recommended repo location:
    agg/build_gamelens_claim_training_examples_v2.py

Why agg/?
    This script materializes an Analytics table used by GameLens modeling/QA.
    It is not a temporary QA output file, even though the first version reads
    local QA payload runs.

Stage 1/V2 scope:
    - Read existing payload JSON files from a payload run folder.
    - Extract one row per pregame GameLens claim.
    - Populate safe/extracted fields.
    - Enrich core_area_comparison, core_area_summary, and category_summary rows with score/gap context.
    - Use primary summary drivers as numeric proxies where available.
    - Leave combo/model FLOAT fields NULL until formulas are intentionally built.
    - Write a dry-run CSV by default.
    - Optionally append rows to BigQuery.

Example dry run:
    python -m agg.build_gamelens_claim_training_examples_v2 \
      --payload-run qa/gamelens_payload_runs/tie_fix_check \
      --run-id tie_fix_stage1 \
      --dry-run

Example full 96-game dry run:
    python -m agg.build_gamelens_claim_training_examples_v2 \
      --payload-run qa/gamelens_payload_runs/baseline_32_v2 \
      --run-id baseline_96_stage1 \
      --dry-run

Example BigQuery write:
    python -m agg.build_gamelens_claim_training_examples_v2 \
      --payload-run qa/gamelens_payload_runs/baseline_32_v2 \
      --run-id baseline_96_stage1 \
      --write-bigquery \
      --replace-run

Target table:
    nfl-stream-406420.Analytics.gamelens_claim_training_examples
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


PROJECT_ID = "nfl-stream-406420"
DATASET_ID = "Analytics"
TABLE_ID = "gamelens_claim_training_examples"

DEFAULT_OUTPUT_ROOT = Path("qa/gamelens_training_example_runs")
DEFAULT_HEADLINE_TOP_METRICS = 3


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def safe_get(obj: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = obj
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return default if cur is None else cur


def as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
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


def as_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "t", "yes", "y", "1"}:
            return True
        if lowered in {"false", "f", "no", "n", "0"}:
            return False
    return None


def clean_key_part(value: Any) -> str:
    text = "missing" if value is None or value == "" else str(value)
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9@._-]+", "_", text)
    return text.strip("_") or "missing"


def make_claim_key(
    game_id: str,
    claim_type: str,
    claim_layer: str,
    group_name: Optional[str],
    metric: Optional[str],
    claimed_team: str,
    claim_rank: Optional[int],
    source_section: str,
) -> str:
    """
    Deterministic key. Include a short hash so repeated-looking claims from
    different sections do not collide.
    """
    raw = "|".join(
        [
            game_id,
            claim_type,
            claim_layer,
            group_name or "",
            metric or "",
            claimed_team,
            str(claim_rank if claim_rank is not None else ""),
            source_section,
        ]
    )
    digest = hashlib.md5(raw.encode("utf-8")).hexdigest()[:10]
    readable = "|".join(
        [
            clean_key_part(game_id),
            clean_key_part(claim_type),
            clean_key_part(claim_layer),
            clean_key_part(group_name),
            clean_key_part(metric),
            clean_key_part(claimed_team),
            str(claim_rank if claim_rank is not None else "na"),
        ]
    )
    return f"{readable}|{digest}"


def side_to_team(side: Optional[str], away_team: Optional[str], home_team: Optional[str]) -> Optional[str]:
    if not side:
        return None
    if side == "away":
        return away_team
    if side == "home":
        return home_team
    return None


def team_to_side(team: Optional[str], away_team: Optional[str], home_team: Optional[str]) -> Optional[str]:
    if not team:
        return None
    if team == away_team:
        return "away"
    if team == home_team:
        return "home"
    return None


def opposite_side(side: Optional[str]) -> Optional[str]:
    if side == "away":
        return "home"
    if side == "home":
        return "away"
    return None


def final_margin_bucket(final_away: Optional[int], final_home: Optional[int]) -> str:
    if final_away is None or final_home is None:
        return "unknown"
    margin = abs(final_away - final_home)
    if margin == 0:
        return "tie"
    if margin <= 3:
        return "close"
    if margin <= 8:
        return "one_score"
    if margin <= 16:
        return "material"
    return "severe"


def get_actual_winner(final_away: Optional[int], final_home: Optional[int], away: Optional[str], home: Optional[str]) -> Optional[str]:
    if final_away is None or final_home is None:
        return None
    if final_away == final_home:
        return "TIE"
    return away if final_away > final_home else home


# ---------------------------------------------------------------------------
# Payload context
# ---------------------------------------------------------------------------

def extract_payload_context(payload: Dict[str, Any], payload_path: Path, payload_run: Path, run_id: str, model_version: Optional[str]) -> Dict[str, Any]:
    header = payload.get("header") or {}
    final_score = payload.get("final_score") or {}
    final_away = as_int(safe_get(final_score, "away", "total"))
    final_home = as_int(safe_get(final_score, "home", "total"))

    away_team = safe_get(header, "away_team", "abbreviation") or safe_get(header, "away_team", "name")
    home_team = safe_get(header, "home_team", "abbreviation") or safe_get(header, "home_team", "name")

    matchup_lean = payload.get("matchup_lean") or {}
    model_outcome = payload.get("model_outcome") or {}
    ranking_context = payload.get("ranking_context") or {}
    core_area_context = matchup_lean.get("core_area_context") or {}
    team_edge_context = safe_get(matchup_lean, "confidence_guardrails", "team_comparison_edge_context", default={}) or {}
    if not team_edge_context:
        team_edge_context = safe_get(payload, "model_trust", "matchup_advantage", default={}) or {}

    game_id = header.get("game_id") or payload.get("game_id") or payload_path.stem
    season = str(header.get("season") or payload.get("season") or game_id[:4])

    actual_winner = model_outcome.get("actual_winner")
    if not actual_winner:
        actual_winner = get_actual_winner(final_away, final_home, away_team, home_team)

    return {
        "run_id": run_id,
        "model_version": model_version,
        "source_payload_run": str(payload_run),
        "source_payload_path": str(payload_path),

        "game_id": game_id,
        "season": season,
        "game_date": header.get("game_date"),
        "game_week": header.get("game_week"),
        "season_type": header.get("season_type"),
        "bucket": None,  # filled from payload-run metadata later if available
        "game_status": header.get("game_status"),

        "away_team": away_team,
        "home_team": home_team,

        "window_type": ranking_context.get("window_type") or safe_get(payload, "matchup_breakdown", "freshness", "window_types", default=[None])[0],
        "as_of_date": ranking_context.get("as_of_date"),
        "source_data_date": None,
        "data_lag_days": as_int(ranking_context.get("max_data_lag_days") or safe_get(payload, "matchup_breakdown", "freshness", "max_data_lag_days")),

        "profile_type": matchup_lean.get("profile_type") or core_area_context.get("profile_type"),
        "profile_strength_code": safe_get(matchup_lean, "profile_strength", "code"),
        "profile_strength_label": safe_get(matchup_lean, "profile_strength", "label"),
        "outcome_confidence_code": safe_get(matchup_lean, "outcome_confidence", "code"),
        "outcome_confidence_label": safe_get(matchup_lean, "outcome_confidence", "label") or matchup_lean.get("confidence"),
        "matchup_label": matchup_lean.get("matchup_label"),

        "core_area_split": core_area_context.get("core_area_split"),
        "core_gap": as_float(core_area_context.get("core_gap")),
        "signal_gap": as_int(safe_get(matchup_lean, "signal_score", "gap")),
        "team_comp_edge_score": as_float(team_edge_context.get("edge_score") or safe_get(payload, "model_trust", "edge", "score")),
        "team_comp_away_count": as_int(team_edge_context.get("away")),
        "team_comp_home_count": as_int(team_edge_context.get("home")),
        "team_comp_neutral_count": as_int(team_edge_context.get("neutral")),
        "team_comp_total_visible": as_int(team_edge_context.get("total_visible")),

        "predicted_team": model_outcome.get("predicted_team") or _extract_predicted_team(matchup_lean, away_team, home_team),
        "actual_winner": actual_winner,
        "model_result": model_outcome.get("result"),
        "is_tie": as_bool(model_outcome.get("is_tie")) if model_outcome.get("is_tie") is not None else (final_away == final_home if final_away is not None and final_home is not None else None),
        "final_away_total": final_away,
        "final_home_total": final_home,
        "final_margin_abs": abs(final_away - final_home) if final_away is not None and final_home is not None else None,
        "final_margin_bucket": final_margin_bucket(final_away, final_home),

        "created_at": utc_now_iso(),
        "updated_at": None,
    }


def _extract_predicted_team(matchup_lean: Dict[str, Any], away_team: Optional[str], home_team: Optional[str]) -> Optional[str]:
    target_side = matchup_lean.get("target_side")
    if target_side in {"away", "home"}:
        return side_to_team(target_side, away_team, home_team)

    target_team = matchup_lean.get("target_team")
    if isinstance(target_team, str):
        cleaned = target_team.replace(" edge", "").strip()
        if cleaned in {away_team, home_team}:
            return cleaned

    return None


def attach_sample_metadata(context: Dict[str, Any], sample_lookup: Dict[str, Dict[str, Any]]) -> None:
    meta = sample_lookup.get(context["game_id"])
    if not meta:
        return
    for field in ["bucket", "game_week", "game_date", "season_type"]:
        if meta.get(field) and not context.get(field):
            context[field] = meta.get(field)


def load_sample_lookup(payload_run: Path) -> Dict[str, Dict[str, Any]]:
    """
    Optional metadata source created by qa_collect_gamelens_payloads.py.
    """
    candidates = [
        payload_run / "sampled_games.json",
        payload_run / "selected_games.json",
    ]

    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            data = read_json(candidate)
        except Exception:
            continue

        games = data.get("games") if isinstance(data, dict) else data
        if not isinstance(games, list):
            continue

        out = {}
        for game in games:
            if isinstance(game, dict) and game.get("game_id"):
                out[game["game_id"]] = game
        return out

    return {}


# ---------------------------------------------------------------------------
# Row building
# ---------------------------------------------------------------------------

def base_row(
    context: Dict[str, Any],
    claimed_side: str,
    claim_type: str,
    claim_layer: str,
    source_section: str,
    claim_rank: Optional[int],
    claim_name: Optional[str],
    claim_text: Optional[str],
    group_name: Optional[str],
    core_area: Optional[str],
    category: Optional[str],
    metric: Optional[str],
    metric_label: Optional[str],
) -> Dict[str, Any]:
    away_team = context.get("away_team")
    home_team = context.get("home_team")
    claimed_team = side_to_team(claimed_side, away_team, home_team)
    opponent_side = opposite_side(claimed_side)
    opponent_team = side_to_team(opponent_side, away_team, home_team)

    if not claimed_team:
        raise ValueError(f"Cannot build claim row without claimed_team. claimed_side={claimed_side}")

    row = {
        "claim_key": make_claim_key(
            game_id=context["game_id"],
            claim_type=claim_type,
            claim_layer=claim_layer,
            group_name=group_name or claim_name,
            metric=metric,
            claimed_team=claimed_team,
            claim_rank=claim_rank,
            source_section=source_section,
        ),
        "feature_build_stage": "raw_extracted",
        "feature_status": "partial",
        "feature_notes": "Stage 1 extracted row. Combo feature scores intentionally left NULL.",

        "claimed_team": claimed_team,
        "claimed_side": claimed_side,
        "opponent_team": opponent_team,
        "opponent_side": opponent_side,

        "claim_type": claim_type,
        "claim_layer": claim_layer,
        "claim_rank": claim_rank,
        "claim_name": claim_name,
        "claim_text": claim_text,
        "source_section": source_section,
        "source_field_path": None,
        "group_name": group_name,
        "core_area": core_area,
        "category": category,
        "metric": metric,
        "metric_label": metric_label,

        # Pregame language / metadata
        "claim_level": None,
        "claim_level_index": None,
        "summary_label": None,
        "signal_strength": None,
        "confidence_eligible": None,
        "headline_eligible": None,
        "edge_language_allowed": None,
        "data_quality_status": None,
        "ranking_kind": None,
        "ranking_usage": None,

        # Pregame numeric fields
        "claimed_team_value": None,
        "opponent_team_value": None,
        "pregame_raw_gap": None,
        "pregame_abs_raw_gap": None,
        "pregame_rank_gap": None,
        "pregame_percentile_gap": None,
        "pregame_abs_percentile_gap": None,
        "claimed_team_league_rank": None,
        "opponent_team_league_rank": None,
        "claimed_team_league_percentile": None,
        "opponent_team_league_percentile": None,
        "claimed_team_tier": None,
        "opponent_team_tier": None,

        # Agreement/simple features
        "core_area_metric_count": None,
        "same_direction_metric_count": None,
        "opposing_signal_count": None,
        "near_even_metric_count": None,
        "core_area_agreement_rate": None,

        # Future combo/model features
        "directional_edge_flag": True,
        "elevated_candidate_flag": None,
        "strong_language_allowed_flag": None,
        "offense_finish_score": None,
        "defensive_suppression_score": None,
        "two_way_edge_score": None,
        "disruption_upside_score": None,
        "hidden_lean_score": None,
        "confidence_cap_reason": None,
        "feature_formula_version": None,

        # Validation labels not filled in Stage 1.
        "actual_team": None,
        "actual_side": None,
        "validation_result": None,
        "validated_flag": None,
        "elevated_deserved_flag": None,
        "actual_gap": None,
        "actual_rank_gap": None,
        "actual_percentile_gap": None,
        "actual_gap_bucket": None,

        # Game-level QA fields not filled in Stage 1 except available outcome fields.
        "qa_read_v2": None,
        "headline_claim_validation_rate": None,
        "unique_claim_validation_rate": None,
    }

    # Add shared context last so required identity fields are present.
    row.update(context)
    return row


def apply_metric_context(row: Dict[str, Any], claim_obj: Dict[str, Any], claimed_side: str) -> None:
    """
    Add metric values/ranks/percentiles from objects shaped like metric_highlights
    or context_notes. These objects have nested 'away' and 'home' dictionaries.
    """
    away = claim_obj.get("away") or {}
    home = claim_obj.get("home") or {}

    claimed = away if claimed_side == "away" else home
    opponent = home if claimed_side == "away" else away

    row["claimed_team_value"] = as_float(claimed.get("value"))
    row["opponent_team_value"] = as_float(opponent.get("value"))

    raw_gap = None
    if row["claimed_team_value"] is not None and row["opponent_team_value"] is not None:
        raw_gap = row["claimed_team_value"] - row["opponent_team_value"]
    row["pregame_raw_gap"] = raw_gap
    row["pregame_abs_raw_gap"] = abs(raw_gap) if raw_gap is not None else None

    # For metric_highlights, percentile_gap/rank_gap generally describe the leader
    # vs opponent. Since rows are created only for the leader, they already favor
    # claimed_team.
    row["pregame_percentile_gap"] = as_float(claim_obj.get("percentile_gap"))
    row["pregame_abs_percentile_gap"] = abs(row["pregame_percentile_gap"]) if row["pregame_percentile_gap"] is not None else None
    row["pregame_rank_gap"] = as_int(claim_obj.get("rank_gap"))

    row["claimed_team_league_rank"] = as_int(claimed.get("league_rank"))
    row["opponent_team_league_rank"] = as_int(opponent.get("league_rank"))
    row["claimed_team_league_percentile"] = as_float(claimed.get("league_percentile"))
    row["opponent_team_league_percentile"] = as_float(opponent.get("league_percentile"))
    row["claimed_team_tier"] = claimed.get("tier")
    row["opponent_team_tier"] = opponent.get("tier")

    for field in [
        "summary_label",
        "signal_strength",
        "confidence_eligible",
        "headline_eligible",
        "edge_language_allowed",
        "data_quality_status",
        "ranking_kind",
        "ranking_usage",
    ]:
        if field in claim_obj:
            value = claim_obj.get(field)
            row[field] = as_bool(value) if field in {"confidence_eligible", "headline_eligible", "edge_language_allowed"} else value

    # Simple, intentionally conservative flags.
    row["elevated_candidate_flag"] = infer_elevated_candidate(row)
    row["strong_language_allowed_flag"] = infer_strong_language_allowed(row)


def infer_elevated_candidate(row: Dict[str, Any]) -> Optional[bool]:
    """
    First-pass conservative flag:
    - claimed team has a meaningful percentile gap, OR
    - claimed team is elite/strong and opponent is average/weak/poor.
    This is not the final elevated_deserved target.
    """
    pct_gap = row.get("pregame_percentile_gap")
    claimed_tier = row.get("claimed_team_tier")
    opponent_tier = row.get("opponent_team_tier")

    if pct_gap is not None and pct_gap >= 25:
        return True

    if claimed_tier in {"elite", "strong", "very_high", "high"} and opponent_tier in {"average", "weak", "poor", "low", "very_low"}:
        return True

    if pct_gap is None and claimed_tier is None:
        return None

    return False


def infer_strong_language_allowed(row: Dict[str, Any]) -> Optional[bool]:
    """
    Stage 1 helper flag. This should be revised once feature formulas exist.
    """
    if row.get("edge_language_allowed") is False:
        return False
    if row.get("confidence_eligible") is False:
        return False
    if row.get("data_quality_status") == "watch":
        return False
    if row.get("pregame_percentile_gap") is not None:
        return row["pregame_percentile_gap"] >= 25
    return None


def agreement_rate(same_direction: Optional[int], total: Optional[int]) -> Optional[float]:
    if same_direction is None or total is None or total == 0:
        return None
    return round(same_direction / total, 4)


def score_agreement_rate(claimed_score: Optional[float], opponent_score: Optional[float]) -> Optional[float]:
    """
    Agreement-style rate for GameLens summary scores.

    Some payload sections expose weighted scores rather than raw metric counts.
    Example:
        away_score = 5
        home_score = 0

    In those cases, this returns claimed_score / (claimed_score + opponent_score).
    This is not a literal metric count; it is a score-share proxy.
    """
    if claimed_score is None or opponent_score is None:
        return None
    denom = claimed_score + opponent_score
    if denom == 0:
        return None
    return round(claimed_score / denom, 4)


def apply_score_context(
    row: Dict[str, Any],
    item: Dict[str, Any],
    leader_side: str,
    *,
    score_kind: str,
) -> None:
    """
    Populate numeric fields for claim sections that expose away_score/home_score.

    score_kind values:
        normalized_core_area_score:
            core_area_comparison scores usually look like 0.857 vs 0.143.
        gamelens_summary_score:
            summary rows often use GameLens score points like 5 vs 0 or 2 vs 4.

    These are not raw football stats. They are useful pregame model-context
    values that describe how strongly the GameLens layer leaned.
    """
    away_score = as_float(item.get("away_score"))
    home_score = as_float(item.get("home_score"))

    if leader_side == "away":
        claimed_score = away_score
        opponent_score = home_score
    else:
        claimed_score = home_score
        opponent_score = away_score

    row["claimed_team_value"] = claimed_score
    row["opponent_team_value"] = opponent_score

    if claimed_score is not None and opponent_score is not None:
        raw_gap = claimed_score - opponent_score
        row["pregame_raw_gap"] = raw_gap
        row["pregame_abs_raw_gap"] = abs(raw_gap)
        row["core_area_agreement_rate"] = score_agreement_rate(claimed_score, opponent_score)

    row["core_area_metric_count"] = as_int(item.get("metric_count"))
    row["near_even_metric_count"] = as_int(item.get("near_even_metric_count"))

    # These fields are count-ish in name, but summary rows expose score points.
    # Store the score values as integers when cleanly available and document it.
    row["same_direction_metric_count"] = as_int(claimed_score)
    row["opposing_signal_count"] = as_int(opponent_score)

    note = (
        f"Stage 1 v2 extracted row. {score_kind} populated from away_score/home_score; "
        "same_direction/opposing fields may represent GameLens score points, not literal metric counts. "
        "Combo feature scores intentionally left NULL."
    )
    row["feature_notes"] = note


def apply_primary_driver_proxy(row: Dict[str, Any], item: Dict[str, Any]) -> None:
    """
    Summary rows often contain a drivers list with the top supporting metric.

    Use the first driver as a numeric proxy so summary/core-area rows are less
    empty for modeling. This is intentionally marked in feature_notes.
    """
    drivers = item.get("drivers") or []
    if not drivers or not isinstance(drivers, list):
        return

    driver = drivers[0] or {}
    if not isinstance(driver, dict):
        return

    row["metric"] = row.get("metric") or driver.get("metric")
    row["metric_label"] = row.get("metric_label") or driver.get("label")
    row["pregame_percentile_gap"] = as_float(driver.get("percentile_gap"))
    row["pregame_abs_percentile_gap"] = (
        abs(row["pregame_percentile_gap"])
        if row.get("pregame_percentile_gap") is not None
        else None
    )

    # Use the driver language label if the summary row itself does not have one.
    if not row.get("summary_label"):
        row["summary_label"] = driver.get("summary_label")

    driver_note = (
        " Primary driver metric used as numeric proxy: "
        f"{driver.get('metric') or driver.get('label') or 'unknown'}."
    )
    row["feature_notes"] = (row.get("feature_notes") or "") + driver_note


# ---------------------------------------------------------------------------
# Claim extractors
# ---------------------------------------------------------------------------

def extract_game_profile_claims(payload: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    for idx, item in enumerate(payload.get("game_profile") or [], start=1):
        claimed_side = item.get("tilt_team")
        if claimed_side not in {"away", "home"}:
            continue

        row = base_row(
            context=context,
            claimed_side=claimed_side,
            claim_type="game_profile",
            claim_layer="headline",
            source_section="game_profile",
            claim_rank=idx,
            claim_name=item.get("category"),
            claim_text=item.get("tilt_text") or item.get("tilt"),
            group_name=item.get("category"),
            core_area=None,
            category=item.get("category"),
            metric=None,
            metric_label=None,
        )
        row["claim_level"] = item.get("level")
        row["claim_level_index"] = as_int(item.get("level_index"))
        row["summary_label"] = item.get("level")
        row["source_field_path"] = f"game_profile[{idx - 1}]"
        rows.append(row)
    return rows


def extract_core_area_comparison_claims(payload: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    for idx, item in enumerate(payload.get("core_area_comparison") or [], start=1):
        leader = item.get("leader")
        if leader not in {"away", "home"}:
            continue

        row = base_row(
            context=context,
            claimed_side=leader,
            claim_type="core_area_comparison",
            claim_layer="headline",
            source_section="core_area_comparison",
            claim_rank=idx,
            claim_name=item.get("core_area"),
            claim_text=None,
            group_name=item.get("core_area"),
            core_area=item.get("core_area"),
            category=None,
            metric=None,
            metric_label=None,
        )
        apply_score_context(
            row=row,
            item=item,
            leader_side=leader,
            score_kind="normalized_core_area_score",
        )
        row["source_field_path"] = f"core_area_comparison[{idx - 1}]"
        rows.append(row)
    return rows


def extract_summary_claims(payload: Dict[str, Any], context: Dict[str, Any], section_name: str, claim_type: str, source_section: str) -> List[Dict[str, Any]]:
    rows = []
    items = safe_get(payload, "matchup_breakdown", section_name, default=[]) or []
    for idx, item in enumerate(items, start=1):
        leader = item.get("leader")
        if leader not in {"away", "home"}:
            continue

        name = item.get("name")
        claim_layer = "supporting"
        row = base_row(
            context=context,
            claimed_side=leader,
            claim_type=claim_type,
            claim_layer=claim_layer,
            source_section=source_section,
            claim_rank=idx,
            claim_name=name,
            claim_text=item.get("summary"),
            group_name=name,
            core_area=name if claim_type == "core_area_summary" else item.get("core_area"),
            category=name if claim_type == "category_summary" else item.get("category"),
            metric=None,
            metric_label=None,
        )
        row["summary_label"] = item.get("summary_label")

        apply_score_context(
            row=row,
            item=item,
            leader_side=leader,
            score_kind="gamelens_summary_score",
        )
        apply_primary_driver_proxy(row, item)

        row["source_field_path"] = f"matchup_breakdown.{section_name}[{idx - 1}]"
        rows.append(row)
    return rows


def extract_metric_highlight_claims(payload: Dict[str, Any], context: Dict[str, Any], headline_top_metrics: int) -> List[Dict[str, Any]]:
    rows = []
    items = safe_get(payload, "matchup_breakdown", "metric_highlights", default=[]) or []
    for idx, item in enumerate(items, start=1):
        leader = item.get("leader")
        if leader not in {"away", "home"}:
            continue

        claim_layer = "headline" if idx <= headline_top_metrics else "supporting"
        label = item.get("label")
        row = base_row(
            context=context,
            claimed_side=leader,
            claim_type="metric_highlight",
            claim_layer=claim_layer,
            source_section="matchup_breakdown.metric_highlights",
            claim_rank=idx,
            claim_name=label,
            claim_text=item.get("summary"),
            group_name=item.get("category") or item.get("core_area") or label,
            core_area=item.get("core_area"),
            category=item.get("category"),
            metric=item.get("metric"),
            metric_label=label,
        )
        row["source_field_path"] = f"matchup_breakdown.metric_highlights[{idx - 1}]"
        apply_metric_context(row, item, leader)
        rows.append(row)
    return rows


def extract_team_comparison_claims(payload: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    for idx, item in enumerate(payload.get("team_comparison") or [], start=1):
        better = item.get("better")
        if better not in {"away", "home"}:
            continue

        label = item.get("label")
        row = base_row(
            context=context,
            claimed_side=better,
            claim_type="team_comparison_metric",
            claim_layer="supporting",
            source_section="team_comparison",
            claim_rank=idx,
            claim_name=label,
            claim_text=None,
            group_name=label,
            core_area=None,
            category=None,
            metric=item.get("metric"),
            metric_label=label,
        )
        row["summary_label"] = item.get("comparison_strength")
        row["pregame_percentile_gap"] = as_float(item.get("percentile_gap"))
        row["pregame_abs_percentile_gap"] = abs(row["pregame_percentile_gap"]) if row["pregame_percentile_gap"] is not None else None
        row["pregame_rank_gap"] = as_int(item.get("rank_gap"))
        raw_gap = as_float(item.get("raw_gap"))
        row["pregame_raw_gap"] = raw_gap
        row["pregame_abs_raw_gap"] = abs(raw_gap) if raw_gap is not None else None
        row["claimed_team_value"] = as_float(item.get(better))
        opp = opposite_side(better)
        row["opponent_team_value"] = as_float(item.get(opp)) if opp else None
        row["elevated_candidate_flag"] = infer_elevated_candidate(row)
        row["strong_language_allowed_flag"] = infer_strong_language_allowed(row)
        row["source_field_path"] = f"team_comparison[{idx - 1}]"
        rows.append(row)
    return rows


def extract_claim_rows(payload: Dict[str, Any], context: Dict[str, Any], headline_top_metrics: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    rows.extend(extract_game_profile_claims(payload, context))
    rows.extend(extract_core_area_comparison_claims(payload, context))
    rows.extend(extract_summary_claims(payload, context, "core_area_summaries", "core_area_summary", "matchup_breakdown.core_area_summaries"))
    rows.extend(extract_summary_claims(payload, context, "category_summaries", "category_summary", "matchup_breakdown.category_summaries"))
    rows.extend(extract_metric_highlight_claims(payload, context, headline_top_metrics=headline_top_metrics))
    rows.extend(extract_team_comparison_claims(payload, context))
    return rows


# ---------------------------------------------------------------------------
# IO
# ---------------------------------------------------------------------------

def find_payload_files(payload_run: Path) -> List[Path]:
    payload_dir = payload_run / "payloads"
    if payload_dir.exists():
        return sorted(payload_dir.rglob("*.json"))
    return sorted(payload_run.rglob("*.json"))


def write_csv(rows: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return

    # Stable union of keys. Keep common schema-ish columns first.
    preferred = [
        "claim_key", "run_id", "feature_build_stage", "feature_status",
        "game_id", "season", "game_date", "game_week", "away_team", "home_team",
        "claimed_team", "claimed_side", "opponent_team", "opponent_side",
        "claim_type", "claim_layer", "claim_rank", "claim_name", "claim_text",
        "group_name", "core_area", "category", "metric", "metric_label",
        "claimed_team_value", "opponent_team_value", "pregame_raw_gap", "pregame_abs_raw_gap",
        "pregame_percentile_gap", "pregame_rank_gap", "claimed_team_league_percentile",
        "opponent_team_league_percentile", "core_area_agreement_rate", "profile_type", "outcome_confidence_label",
        "model_result", "actual_winner", "is_tie", "final_margin_abs", "created_at",
    ]
    all_keys = sorted({k for row in rows for k in row.keys()})
    fieldnames = [k for k in preferred if k in all_keys] + [k for k in all_keys if k not in preferred]

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(rows: Any, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")


def load_to_bigquery(rows: List[Dict[str, Any]], table_ref: str, replace_run: bool, run_id: str) -> None:
    from google.cloud import bigquery

    if not rows:
        print("No rows to write to BigQuery.")
        return

    client = bigquery.Client(project=PROJECT_ID)

    if replace_run:
        query = f"""
        DELETE FROM `{table_ref}`
        WHERE run_id = @run_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
        )
        client.query(query, job_config=job_config).result()
        print(f"Deleted existing rows for run_id={run_id}")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
    )
    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()
    print(f"✅ Loaded {len(rows)} rows into {table_ref}")


def build_summary(rows: List[Dict[str, Any]], payload_files: List[Path], errors: List[Dict[str, Any]]) -> Dict[str, Any]:
    def count_by(field: str) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for row in rows:
            key = str(row.get(field) or "missing")
            out[key] = out.get(key, 0) + 1
        return dict(sorted(out.items()))

    return {
        "payload_files_found": len(payload_files),
        "rows_built": len(rows),
        "errors": len(errors),
        "by_claim_type": count_by("claim_type"),
        "by_claim_layer": count_by("claim_layer"),
        "by_season": count_by("season"),
        "by_feature_status": count_by("feature_status"),
        "by_model_result": count_by("model_result"),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build GameLens claim training example rows from saved payload JSON. V2 enriches summary/core-area numeric context.")
    parser.add_argument("--payload-run", required=True, help="Path to saved payload run folder, e.g. qa/gamelens_payload_runs/baseline_32_v2")
    parser.add_argument("--run-id", default=None, help="Run id to stamp on output rows. Defaults to timestamp-based id.")
    parser.add_argument("--model-version", default=None, help="Optional GameLens model/rules version label.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Output root for dry-run CSV/JSON artifacts.")
    parser.add_argument("--headline-top-metrics", type=int, default=DEFAULT_HEADLINE_TOP_METRICS, help="Number of metric_highlights treated as headline claims.")
    parser.add_argument("--dry-run", action="store_true", help="Write local CSV/JSON only. This is the recommended first run.")
    parser.add_argument("--write-bigquery", action="store_true", help="Append rows to BigQuery target table.")
    parser.add_argument("--replace-run", action="store_true", help="Before BigQuery write, delete existing rows with the same run_id.")
    parser.add_argument("--table-ref", default=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", help="BigQuery table ref.")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit on payload files for quick testing.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    payload_run = Path(args.payload_run)
    if not payload_run.exists():
        raise FileNotFoundError(f"Payload run folder does not exist: {payload_run}")

    run_id = args.run_id or f"claim_training_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_utc')}"
    output_dir = Path(args.output_root) / run_id

    payload_files = find_payload_files(payload_run)
    if args.limit:
        payload_files = payload_files[: args.limit]

    sample_lookup = load_sample_lookup(payload_run)

    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    print(f"Payload run: {payload_run}")
    print(f"Payload files found: {len(payload_files)}")
    print(f"Run id: {run_id}")

    for idx, payload_path in enumerate(payload_files, start=1):
        try:
            payload = read_json(payload_path)
            context = extract_payload_context(
                payload=payload,
                payload_path=payload_path,
                payload_run=payload_run,
                run_id=run_id,
                model_version=args.model_version,
            )
            attach_sample_metadata(context, sample_lookup)

            claim_rows = extract_claim_rows(
                payload=payload,
                context=context,
                headline_top_metrics=args.headline_top_metrics,
            )
            rows.extend(claim_rows)

            print(f"[{idx}/{len(payload_files)}] {context['game_id']}: {len(claim_rows)} rows")
        except Exception as exc:
            error = {
                "payload_path": str(payload_path),
                "error": str(exc),
            }
            errors.append(error)
            print(f"[{idx}/{len(payload_files)}] ERROR {payload_path}: {exc}")

    summary = build_summary(rows, payload_files, errors)

    write_csv(rows, output_dir / "claim_training_examples_preview.csv")
    write_json(summary, output_dir / "summary.json")
    write_json(errors, output_dir / "errors.json")

    print("\nBuild complete")
    print("--------------")
    print(f"Rows built: {len(rows)}")
    print(f"Errors: {len(errors)}")
    print(f"Preview CSV: {output_dir / 'claim_training_examples_preview.csv'}")
    print(f"Summary JSON: {output_dir / 'summary.json'}")

    if args.write_bigquery:
        load_to_bigquery(
            rows=rows,
            table_ref=args.table_ref,
            replace_run=args.replace_run,
            run_id=run_id,
        )
    else:
        print("BigQuery write skipped. Use --write-bigquery to load rows.")

    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
