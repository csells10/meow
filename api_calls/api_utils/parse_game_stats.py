# api_utils/parse_game_stats.py
"""Parse Tank01 NFL box‑score JSON into flat BigQuery‑ready rows.

This module extracts **team‑level** metrics from the box‑score endpoint and
normalises them into a list[dict] with the schema:
    team_id · team_abv · data_date · category · metric · core_area · value

Four Core Areas are enforced:
    1. Defensive Control
    2. Disruption and Turnovers
    3. Field Control (Special Teams)
    4. Offensive Output

Missing / null values are returned as 0.0 so downstream joins don’t break.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple

__all__ = ["parse_game_stats"]

# ────────────────────────────────────────────────────────────────
# Helper: split composite strings like "13-18" → (13, 18)
# ────────────────────────────────────────────────────────────────

def _split_pair(pair_str: str, fields: Tuple[str, str]) -> dict[str, float]:
    try:
        a, b = map(float, pair_str.split("-"))
        return {fields[0]: a, fields[1]: b}
    except Exception:
        return {fields[0]: 0.0, fields[1]: 0.0}


# Map composite keys to the new metric names
_COMPOSITE_MAP: dict[str, Tuple[str, str]] = {
    "penalties": ("penalty_count", "penalty_yards"),
    "passCompletionsAndAttempts": ("pass_completions", "pass_attempts"),
    "sacksAndYardsLost": ("sacks_taken", "sack_yards_lost"),
    "thirdDownEfficiency": ("third_down_conversions", "third_down_attempts"),
    "fourthDownEfficiency": ("fourth_down_conversions", "fourth_down_attempts"),
    "redZoneScoredAndAttempted": ("red_zone_tds", "red_zone_attempts"),
}


# Metric → (core_area, normalised_metric)
_METRIC_MAP: dict[str, Tuple[str, str]] = {
    # Defensive Control
    "ptsAllowed": ("Defensive Control", "points_allowed"),
    "ydsAllowed": ("Defensive Control", "yards_allowed"),
    "passingYardsAllowed": ("Defensive Control", "passing_yards_allowed"),
    "rushingYardsAllowed": ("Defensive Control", "rushing_yards_allowed"),
    # Disruption & Turnovers
    "defensiveInterceptions": ("Disruption and Turnovers", "defensive_interceptions"),
    "sacks": ("Disruption and Turnovers", "sacks"),
    "fumblesRecovered": ("Disruption and Turnovers", "fumbles_recovered"),
    "defTD": ("Disruption and Turnovers", "defensive_tds"),
    "turnovers": ("Disruption and Turnovers", "turnovers"),
    "interceptionsThrown": ("Disruption and Turnovers", "interceptions_thrown"),
    "fumblesLost": ("Disruption and Turnovers", "fumbles_lost"),
    # Field Control (ST)
    "blockedPunt": ("Field Control (Special Teams)", "blocked_punt"),
    "blockedFG": ("Field Control (Special Teams)", "blocked_fg"),
    "blockedXP": ("Field Control (Special Teams)", "blocked_xp"),
    "safeties": ("Field Control (Special Teams)", "safeties"),
    "puntYards": ("Field Control (Special Teams)", "punt_yards"),
    # Offensive Output
    "passingYards": ("Offensive Output", "passing_yards"),
    "rushingYards": ("Offensive Output", "rushing_yards"),
    "totalYards": ("Offensive Output", "total_yards"),
    "passTD": ("Offensive Output", "passing_tds"),
    "rushTD": ("Offensive Output", "rushing_tds"),
    "totalPlays": ("Offensive Output", "total_plays"),
    "firstDowns": ("Offensive Output", "first_downs"),
    "yardsPerPlay": ("Offensive Output", "yards_per_play"),
    "yardsPerPass": ("Offensive Output", "yards_per_pass"),
    "yardsPerRush": ("Offensive Output", "yards_per_rush"),
}

# Derived metric names
_DERIVED = {
    "points_per_yard": ("Offensive Output", "points_per_yard"),
    "points_allowed_per_yard": ("Defensive Control", "points_allowed_per_yard"),
}


# ────────────────────────────────────────────────────────────────
# Main entry point
# ────────────────────────────────────────────────────────────────

def parse_game_stats(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return list[dict] ready for BigQuery insert."""

    body = data.get("body", {})
    game_date_raw = (body.get("gameDate") or "")[:8]
    if not game_date_raw:
        raise ValueError("gameDate missing in payload")
    game_date = datetime.strptime(game_date_raw, "%Y%m%d").date().isoformat()

    rows: List[Dict[str, Any]] = []

    # Pull score from lineScore (if present) for derived metrics
    line_home = body.get("lineScore", {}).get("home", {})
    line_away = body.get("lineScore", {}).get("away", {})

    # ── iterate both sides ──
    for side in ("home", "away"):
        t_stats = body.get("teamStats", {}).get(side, {})
        dst_stats = body.get("DST", {}).get(side, {})
        team_id = t_stats.get("teamID") or dst_stats.get("teamID")
        team_abv = t_stats.get("teamAbv") or dst_stats.get("teamAbv")

        # — composite fields —
        for raw_key, new_names in _COMPOSITE_MAP.items():
            if raw_key in t_stats:
                for metric_name, val in _split_pair(t_stats[raw_key], new_names).items():
                    core_area = (
                        "Offensive Output"
                        if "attempt" in metric_name or "yards" in metric_name else
                        "Disruption and Turnovers"
                    )
                    rows.append({
                        "team_id": team_id,
                        "team_abv": team_abv,
                        "data_date": game_date,
                        "category": metric_name,
                        "metric": metric_name,
                        "core_area": core_area,
                        "value": val,
                    })

        # — flat numeric metrics —
        merged = {**t_stats, **dst_stats}
        for raw_key, (core_area, metric_name) in _METRIC_MAP.items():
            if raw_key not in merged:
                continue
            try:
                val = float(merged[raw_key])
            except (ValueError, TypeError):
                val = 0.0
            rows.append({
                "team_id": team_id,
                "team_abv": team_abv,
                "data_date": game_date,
                "category": metric_name,
                "metric": metric_name,
                "core_area": core_area,
                "value": val,
            })

        # — derived points per yard metrics —
        try:
            points_scored = float((line_home if side == "home" else line_away).get("score", 0))
        except (ValueError, TypeError):
            points_scored = 0.0
        total_yards = float(t_stats.get("totalYards", 0) or 0)
        yards_allowed = float(dst_stats.get("ydsAllowed", 0) or 0)
        points_allowed = float(dst_stats.get("ptsAllowed", 0) or 0)

        # Offensive PPY
        ppy = round(points_scored / total_yards, 3) if total_yards else 0.0
        rows.append({
            "team_id": team_id,
            "team_abv": team_abv,
            "data_date": game_date,
            "category": _DERIVED["points_per_yard"][1],
            "metric": _DERIVED["points_per_yard"][1],
            "core_area": _DERIVED["points_per_yard"][0],
            "value": ppy,
        })

        # Defensive PPY
        papy = round(points_allowed / yards_allowed, 3) if yards_allowed else 0.0
        rows.append({
            "team_id": team_id,
            "team_abv": team_abv,
            "data_date": game_date,
            "category": _DERIVED["points_allowed_per_yard"][1],
            "metric": _DERIVED["points_allowed_per_yard"][1],
            "core_area": _DERIVED["points_allowed_per_yard"][0],
            "value": papy,
        })

    return rows
