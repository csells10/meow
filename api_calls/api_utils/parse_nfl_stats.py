from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple

__all__ = ["parse_game_stats"]

# ────────────────────────────────────────────────────────────────
# Constants: Field Mappings
# ────────────────────────────────────────────────────────────────

_COMPOSITE_MAP: dict[str, Tuple[str, str]] = {
    "penalties": ("penalty_count", "penalty_yards"),
    "passCompletionsAndAttempts": ("pass_completions", "pass_attempts"),
    "sacksAndYardsLost": ("sacks_taken", "sack_yards_lost"),
    "thirdDownEfficiency": ("third_down_conversions", "third_down_attempts"),
    "fourthDownEfficiency": ("fourth_down_conversions", "fourth_down_attempts"),
    "redZoneScoredAndAttempted": ("red_zone_tds", "red_zone_attempts"),
}

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

_DERIVED = {
    "points_per_yard": ("Offensive Output", "points_per_yard"),
    "points_allowed_per_yard": ("Defensive Control", "points_allowed_per_yard"),
}

# ────────────────────────────────────────────────────────────────
# Helper: split composite strings like "13-18" → (13, 18)
# ────────────────────────────────────────────────────────────────

def _split_pair(pair_str: str, fields: Tuple[str, str]) -> dict[str, float]:
    try:
        a, b = map(float, pair_str.split("-"))
        return {fields[0]: a, fields[1]: b}
    except Exception:
        return {fields[0]: 0.0, fields[1]: 0.0}

# ────────────────────────────────────────────────────────────────
# Main entry point
# ────────────────────────────────────────────────────────────────

def parse_game_stats(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    body = data.get("body", {})
    game_date_raw = (body.get("gameDate") or "")[:8]
    if not game_date_raw:
        raise ValueError("gameDate missing in payload")
    game_date = datetime.strptime(game_date_raw, "%Y%m%d").date().isoformat()

    rows: List[Dict[str, Any]] = []
    line_home = body.get("lineScore", {}).get("home", {})
    line_away = body.get("lineScore", {}).get("away", {})
    game_stats = {}

    for side in ("home", "away"):
        t_stats = body.get("teamStats", {}).get(side, {})
        dst_stats = body.get("DST", {}).get(side, {})
        team_id = t_stats.get("teamID") or dst_stats.get("teamID")
        team_abv = t_stats.get("teamAbv") or dst_stats.get("teamAbv")

        for raw_key, new_names in _COMPOSITE_MAP.items():
            if raw_key in t_stats:
                for metric_name, val in _split_pair(t_stats[raw_key], new_names).items():
                    core_area = (
                        "Offensive Output"
                        if "attempt" in metric_name or "yards" in metric_name
                        else "Disruption and Turnovers"
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

        merged = {**t_stats, **dst_stats}
        metrics = {}
        for raw_key, (core_area, metric_name) in _METRIC_MAP.items():
            try:
                val = float(merged.get(raw_key, 0.0))
            except (ValueError, TypeError):
                val = 0.0
            metrics[metric_name] = val
            rows.append({
                "team_id": team_id,
                "team_abv": team_abv,
                "data_date": game_date,
                "category": metric_name,
                "metric": metric_name,
                "core_area": core_area,
                "value": val,
            })

        game_stats[side] = {
            "team_id": team_id,
            "team_abv": team_abv,
            "metrics": metrics,
            "raw": {k.lower(): float(v) if str(v).replace('.', '', 1).isdigit() else 0.0 for k, v in merged.items()}
        }

        try:
            points_scored = float((line_home if side == "home" else line_away).get("score", 0))
        except (ValueError, TypeError):
            points_scored = 0.0

        total_yards = float(t_stats.get("totalYards", 0) or 0)
        yards_allowed = float(dst_stats.get("ydsAllowed", 0) or 0)
        points_allowed = float(dst_stats.get("ptsAllowed", 0) or 0)

        rows.append({
            "team_id": team_id,
            "team_abv": team_abv,
            "data_date": game_date,
            "category": _DERIVED["points_per_yard"][1],
            "metric": _DERIVED["points_per_yard"][1],
            "core_area": _DERIVED["points_per_yard"][0],
            "value": round(points_scored / total_yards, 3) if total_yards else 0.0,
        })

        rows.append({
            "team_id": team_id,
            "team_abv": team_abv,
            "data_date": game_date,
            "category": _DERIVED["points_allowed_per_yard"][1],
            "metric": _DERIVED["points_allowed_per_yard"][1],
            "core_area": _DERIVED["points_allowed_per_yard"][0],
            "value": round(points_allowed / yards_allowed, 3) if yards_allowed else 0.0,
        })

    for side in ("home", "away"):
        stats = game_stats[side]["raw"]
        opp_stats = game_stats["away" if side == "home" else "home"]["raw"]
        team_id = game_stats[side]["team_id"]
        team_abv = game_stats[side]["team_abv"]

        def add_metric(category, metric, core_area, value):
            rows.append({
                "team_id": team_id,
                "team_abv": team_abv,
                "data_date": game_date,
                "category": category,
                "metric": metric,
                "core_area": core_area,
                "value": round(value, 3),
            })

        add_metric("Offense", "yards_per_rush", "Offensive Output", stats.get("rushingyards", 0) / stats.get("rushattempts", 1))
        add_metric("Offense", "completion_pct", "Offensive Output", stats.get("passcompletions", 0) / stats.get("passattempts", 1))
        add_metric("Offense", "catch_rate", "Offensive Output", stats.get("receptions", 0) / stats.get("targets", 1))
        add_metric("Offense", "pass_run_ratio", "Offensive Output", stats.get("passattempts", 0) / stats.get("rushattempts", 1))
        add_metric("Offense", "total_yards", "Offensive Output", stats.get("rushingyards", 0) + stats.get("receivingyards", 0) + stats.get("passingyards", 0))
        add_metric("Offense", "1st_down_rate", "Offensive Output", stats.get("firstdowns", 0) / stats.get("totalplays", 1))
        add_metric("Offense", "td_rate", "Offensive Output", (stats.get("passingtds", 0) + stats.get("rushingtds", 0)) / stats.get("totalplays", 1))
        add_metric("Offense", "red_zone_efficiency", "Offensive Output", stats.get("red_zonetds", 0) / stats.get("red_zoneattempts", 1))
        add_metric("Offense", "third_down_pct", "Offensive Output", stats.get("third_down_conversions", 0) / stats.get("third_down_attempts", 1))
        add_metric("Offense", "fourth_down_pct", "Offensive Output", stats.get("fourth_down_conversions", 0) / stats.get("fourth_down_attempts", 1))
        add_metric("Offense", "run_play_pct", "Offensive Output", stats.get("rushattempts", 0) / stats.get("totalplays", 1))
        add_metric("Offense", "pass_play_pct", "Offensive Output", stats.get("passattempts", 0) / stats.get("totalplays", 1))
        add_metric("Offense", "touchdown_distribution", "Offensive Output", stats.get("passingtds", 0) / (stats.get("passingtds", 0) + stats.get("rushingtds", 0) + 1e-6))

        add_metric("Defense", "turnover_margin", "Disruption and Turnovers", (stats.get("defensiveinterceptions", 0) + stats.get("fumblesrecovered", 0) - stats.get("interceptionsthrown", 0) - stats.get("fumbleslost", 0)))
        add_metric("Defense", "sack_to_turnover_ratio", "Disruption and Turnovers", stats.get("sacks", 0) / (stats.get("defensiveinterceptions", 0) + 1e-6))
        add_metric("Defense", "pressure_rate", "Disruption and Turnovers", (stats.get("sacks", 0) + stats.get("sacks_taken", 0)) / stats.get("passattempts", 1))
        add_metric("Defense", "defensive_success_rate", "Defensive Control", 1 - (opp_stats.get("yardsallowed", 0) / opp_stats.get("totalplays", 1)))

    return rows
