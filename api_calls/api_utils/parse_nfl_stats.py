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

_METRIC_MAP: dict[str, Tuple[str, str, str]] = {
    # Defensive Control
    "ptsAllowed": ("Defensive Control", "points_allowed", "Defense"),
    "ydsAllowed": ("Defensive Control", "yards_allowed", "Defense"),

    # Disruption & Turnovers
    "defensiveInterceptions": ("Disruption and Turnovers", "defensive_interceptions", "Defense"),
    "sacks": ("Disruption and Turnovers", "sacks", "Defense"),
    "fumblesRecovered": ("Disruption and Turnovers", "fumbles_recovered", "Defense"),
    "defTD": ("Disruption and Turnovers", "defensive_tds", "Defense"),
    "turnovers": ("Disruption and Turnovers", "turnovers", "Defense"),
    "interceptionsThrown": ("Disruption and Turnovers", "interceptions_thrown", "Offense"),
    "fumblesLost": ("Disruption and Turnovers", "fumbles_lost", "Offense"),

    # Field Control (Special Teams)
    "blockedPunt": ("Field Control (Special Teams)", "blocked_punt", "Special Teams"),
    "blockedFG": ("Field Control (Special Teams)", "blocked_fg", "Special Teams"),
    "blockedXP": ("Field Control (Special Teams)", "blocked_xp", "Special Teams"),
    "safeties": ("Field Control (Special Teams)", "safeties", "Special Teams"),

    # Offensive Output
    "passingYards": ("Offensive Output", "passing_yards", "Passing"),
    "rushingYards": ("Offensive Output", "rushing_yards", "Rushing"),
    "totalYards": ("Offensive Output", "total_yards", "Offense"),
    "passTD": ("Offensive Output", "passing_tds", "Passing"),
    "rushTD": ("Offensive Output", "rushing_tds", "Rushing"),
    "totalPlays": ("Offensive Output", "total_plays", "Offense"),
    "firstDowns": ("Offensive Output", "first_downs", "Offense"),
    "yardsPerPlay": ("Offensive Output", "yards_per_play", "Offense"),
    "yardsPerPass": ("Offensive Output", "yards_per_pass", "Passing"),
    "yardsPerRush": ("Offensive Output", "yards_per_rush", "Rushing"),
    "possession": ("Offensive Output", "time_of_possession", "Offense"),
    "totalDrives": ("Offensive Output", "total_drives", "Offense"),
}

_DERIVED = {
    "points_per_yard": ("Offensive Output", "points_per_yard", "Offense"),
    "points_allowed_per_yard": ("Defensive Control", "points_allowed_per_yard", "Defense"),
}

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
    game_stats = {}

    # Ensure game_stats is built before it is referenced
    for side in ("home", "away"):
        t_stats = body.get("teamStats", {}).get(side, {})
        dst_stats = body.get("DST", {}).get(side, {})

        team_id = t_stats.get("teamID") or dst_stats.get("teamID")
        team_abv = t_stats.get("teamAbv") or dst_stats.get("teamAbv")

        merged = {**t_stats, **dst_stats}
        game_stats[side] = {
            "team_id": team_id,
            "team_abv": team_abv,
            "raw": {
                k.lower(): float(v) if str(v).replace(".", "", 1).isdigit() else 0.0
                for k, v in merged.items()
            },
        }

    for side in ("home", "away"):
        t_stats = body.get("teamStats", {}).get(side, {})
        dst_stats = body.get("DST", {}).get(side, {})
        
        team_id = t_stats.get("teamID") or dst_stats.get("teamID")
        team_abv = t_stats.get("teamAbv") or dst_stats.get("teamAbv")

        def try_add_metric(category: str, metric: str, core_area: str, num: float, den: float, label: str):
            try:
                value = num / (den or 1e-6)  # avoids division by 0
                rows.append({
                    "team_id": team_id,
                    "team_abv": team_abv,
                    "data_date": game_date,
                    "category": category,
                    "metric": metric,
                    "core_area": core_area,
                    "value": round(value, 3),
                })
            except Exception as e:
                print(f"⚠️ Metric failed: {label} → {e}")

        def add_metric(category: str, metric: str, core_area: str, value: float):
            rows.append({
                "team_id": team_id,
                "team_abv": team_abv,
                "data_date": game_date,
                "category": category,
                "metric": metric,
                "core_area": core_area,
                "value": round(value, 3),
            })
            
        
        actual_points = float(body.get("homePts" if side == "home" else "awayPts", 0))
        add_metric("Offense", "actual_points", "Offensive Output", actual_points)

        # Add raw metrics from _METRIC_MAP
        merged = {**t_stats, **dst_stats}
        # Start with base fields
        raw = {
            k.lower(): float(v) if str(v).replace(".", "", 1).isdigit() else 0.0
            for k, v in merged.items()
        }

        # Add parsed composite stats to raw
        for raw_key, (first_name, second_name) in _COMPOSITE_MAP.items():
            if raw_key in t_stats:
                try:
                    a, b = map(float, t_stats[raw_key].split("-"))
                except Exception:
                    a, b = 0.0, 0.0
                raw[first_name.lower()] = a
                raw[second_name.lower()] = b

        game_stats[side] = {
            "team_id": team_id,
            "team_abv": team_abv,
            "raw": raw,
        }
        
        # Avoid parsing "possession" as a float from mm:ss
        if "possession" in merged:
            del merged["possession"]
        # Handle possession string (e.g., "23:02" → 23.03 mins)
        possession_str = t_stats.get("possession", "")
        if isinstance(possession_str, str) and ":" in possession_str:
            try:
                mins, secs = map(int, possession_str.split(":"))
                possession_minutes = mins + secs / 60
                add_metric("Offense", "time_of_possession", "Offensive Output", possession_minutes)
            except Exception:
                pass  # silently skip malformed time
            
        for raw_key, (core_area, metric_name, category) in _METRIC_MAP.items():
            if raw_key == "possession":
                continue  # already handled manually above
            try:
                val = float(merged.get(raw_key, 0.0))
            except (ValueError, TypeError):
                val = 0.0
            add_metric(category, metric_name, core_area, val)
            
        snap_counts = t_stats.get("snapCounts", {})
        total_off = float(snap_counts.get("totalOffensive", 0))
        total_def = float(snap_counts.get("totalDefensive", 0))
        total_st  = float(snap_counts.get("totalSpecialTeams", 0))
        total_all = total_off + total_def + total_st

        if total_all > 0:
            try_add_metric("Offense", "offensive_snap_load", "Offensive Output", total_off, total_all, "Offensive Snap Load")
            try_add_metric("Defense", "defensive_snap_load", "Defensive Control", total_def, total_all, "Defensive Snap Load")
            try_add_metric("Special Teams", "special_teams_snap_pct", "Field Control (Special Teams)", total_st, total_all, "Special Teams Snap Load")

        # Extract composite metrics like "13-18"
        for raw_key, new_names in _COMPOSITE_MAP.items():
            if raw_key in t_stats:
                try:
                    a, b = map(float, t_stats[raw_key].split("-"))
                except Exception:
                    a, b = 0.0, 0.0
                # Derive core_area and category based on field name
                for name, val in zip(new_names, [a, b]):
                    core_area = "Offensive Output" if "yards" in name or "attempts" in name else "Disruption and Turnovers"
                    category = "Offense" if "pass" in name or "rush" in name else "Defense"
                    add_metric(category, name, core_area, val)
                    
        stats = game_stats[side]["raw"]
        opp_stats = game_stats["away" if side == "home" else "home"]["raw"]

        try:
            points_scored = float(body.get("homePts" if side == "home" else "awayPts", 0))
        except (ValueError, TypeError):
            points_scored = 0.0

        try:
            points_allowed = float(dst_stats.get("ptsAllowed", 0) or 0)
        except (ValueError, TypeError):
            points_allowed = 0.0
                
        pass_tds = stats.get("passtd", stats.get("passTD", 0))
        rush_tds = stats.get("rushtd", stats.get("rushTD", 0))
        total_tds = pass_tds + rush_tds + 1e-6

        try_add_metric("Offense", "points_per_play", "Scoring Efficiency", points_scored, stats.get("totalplays", 1), "Points Per Play")
        try_add_metric("Defense", "points_allowed_per_play", "Scoring Efficiency", points_allowed, opp_stats.get("totalplays", 1), "Points Allowed Per Play")
        den = stats.get("rushingattempts", 0)
        try_add_metric("Offense", "pass_run_ratio", "Offensive Output", stats.get("pass_attempts", 0), (den or 1), "Pass to Run Ratio")
        try_add_metric("Offense", "1st_down_rate", "Offensive Output", stats.get("firstdowns", 0), stats.get("totalplays", 1), "1st Down Rate")
        try_add_metric("Offense", "td_rate", "Offensive Output", (pass_tds + rush_tds), stats.get("totalplays", 1), "Touchdown Rate")
        try_add_metric("Offense", "red_zone_efficiency", "Offensive Output", stats.get("red_zone_tds", 0), stats.get("red_zone_attempts", 1), "Red Zone Efficiency")
        try_add_metric("Offense", "third_down_pct", "Offensive Output", stats.get("third_down_conversions", 0), stats.get("third_down_attempts", 1), "Third Down Conversion Rate")
        try_add_metric("Offense", "fourth_down_pct", "Offensive Output", stats.get("fourth_down_conversions", 0), stats.get("fourth_down_attempts", 1), "Fourth Down Conversion Rate")
        try_add_metric("Offense", "run_play_pct", "Offensive Output", stats.get("rushingattempts", 0), stats.get("totalplays", 1), "Run Play Percentage")
        try_add_metric("Offense", "pass_play_pct", "Offensive Output", stats.get("pass_attempts", 0), stats.get("totalplays", 1), "Pass Play Percentage")
        try_add_metric("Offense", "pass_td_share", "Offensive Output", pass_tds, total_tds, "Pass Touchdown Share")
        try_add_metric("Offense", "rush_td_share", "Offensive Output", rush_tds, total_tds, "Rush Touchdown Share")
        add_metric("Defense", "turnover_margin", "Disruption and Turnovers", (stats.get("defensiveinterceptions", 0) + stats.get("fumblesrecovered", 0) - stats.get("interceptionsthrown", 0) - stats.get("fumbleslost", 0)))
        den = stats.get("defensiveinterceptions", 0)
        try_add_metric("Defense", "sack_to_turnover_ratio", "Disruption and Turnovers", stats.get("sacks", 0), (den or 1), "Sack to Turnover Ratio")
        try_add_metric("Defense", "pressure_rate", "Disruption and Turnovers", (stats.get("sacks", 0) + stats.get("sacks_taken", 0)), stats.get("pass_attempts", 1), "Pressure Rate")
        numerator = stats.get("ydsallowed", 0)
        denominator = opp_stats.get("totalplays", 1)
        value = numerator / denominator
        add_metric("Defense", "defensive_success_rate", "Defensive Control", value)
        den = stats.get("ydsallowed", 0)
        try_add_metric("Defense", "points_allowed_per_yard", "Defensive Control", points_allowed, (den or 1), "Points Allowed Per Yard")

    return rows
