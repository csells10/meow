from queries.game_queries import (
    get_game_header,
    get_team_metrics,
    get_final_score,
)


def fmt(val):
    return round(val, 3) if isinstance(val, float) else val

def build_team_comparison(away_metrics: dict, home_metrics: dict):
    METRICS = [
        ("Scoring & Efficiency::points_per_play", "Points per Play", "higher"),
        ("Scoring & Efficiency::points_allowed_per_play", "Points Allowed per Play", "lower"),
        ("Red Zone & Conversion::third_down_pct", "3rd Down %", "higher"),
        ("Red Zone & Conversion::red_zone_efficiency", "Red Zone TD %", "higher"),
        ("Defense::turnover_margin_per_game", "Turnover Margin / Game", "higher")
    ]

    comparison = []

    for key, label, direction in METRICS:
        away_val = away_metrics.get(key)
        home_val = home_metrics.get(key)

        if away_val is None or home_val is None:
            continue

        if direction == "higher":
            better = "away" if away_val > home_val else "home"
        else:
            better = "away" if away_val < home_val else "home"

        comparison.append({
            "label": label,
            "away": fmt(away_val),
            "home": fmt(home_val),
            "better": better
        })

    return comparison

FINAL_STATUSES = {"Final", "Final/OT"}


def get_game_details(game_id: str) -> dict:
    """
    Build the V1 game details response.

    V1 includes:
    - header
    - final_score (only if game is final)
    - game_profile (empty for now)
    - matchup_lean (empty for now)
    - team_comparison (empty for now)
    """
    header = get_game_header(game_id)

    if not header:
        return {
            "header": {},
            "final_score": None,
            "game_profile": [],
            "matchup_lean": {},
            "team_comparison": []
        }

    away_metrics, home_metrics = get_team_metrics(game_id)

    game_status = header.get("game_status")
    final_score = get_final_score(game_id) if game_status in FINAL_STATUSES else None
    game_profile = build_game_profile(away_metrics, home_metrics, header)
    team_comparison = build_team_comparison(away_metrics, home_metrics)

    return {
        "header": header,
        "final_score": final_score,
        "game_profile": game_profile,
        "matchup_lean": {},
        "team_comparison": team_comparison
    }

def build_game_profile(away_metrics: dict, home_metrics: dict, header: dict):
    profile = []

    away = header["away_team"]["abbreviation"]
    home = header["home_team"]["abbreviation"]

    def compare(a, b):
        if a is None or b is None:
            return None
        return a - b

    # -------------------------
    # Pressure (basic version)
    # -------------------------
    away_pressure = away_metrics.get("Pressure & Turnovers::pressure_rate")
    home_pressure = home_metrics.get("Pressure & Turnovers::pressure_rate")

    if away_pressure is not None and home_pressure is not None:
        diff = compare(away_pressure, home_pressure)

        if abs(diff) > 0.05:
            level = "Elevated"
        elif abs(diff) > 0.02:
            level = "Moderate"
        else:
            level = "Neutral"

        if diff > 0:
            tilt = f"{away} generating more pressure"
        else:
            tilt = f"{home} generating more pressure"

        profile.append({
            "category": "Pressure",
            "level": level,
            "tilt": tilt
        })

    # -------------------------
    # Turnover Environment
    # -------------------------
    away_to = away_metrics.get("Defense::turnover_margin_per_game")
    home_to = home_metrics.get("Defense::turnover_margin_per_game")

    if away_to is not None and home_to is not None:
        diff = compare(away_to, home_to)

        if abs(diff) > 0.5:
            level = "Elevated"
        elif abs(diff) > 0.2:
            level = "Moderate"
        else:
            level = "Neutral"

        if diff > 0:
            tilt = f"{away} better turnover profile"
        else:
            tilt = f"{home} better turnover profile"

        profile.append({
            "category": "Turnover Environment",
            "level": level,
            "tilt": tilt
        })

    # -------------------------
    # Scoring Efficiency
    # -------------------------
    away_ppp = away_metrics.get("Scoring & Efficiency::points_per_play")
    home_ppp = home_metrics.get("Scoring & Efficiency::points_per_play")

    if away_ppp is not None and home_ppp is not None:
        diff = compare(away_ppp, home_ppp)

        if abs(diff) > 0.07:
            level = "Elevated"
        elif abs(diff) > 0.03:
            level = "Moderate"
        else:
            level = "Neutral"

        if diff > 0:
            tilt = f"{away} more efficient scoring"
        else:
            tilt = f"{home} more efficient scoring"

        profile.append({
            "category": "Scoring",
            "level": level,
            "tilt": tilt
        })

    return profile