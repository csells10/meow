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
    
    team_comparison = build_team_comparison(away_metrics, home_metrics)

    return {
        "header": header,
        "final_score": final_score,
        "game_profile": [],
        "matchup_lean": {},
        "team_comparison": team_comparison
    }