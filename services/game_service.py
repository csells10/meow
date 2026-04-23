from queries.game_queries import (
    get_game_header,
    get_team_metrics,
    get_final_score,
)

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

    return {
        "header": header,
        "final_score": final_score,
        "game_profile": [],
        "matchup_lean": {},
        "team_comparison": []
    }