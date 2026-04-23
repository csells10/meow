from queries.game_queries import (
    get_game_header,
    get_final_score,
    get_team_metrics
)

def build_game_details(game_id: str) -> dict:
    header = get_game_header(game_id)
    final_score = get_final_score(game_id)

    away_metrics, home_metrics = get_team_metrics(game_id)

    team_comparison = build_team_comparison(away_metrics, home_metrics)
    game_profile = build_game_profile(away_metrics, home_metrics)
    matchup_lean = build_matchup_lean(game_profile, away_metrics, home_metrics, header)

    return {
        "header": header,
        "final_score": final_score,
        "game_profile": game_profile,
        "matchup_lean": matchup_lean,
        "team_comparison": team_comparison
    }