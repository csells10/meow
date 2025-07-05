from api_calls.api_call_nfl_games import fetch_nfl_games
from api_calls.api_call_nfl_stats import get_nfl_stats
from api_calls.api_call_nfl_scores import fetch_scores_to_process

# List of API calls and their schedules
API_CALLS = [
    {
        'name': 'NFL Game Schedule API Call',
        'function': fetch_nfl_games,
        'max_cycles': 1
    },
    {
        'name': 'NFL Stats API Call',
        'function': get_nfl_stats,
        'max_cycles': 1
    },
    {
        'name': 'NFL Scores API Call',
        'function': fetch_scores_to_process,
        'max_cycles': 1
    }
]
