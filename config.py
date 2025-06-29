from api_calls.api_call_nfl_games import fetch_nfl_games
from api_calls.api_call_nfl_teams import fetch_nfl_teams
from api_calls.api_call_nfl_scores import fetch_nfl_scores

# List of API calls and their schedules
API_CALLS = [
    {
        'name': 'NFL Games API Call',
        'function': fetch_nfl_games,
        'max_cycles': 1
    },
    {
        'name': 'NFL Teams API Call',
        'function': fetch_nfl_teams,
        'max_cycles': 1
    },
    {
        'name': 'NFL Scores API Call',
        'function': fetch_nfl_scores,
        'max_cycles': 1
    }
]
