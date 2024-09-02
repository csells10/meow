from api_calls.api_call_nfl_games import fetch_nfl_games

API_CALLS = [
    {
        'name': 'NFL Games API Call',
        'function': fetch_nfl_games,
        'schedule': '30 seconds',
        'max_cycles': 10  # Run the API call for 10 cycles
    }
]
