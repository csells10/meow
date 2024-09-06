from api_calls.api_call_nfl_games import fetch_nfl_games

# List of API calls and their schedules
API_CALLS = [
    {
        'name': 'NFL Games API Call',
        'function': fetch_nfl_games,
        'max_cycles': 1,  # Run the API call for 10 cycles
    }
]
