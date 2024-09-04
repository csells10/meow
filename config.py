from api_call_nfl_games import fetch_nfl_games

# List of API calls and their schedules
API_CALLS = [
    {
        'name': 'NFL Games API Call',
        'function': fetch_nfl_games,
        'schedule': '07:00',  # Set to 7:00 AM, daily run for NFL games
        'max_cycles': 1,  # Run the API call for 10 cycles
        'interval': 2,  # Interval in minutes between each cycle
    }
]
