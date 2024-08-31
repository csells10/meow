from api_calls.api_call_nfl_games import fetch_nfl_games

API_CALLS = [
    {
        "name": "NFL Games API Call",
        "schedule": "10:00",  # Example time, format HH:MM
        "function": fetch_nfl_games,
    },
]
