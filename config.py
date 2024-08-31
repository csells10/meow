from api_calls.api_call_1 import fetch_data as fetch_data_1
# from api_calls.api_call_2 import fetch_data as fetch_data_2

API_CALLS = [
    {
        "name": "API Call 1",
        "schedule": "10:00",  # Time format should be HH:MM
        "function": fetch_data_1,
    }
   # {
   #     "name": "API Call 2",
   #     "schedule": "12:00",
    #    "function": fetch_data_2,
    #},
    # Add more API calls here
]
