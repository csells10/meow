import schedule
import time
from config import API_CALLS

def run_api_calls():
    for api_call in API_CALLS:
        print(f"Running {api_call['name']}")
        api_call['function']()  # Call the API function

def setup_schedules():
    for api_call in API_CALLS:
        print(f"Scheduling {api_call['name']} at {api_call['schedule']}")
        schedule.every().day.at(api_call['schedule']).do(api_call['function'])

if __name__ == "__main__":
    setup_schedules()
    while True:
        schedule.run_pending()
        time.sleep(1)

