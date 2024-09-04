import schedule
import time
from config import API_CALLS

# Dictionary to keep track of the cycles for each API call
api_cycles = {}

def run_api_call(api_call):
    print(f"Running {api_call['name']}")
    try:
        api_call['function']()  # Call the API function

        # Increment cycle count
        api_cycles[api_call['name']] += 1

        # Check if max cycles reached
        if api_cycles[api_call['name']] >= api_call['max_cycles']:
            print(f"Max cycles reached for {api_call['name']}. Stopping schedule.")
            schedule.cancel_job(api_call['job'])

    except Exception as e:
        print(f"Error while running {api_call['name']}: {e}")

def setup_schedules():
    for api_call in API_CALLS:
        print(f"Scheduling {api_call['name']} at {api_call['schedule']}")

        # Initialize cycle count
        api_cycles[api_call['name']] = 0

        # Schedule the job and store the job object
        job = schedule.every().day.at(api_call['schedule']).do(run_api_call, api_call=api_call)
        api_call['job'] = job

if __name__ == "__main__":
    setup_schedules()
    while True:
        schedule.run_pending()
        time.sleep(1)
