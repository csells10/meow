import schedule
import time
import sys
import os
from config import API_CALLS
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), 'api_calls'))

# Dictionary to keep track of the cycles for each API call
api_cycles = {}

def run_api_calls():
    for api_call in API_CALLS:
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
        print(f"Scheduling {api_call['name']} to run.")

        # Initialize cycle count (for max cycles if needed later)
        api_cycles[api_call['name']] = 0

        # Run the API call directly (this replaces the need for the infinite loop)
        run_api_calls()

if __name__ == "__main__":
    setup_schedules()

    # Instead of an infinite loop, we rely on Cloud Scheduler to trigger this script
    print("Job finished. Cloud Run will shut down now.")
