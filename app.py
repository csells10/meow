from flask import Flask, request
import sys
import os
import logging
from config import API_CALLS

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

sys.path.append(os.path.join(os.path.dirname(__file__), 'api_calls'))

# Dictionary to keep track of the cycles for each API call
api_cycles = {}

app = Flask(__name__)

def run_api_calls():
    """
    Function to run the scheduled API calls.
    """
    for api_call in API_CALLS:
        logging.info(f"Running {api_call['name']} API call")
        try:
            api_call['function']()  # Call the API function
            
            # Increment cycle count
            api_cycles[api_call['name']] += 1
            logging.info(f"Cycle count for {api_call['name']} incremented to {api_cycles[api_call['name']]}")
            
            # Check if max cycles reached
            if api_cycles[api_call['name']] >= api_call['max_cycles']:
                logging.info(f"Max cycles reached for {api_call['name']}.")
        except Exception as e:
            logging.error(f"Error while running {api_call['name']}: {e}")

def setup_schedules():
    """
    Setup the API call schedules.
    """
    for api_call in API_CALLS:
        logging.info(f"Scheduling {api_call['name']} to run.")
        # Initialize cycle count (for max cycles if needed later)
        api_cycles[api_call['name']] = 0
        # Run the API call directly
        run_api_calls()

@app.route("/", methods=["POST"])
def run_scheduled_job():
    """
    Endpoint that will be triggered by Cloud Scheduler.
    It sets up and runs the scheduled API calls.
    """
    logging.info("Received POST request from Cloud Scheduler to / endpoint.")
    
    setup_schedules()  # Setup and run the scheduled API calls when the POST request is received.
    return "API calls executed successfully", 200

if __name__ == "__main__":
    # Run Flask app (Cloud Run will handle invoking the function)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
