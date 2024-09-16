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

def run_api_calls(load_date=None):
    """
    Function to run the scheduled API calls with optional date parameter.
    
    Args:
        load_date (str): Optional date string in 'YYYY-MM-DD' format to reload data for that date.
                         If None, defaults to today's date.
    """
    for api_call in API_CALLS:
        logging.info(f"Running {api_call['name']} API call with load_date={load_date}")
        try:
            # Pass the load_date if available
            if load_date:
                api_call['function'](load_date=load_date)
            else:
                api_call['function']()
            
            # Increment cycle count
            api_cycles[api_call['name']] += 1
            logging.info(f"Cycle count for {api_call['name']} incremented to {api_cycles[api_call['name']]}")
            
            # Check if max cycles reached
            if api_cycles[api_call['name']] >= api_call['max_cycles']:
                logging.info(f"Max cycles reached for {api_call['name']}.")
        except Exception as e:
            logging.error(f"Error while running {api_call['name']}: {e}")

def setup_schedules(load_date=None):
    """
    Setup the API call schedules with optional load_date.
    
    Args:
        load_date (str): Optional date string in 'YYYY-MM-DD' format to reload data for that date.
    """
    for api_call in API_CALLS:
        logging.info(f"Scheduling {api_call['name']} to run with load_date={load_date}")
        # Initialize cycle count (for max cycles if needed later)
        api_cycles[api_call['name']] = 0
        # Run the API call directly with the provided load_date
        run_api_calls(load_date=load_date)

@app.route("/", methods=["POST"])
def run_scheduled_job():
    """
    Endpoint that will be triggered by Cloud Scheduler.
    It sets up and runs the scheduled API calls with an optional load_date parameter.
    """
    logging.info("Received POST request from Cloud Scheduler to / endpoint.")
    
    # Check if a 'load_date' is provided in the request body (e.g., from Cloud Scheduler)
    request_data = request.get_json(silent=True)  # Parse JSON body if available
    load_date = request_data.get('load_date') if request_data else None  # Extract 'load_date' if provided
    
    logging.info(f"load_date parameter received: {load_date}")
    
    setup_schedules(load_date=load_date)  # Setup and run the scheduled API calls with the load_date if provided.
    return "API calls executed successfully", 200

if __name__ == "__main__":
    # Run Flask app (Cloud Run will handle invoking the function)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
