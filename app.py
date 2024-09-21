from flask import Flask, request
import sys
import os
from config import API_CALLS
from utils.logging import setup_logging  

# Setup structured logging
setup_logging() 

# Dictionary to keep track of the cycles for each API call
api_cycles = {}

app = Flask(__name__)

def run_api_calls(load_date=None):
    """
    Function to run the scheduled API calls with optional date parameter.
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
            logging.error(f"Error while running {api_call['name']}: {e}", exc_info=True)  # Capture full traceback

def setup_schedules(load_date=None):
    """
    Setup the API call schedules with optional load_date.
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
    """
    logging.info("Received POST request from Cloud Scheduler to / endpoint.")
    
    request_data = request.get_json(silent=True)
    load_date = request_data.get('load_date') if request_data else None
    logging.info(f"load_date parameter received: {load_date}")
    
    setup_schedules(load_date=load_date)
    return "API calls executed successfully", 200

@app.route("/test", methods=["GET"])
def test_api_calls():
    """
    Endpoint for testing API calls manually without the need for Cloud Scheduler.
    """
    load_date = request.args.get('load_date')
    logging.info(f"Testing API calls with load_date={load_date}")
    
    try:
        setup_schedules(load_date=load_date)
        return f"Test successful. API calls executed with load_date={load_date}", 200
    except Exception as e:
        logging.error(f"Error during test: {e}", exc_info=True)
        return f"Error: {e}", 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
