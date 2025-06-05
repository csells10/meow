from flask import Flask, request
from utils.logging import setup_logging 
 
import sys
import os
import logging
from config import API_CALLS

#Creating logging file
logging.basicConfig(
    filename='app.log',
    filemode='w',  # Overwrites file on each app run
    format='%(asctime)s %(levelname)s:%(message)s',
    level=logging.INFO
)

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
        api_name = api_call['name']
        logging.info(f"Starting API call: {api_name} (load_date={load_date})")

        try:
            # Call the API with or without the load_date
            if load_date:
                api_call['function'](load_date=load_date)
            else:
                api_call['function']()

            # If the API call is tracked, update cycle counts
            if api_name in api_cycles:
                api_cycles[api_name] += 1
                logging.info(f"Cycle count for '{api_name}' is now {api_cycles[api_name]}")

                # Check for max cycle threshold
                if 'max_cycles' in api_call and api_cycles[api_name] >= api_call['max_cycles']:
                    logging.info(f"Max cycles reached for '{api_name}' ({api_cycles[api_name]} cycles).")

        except Exception as e:
            logging.error(f"Error while running '{api_name}': {e}", exc_info=True)

def setup_schedules(load_date=None):
    """
    Setup the API call schedules with optional load_date.
    """
    for api_call in API_CALLS:
        logging.info(f"Scheduling {api_call['name']} to run with load_date={load_date}")

        # Initialize cycle count (for max cycles if needed later)
        api_cycles = {api['name']: 0 for api in API_CALLS}

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
