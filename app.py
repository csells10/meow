from flask import Flask, request
import os
import sys
from config import API_CALLS
from utils.gcs import upload_file_to_gcs
from utils.logging_setup import setup_logging, log_event  # ✅ use new helper

# Setup structured logging
setup_logging()

# Dictionary to track cycles per API call
api_cycles = {}

app = Flask(__name__)

def run_api_calls(load_date=None):
    stats_inserted = 0  # Track number of stats inserted

    for api_call in API_CALLS:
        api_name = api_call['name']
        log_event("info", "start_api_call", api_name=api_name, load_date=load_date)

        try:
            result = api_call['function'](load_date=load_date) if load_date else api_call['function']()

            # ✅ If it's the stats function, store the inserted count
            if api_name == "NFL Stats API Call":
                stats_inserted = result or 0

            if api_name in api_cycles:
                api_cycles[api_name] += 1
                log_event("info", "api_cycle_incremented", api_name=api_name, cycle_count=api_cycles[api_name])

                if 'max_cycles' in api_call and api_cycles[api_name] >= api_call['max_cycles']:
                    log_event("info", "max_cycles_reached", api_name=api_name, cycle_count=api_cycles[api_name])

        except Exception as e:
            log_event("error", "api_call_error", api_name=api_name, error=str(e))

    # ✅ After all API calls — conditionally run aggregation
    if stats_inserted > 0:
        log_event("info", "running_aggregate_job", reason="stats_inserted", count=stats_inserted)
        from agg.aggregate_nfl_metrics_2025 import run_aggregate_for_season
        run_aggregate_for_season("2025")
    else:
        log_event("info", "aggregate_skipped", reason="no_stats_inserted")

def setup_schedules(load_date=None):
    """
    Set up and run scheduled API calls.
    """
    log_event("info", "setup_schedules", load_date=load_date)

    global api_cycles
    api_cycles = {api['name']: 0 for api in API_CALLS}
    run_api_calls(load_date=load_date)

@app.route("/", methods=["POST"])
def run_scheduled_job():
    """
    Triggered by Cloud Scheduler to initiate API call routines.
    """
    log_event("info", "scheduler_trigger_received")

    request_data = request.get_json(silent=True)
    load_date = request_data.get('load_date') if request_data else None
    log_event("info", "load_date_extracted", load_date=load_date)

    setup_schedules(load_date=load_date)
    return "API calls executed successfully", 200

@app.route("/test", methods=["GET"])
def test_api_calls():
    """
    Manual test endpoint to trigger API routines with optional load_date.
    """
    load_date = request.args.get('load_date')
    log_event("info", "test_route_triggered", load_date=load_date)

    try:
        setup_schedules(load_date=load_date)
        return f"Test successful. API calls executed with load_date={load_date}", 200
    except Exception as e:
        log_event("error", "test_api_call_error", error=str(e))
        return f"Error: {e}", 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
