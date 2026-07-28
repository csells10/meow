from flask import Flask, request
from flask_cors import CORS
import os

from config import API_CALLS
from routes.games import games_bp
from routes.game_routes import game_routes
from routes.admin_claim_health_routes import admin_claim_health_routes
from routes.user_routes import user_routes
from utils.logging_setup import setup_logging, log_event

# ------------------------------------------------------------
# Application setup
# ------------------------------------------------------------
# Configure structured logging once when the app starts.
setup_logging()

# Flask app instance
app = Flask(__name__)

# Enable CORS
CORS(
    app,
    resources={r"/*": {"origins": "*"}},
    allow_headers=["Content-Type", "Authorization"],
    methods=["GET", "POST", "OPTIONS"],
)

# Register route blueprints
# This keeps app.py thin and allows routes to live in their own modules.
app.register_blueprint(games_bp)
app.register_blueprint(game_routes)
app.register_blueprint(admin_claim_health_routes)
app.register_blueprint(user_routes)

# Dictionary used to track how many times each API call has run
# during a given scheduled execution cycle.
api_cycles = {}


def run_api_calls(load_date=None):
    """
    Execute each configured ingestion API call in sequence.

    Parameters
    ----------
    load_date : str | None
        Optional date string used for historical or targeted loads.
        If not provided, the API call runs with its default behavior.

    Notes
    -----
    - Tracks how many stat rows were inserted so we can decide whether
      to run the aggregate job afterward.
    - Logs each API call start, cycle increment, and any errors.
    """
    stats_inserted = 0  # Track number of stats rows inserted

    for api_call in API_CALLS:
        api_name = api_call["name"]
        log_event("info", "start_api_call", api_name=api_name, load_date=load_date)

        try:
            # If a load_date is provided, pass it into the function.
            # Otherwise run the function normally.
            result = api_call["function"](load_date=load_date) if load_date else api_call["function"]()

            # Only the NFL Stats API call returns the inserted row count
            # that we use to decide whether aggregation should run.
            if api_name == "NFL Stats API Call":
                stats_inserted = result or 0

            # Track execution cycles for visibility/debugging.
            if api_name in api_cycles:
                api_cycles[api_name] += 1
                log_event(
                    "info",
                    "api_cycle_incremented",
                    api_name=api_name,
                    cycle_count=api_cycles[api_name]
                )

                # Log when a configured max cycle count is reached.
                if "max_cycles" in api_call and api_cycles[api_name] >= api_call["max_cycles"]:
                    log_event(
                        "info",
                        "max_cycles_reached",
                        api_name=api_name,
                        cycle_count=api_cycles[api_name]
                    )

        except Exception as e:
            log_event("error", "api_call_error", api_name=api_name, error=str(e))

    # ------------------------------------------------------------
    # Conditional aggregation
    # ------------------------------------------------------------
    # Only run the aggregation job if stats were actually inserted.
    if stats_inserted > 0:
        log_event("info", "running_aggregate_job", reason="stats_inserted", count=stats_inserted)
        from agg.aggregate_nfl_metrics_2025 import run_aggregate_for_season
        run_aggregate_for_season("2025")
    else:
        log_event("info", "aggregate_skipped", reason="no_stats_inserted")


def setup_schedules(load_date=None):
    """
    Initialize API cycle tracking and run the configured ingestion jobs.

    Parameters
    ----------
    load_date : str | None
        Optional date used to drive targeted or historical ingestion.
    """
    log_event("info", "setup_schedules", load_date=load_date)

    global api_cycles
    api_cycles = {api["name"]: 0 for api in API_CALLS}
    run_api_calls(load_date=load_date)


@app.route("/", methods=["POST"])
def run_scheduled_job():
    """
    Cloud Scheduler entry point.

    Expected behavior
    -----------------
    - Receives an optional JSON body with:
        { "load_date": "YYYY-MM-DD" }
    - Triggers the ingestion workflow.
    """
    log_event("info", "scheduler_trigger_received")

    request_data = request.get_json(silent=True)
    load_date = request_data.get("load_date") if request_data else None
    log_event("info", "load_date_extracted", load_date=load_date)

    setup_schedules(load_date=load_date)
    return {"message": "API calls executed successfully"}, 200


@app.route("/test", methods=["GET"])
def test_api_calls():
    """
    Manual test endpoint for ingestion.

    Query Params
    ------------
    load_date : str | None
        Optional date in YYYY-MM-DD format.

    Example
    -------
    /test?load_date=2025-09-14
    """
    load_date = request.args.get("load_date")
    log_event("info", "test_route_triggered", load_date=load_date)

    try:
        setup_schedules(load_date=load_date)
        return {"message": "Test successful. API calls executed", "load_date": load_date}, 200
    
    except Exception as e:
        log_event("error", "test_api_call_error", error=str(e))
        return {"error": str(e)}, 500
    
    
@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}, 200


if __name__ == "__main__":
    debug_mode = os.environ.get("FLASK_DEBUG", "false").lower() == "true"

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8080)),
        debug=debug_mode,
    )