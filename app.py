from flask import Flask, request
from flask_cors import CORS
import os

from config import API_CALLS
from routes.games import games_bp
from routes.game_routes import game_routes
from routes.admin_claim_health_routes import admin_claim_health_routes
from routes.user_routes import user_routes
from services.gamelens_metric_pipeline_conductor import (
    run_gamelens_metric_pipeline,
)
from runtime_config import load_runtime_config
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

# Production defaults to 2026. Dev replay mode must provide an explicit,
# validated season and isolated targets before this module can finish loading.
RUNTIME_CONFIG = load_runtime_config()

# Dictionary used to track how many times each API call has run
# during a given scheduled execution cycle.
api_cycles = {}


def _normalize_api_result(api_name, result):
    """Normalize legacy counts and structured ingestion results."""
    if isinstance(result, dict):
        stage_summary = dict(result)
        stage_summary.setdefault("status", "success")
        return stage_summary

    stage_summary = {"status": "success"}
    if api_name == "NFL Stats API Call":
        stage_summary["successful_game_count"] = int(result or 0)
    elif api_name == "NFL Scores API Call":
        stage_summary["successful_game_count"] = int(result or 0)
    return stage_summary


def run_api_calls(load_date=None):
    """
    Execute each configured ingestion API call in sequence.

    Parameters
    ----------
    load_date : str | None
        Optional date string used for historical or targeted loads.
        If not provided, the API call runs with its default behavior.

    Returns
    -------
    dict
        Execution summary covering ingestion, the accepted Stats game count,
        and the Stats-gated GameLens metric pipeline.

    Notes
    -----
    - Preserves the configured Schedule -> Stats -> Scores ordering.
    - Uses successfully accepted Stats games to gate the metric pipeline.
    - Reports ingestion and metric-pipeline failures instead of hiding them.
    """
    accepted_stats_games = 0
    ingestion = {}

    for api_call in API_CALLS:
        api_name = api_call["name"]
        log_event(
            "info",
            "start_api_call",
            api_name=api_name,
            load_date=load_date,
        )

        try:
            # If a load_date is provided, pass it into the function.
            # Otherwise run the function normally.
            result = (
                api_call["function"](load_date=load_date)
                if load_date
                else api_call["function"]()
            )
            ingestion[api_name] = _normalize_api_result(api_name, result)

            # The NFL Stats API call returns successfully accepted games.
            if api_name == "NFL Stats API Call":
                accepted_stats_games = ingestion[api_name].get(
                    "successful_game_count",
                    0,
                )
                ingestion[api_name]["accepted_games"] = accepted_stats_games

            # Track execution cycles for visibility/debugging.
            if api_name in api_cycles:
                api_cycles[api_name] += 1
                log_event(
                    "info",
                    "api_cycle_incremented",
                    api_name=api_name,
                    cycle_count=api_cycles[api_name],
                )

                # Log when a configured max cycle count is reached.
                if (
                    "max_cycles" in api_call
                    and api_cycles[api_name] >= api_call["max_cycles"]
                ):
                    log_event(
                        "info",
                        "max_cycles_reached",
                        api_name=api_name,
                        cycle_count=api_cycles[api_name],
                    )

        except Exception as exc:
            ingestion[api_name] = {
                "status": "failed",
                "error": str(exc),
                "failures": [{"error": str(exc)}],
            }
            if api_name in {
                "NFL Stats API Call",
                "NFL Scores API Call",
            }:
                ingestion[api_name].update({
                    "successful_game_count": 0,
                    "failed_game_count": 1,
                })
            log_event(
                "error",
                "api_call_error",
                api_name=api_name,
                error=str(exc),
            )

    failed_ingestion = [
        api_name
        for api_name, stage_summary in ingestion.items()
        if stage_summary["status"] in {"failed", "partial_failure"}
    ]
    successful_ingestion_count = sum(
        stage_summary["status"] in {"success", "no_op"}
        for stage_summary in ingestion.values()
    )

    stats_summary = ingestion.get("NFL Stats API Call", {})
    scores_summary = ingestion.get("NFL Scores API Call", {})
    selected_game_ids = stats_summary.get("selected_game_ids") or (
        scores_summary.get("selected_game_ids") or []
    )
    selected_game_ids = list(selected_game_ids)

    # ------------------------------------------------------------
    # Stats-gated GameLens metric pipeline
    # ------------------------------------------------------------
    if accepted_stats_games > 0:
        log_event(
            "info",
            "running_gamelens_metric_pipeline",
            reason="stats_accepted",
            count=accepted_stats_games,
            season=RUNTIME_CONFIG.active_season,
        )
        try:
            metric_pipeline = run_gamelens_metric_pipeline(
                season=RUNTIME_CONFIG.active_season,
                write=True,
            )
            if not isinstance(metric_pipeline, dict):
                raise TypeError(
                    "GameLens metric pipeline returned a non-dictionary summary"
                )
        except Exception as exc:
            metric_pipeline = {
                "season": RUNTIME_CONFIG.active_season,
                "status": "failed",
                "failed_stage": None,
                "stages": {},
                "error": str(exc),
            }
            log_event(
                "error",
                "gamelens_metric_pipeline_error",
                season=RUNTIME_CONFIG.active_season,
                error=str(exc),
            )
    else:
        log_event(
            "info",
            "gamelens_metric_pipeline_skipped",
            reason="no_accepted_stats_games",
        )
        metric_pipeline = {
            "season": RUNTIME_CONFIG.active_season,
            "status": "skipped",
            "reason": "no_accepted_stats_games",
        }

    if (
        accepted_stats_games > 0
        and metric_pipeline.get("status") != "success"
    ):
        status = "failure"
    elif failed_ingestion:
        status = (
            "partial_failure"
            if successful_ingestion_count > 0
            else "failure"
        )
    elif accepted_stats_games == 0:
        status = "no_op"
    else:
        status = "success"

    summary = {
        "status": status,
        "execution_mode": RUNTIME_CONFIG.run_mode,
        "active_season": RUNTIME_CONFIG.active_season,
        "load_date": load_date,
        "selected_game_count": len(selected_game_ids),
        "selected_game_ids": selected_game_ids,
        "accepted_stats_games": accepted_stats_games,
        "ingestion": ingestion,
        "metric_pipeline": metric_pipeline,
    }
    if status == "no_op":
        summary["no_op_reason"] = "no_accepted_stats_games"
    return summary


def setup_schedules(load_date=None):
    """
    Initialize API cycle tracking and run the configured ingestion jobs.

    Parameters
    ----------
    load_date : str | None
        Optional date used to drive targeted or historical ingestion.

    Returns
    -------
    dict
        The complete ingestion and metric-pipeline execution summary.
    """
    log_event("info", "setup_schedules", load_date=load_date)

    global api_cycles
    api_cycles = {api["name"]: 0 for api in API_CALLS}
    return run_api_calls(load_date=load_date)


@app.route("/", methods=["POST"])
def run_scheduled_job():
    """
    Cloud Scheduler entry point.

    Expected behavior
    -----------------
    - Receives an optional JSON body with:
        { "load_date": "YYYY-MM-DD" }
    - Triggers the ingestion workflow.
    - Returns HTTP 200 only for success or an explicit no-op.
    """
    log_event("info", "scheduler_trigger_received")

    request_data = request.get_json(silent=True)
    load_date = request_data.get("load_date") if request_data else None
    log_event("info", "load_date_extracted", load_date=load_date)

    summary = setup_schedules(load_date=load_date)
    http_status = (
        200
        if summary.get("status") in {"success", "no_op"}
        else 500
    )
    return summary, http_status


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
        return {
            "message": "Test successful. API calls executed",
            "load_date": load_date,
        }, 200

    except Exception as exc:
        log_event("error", "test_api_call_error", error=str(exc))
        return {"error": str(exc)}, 500


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
