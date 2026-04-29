from flask import Blueprint, request, jsonify
from datetime import datetime
from utils.logging_setup import log_event

from auth.firebase_auth import require_firebase_auth
from services.get_games_by_date import fetch_games_by_date


games_bp = Blueprint("games", __name__)


@games_bp.route("/games", methods=["GET"])
@require_firebase_auth
def get_games_by_date():
    """
    GET /games?date=YYYY-MM-DD
    Returns all games for a given date.

    Protected:
    Requires Firebase Authorization Bearer token.
    User must exist in Firestore allowed_users collection with active=true.
    """

    date_str = request.args.get("date")
    log_event("info", "games_route_triggered", date=date_str)

    # Missing date
    if not date_str:
        return jsonify({
            "error": "Missing required query parameter: date",
            "example": "/games?date=2025-09-14"
        }), 400

    # Validate format
    try:
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({
            "error": "Invalid date format. Use YYYY-MM-DD.",
            "received": date_str
        }), 400

    try:
        games = fetch_games_by_date(parsed_date)

        log_event("info", "games_query_success", date=date_str, count=len(games))

        return jsonify({
            "date": date_str,
            "games": games
        }), 200

    except Exception as e:
        log_event("error", "games_query_failed", error=str(e), date=date_str)

        return jsonify({
            "error": "Failed to fetch games"
        }), 500