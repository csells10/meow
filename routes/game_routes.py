from flask import Blueprint, jsonify

from auth.firebase_auth import require_firebase_auth
from services.game_service import get_game_details


game_routes = Blueprint("game_routes", __name__)


@game_routes.route("/game/<path:game_id>", methods=["GET"])
@require_firebase_auth
def game_details(game_id):
    """
    GET /game/<game_id>

    Protected:
    Requires Firebase Authorization Bearer token.
    User must exist in Firestore allowed_users collection with active=true.
    """
    data = get_game_details(game_id)
    return jsonify(data), 200