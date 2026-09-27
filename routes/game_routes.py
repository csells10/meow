from flask import Blueprint, Response, jsonify

from auth.firebase_auth import require_firebase_auth
from services.game_service import get_game_details
from services.matchup_lens_service import serialize_matchup_lens_context


game_routes = Blueprint("game_routes", __name__)


@game_routes.route("/game/<path:game_id>/lens-context", methods=["GET"])
@require_firebase_auth
def matchup_lens_context(game_id):
    """GET /game/<game_id>/lens-context (frozen matchup_lens_v1 evidence)."""
    payload, status = serialize_matchup_lens_context(game_id)
    return Response(payload, status=status, content_type="application/json; charset=utf-8")


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
