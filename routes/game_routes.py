from flask import Blueprint, jsonify
from services.game_service import get_game_details

game_routes = Blueprint("game_routes", __name__)

@game_routes.route("/game/<path:game_id>", methods=["GET"])
def game_details(game_id):
    data = get_game_details(game_id)
    return jsonify(data), 200