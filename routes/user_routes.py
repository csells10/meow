from flask import Blueprint, jsonify, g

from auth.firebase_auth import require_firebase_auth

user_routes = Blueprint("user_routes", __name__)


@user_routes.route("/me", methods=["GET"])
@require_firebase_auth
def me():
    firebase_user = g.firebase_user or {}
    gamelens_user = firebase_user.get("gamelens_user", {})

    email = gamelens_user.get("email") or firebase_user.get("email")
    role = str(gamelens_user.get("role") or "user").strip().lower()
    active = gamelens_user.get("active") is True

    return jsonify({
        "email": email,
        "role": role,
        "active": active,
        "is_admin": role == "admin",
    }), 200