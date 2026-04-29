import os
from functools import wraps

import firebase_admin
from firebase_admin import auth as firebase_admin_auth
from firebase_admin import firestore
from flask import request, jsonify, g


FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID", "nfl-stream-406420")

# Firestore collection used as the backend source of truth for access.
# Each document ID should be the lowercase email address.
#
# Collection: allowed_users
# Document ID: csells10@gmail.com
#
# Example document:
# {
#   "email": "csells10@gmail.com",
#   "active": true,
#   "role": "admin",
#   "notes": "Owner / QA"
# }
ALLOWED_USERS_COLLECTION = os.getenv(
    "ALLOWED_USERS_COLLECTION",
    "allowed_users",
)


def init_firebase_admin():
    """
    Initializes Firebase Admin SDK once.

    On Cloud Run, this uses Application Default Credentials.
    Locally, run:
      gcloud auth application-default login
    if needed.
    """
    if not firebase_admin._apps:
        firebase_admin.initialize_app(
            options={"projectId": FIREBASE_PROJECT_ID}
        )


def get_firestore_client():
    """
    Returns a Firestore client after Firebase Admin is initialized.
    """
    init_firebase_admin()
    return firestore.client()


def normalize_email(email: str) -> str:
    """
    Normalizes email addresses so Firestore lookups are consistent.
    """
    return str(email or "").strip().lower()


def extract_bearer_token():
    """
    Extracts Firebase ID token from:

      Authorization: Bearer <token>
    """
    auth_header = request.headers.get("Authorization", "")

    if not auth_header.startswith("Bearer "):
        return None

    return auth_header.replace("Bearer ", "", 1).strip()


def unauthorized_response(message="Unauthorized"):
    return jsonify({
        "error": "unauthorized",
        "message": message,
    }), 401


def forbidden_response(message="Forbidden"):
    return jsonify({
        "error": "forbidden",
        "message": message,
    }), 403


def get_allowed_user_record(email: str):
    """
    Looks up the authenticated user's email in Firestore.

    Firestore path:
      allowed_users/{email}

    Example:
      allowed_users/csells10@gmail.com
    """
    normalized_email = normalize_email(email)

    if not normalized_email:
        return None

    db = get_firestore_client()

    doc_ref = (
        db.collection(ALLOWED_USERS_COLLECTION)
        .document(normalized_email)
    )

    doc = doc_ref.get()

    if not doc.exists:
        return None

    return doc.to_dict() or {}


def is_user_allowed(email: str) -> tuple[bool, dict | None]:
    """
    Returns whether the user is allowed to access GameLens.

    A user is allowed only if:
      1. A document exists at allowed_users/{email}
      2. The document has active == true

    If active is missing, default to False.
    """
    user_record = get_allowed_user_record(email)

    if not user_record:
        return False, None

    is_active = user_record.get("active") is True

    return is_active, user_record


def verify_firebase_request():
    """
    Validates:
    1. Authorization Bearer token exists
    2. Firebase ID token is valid
    3. User email exists in Firestore allowed_users collection
    4. User document has active == true

    Returns:
      (decoded_token, None) on success
      (None, response_tuple) on failure
    """
    init_firebase_admin()

    token = extract_bearer_token()

    if not token:
        return None, unauthorized_response(
            "Missing Authorization Bearer token"
        )

    try:
        decoded_token = firebase_admin_auth.verify_id_token(token)
    except Exception:
        return None, unauthorized_response(
            "Invalid or expired Firebase token"
        )

    email = normalize_email(decoded_token.get("email"))

    if not email:
        return None, forbidden_response(
            "Firebase token does not include an email address"
        )

    is_allowed, user_record = is_user_allowed(email)

    if not is_allowed:
        return None, forbidden_response(
            "This Google account is not allowed to access GameLens"
        )

    # Store normalized/user-facing auth context for downstream use if needed.
    decoded_token["email"] = email
    decoded_token["gamelens_user"] = {
        "email": email,
        "role": user_record.get("role", "user") if user_record else "user",
        "active": True,
    }

    return decoded_token, None


def require_firebase_auth(route_func):
    """
    Decorator for protected API routes.

    Adds decoded Firebase user info to:
      g.firebase_user

    Usage:

      @games_bp.route("/games", methods=["GET"])
      @require_firebase_auth
      def get_games():
          ...
    """
    @wraps(route_func)
    def wrapper(*args, **kwargs):
        # Let CORS preflight requests pass cleanly.
        if request.method == "OPTIONS":
            return "", 204

        decoded_token, error_response = verify_firebase_request()

        if error_response:
            return error_response

        g.firebase_user = decoded_token
        return route_func(*args, **kwargs)

    return wrapper