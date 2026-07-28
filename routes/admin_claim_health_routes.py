from flask import Blueprint, jsonify, request

from auth.firebase_auth import require_admin_auth
from services.admin_claim_health_service import build_claim_health_response


admin_claim_health_routes = Blueprint("admin_claim_health_routes", __name__)


@admin_claim_health_routes.route("/admin/gamelens/claim-health", methods=["GET"])
@require_admin_auth
def admin_claim_health():
    """
    GET /admin/gamelens/claim-health?run_id=...&season=2025

    Aggregate admin endpoint for GameLens calibration + claim-health matrices.

    Backend-only scope:
    - tab metadata
    - formula metadata
    - aggregate calibration sections
    - preserved legacy claim-health sections

    This endpoint intentionally does not return game-level drilldown rows.
    """

    run_id = request.args.get("run_id")
    season = request.args.get("season", "2025")
    calibration_grain = request.args.get("grain", "week")

    if not run_id:
        return jsonify({
            "error": "missing_run_id",
            "message": "Missing required query parameter: run_id",
            "example": "/admin/gamelens/claim-health?run_id=full_2025_reg_post_claim_matrix_pilot&season=2025",
        }), 400

    try:
        data = build_claim_health_response(
            run_id=run_id,
            season=season,
            calibration_grain=calibration_grain,
        )
        return jsonify(data), 200

    except Exception as e:
        return jsonify({
            "error": "claim_health_query_failed",
            "message": str(e),
        }), 500