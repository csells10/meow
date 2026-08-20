"""Protected, read-only HTTP doorway for Packet 5 Game Journey visibility."""

from __future__ import annotations

from datetime import date
from typing import Any

from flask import Blueprint, jsonify, request

from auth.firebase_auth import require_admin_auth
from runtime_config import load_runtime_config
from services.gamelens_admin_run_visibility_service import (
    MAX_GAME_ROWS,
    DevelopmentRunVisibilityUnavailable,
    GameWeekVisibilityNotFound,
    GameVisibilityNotFound,
    get_admin_run_visibility,
)


admin_run_visibility_routes = Blueprint(
    "admin_run_visibility_routes", __name__
)


def _required_arg(name: str) -> str:
    value = str(request.args.get(name) or "").strip()
    if not value:
        raise ValueError(f"Missing required query parameter: {name}")
    return value


def _date_arg(name: str) -> date:
    raw = _required_arg(name)
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must use YYYY-MM-DD format") from exc


def _limit_arg() -> int:
    raw = str(request.args.get("limit", MAX_GAME_ROWS)).strip()
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError("limit must be an integer") from exc


def _create_bigquery_client(project_id: str) -> Any:
    from google.cloud import bigquery

    return bigquery.Client(project=project_id)


def _error(code: str, message: str, status_code: int):
    return jsonify({"error": code, "message": message}), status_code


@admin_run_visibility_routes.route(
    "/admin/gamelens/run-visibility", methods=["GET"]
)
@require_admin_auth
def admin_run_visibility():
    """Return the accepted compact slate or one selected game's evidence."""
    try:
        runtime_config = load_runtime_config()
        if not runtime_config.is_dev:
            raise DevelopmentRunVisibilityUnavailable(
                "GameLens development run visibility is dev-only"
            )

        season = str(
            request.args.get("season", runtime_config.active_season)
        ).strip()
        season_type = _required_arg("season_type")
        learning_run_id = _required_arg("learning_run_id")
        start_date = _date_arg("start_date")
        end_date = _date_arg("end_date")
        game_week = str(request.args.get("game_week") or "").strip() or None
        game_id = str(request.args.get("game_id") or "").strip() or None
        game_limit = _limit_arg()

        data = get_admin_run_visibility(
            client=_create_bigquery_client(runtime_config.project_id),
            bigquery=_bigquery_module(),
            runtime_config=runtime_config,
            season=season,
            season_type=season_type,
            learning_run_id=learning_run_id,
            start_date=start_date,
            end_date=end_date,
            game_week=game_week,
            game_id=game_id,
            game_limit=game_limit,
        )
        return jsonify(data), 200
    except DevelopmentRunVisibilityUnavailable:
        return _error(
            "development_source_unavailable",
            "Development GameLens run visibility is unavailable in this runtime.",
            403,
        )
    except GameWeekVisibilityNotFound:
        return _error(
            "game_week_not_found",
            "The selected game week is not present in the requested slate.",
            404,
        )
    except GameVisibilityNotFound:
        return _error(
            "game_not_found",
            "The selected game is not present in the requested slate.",
            404,
        )
    except ValueError as exc:
        return _error("invalid_run_visibility_request", str(exc), 400)
    except Exception:
        return _error(
            "run_visibility_query_failed",
            "GameLens run visibility could not be loaded.",
            500,
        )


def _bigquery_module() -> Any:
    from google.cloud import bigquery

    return bigquery
