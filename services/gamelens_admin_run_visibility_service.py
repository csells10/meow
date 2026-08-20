"""Hierarchical, read-only Admin view over accepted Packet 5 evidence.

This service is intentionally development-only.  It adapts the proven Packet 5
inventory into a compact overview/week/game response and returns full evidence
only for one explicitly selected game through the protected registered route.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Callable, Iterable, Mapping

from runtime_config import RuntimeConfig


SOURCE_PROFILE = "development_learning"
MAX_DATE_RANGE_DAYS = 31
MAX_GAME_ROWS = 50
CLOCKS = (
    ("data_load", "Daily Data Load"),
    ("pregame", "GameLens Pregame"),
    ("postgame", "Postgame Learning"),
)
_GAME_ID_PATTERN = re.compile(r"20\d{6}_[A-Z0-9]+@[A-Z0-9]+")
ReportLoader = Callable[..., dict[str, Any]]


class DevelopmentRunVisibilityUnavailable(PermissionError):
    """The development-only source was requested outside a dev runtime."""


class GameVisibilityNotFound(LookupError):
    """The selected game is not present in the bounded requested slate."""


class GameWeekVisibilityNotFound(LookupError):
    """The selected descriptive week is not present in the bounded slate."""


def _validate_game_limit(game_limit: int) -> int:
    if isinstance(game_limit, bool) or not isinstance(game_limit, int):
        raise ValueError("game_limit must be an integer")
    if not 1 <= game_limit <= MAX_GAME_ROWS:
        raise ValueError(f"game_limit must be between 1 and {MAX_GAME_ROWS}")
    return game_limit


def _validate_selected_game_id(game_id: str | None) -> str | None:
    selected = str(game_id or "").strip() or None
    if selected and not _GAME_ID_PATTERN.fullmatch(selected):
        raise ValueError("game_id must use canonical YYYYMMDD_AWAY@HOME format")
    return selected


def _validate_selected_game_week(game_week: str | None) -> str | None:
    selected = str(game_week or "").strip() or None
    if selected and len(selected) > 80:
        raise ValueError("game_week cannot exceed 80 characters")
    return selected


def _all_stages(game: Mapping[str, Any]) -> Iterable[tuple[str, Mapping[str, Any]]]:
    for clock_id, _ in CLOCKS:
        for stage in game.get(clock_id, []):
            yield clock_id, stage


def _rollup_state(stages: Iterable[Mapping[str, Any]]) -> str:
    values = list(stages)
    if any(stage.get("attention") == "action_required" for stage in values):
        return "needs_attention"
    if any(stage.get("attention") == "known_gap" for stage in values):
        return "known_gap"
    statuses = {str(stage.get("status") or "") for stage in values}
    if "failed" in statuses:
        return "needs_attention"
    if "warning" in statuses:
        return "warning"
    if "waiting" in statuses:
        return "waiting"
    meaningful = statuses - {"", "not_applicable"}
    if not meaningful:
        return "not_applicable"
    if meaningful <= {"complete", "no_work_needed"}:
        return "complete"
    return "mixed"


def _compact_stage(stage: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "stage": stage.get("stage"),
        "status": stage.get("status"),
        "attention": stage.get("attention", "none"),
        "count": stage.get("count"),
    }


def _detailed_stage(stage: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_compact_stage(stage),
        "reason": stage.get("reason"),
        "source": stage.get("source"),
        "details": dict(stage.get("details") or {}),
    }


def _clock_payload(
    game: Mapping[str, Any], clock_id: str, label: str, *, detailed: bool
) -> dict[str, Any]:
    stages = list(game.get(clock_id, []))
    stage_builder = _detailed_stage if detailed else _compact_stage
    return {
        "id": clock_id,
        "label": label,
        "state": _rollup_state(stages),
        "stages": [stage_builder(stage) for stage in stages],
    }


def _game_identity(game: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "game_id": game.get("game_id"),
        "game_week": game.get("game_week"),
        "matchup": game.get("matchup"),
        "game_date": game.get("game_date"),
        "scheduled_kickoff": game.get("scheduled_kickoff"),
        "game_status": game.get("game_status"),
    }


def _compact_game(game: Mapping[str, Any]) -> dict[str, Any]:
    stage_rows = [stage for _, stage in _all_stages(game)]
    return {
        **_game_identity(game),
        "state": _rollup_state(stage_rows),
        "first_issue": game.get("first_issue"),
        "clocks": {
            clock_id: _clock_payload(game, clock_id, label, detailed=False)
            for clock_id, label in CLOCKS
        },
        "detail_available": True,
    }


def _selected_game(game: Mapping[str, Any]) -> dict[str, Any]:
    stage_rows = [stage for _, stage in _all_stages(game)]
    return {
        **_game_identity(game),
        "state": _rollup_state(stage_rows),
        "season": game.get("season"),
        "season_type": game.get("season_type"),
        "lineage": {
            "learning_run_id": game.get("learning_run_id"),
            "capture_id": game.get("capture_id"),
        },
        "first_issue": game.get("first_issue"),
        "clocks": {
            clock_id: _clock_payload(game, clock_id, label, detailed=True)
            for clock_id, label in CLOCKS
        },
        "traceability": dict(game.get("traceability") or {}),
    }


def _attention_rows(
    games: Iterable[Mapping[str, Any]], attention: str
) -> list[dict[str, Any]]:
    rows = []
    for game in games:
        for clock_id, stage in _all_stages(game):
            if stage.get("attention") != attention:
                continue
            rows.append(
                {
                    "game_id": game.get("game_id"),
                    "game_week": game.get("game_week"),
                    "matchup": game.get("matchup"),
                    "clock": clock_id,
                    "stage": stage.get("stage"),
                    "status": stage.get("status"),
                    "reason": stage.get("reason"),
                }
            )
    return rows


def _week_rollups(
    games: Iterable[Mapping[str, Any]], *, selected_game_week: str | None
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for game in games:
        label = str(game.get("game_week") or "").strip()
        grouped.setdefault(label, []).append(game)

    rows = []
    for label, week_games in grouped.items():
        stages = [
            stage
            for game in week_games
            for _, stage in _all_stages(game)
        ]
        states = [
            _rollup_state(stage for _, stage in _all_stages(game))
            for game in week_games
        ]
        game_dates = sorted(
            str(game.get("game_date"))
            for game in week_games
            if game.get("game_date")
        )
        rows.append(
            {
                "game_week": label,
                "first_game_date": game_dates[0] if game_dates else None,
                "last_game_date": game_dates[-1] if game_dates else None,
                "scheduled": len(week_games),
                "captured": sum(bool(game.get("capture_id")) for game in week_games),
                "need_attention": states.count("needs_attention"),
                "known_gaps": states.count("known_gap"),
                "state": _rollup_state(stages),
                "selected": label == selected_game_week,
            }
        )
    return rows


def _source_tables(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "table": str(table.get("table") or "").rsplit(".", 1)[-1],
            "status": table.get("status"),
            "row_count": table.get("row_count", table.get("metadata_row_count")),
            "logical_key_count": table.get("logical_key_count"),
            "duplicate_key_count": table.get("duplicate_key_count"),
            "invalid_key_count": table.get("missing_key_row_count"),
        }
        for table in report.get("tables", [])
    ]


def _recent_runs(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "source": run.get("source"),
            "attempt_id": run.get("attempt_id"),
            "stage": run.get("stage_name"),
            "status": run.get("status"),
            "reason": run.get("reason"),
            "input_count": run.get("input_count"),
            "output_count": run.get("output_count"),
            "duration_ms": run.get("duration_ms"),
            "finished_at": run.get("finished_at"),
        }
        for run in report.get("run_summary", [])
    ]


def build_admin_run_visibility_response(
    report: Mapping[str, Any], *, game_week: str | None = None,
    game_id: str | None = None,
    game_limit: int = MAX_GAME_ROWS,
) -> dict[str, Any]:
    """Adapt the accepted inventory into an overview-first drill contract."""
    limit = _validate_game_limit(game_limit)
    selected_game_week = _validate_selected_game_week(game_week)
    selected_game_id = _validate_selected_game_id(game_id)
    if report.get("access_mode") != "read_only" or report.get("write_performed"):
        raise ValueError("Packet 5 Admin service accepts read-only reports only")

    all_games = list(report.get("games", []))
    week_rollups = _week_rollups(
        all_games,
        selected_game_week=selected_game_week,
    )
    if selected_game_week and not any(
        week["game_week"] == selected_game_week for week in week_rollups
    ):
        raise GameWeekVisibilityNotFound(
            "selected game_week is not in the requested slate"
        )
    games = [
        game
        for game in all_games
        if not selected_game_week or game.get("game_week") == selected_game_week
    ]
    selected = None
    if selected_game_id:
        selected = next(
            (game for game in games if game.get("game_id") == selected_game_id),
            None,
        )
        if selected is None:
            raise GameVisibilityNotFound(
                "selected game_id is not in the requested week or slate"
            )

    inventory_summary = dict(report.get("inventory_summary") or {})
    needs_attention = _attention_rows(games, "action_required")
    known_gaps = _attention_rows(games, "known_gap")
    visible_games = games[:limit]
    game_states = [
        _rollup_state(stage for _, stage in _all_stages(game))
        for game in games
    ]
    filters = dict(report.get("filters") or {})
    filters["game_week"] = selected_game_week

    return {
        "available": True,
        "access_mode": "read_only",
        "scope": "admin_gamelens_run_visibility",
        "source_profile": SOURCE_PROFILE,
        "generated_at": report.get("audited_at"),
        "filters": filters,
        "navigation": {
            "default_level": "overview",
            "levels": ["overview", "week", "game", "clock", "stage_evidence"],
            "default_expanded": ["overview", "needs_attention"],
            "selected_game_week": selected_game_week,
            "selected_game_id": selected_game_id,
        },
        "overview": {
            "source_health": {
                **inventory_summary,
                "tables": _source_tables(report),
            },
            "weeks": week_rollups,
            "games": {
                "scheduled": len(games),
                "captured": sum(bool(game.get("capture_id")) for game in games),
                "need_attention": game_states.count("needs_attention"),
                "known_gaps": game_states.count("known_gap"),
                "returned": len(visible_games),
                "truncated": len(games) > limit,
            },
            "recent_run_count": len(report.get("run_summary", [])),
        },
        "attention": {
            "needs_attention": needs_attention,
            "known_gaps": known_gaps,
        },
        "recent_runs": _recent_runs(report),
        "games": [_compact_game(game) for game in visible_games],
        "selected_game": _selected_game(selected) if selected else None,
    }


def get_admin_run_visibility(
    *, client: Any, bigquery: Any, runtime_config: RuntimeConfig,
    season: str, season_type: str, learning_run_id: str,
    start_date: date, end_date: date, game_week: str | None = None,
    game_id: str | None = None,
    game_limit: int = MAX_GAME_ROWS, now: datetime | None = None,
    report_loader: ReportLoader | None = None,
) -> dict[str, Any]:
    """Load and adapt Game Journey evidence behind a fail-closed dev guard."""
    if not runtime_config.is_dev:
        raise DevelopmentRunVisibilityUnavailable(
            "GameLens development run visibility is dev-only"
        )
    if str(season) != str(runtime_config.active_season):
        raise ValueError("season must match the configured active dev season")
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")
    if (end_date - start_date).days + 1 > MAX_DATE_RANGE_DAYS:
        raise ValueError(
            f"date range cannot exceed {MAX_DATE_RANGE_DAYS} inclusive days"
        )
    _validate_game_limit(game_limit)
    selected_game_week = _validate_selected_game_week(game_week)
    selected_game_id = _validate_selected_game_id(game_id)
    if not str(learning_run_id or "").strip():
        raise ValueError("learning_run_id is required")

    if report_loader is None:
        from qa_gamelens_packet5_admin_inventory import build_packet5_inventory

        report_loader = build_packet5_inventory

    report = report_loader(
        client=client,
        bigquery=bigquery,
        season=str(season),
        season_type=str(season_type),
        learning_run_id=str(learning_run_id),
        start_date=start_date,
        end_date=end_date,
        now=now,
    )
    return build_admin_run_visibility_response(
        report,
        game_week=selected_game_week,
        game_id=selected_game_id,
        game_limit=game_limit,
    )
