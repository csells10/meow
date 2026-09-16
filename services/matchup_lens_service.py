"""Read-only builder for the frozen ``matchup_lens_v1`` endpoint."""

from __future__ import annotations

import json
import math
import re
from collections import Counter, defaultdict
from datetime import date
from typing import Any

from google.api_core.exceptions import DeadlineExceeded

from queries.game_queries import (
    get_game_header,
    get_matchup_lens_ranking_boundary,
    get_matchup_lens_source_aligned_windows,
    get_team_rankings_for_game,
    select_window_type,
)


GAME_ID_RE = re.compile(r"^[0-9]{8}_[A-Z0-9]{2,4}@[A-Z0-9]{2,4}$")
METHOD = {
    "selection": "Latest phase-appropriate ranking snapshot strictly before the scheduled game date.",
    "frontend_role": "Existing Matchup Lens formulas transform this evidence into lens scores and comparison language.",
    "forecast": False,
}
LEAGUE_CONTEXT = {
    "mode": "suppressed",
    "reason_code": "LEAGUE_CONTEXT_NOT_INCLUDED",
    "message": "League-wide evidence is not included; League Standing and trace rank output must be hidden.",
}
REASONS = {
    "NO_EVIDENCE": "Matchup Lens evidence is not available for this game.",
    "MISSING_TEAM_EVIDENCE": "Matchup Lens evidence is unavailable because one scheduled team has no ranking evidence.",
    "SOURCE_ALIGNMENT_UNAVAILABLE": "Matchup Lens evidence is unavailable because its exact historical window context is missing.",
    "INVALID_GAME_ID": "Game ID is missing or structurally invalid.",
    "GAME_NOT_FOUND": "No scheduled game exists for this game ID.",
    "UNSAFE_EVIDENCE_DATES": "Matchup Lens evidence failed the pregame date safety check.",
    "WINDOW_MISMATCH": "Matchup Lens evidence does not match the selected game window.",
    "SOURCE_ALIGNMENT_CONFLICT": "Matchup Lens evidence has conflicting historical window context.",
    "DUPLICATE_RANKING_ROWS": "Matchup Lens evidence contains duplicate ranking rows.",
    "INVALID_METRIC_EVIDENCE": "Matchup Lens evidence contains invalid metric data.",
    "UPSTREAM_TIMEOUT": "Matchup Lens evidence could not be loaded before the request timed out.",
    "UNEXPECTED_SERVER_ERROR": "Matchup Lens evidence could not be loaded.",
}
LENSES = (
    ("explosiveness", "Explosiveness", {"explosiveness", "offensive-efficiency", "passing-efficiency", "rushing-efficiency"}, set()),
    ("drive-control", "Drive Control", {"drive-sustainability", "third-down", "fourth-down", "drive-efficiency", "drive-conversion"}, set()),
    ("scoring-finish", "Scoring Finish", {"scoring-efficiency", "scoring", "touchdowns", "red-zone", "touchdown-efficiency", "efficiency"}, set()),
    ("defensive-resistance", "Defensive Resistance", {"defense", "scoring-suppression", "scoring-efficiency-allowed"}, {"defensive-scoring", "swing-play"}),
    ("disruption-protection", "Disruption & Protection", {"disruption", "negative-plays", "protection", "pressure-allowed"}, {"blocked-kicks", "special-teams"}),
    ("turnover-balance", "Turnover Balance", {"turnovers", "giveaways", "takeaways", "takeaway-margin"}, set()),
)
NAMED_CONSUMERS = [
    "sacks_taken", "sack_yards_lost", "sacks", "interceptions_thrown",
    "fumbles_lost", "turnovers", "defensive_interceptions", "fumbles_recovered",
    "points_per_play", "td_rate", "red_zone_efficiency", "points_allowed_per_play",
]


class LensError(Exception):
    def __init__(self, status: int, code: str, game: dict | None = None):
        self.status, self.code, self.game = status, code, game


def _date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    text = str(value)
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise LensError(409, "INVALID_METRIC_EVIDENCE") from exc


def _finite(value: Any, nullable: bool = True) -> float | int | None:
    if value is None and nullable:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise LensError(409, "INVALID_METRIC_EVIDENCE")
    return value


def _nonnegative_integer(value: Any, nullable: bool = False) -> int | None:
    if value is None and nullable:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LensError(409, "INVALID_METRIC_EVIDENCE")
    return value


def _positive_integer(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise LensError(409, "INVALID_METRIC_EVIDENCE")
    return value


def _string(value: Any, required: bool = False) -> str | None:
    if value is None:
        if required:
            raise LensError(409, "INVALID_METRIC_EVIDENCE")
        return None
    if not isinstance(value, str):
        raise LensError(409, "INVALID_METRIC_EVIDENCE")
    result = value.strip()
    if required and not result:
        raise LensError(409, "INVALID_METRIC_EVIDENCE")
    return result or None


def _bool_or_null(value: Any) -> bool | None:
    if value is None or isinstance(value, bool):
        return value
    raise LensError(409, "INVALID_METRIC_EVIDENCE")


def _tags(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        raise LensError(409, "INVALID_METRIC_EVIDENCE")
    cleaned = []
    for tag in value:
        if not isinstance(tag, str) or not tag.strip():
            raise LensError(409, "INVALID_METRIC_EVIDENCE")
        cleaned.append(tag.strip())
    return sorted(set(cleaned))


def _team_identity(team: dict) -> dict:
    team_id = _string(str(team.get("id")) if team.get("id") is not None else None, required=True)
    return {
        "team_id": team_id,
        "team_abv": _string(team.get("abbreviation"), required=True),
        "logo_url": _string(team.get("logo")),
    }


def _game_payload(header: dict) -> dict:
    season = str(header.get("season") or "")[:4]
    if not re.fullmatch(r"[0-9]{4}", season):
        raise LensError(409, "INVALID_METRIC_EVIDENCE")
    game_date = _date(header.get("game_date"))
    if game_date is None:
        raise LensError(409, "INVALID_METRIC_EVIDENCE")
    return {
        "game_id": _string(header.get("game_id"), required=True),
        "game_date": game_date,
        "game_time": _string(header.get("game_time")),
        "game_status": _string(header.get("game_status")),
        "season": season,
        "game_week": _string(header.get("game_week")),
        "season_type": _string(header.get("season_type"), required=True),
        "away_team": _team_identity(header.get("away_team") or {}),
        "home_team": _team_identity(header.get("home_team") or {}),
    }


def _envelope(available: bool, reason: str | None, game: dict | None) -> dict:
    return {
        "schema_version": "matchup_lens_v1", "available": available,
        "reason": None if reason is None else {"code": reason, "message": REASONS[reason]},
        "game": game, "display": None, "basis": None, "metric_catalog": [],
        "teams": {"away": None, "home": None}, "coverage": None,
        "league_context": LEAGUE_CONTEXT, "method": METHOD,
    }


def _unavailable(status: int, code: str, game: dict | None = None) -> tuple[dict, int]:
    return _envelope(False, code, game), status


def _metric_from_payload(payload: dict) -> dict:
    percentile = _finite(payload.get("league_percentile"))
    if percentile is not None and not 0 <= percentile <= 100:
        raise LensError(409, "INVALID_METRIC_EVIDENCE")
    signal = _string(payload.get("signal_strength"), required=True)
    if signal not in {"strong", "supporting"}:
        raise LensError(409, "INVALID_METRIC_EVIDENCE")
    league_rank, teams_ranked = _positive_integer(payload.get("league_rank")), _positive_integer(payload.get("teams_ranked"))
    if league_rank is not None and teams_ranked is not None and league_rank > teams_ranked:
        raise LensError(409, "INVALID_METRIC_EVIDENCE")
    return {
        "metric": _string(payload.get("metric"), required=True),
        "value": _finite(payload.get("value")), "label": _string(payload.get("label"), required=True),
        "definition": _string(payload.get("definition")), "category": _string(payload.get("category")),
        "core_area": _string(payload.get("core_area")), "comparison_direction": _string(payload.get("comparison_direction")),
        "higher_is_better": _bool_or_null(payload.get("higher_is_better")),
        "raw_or_derived": _string(payload.get("raw_or_derived")), "aggregation_method": _string(payload.get("aggregation_method")),
        "numerator": _finite(payload.get("numerator")), "denominator": _finite(payload.get("denominator")),
        "format": _string(payload.get("format")), "decimals": _nonnegative_integer(payload.get("decimals"), nullable=True),
        "notes": _string(payload.get("notes")), "ranking_usage": _string(payload.get("ranking_usage")),
        "signal_strength": signal, "edge_language_allowed": _bool_or_null(payload.get("edge_language_allowed")),
        "include_in_core_area_advantage": _bool_or_null(payload.get("include_in_core_area_advantage")),
        "confidence_eligible": _bool_or_null(payload.get("confidence_eligible")),
        "data_quality_status": _string(payload.get("data_quality_status")), "lens_tags": _tags(payload.get("lens_tags")),
        "league_rank": league_rank, "league_percentile": percentile, "tier": _string(payload.get("tier")),
        "tier_label": _string(payload.get("tier_label")), "teams_ranked": teams_ranked,
        "ranking_kind": _string(payload.get("ranking_kind")), "rank_direction": _string(payload.get("rank_direction")),
        "rank_interpretation": _string(payload.get("rank_interpretation")), "rank_tie_method": _string(payload.get("rank_tie_method")),
        "source_data_date": _date(payload.get("source_data_date")),
        "data_lag_days": _nonnegative_integer(payload.get("data_lag_days")),
    }


def _eligible(tags: list[str], included: set[str], excluded: set[str]) -> bool:
    return bool(set(tags) & included) and not bool(set(tags) & (excluded | {"rare-event"}))


def _readiness(catalog: list[str], definitions: dict, team_metrics: dict) -> tuple[list[dict], list[dict]]:
    rows, warnings = [], []
    for key, display, included, excluded in LENSES:
        expected = [metric for metric in catalog if _eligible(definitions[metric]["lens_tags"], included, excluded)]
        sides = {}
        for side in ("away", "home"):
            metrics = team_metrics[side]
            numeric = [metric for metric in expected if metric in metrics and metrics[metric]["league_percentile"] is not None]
            missing = [metric for metric in expected if metric not in numeric]
            status = "complete" if expected and len(numeric) == len(expected) else "partial" if numeric else "unavailable"
            sides[side] = {"status": status, "catalog_eligible_metric_count": len(expected), "eligible_numeric_metric_count": len(numeric), "missing_metrics": missing}
            if status != "complete":
                warnings.append({"code": "UNAVAILABLE_LENS_EVIDENCE" if status == "unavailable" else "PARTIAL_LENS_EVIDENCE", "message": f"{display} evidence is {status} for {side}.", "lens_key": key, "team_side": side, "metrics": missing})
        comparison = "unavailable" if "unavailable" in {sides["away"]["status"], sides["home"]["status"]} else "complete" if sides["away"]["status"] == sides["home"]["status"] == "complete" else "partial"
        if sides["away"]["status"] != sides["home"]["status"]:
            warnings.append({"code": "ASYMMETRIC_LENS_EVIDENCE", "message": f"{display} evidence differs between the two teams.", "lens_key": key, "team_side": None, "metrics": sorted(set(sides["away"]["missing_metrics"] + sides["home"]["missing_metrics"]))})
        rows.append({"lens_key": key, "display_name": display, "away": sides["away"], "home": sides["home"], "comparison_status": comparison})
    return rows, warnings


def build_matchup_lens_context(game_id: str) -> tuple[dict, int]:
    """Build a JSON-safe response and its frozen HTTP status without Flask state."""
    try:
        if not isinstance(game_id, str) or len(game_id) > 32 or not GAME_ID_RE.fullmatch(game_id):
            return _unavailable(400, "INVALID_GAME_ID")
        header = get_game_header(game_id)
        if not header:
            return _unavailable(404, "GAME_NOT_FOUND")
        game = _game_payload(header)
        window_type = select_window_type(header)
        season, game_date = game["season"], game["game_date"]
        raw_rows = get_matchup_lens_ranking_boundary(season, game_date, window_type)
        if not raw_rows:
            return _unavailable(200, "NO_EVIDENCE", game)
        away_id, home_id = game["away_team"]["team_id"], game["home_team"]["team_id"]
        selected = [row for row in raw_rows if str(row.get("team_id")) in {away_id, home_id}]
        if not selected:
            return _unavailable(200, "NO_EVIDENCE", game)
        selected_sides = {str(row.get("team_id")) for row in selected}
        if away_id not in selected_sides or home_id not in selected_sides:
            return _unavailable(200, "MISSING_TEAM_EVIDENCE", game)
        protected = Counter((str(row.get("season")), _date(row.get("as_of_date")), row.get("window_type"), row.get("metric"), str(row.get("team_id"))) for row in selected)
        if any(count > 1 for count in protected.values()):
            return _unavailable(409, "DUPLICATE_RANKING_ROWS", game)
        as_of_dates = {_date(row.get("as_of_date")) for row in raw_rows}
        windows = {row.get("window_type") for row in raw_rows}
        if len(as_of_dates) != 1 or windows != {window_type}:
            return _unavailable(409, "WINDOW_MISMATCH", game)
        as_of_date = next(iter(as_of_dates))
        if as_of_date is None or as_of_date >= game_date:
            return _unavailable(409, "UNSAFE_EVIDENCE_DATES", game)
        definitions, definition_values = {}, {}
        for row in raw_rows:
            metric = _string(row.get("metric"), required=True)
            definition = (_string(row.get("label"), required=True), _string(row.get("signal_strength"), required=True), tuple(_tags(row.get("lens_tags"))))
            if definition[1] not in {"strong", "supporting"}:
                return _unavailable(409, "INVALID_METRIC_EVIDENCE", game)
            if metric in definition_values and definition_values[metric] != definition:
                return _unavailable(409, "INVALID_METRIC_EVIDENCE", game)
            definition_values[metric] = definition
            definitions[metric] = {"label": definition[0], "signal_strength": definition[1], "lens_tags": list(definition[2])}
        catalog = sorted(definitions)
        away_helper, home_helper, meta = get_team_rankings_for_game(game_id)
        raw_keys = {away_id: {row["metric"] for row in selected if str(row.get("team_id")) == away_id}, home_id: {row["metric"] for row in selected if str(row.get("team_id")) == home_id}}
        if set(away_helper) != raw_keys[away_id] or set(home_helper) != raw_keys[home_id] or not meta.get("available"):
            return _unavailable(409, "INVALID_METRIC_EVIDENCE", game)
        team_payloads = {"away": away_helper, "home": home_helper}
        team_ids = {"away": away_id, "home": home_id}
        normalized = {side: {metric: _metric_from_payload(payload) for metric, payload in payloads.items()} for side, payloads in team_payloads.items()}
        for side in ("away", "home"):
            for metric, payload in normalized[side].items():
                if metric != payload["metric"] or payload["label"] != definitions[metric]["label"] or payload["signal_strength"] != definitions[metric]["signal_strength"] or payload["lens_tags"] != definitions[metric]["lens_tags"]:
                    return _unavailable(409, "INVALID_METRIC_EVIDENCE", game)
                if payload["source_data_date"] is None or payload["source_data_date"] > as_of_date or payload["source_data_date"] >= game_date:
                    return _unavailable(409, "UNSAFE_EVIDENCE_DATES", game)
                payload["data_lag_days"] = (
                    date.fromisoformat(as_of_date)
                    - date.fromisoformat(payload["source_data_date"])
                ).days
        requested = [(team_ids[side], metric, payload["source_data_date"]) for side in ("away", "home") for metric, payload in normalized[side].items()]
        window_rows = get_matchup_lens_source_aligned_windows(season, window_type, [row[0] for row in requested], [row[1] for row in requested], [row[2] for row in requested])
        window_map = defaultdict(list)
        for row in window_rows:
            window_map[(str(row.get("team_id")), row.get("metric"), _date(row.get("data_date")))].append(row)
        if any(len(window_map[key]) > 1 for key in window_map):
            return _unavailable(409, "SOURCE_ALIGNMENT_CONFLICT", game)
        if any(len(window_map[row]) != 1 for row in requested):
            return _unavailable(200, "SOURCE_ALIGNMENT_UNAVAILABLE", game)
        teams, source_dates, lags = {}, set(), []
        for side in ("away", "home"):
            rows = [window_map[(team_ids[side], metric, payload["source_data_date"])][0] for metric, payload in normalized[side].items()]
            counts = {_nonnegative_integer(row.get("games_in_window")) for row in rows}
            latest = {_string(row.get("latest_included_game_id")) for row in rows}
            if len(counts) != 1 or len(latest) != 1:
                return _unavailable(409, "SOURCE_ALIGNMENT_CONFLICT", game)
            games = next(iter(counts)); latest_game = next(iter(latest))
            if (games == 0 and latest_game is not None) or (games > 0 and latest_game is None):
                return _unavailable(409, "SOURCE_ALIGNMENT_CONFLICT", game)
            dates = [payload["source_data_date"] for payload in normalized[side].values()]
            side_lags = [payload["data_lag_days"] for payload in normalized[side].values()]
            teams[side] = {"team_id": team_ids[side], "team_abv": game[f"{side}_team"]["team_abv"], "games_in_window": games, "latest_included_game_id": latest_game, "latest_source_date": max(dates), "data_lag_days": max(side_lags), "metrics": {metric: normalized[side][metric] for metric in catalog if metric in normalized[side]}}
            source_dates.update(dates); lags.extend(side_lags)
        readiness, warnings = _readiness(catalog, definitions, normalized)
        named = {side: [metric for metric in NAMED_CONSUMERS if metric in normalized[side] and normalized[side][metric]["league_percentile"] is not None] for side in ("away", "home")}
        named_coverage = {"consumers": NAMED_CONSUMERS, "available_away_metrics": named["away"], "available_home_metrics": named["home"], "missing_away_metrics": [m for m in NAMED_CONSUMERS if m not in named["away"]], "missing_home_metrics": [m for m in NAMED_CONSUMERS if m not in named["home"]]}
        if named_coverage["missing_away_metrics"] or named_coverage["missing_home_metrics"]:
            warnings.append({"code": "NAMED_METRIC_GAPS", "message": "Some named Collision and Turnover Watch metrics are unavailable.", "lens_key": None, "team_side": None, "metrics": sorted(set(named_coverage["missing_away_metrics"] + named_coverage["missing_home_metrics"]))})
        warnings.append({"code": "LEAGUE_RANK_OUTPUT_SUPPRESSED", "message": "League Standing and trace rank output are hidden because league-wide evidence is not included.", "lens_key": None, "team_side": None, "metrics": []})
        if len(source_dates) > 1:
            warnings.append({"code": "MULTIPLE_SOURCE_DATES", "message": "Matchup evidence uses multiple safe source dates.", "lens_key": None, "team_side": None, "metrics": []})
        warnings.sort(key=lambda item: (item["code"], item["lens_key"] or "", item["team_side"] or "", ",".join(item["metrics"])))
        result = _envelope(True, None, game)
        result["display"] = {"window_label": {"preseason_to_date": "Preseason to date", "regular_season_to_date": "Regular season to date", "regular_plus_postseason_to_date": "Regular season + postseason to date"}[window_type], "games_label": f"{teams['away']['team_abv']}: {teams['away']['games_in_window']} game{'s' if teams['away']['games_in_window'] != 1 else ''} | {teams['home']['team_abv']}: {teams['home']['games_in_window']} game{'s' if teams['home']['games_in_window'] != 1 else ''}", "context_label": f"Pregame evidence through {as_of_date}; comparison, not forecast."}
        result["basis"] = {"window_type": window_type, "as_of_date": as_of_date, "source_data_dates": sorted(source_dates), "max_data_lag_days": max(lags), "pregame_safe": True, "comparison_not_forecast": True, "rankings_source": f"Analytics.team_metric_rankings_{season}", "window_source": f"Analytics.team_metrics_windowed_{season}"}
        result["metric_catalog"], result["teams"] = catalog, teams
        result["coverage"] = {"catalog_metric_count": len(catalog), "away_metric_count": len(normalized["away"]), "home_metric_count": len(normalized["home"]), "shared_metric_count": len(set(normalized["away"]) & set(normalized["home"])), "missing_away_metrics": [metric for metric in catalog if metric not in normalized["away"]], "missing_home_metrics": [metric for metric in catalog if metric not in normalized["home"]], "lens_readiness": readiness, "named_metric_coverage": named_coverage, "warnings": warnings}
        return result, 200
    except LensError as exc:
        return _unavailable(exc.status, exc.code, exc.game if exc.game is not None else locals().get("game"))
    except DeadlineExceeded:
        return _unavailable(504, "UPSTREAM_TIMEOUT")
    except Exception:
        return _unavailable(500, "UNEXPECTED_SERVER_ERROR")


def serialize_matchup_lens_context(game_id: str) -> tuple[str, int]:
    """Return frozen compact UTF-8 JSON bytes as text, with a trailing newline."""
    payload, status = build_matchup_lens_context(game_id)
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n", status
