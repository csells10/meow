from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Union

__all__ = ["BoxscoreValidationResult", "validate_nfl_boxscore"]


_FINAL_STATUSES = frozenset({"completed", "final", "final/ot"})

_NUMERIC_STAT_FIELDS = frozenset(
    {
        "blockedFG",
        "blockedPunt",
        "blockedXP",
        "defTD",
        "defensiveOrSpecialTeamsTds",
        "defensiveInterceptions",
        "defensiveTwoPointConversionReturns",
        "firstDowns",
        "firstDownsFromPenalties",
        "fumblesLost",
        "fumblesRecovered",
        "interceptionsThrown",
        "passTD",
        "passingFirstDowns",
        "passingYards",
        "ptsAllowed",
        "rushTD",
        "rushingAttempts",
        "rushingFirstDowns",
        "rushingYards",
        "sacks",
        "safeties",
        "totalDrives",
        "totalPlays",
        "totalYards",
        "turnovers",
        "twoPointConversions",
        "yardsAllowed",
        "yardsPerPass",
        "yardsPerPlay",
        "yardsPerRush",
        "ydsAllowed",
    }
)

_COMPOSITE_STAT_FIELDS = frozenset(
    {
        "fourthDownEfficiency",
        "passCompletionsAndAttempts",
        "penalties",
        "redZoneScoredAndAttempted",
        "sacksAndYardsLost",
        "thirdDownEfficiency",
    }
)

_MEANINGFUL_STAT_FIELDS = frozenset(
    {
        "firstDowns",
        "passingYards",
        "rushingYards",
        "totalPlays",
        "totalYards",
    }
)

_SNAP_COUNT_FIELDS = frozenset(
    {
        "totalDefensive",
        "totalOffensive",
        "totalSpecialTeams",
    }
)


@dataclass(frozen=True)
class BoxscoreValidationResult:
    accepted: bool
    code: str
    reason: str
    game_id: Optional[str] = None
    team_ids: tuple[str, ...] = ()


def validate_nfl_boxscore(
    payload: Optional[Mapping[str, Any]],
    *,
    expected_game_id: Optional[str] = None,
) -> BoxscoreValidationResult:
    """Return whether a Tank01 NFL box-score payload is safe to parse.

    This function is deliberately pure. It performs no API calls, logging,
    file writes, or BigQuery work. ``expected_game_id`` lets a future caller
    supply the ID already used in the Tank01 request when the response body
    does not repeat it.
    """
    if not isinstance(payload, Mapping):
        return _reject("invalid_payload", "Box-score payload must be an object.")

    body = payload.get("body")
    if not isinstance(body, Mapping) or not body:
        return _reject(
            "missing_body",
            "Box-score payload must contain a non-empty body object.",
        )

    status_result = _validate_final_status(body)
    if status_result is not None:
        return status_result

    game_id = _first_nonblank(
        expected_game_id,
        body.get("gameID"),
        payload.get("gameID"),
    )
    if game_id is None:
        return _reject(
            "missing_game_identity",
            "Box-score payload is missing a game ID.",
        )

    game_date = _first_nonblank(body.get("gameDate"))
    if game_date is None or not _is_valid_game_date(game_date):
        return _reject(
            "invalid_game_date",
            "Box-score body must contain gameDate in YYYYMMDD format.",
            game_id=game_id,
        )

    score_result = _validate_scores(body, game_id)
    if score_result is not None:
        return score_result

    team_stats = body.get("teamStats")
    dst_stats = body.get("DST")
    if team_stats is not None and not isinstance(team_stats, Mapping):
        return _reject(
            "malformed_stats",
            "teamStats must be an object.",
            game_id=game_id,
        )
    if dst_stats is not None and not isinstance(dst_stats, Mapping):
        return _reject(
            "malformed_stats",
            "DST must be an object.",
            game_id=game_id,
        )

    team_stats = team_stats or {}
    dst_stats = dst_stats or {}
    team_ids: list[str] = []

    for side in ("home", "away"):
        side_team_stats = team_stats.get(side)
        side_dst_stats = dst_stats.get(side)

        if not _is_nonempty_mapping(side_team_stats) and not _is_nonempty_mapping(
            side_dst_stats
        ):
            return _reject(
                "missing_team",
                f"Box-score body is missing {side} team data.",
                game_id=game_id,
                team_ids=team_ids,
            )

        if side_team_stats is not None and not isinstance(side_team_stats, Mapping):
            return _reject(
                "malformed_stats",
                f"teamStats.{side} must be an object.",
                game_id=game_id,
                team_ids=team_ids,
            )
        if side_dst_stats is not None and not isinstance(side_dst_stats, Mapping):
            return _reject(
                "malformed_stats",
                f"DST.{side} must be an object.",
                game_id=game_id,
                team_ids=team_ids,
            )

        side_team_stats = side_team_stats or {}
        side_dst_stats = side_dst_stats or {}

        team_id = _first_nonblank(
            side_team_stats.get("teamID"),
            side_dst_stats.get("teamID"),
        )
        team_abv = _first_nonblank(
            side_team_stats.get("teamAbv"),
            side_dst_stats.get("teamAbv"),
        )
        if team_id is None or team_abv is None:
            return _reject(
                "missing_team_identity",
                f"Box-score {side} team is missing teamID or teamAbv.",
                game_id=game_id,
                team_ids=team_ids,
            )

        stat_result = _validate_side_stats(
            side,
            side_team_stats,
            side_dst_stats,
            game_id,
            team_ids,
        )
        if stat_result is not None:
            return stat_result

        team_ids.append(team_id)

    if len(set(team_ids)) != 2:
        return _reject(
            "duplicate_team_identity",
            "Home and away teams must have different team IDs.",
            game_id=game_id,
            team_ids=team_ids,
        )

    return BoxscoreValidationResult(
        accepted=True,
        code="accepted",
        reason="Final box-score payload contains two identified teams and parseable stats.",
        game_id=game_id,
        team_ids=tuple(team_ids),
    )


def _validate_final_status(
    body: Mapping[str, Any],
) -> Optional[BoxscoreValidationResult]:
    status_value = body.get("gameStatus")
    status_code_value = body.get("gameStatusCode")

    has_status = _first_nonblank(status_value) is not None
    has_status_code = _first_nonblank(status_code_value) is not None
    status_is_final = (
        str(status_value).strip().lower() in _FINAL_STATUSES if has_status else False
    )
    status_code_is_final = (
        str(status_code_value).strip() == "2" if has_status_code else False
    )

    if (
        has_status
        and has_status_code
        and status_is_final != status_code_is_final
    ):
        return _reject(
            "conflicting_game_status",
            "gameStatus and gameStatusCode disagree about whether the game is final.",
        )

    if not status_is_final and not status_code_is_final:
        return _reject(
            "game_not_final",
            "Box-score payload is not for a final game.",
        )

    return None


def _validate_scores(
    body: Mapping[str, Any],
    game_id: str,
) -> Optional[BoxscoreValidationResult]:
    for field in ("homePts", "awayPts"):
        if field not in body:
            return _reject(
                "missing_score",
                f"Box-score body is missing {field}.",
                game_id=game_id,
            )
        if not _is_finite_number(body[field]):
            return _reject(
                "malformed_score",
                f"Box-score field {field} must be numeric.",
                game_id=game_id,
            )

    return None


def _validate_side_stats(
    side: str,
    team_stats: Mapping[str, Any],
    dst_stats: Mapping[str, Any],
    game_id: str,
    team_ids: list[str],
) -> Optional[BoxscoreValidationResult]:
    combined_stats = {**team_stats, **dst_stats}

    if not any(field in combined_stats for field in _MEANINGFUL_STAT_FIELDS):
        return _reject(
            "missing_meaningful_stats",
            f"Box-score {side} team has no recognized game statistics.",
            game_id=game_id,
            team_ids=team_ids,
        )

    for field in sorted(_NUMERIC_STAT_FIELDS):
        if field in combined_stats and not _is_finite_number(combined_stats[field]):
            return _reject(
                "malformed_stats",
                f"Box-score field {side}.{field} must be numeric.",
                game_id=game_id,
                team_ids=team_ids,
            )

    for field in sorted(_COMPOSITE_STAT_FIELDS):
        if field in team_stats and not _is_numeric_pair(team_stats[field]):
            return _reject(
                "malformed_stats",
                f"Box-score field {side}.{field} must contain two numeric values.",
                game_id=game_id,
                team_ids=team_ids,
            )

    if "possession" in team_stats and not _is_possession_time(
        team_stats["possession"]
    ):
        return _reject(
            "malformed_stats",
            f"Box-score field {side}.possession must use MM:SS format.",
            game_id=game_id,
            team_ids=team_ids,
        )

    snap_counts = team_stats.get("snapCounts")
    if snap_counts is not None:
        if not isinstance(snap_counts, Mapping):
            return _reject(
                "malformed_stats",
                f"Box-score field {side}.snapCounts must be an object.",
                game_id=game_id,
                team_ids=team_ids,
            )
        for field in sorted(_SNAP_COUNT_FIELDS):
            if field in snap_counts and not _is_finite_number(snap_counts[field]):
                return _reject(
                    "malformed_stats",
                    f"Box-score field {side}.snapCounts.{field} must be numeric.",
                    game_id=game_id,
                    team_ids=team_ids,
                )

    return None


def _reject(
    code: str,
    reason: str,
    *,
    game_id: Optional[str] = None,
    team_ids: Union[list[str], tuple[str, ...]] = (),
) -> BoxscoreValidationResult:
    return BoxscoreValidationResult(
        accepted=False,
        code=code,
        reason=reason,
        game_id=game_id,
        team_ids=tuple(team_ids),
    )


def _first_nonblank(*values: Any) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            return normalized
    return None


def _is_nonempty_mapping(value: Any) -> bool:
    return isinstance(value, Mapping) and bool(value)


def _is_valid_game_date(value: str) -> bool:
    try:
        datetime.strptime(value[:8], "%Y%m%d")
    except (TypeError, ValueError):
        return False
    return len(value) >= 8


def _is_finite_number(value: Any) -> bool:
    if isinstance(value, bool) or value is None:
        return False
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _is_numeric_pair(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    parts = value.split("-")
    return len(parts) == 2 and all(_is_finite_number(part.strip()) for part in parts)


def _is_possession_time(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    parts = value.split(":")
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        return False
    minutes, seconds = (int(part) for part in parts)
    return minutes >= 0 and 0 <= seconds < 60
