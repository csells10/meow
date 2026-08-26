"""Pure, side-effect-free contracts for a GameLens pregame response.

This module intentionally imports no application, persistence, or Google Cloud
code.  It only validates, identifies, and hashes a response that was already
constructed by the canonical game-response builder.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Mapping


POSTGAME_TOP_LEVEL_FIELDS = frozenset({"final_score", "model_outcome"})
POSTGAME_NESTED_FIELD_NAMES = frozenset(
    {
        "actual_winner",
        "final_margin",
        "final_margin_abs",
        "model_result",
        "result_code",
    }
)
ELIGIBLE_GAME_STATUSES = frozenset({"scheduled"})
KNOWN_SEASON_TYPES = frozenset(
    {"preseason", "regular season", "postseason"}
)


def _normalized_identifier(value: Any) -> str:
    normalized = re.sub(
        r"[^a-z0-9]+",
        "_",
        str(value or "").strip().casefold(),
    ).strip("_")
    if not normalized:
        raise ValueError("identifier component is required")
    return normalized


def _utc(value: datetime, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None:
        raise ValueError(f"{field_name} must include a timezone")
    return value.astimezone(timezone.utc)


def _populated(value: Any) -> bool:
    return value is not None and value != "" and value != {} and value != []


def build_capture_id(
    *,
    learning_run_id: str,
    game_id: Any,
    scheduled_kickoff: datetime,
) -> str:
    """Return one stable identity for retries of the same pregame response."""
    source = "|".join(
        (
            _normalized_identifier(learning_run_id),
            _normalized_identifier(game_id),
            _utc(
                scheduled_kickoff,
                field_name="scheduled_kickoff",
            ).isoformat(),
        )
    )
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()
    return f"capture_{digest[:24]}"


def find_postgame_fields(payload: Mapping[str, Any]) -> list[str]:
    """Return populated outcome-bearing paths in a proposed pregame payload."""
    violations: list[str] = []

    for field in POSTGAME_TOP_LEVEL_FIELDS:
        if _populated(payload.get(field)):
            violations.append(field)

    def walk(value: Any, path: str = "") -> None:
        if isinstance(value, Mapping):
            for key, nested in value.items():
                child_path = f"{path}.{key}" if path else str(key)
                if key in POSTGAME_NESTED_FIELD_NAMES and _populated(nested):
                    violations.append(child_path)
                walk(nested, child_path)
        elif isinstance(value, list):
            for index, nested in enumerate(value):
                walk(nested, f"{path}[{index}]")

    walk(payload)
    return sorted(set(violations))


def validate_pregame_payload(payload: Mapping[str, Any]) -> dict:
    """Accept honest absence, but reject any populated postgame evidence."""
    if not isinstance(payload, Mapping):
        raise TypeError("pregame payload must be a mapping")

    violations = find_postgame_fields(payload)
    ranking_context = payload.get("ranking_context") or {}
    if violations:
        return {
            "valid": False,
            "reason": "postgame_fields_populated",
            "violations": violations,
        }

    return {
        "valid": True,
        "reason": None,
        "violations": [],
        "ranking_context_available": bool(ranking_context.get("available")),
        "ranking_context_reason": ranking_context.get("reason"),
    }


def require_pregame_payload(payload: Mapping[str, Any]) -> None:
    """Fail closed when a response contains outcome-bearing evidence."""
    result = validate_pregame_payload(payload)
    if not result["valid"]:
        raise ValueError(
            f"{result['reason']}: {', '.join(result['violations'])}"
        )


def require_eligible_pregame_payload(
    *,
    payload: Mapping[str, Any],
    game_id: Any,
    captured_at: datetime,
    scheduled_kickoff: datetime,
) -> None:
    """Require one scheduled, supported-phase payload captured pre-kickoff."""
    require_pregame_payload(payload)

    header = payload.get("header") or {}
    payload_game_id = str(header.get("game_id") or "")
    requested_game_id = str(game_id or "")
    if not payload_game_id or payload_game_id != requested_game_id:
        raise ValueError("payload_game_id_mismatch")

    game_status = str(header.get("game_status") or "").strip().casefold()
    if game_status not in ELIGIBLE_GAME_STATUSES:
        raise ValueError("game_not_scheduled")

    season_type = str(header.get("season_type") or "").strip().casefold()
    if season_type not in KNOWN_SEASON_TYPES:
        raise ValueError("season_type_unknown")

    captured_at_utc = _utc(captured_at, field_name="captured_at")
    kickoff_utc = _utc(
        scheduled_kickoff,
        field_name="scheduled_kickoff",
    )
    if captured_at_utc >= kickoff_utc:
        raise ValueError("kickoff_reached")


def canonical_payload_json(payload: Mapping[str, Any]) -> str:
    """Return the stable JSON representation used for response hashing."""
    require_pregame_payload(payload)
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def payload_sha256(payload: Mapping[str, Any]) -> str:
    """Return a SHA-256 over canonical, validated pregame JSON."""
    canonical = canonical_payload_json(payload)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def identify_pregame_payload(
    *,
    payload: Mapping[str, Any],
    learning_run_id: str,
    game_id: Any,
    captured_at: datetime,
    scheduled_kickoff: datetime,
) -> dict:
    """Identify one valid pregame payload without reading or writing data."""
    require_eligible_pregame_payload(
        payload=payload,
        game_id=game_id,
        captured_at=captured_at,
        scheduled_kickoff=scheduled_kickoff,
    )
    return {
        "capture_id": build_capture_id(
            learning_run_id=learning_run_id,
            game_id=game_id,
            scheduled_kickoff=scheduled_kickoff,
        ),
        "payload_sha256": payload_sha256(payload),
    }
