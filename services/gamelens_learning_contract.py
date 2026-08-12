"""Pure contracts for GameLens pregame capture and postgame readiness.

Packet 1 intentionally contains no persistence and imports no application or
Google Cloud code.  Later packets can reuse these decisions without teaching
the conductor football logic or duplicating the canonical game payload builder.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional


DEFAULT_LOOKAHEAD_DAYS = 2
ELIGIBLE_GAME_STATUSES = frozenset({"scheduled"})
FINAL_GAME_STATUSES = frozenset({"final", "final/ot"})
PRESEASON_TYPES = frozenset({"preseason"})
REGULAR_SEASON_TYPES = frozenset({"regular season"})
POSTSEASON_TYPES = frozenset({"postseason"})
KNOWN_SEASON_TYPES = (
    PRESEASON_TYPES | REGULAR_SEASON_TYPES | POSTSEASON_TYPES
)
PREGAME_CAPTURE_STATUS = "captured"

# ``model_trust`` is intentionally not denied: it contains the pregame model
# read. Only outcome-bearing fields are forbidden in a canonical snapshot.
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


def _normalized_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _slug(value: Any) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", _normalized_text(value)).strip("_")
    if not text:
        raise ValueError("identifier component is required")
    return text


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("expected datetime")
    if value.tzinfo is None:
        raise ValueError("datetime must include a timezone")
    return value.astimezone(timezone.utc)


def build_learning_run_id(
    *, season: Any, season_type: Any, ruleset_version: Any
) -> str:
    """Return one stable season/phase/ruleset cohort identity."""
    return "gamelens_{season}_{phase}_{ruleset}".format(
        season=_slug(season),
        phase=_slug(season_type),
        ruleset=_slug(ruleset_version),
    )


def build_capture_id(
    *, learning_run_id: str, game_id: Any, scheduled_kickoff: datetime
) -> str:
    """Return the same capture identity for every retry of one eligible game."""
    source = "|".join(
        (
            _slug(learning_run_id),
            _slug(game_id),
            _utc(scheduled_kickoff).isoformat(),
        )
    )
    return f"capture_{hashlib.sha256(source.encode('utf-8')).hexdigest()[:24]}"


def build_claim_key(
    *, capture_id: str, source_field_path: Any, claim_type: Any, claim_rank: Any
) -> str:
    """Return a deterministic key for one extracted claim inside a capture."""
    source = "|".join(
        (
            _slug(capture_id),
            _slug(source_field_path),
            _slug(claim_type),
            _slug(claim_rank),
        )
    )
    return f"claim_{hashlib.sha256(source.encode('utf-8')).hexdigest()[:24]}"


def evaluate_capture_candidate(
    game: Mapping[str, Any],
    *,
    captured_at: datetime,
    scheduled_kickoff: datetime,
    production: bool = True,
) -> Dict[str, Any]:
    """Apply the timing, status, and exact season-phase capture boundary."""
    captured_at_utc = _utc(captured_at)
    kickoff_utc = _utc(scheduled_kickoff)
    status = _normalized_text(
        game.get("game_status") or game.get("gameStatus") or game.get("status")
    )
    season_type = _normalized_text(
        game.get("season_type") or game.get("seasonType")
    )

    if status not in ELIGIBLE_GAME_STATUSES:
        return {"eligible": False, "reason": "game_not_scheduled"}
    if captured_at_utc >= kickoff_utc:
        return {"eligible": False, "reason": "kickoff_reached"}
    if not season_type:
        return {"eligible": False, "reason": "season_type_blank"}
    if season_type not in KNOWN_SEASON_TYPES:
        return {"eligible": False, "reason": "season_type_unknown"}
    if production and season_type in PRESEASON_TYPES:
        return {"eligible": False, "reason": "preseason_shadow_only"}
    return {"eligible": True, "reason": None}


def _populated(value: Any) -> bool:
    return value is not None and value != "" and value != {} and value != []


def find_postgame_fields(payload: Mapping[str, Any]) -> List[str]:
    """Return populated outcome-bearing field paths in a proposed snapshot."""
    violations: List[str] = []
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


def validate_pregame_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Accept missing rankings but reject postgame evidence."""
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


def payload_sha256(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_capture_manifest(
    *,
    payload: Mapping[str, Any],
    game: Mapping[str, Any],
    pipeline_run_id: Any,
    learning_run_id: str,
    captured_at: datetime,
    scheduled_kickoff: datetime,
    model_version: Any,
    ruleset_version: Any,
    source_dates: Optional[Mapping[str, Any]] = None,
    production: bool = True,
) -> Dict[str, Any]:
    """Build auditable metadata only after eligibility and payload validation."""
    eligibility = evaluate_capture_candidate(
        game,
        captured_at=captured_at,
        scheduled_kickoff=scheduled_kickoff,
        production=production,
    )
    if not eligibility["eligible"]:
        raise ValueError(eligibility["reason"])

    validation = validate_pregame_payload(payload)
    if not validation["valid"]:
        raise ValueError(
            f"{validation['reason']}: {', '.join(validation['violations'])}"
        )

    game_id = game.get("game_id") or game.get("gameID")
    if not game_id:
        raise ValueError("game_id is required")

    return {
        "pipeline_run_id": str(pipeline_run_id),
        "learning_run_id": learning_run_id,
        "capture_id": build_capture_id(
            learning_run_id=learning_run_id,
            game_id=game_id,
            scheduled_kickoff=scheduled_kickoff,
        ),
        "game_id": str(game_id),
        "season": str(game.get("season") or ""),
        "season_type": str(
            game.get("season_type") or game.get("seasonType") or ""
        ),
        "game_week": str(game.get("game_week") or game.get("gameWeek") or ""),
        "game_status": str(
            game.get("game_status") or game.get("gameStatus") or ""
        ),
        "scheduled_kickoff": _utc(scheduled_kickoff).isoformat(),
        "captured_at": _utc(captured_at).isoformat(),
        "capture_status": PREGAME_CAPTURE_STATUS,
        "payload_sha256": payload_sha256(payload),
        "ranking_context_available": validation["ranking_context_available"],
        "ranking_context_reason": validation["ranking_context_reason"],
        "model_version": str(model_version),
        "ruleset_version": str(ruleset_version),
        "source_dates": dict(source_dates or {}),
        "postgame_fields_present": False,
        "outcome_write_allowed": False,
    }


def decide_canonical_capture(
    *,
    existing_manifest: Optional[Mapping[str, Any]],
    candidate_manifest: Mapping[str, Any],
) -> Dict[str, Any]:
    """Keep the first valid payload canonical and make retries deterministic."""
    if not existing_manifest:
        return {"action": "create", "reason": None}
    if existing_manifest.get("capture_id") != candidate_manifest.get("capture_id"):
        return {"action": "quarantine", "reason": "capture_identity_conflict"}
    if existing_manifest.get("payload_sha256") != candidate_manifest.get(
        "payload_sha256"
    ):
        return {"action": "quarantine", "reason": "canonical_payload_conflict"}
    return {"action": "no_op", "reason": "canonical_capture_exists"}


def summarize_capture_candidates(results: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    """Distinguish an expected empty/no-game run from failure."""
    items = list(results)
    accepted = sum(bool(item.get("eligible")) for item in items)
    return {
        "status": "success" if accepted else "no_op",
        "games_checked": len(items),
        "games_eligible": accepted,
        "games_skipped": len(items) - accepted,
    }


def evaluate_postgame_readiness(
    *, has_valid_capture: bool, has_final_score: bool, has_accepted_facts: bool
) -> Dict[str, Any]:
    """Apply the separate game-grade and Levels 2-3 readiness gates."""
    if not has_valid_capture:
        return {
            "outcome_grade": {"ready": False, "reason": "capture_missing"},
            "levels_2_3": {"ready": False, "reason": "capture_missing"},
        }
    if not has_final_score:
        return {
            "outcome_grade": {"ready": False, "reason": "final_score_missing"},
            "levels_2_3": {"ready": False, "reason": "final_score_missing"},
        }
    return {
        "outcome_grade": {"ready": True, "reason": None},
        "levels_2_3": {
            "ready": bool(has_accepted_facts),
            "reason": None if has_accepted_facts else "accepted_facts_missing",
        },
    }
