"""Capture-aware, write-free postgame grading for GameLens Packet 4.

This module deliberately does not call ``get_game_details`` or
``save_model_results``.  It revalidates one canonical frozen payload, then
passes the frozen product sections to the existing Model Outcome and Model
Trust calculation owners.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, Mapping, Optional

from services.gamelens_learning_contract import (
    payload_sha256,
    validate_pregame_payload,
)


GRADE_VERSION = "frozen_capture_outcome_v1"


class PostgameGradeError(ValueError):
    """Raised when a frozen capture cannot be graded safely."""


def _required_text(row: Mapping[str, Any], field: str) -> str:
    value = str(row.get(field) or "").strip()
    if not value:
        raise PostgameGradeError(f"{field}_missing")
    return value


def _payload_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise PostgameGradeError("response_payload_invalid_json") from exc
    if not isinstance(value, Mapping):
        raise PostgameGradeError("response_payload_not_object")
    return dict(value)


def _load_default_builders() -> tuple[Callable[..., Any], Callable[..., Any]]:
    # Keep imports lazy so this pure adapter does not initialize application or
    # Google Cloud dependencies merely by being imported by a coordinator.
    from services.game_service import build_model_outcome
    from services.model_trust_service import build_model_trust

    return build_model_outcome, build_model_trust


def grade_frozen_capture(
    *,
    snapshot: Mapping[str, Any],
    final_score: Mapping[str, Any],
    outcome_builder: Optional[Callable[..., Any]] = None,
    trust_builder: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    """Grade one canonical frozen capture without reading or writing storage.

    The returned dictionary is a dry result.  It includes deterministic source
    hashes and canonical lineage, but intentionally has no persistence side
    effects and does not claim a postgame Facts pipeline identity.
    """

    capture_id = _required_text(snapshot, "capture_id")
    learning_run_id = _required_text(snapshot, "learning_run_id")
    game_id = _required_text(snapshot, "game_id")
    environment = _required_text(snapshot, "environment").casefold()
    stored_payload_hash = _required_text(snapshot, "payload_sha256")

    if environment != "dev":
        raise PostgameGradeError("development_only")
    if str(snapshot.get("capture_status") or "").strip().casefold() != "captured":
        raise PostgameGradeError("capture_not_canonical")

    payload = _payload_object(snapshot.get("response_payload"))
    recalculated_hash = payload_sha256(payload)
    if recalculated_hash != stored_payload_hash:
        raise PostgameGradeError("payload_hash_mismatch")

    pregame_validation = validate_pregame_payload(payload)
    if not pregame_validation.get("valid"):
        raise PostgameGradeError(
            str(pregame_validation.get("reason") or "pregame_payload_invalid")
        )

    header = payload.get("header")
    if not isinstance(header, Mapping):
        raise PostgameGradeError("header_missing")
    if str(header.get("game_id") or "").strip() != game_id:
        raise PostgameGradeError("snapshot_game_identity_mismatch")

    if not isinstance(final_score, Mapping):
        raise PostgameGradeError("final_score_missing")

    if outcome_builder is None or trust_builder is None:
        default_outcome_builder, default_trust_builder = _load_default_builders()
        outcome_builder = outcome_builder or default_outcome_builder
        trust_builder = trust_builder or default_trust_builder

    matchup_lean = payload.get("matchup_lean") or {}
    game_profile = payload.get("game_profile") or []
    team_comparison = payload.get("team_comparison") or []
    if not isinstance(matchup_lean, Mapping):
        raise PostgameGradeError("matchup_lean_not_object")
    if not isinstance(game_profile, list):
        raise PostgameGradeError("game_profile_not_array")
    if not isinstance(team_comparison, list):
        raise PostgameGradeError("team_comparison_not_array")

    model_outcome = outcome_builder(
        matchup_lean=matchup_lean,
        final_score=dict(final_score),
        header=dict(header),
    )
    if not isinstance(model_outcome, Mapping) or not model_outcome:
        raise PostgameGradeError("final_score_incomplete")
    model_outcome = dict(model_outcome)

    model_trust = trust_builder(
        game_profile=game_profile,
        team_comparison=team_comparison,
        matchup_lean=matchup_lean,
        model_outcome=model_outcome,
        header=dict(header),
    )

    if not isinstance(model_trust, Mapping):
        raise PostgameGradeError("model_trust_unavailable")

    return {
        "grade_version": GRADE_VERSION,
        "pipeline_run_id": snapshot.get("metric_pipeline_run_id"),
        "learning_run_id": learning_run_id,
        "capture_id": capture_id,
        "game_id": game_id,
        "season": snapshot.get("season"),
        "season_type": snapshot.get("season_type"),
        "game_week": snapshot.get("game_week"),
        "source_payload_sha256": stored_payload_hash,
        "final_score_sha256": payload_sha256(dict(final_score)),
        "model_outcome": model_outcome,
        "model_trust": dict(model_trust),
    }
