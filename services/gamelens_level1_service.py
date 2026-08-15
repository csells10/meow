"""Prepare production-safe Level 1 claims from one canonical snapshot.

This first Packet 3 slice is intentionally read-only.  It adapts the existing
historical claim extractor to the Packet 2 snapshot contract, replaces legacy
claim identities with capture-aware identities, and returns rows for review.
Storage, receipts, and slate coordination belong to later slices.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from agg.gamelens_training.build_claim_training_examples import (
    DEFAULT_HEADLINE_TOP_METRICS,
    extract_claim_rows,
    extract_payload_context,
)
from services.gamelens_learning_contract import (
    PREGAME_CAPTURE_STATUS,
    build_claim_key,
    payload_sha256,
    validate_pregame_payload,
)


SOURCE_SNAPSHOT_TABLE = "GameLens_dev.pregame_snapshots"
EXTRACTION_VERSION = "level1_snapshot_adapter_v1"

POSTGAME_NULL_FIELDS = (
    "actual_team",
    "actual_side",
    "validation_result",
    "validated_flag",
    "elevated_deserved_flag",
    "actual_gap",
    "actual_rank_gap",
    "actual_percentile_gap",
    "actual_gap_bucket",
    "actual_winner",
    "model_result",
    "is_tie",
    "final_away_total",
    "final_home_total",
    "final_margin_abs",
    "final_margin_bucket",
    "qa_read_v2",
    "headline_claim_validation_rate",
    "unique_claim_validation_rate",
)


class Level1PreparationError(ValueError):
    """The selected snapshot cannot safely become Level 1 claim rows."""


def _required_text(snapshot: Mapping[str, Any], field: str) -> str:
    value = str(snapshot.get(field) or "").strip()
    if not value:
        raise Level1PreparationError(f"snapshot {field} is required")
    return value


def _extracted_at(value: Optional[datetime]) -> str:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise Level1PreparationError("extracted_at must include a timezone")
    return current.astimezone(timezone.utc).isoformat()


def _count_by(rows: List[Dict[str, Any]], field: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        key = str(row.get(field) or "missing")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _validate_snapshot(snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    capture_id = _required_text(snapshot, "capture_id")
    learning_run_id = _required_text(snapshot, "learning_run_id")
    game_id = _required_text(snapshot, "game_id")
    expected_hash = _required_text(snapshot, "payload_sha256")

    if str(snapshot.get("environment") or "").strip().casefold() != "dev":
        raise Level1PreparationError("Packet 3 snapshot must be from dev")
    if (
        str(snapshot.get("capture_status") or "").strip().casefold()
        != PREGAME_CAPTURE_STATUS
    ):
        raise Level1PreparationError("snapshot capture_status must be captured")

    payload = snapshot.get("response_payload")
    if not isinstance(payload, Mapping):
        raise Level1PreparationError("snapshot response_payload must be a JSON object")

    header = payload.get("header")
    if not isinstance(header, Mapping):
        raise Level1PreparationError("snapshot response_payload.header is required")
    header_game_id = str(header.get("game_id") or "").strip()
    if not header_game_id:
        raise Level1PreparationError("snapshot response_payload.header.game_id is required")
    if header_game_id != game_id:
        raise Level1PreparationError("snapshot row and payload header game_id disagree")

    actual_hash = payload_sha256(payload)
    if actual_hash != expected_hash:
        raise Level1PreparationError("snapshot payload_sha256 does not match response_payload")

    validation = validate_pregame_payload(payload)
    if not validation["valid"]:
        violations = ", ".join(validation.get("violations") or [])
        raise Level1PreparationError(
            f"snapshot is not pregame-safe: {validation['reason']}: {violations}"
        )

    return {
        "capture_id": capture_id,
        "learning_run_id": learning_run_id,
        "game_id": game_id,
        "payload_sha256": expected_hash,
        "payload": dict(payload),
    }


def prepare_level1_claims(
    snapshot: Mapping[str, Any],
    *,
    headline_top_metrics: int = DEFAULT_HEADLINE_TOP_METRICS,
    extracted_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Return canonical Level 1 rows and a visual-review summary for one capture."""
    canonical = _validate_snapshot(snapshot)
    captured_at = _extracted_at(extracted_at)
    capture_id = canonical["capture_id"]
    learning_run_id = canonical["learning_run_id"]
    game_id = canonical["game_id"]
    source_hash = canonical["payload_sha256"]
    payload = canonical["payload"]

    context = extract_payload_context(
        payload=payload,
        payload_path=Path(f"{capture_id}.json"),
        payload_run=Path(SOURCE_SNAPSHOT_TABLE),
        run_id=learning_run_id,
        model_version=snapshot.get("model_version"),
    )
    context.update(
        {
            "run_id": learning_run_id,
            "learning_run_id": learning_run_id,
            "capture_id": capture_id,
            "pipeline_run_id": snapshot.get("metric_pipeline_run_id"),
            "source_payload_sha256": source_hash,
            "extraction_version": EXTRACTION_VERSION,
            "extracted_at": captured_at,
            "source_payload_run": SOURCE_SNAPSHOT_TABLE,
            "source_payload_path": f"{SOURCE_SNAPSHOT_TABLE}#capture_id={capture_id}",
            "created_at": captured_at,
            "updated_at": None,
        }
    )

    try:
        extracted_rows = extract_claim_rows(
            payload=payload,
            context=context,
            headline_top_metrics=headline_top_metrics,
        )
    except Exception as exc:
        raise Level1PreparationError(f"claim extraction failed: {exc}") from exc

    rows: List[Dict[str, Any]] = []
    seen_keys = set()
    for extracted in extracted_rows:
        row = dict(extracted)
        source_field_path = str(row.get("source_field_path") or "").strip()
        claim_type = str(row.get("claim_type") or "").strip()
        claim_rank = row.get("claim_rank")
        if not source_field_path or not claim_type or claim_rank in (None, ""):
            raise Level1PreparationError(
                "extracted claim requires source_field_path, claim_type, and claim_rank"
            )

        claim_key = build_claim_key(
            capture_id=capture_id,
            source_field_path=source_field_path,
            claim_type=claim_type,
            claim_rank=claim_rank,
        )
        if claim_key in seen_keys:
            raise Level1PreparationError("duplicate canonical claim_key extracted")
        seen_keys.add(claim_key)

        row.update(context)
        row["claim_key"] = claim_key
        for field in POSTGAME_NULL_FIELDS:
            row[field] = None
        rows.append(row)

    return {
        "status": "success",
        "write_performed": False,
        "source_table": SOURCE_SNAPSHOT_TABLE,
        "capture_id": capture_id,
        "game_id": game_id,
        "learning_run_id": learning_run_id,
        "pipeline_run_id": snapshot.get("metric_pipeline_run_id"),
        "payload_sha256": source_hash,
        "extraction_version": EXTRACTION_VERSION,
        "claim_count": len(rows),
        "unique_claim_key_count": len(seen_keys),
        "by_claim_type": _count_by(rows, "claim_type"),
        "by_claim_layer": _count_by(rows, "claim_layer"),
        "rows": rows,
    }
