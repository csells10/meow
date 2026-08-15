"""Prepare and persist Level 1 claims from one canonical snapshot."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional

from agg.gamelens_training.build_claim_training_examples import (
    DEFAULT_HEADLINE_TOP_METRICS,
    extract_claim_rows,
    extract_payload_context,
)
from services.gamelens_learning_contract import (
    LEVEL1_POSTGAME_NULL_FIELDS,
    PREGAME_CAPTURE_STATUS,
    build_claim_key,
    payload_sha256,
    validate_pregame_payload,
)


SOURCE_SNAPSHOT_TABLE = "GameLens_dev.pregame_snapshots"
EXTRACTION_VERSION = "level1_snapshot_adapter_v1"
LEVEL1_STAGE_NAME = "level1_claim_extraction"

LEVEL1_REQUIRED_ROW_FIELDS = (
    "claim_key",
    "run_id",
    "learning_run_id",
    "capture_id",
    "game_id",
    "claimed_team",
    "claim_type",
    "claim_layer",
    "source_field_path",
    "source_payload_sha256",
    "extraction_version",
    "extracted_at",
    "created_at",
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


def _validate_prepared_row(
    row: Mapping[str, Any],
    *,
    game_id: str,
    capture_id: str,
    learning_run_id: str,
    source_hash: str,
) -> None:
    missing = [
        field
        for field in LEVEL1_REQUIRED_ROW_FIELDS
        if row.get(field) is None or row.get(field) == ""
    ]
    if missing:
        raise Level1PreparationError(
            "prepared claim missing required fields: " + ",".join(missing)
        )
    if (
        row["run_id"] != learning_run_id
        or row["learning_run_id"] != learning_run_id
    ):
        raise Level1PreparationError("prepared claim cohort identity disagrees")
    if row["capture_id"] != capture_id:
        raise Level1PreparationError("prepared claim capture identity disagrees")
    if row["game_id"] != game_id:
        raise Level1PreparationError("prepared claim game identity disagrees")
    if row["source_payload_sha256"] != source_hash:
        raise Level1PreparationError("prepared claim payload hash disagrees")

    populated_postgame = [
        field
        for field in LEVEL1_POSTGAME_NULL_FIELDS
        if row.get(field) is not None
    ]
    if populated_postgame:
        raise Level1PreparationError(
            "prepared Level 1 claim contains postgame fields: "
            + ",".join(populated_postgame)
        )


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

    try:
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
        extracted_rows = extract_claim_rows(
            payload=payload,
            context=context,
            headline_top_metrics=headline_top_metrics,
        )
    except Exception as exc:
        raise Level1PreparationError(
            f"Level 1 preparation failed: {exc}"
        ) from exc

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

        row["claim_key"] = claim_key
        for field in LEVEL1_POSTGAME_NULL_FIELDS:
            row[field] = None
        _validate_prepared_row(
            row,
            game_id=game_id,
            capture_id=capture_id,
            learning_run_id=learning_run_id,
            source_hash=source_hash,
        )
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


def extract_level1_from_capture(
    capture_id: str,
    attempt_id: str,
    *,
    snapshot_storage,
    claim_storage,
    write: bool = False,
    headline_top_metrics: int = DEFAULT_HEADLINE_TOP_METRICS,
    extracted_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Run one capture through preparation, MERGE planning, and its receipt."""
    requested_capture = str(capture_id or "").strip()
    attempt = str(attempt_id or "").strip()
    if not requested_capture:
        raise Level1PreparationError("capture_id is required")
    if not attempt:
        raise Level1PreparationError("attempt_id is required")

    started_at = datetime.now(timezone.utc)
    started_clock = perf_counter()
    snapshot = snapshot_storage.read_snapshot(requested_capture)
    if snapshot is None:
        raise Level1PreparationError(
            f"canonical snapshot not found: {requested_capture}"
        )

    prepared = prepare_level1_claims(
        snapshot,
        headline_top_metrics=headline_top_metrics,
        extracted_at=extracted_at,
    )
    rows = prepared["rows"]
    storage_args = {
        "learning_run_id": prepared["learning_run_id"],
        "capture_id": prepared["capture_id"],
    }

    receipt_result = None
    if write:
        storage_result = claim_storage.merge_claims(
            rows,
            attempt_id=attempt,
            **storage_args,
        )
        claim_count = prepared["claim_count"]
        if claim_count == 0:
            stage_status = "success"
            reason = "zero_claims_extracted"
        elif storage_result["inserted"] == 0:
            stage_status = "no_op"
            reason = "claims_unchanged"
        else:
            stage_status = "success"
            reason = "claims_merged"

        recorded_at = datetime.now(timezone.utc).isoformat()
        receipt_result = snapshot_storage.write_stage_game_results(
            [
                {
                    "attempt_id": attempt,
                    "stage_name": LEVEL1_STAGE_NAME,
                    "game_id": prepared["game_id"],
                    "capture_id": prepared["capture_id"],
                    "learning_run_id": prepared["learning_run_id"],
                    "season": snapshot.get("season"),
                    "season_type": snapshot.get("season_type"),
                    "status": stage_status,
                    "reason": reason,
                    "eligible": True,
                    "rebuilt": False,
                    "input_count": 1,
                    "output_count": claim_count,
                    "upstream_run_id": prepared.get("pipeline_run_id"),
                    "recorded_at": recorded_at,
                    "is_backfill": False,
                    "backfill_source": None,
                }
            ]
        )
    else:
        storage_result = claim_storage.plan_claims(rows, **storage_args)
        if storage_result["conflict_count"]:
            raise Level1PreparationError(
                "dry-write plan found immutable Level 1 conflicts"
            )
        stage_status = "ready"
        reason = "zero_claims_extracted" if not rows else "dry_write_ready"

    finished_at = datetime.now(timezone.utc)
    receipt_saved = bool(
        receipt_result
        and receipt_result.get("inserted_count", 0)
        + receipt_result.get("existing_count", 0)
        == 1
    )
    return {
        "status": stage_status,
        "reason": reason,
        "attempt_id": attempt,
        "stage_name": LEVEL1_STAGE_NAME,
        "capture_id": prepared["capture_id"],
        "game_id": prepared["game_id"],
        "learning_run_id": prepared["learning_run_id"],
        "pipeline_run_id": prepared.get("pipeline_run_id"),
        "payload_sha256": prepared["payload_sha256"],
        "extraction_version": prepared["extraction_version"],
        "claim_count": prepared["claim_count"],
        "unique_claim_key_count": prepared["unique_claim_key_count"],
        "by_claim_type": prepared["by_claim_type"],
        "by_claim_layer": prepared["by_claim_layer"],
        "inserted_count": storage_result.get(
            "inserted", storage_result.get("expected_inserted_count", 0)
        ),
        "unchanged_count": storage_result.get(
            "unchanged", storage_result.get("expected_unchanged_count", 0)
        ),
        "conflict_count": storage_result["conflict_count"]
        if "conflict_count" in storage_result
        else storage_result.get("conflicts", 0),
        "claims_out": storage_result.get(
            "claims_out", prepared["claim_count"]
        ),
        "source_table": prepared["source_table"],
        "target_table": storage_result["target_table"],
        "merge_key": storage_result["merge_key"],
        "write_requested": write,
        "write_performed": storage_result.get("write_performed", False),
        "receipt_saved": receipt_saved,
        "receipt_result": receipt_result,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_ms": int((perf_counter() - started_clock) * 1000),
        "rows": rows,
    }
