"""One-time, fail-closed recovery for the failed LL-3 proof capture."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from google.cloud import bigquery

from runtime_config import load_runtime_config
from services.gamelens_pregame_contract import (
    payload_sha256,
    require_eligible_pregame_payload,
)
from services.gamelens_snapshot_storage import BigQuerySnapshotStorage


EXPECTED_TABLE_ID = "nfl-stream-406420.GameLens_dev.pregame_snapshots"
FAILED_CAPTURE_ID = "capture_78f546a459294669fd10da22"
FAILED_GAME_ID = "20260827_PIT@BUF"
FAILED_LEARNING_RUN_ID = (
    "gamelens_2026_preseason_learning_lite_ll3_v1"
)
FAILED_MODEL_VERSION = "game_service_a048842c"
FAILED_RULESET_VERSION = "learning_lite_ll3_v1"
FAILED_CAPTURED_AT = datetime(
    2026,
    8,
    26,
    19,
    54,
    20,
    268059,
    tzinfo=timezone.utc,
)
FAILED_STORED_PAYLOAD_SHA256 = (
    "e49483554d559791611822653f7bc2b335500c462bb5d277840e84c3d1719a20"
)
CORRECTED_READBACK_PAYLOAD_SHA256 = (
    "115187701cad1a5fffe511e1108f07234cc8c7a3db461fcb20478a0e2d944c46"
)
EXPECTED_LENS_TAG_COUNT = 96


class SnapshotRecoveryError(RuntimeError):
    pass


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _json_safe(nested) for key, nested in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(nested) for nested in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _normalized_timestamp(value: Any) -> str:
    if not isinstance(value, datetime):
        raise SnapshotRecoveryError("failed_capture_timestamp_not_datetime")
    if value.tzinfo is None:
        raise SnapshotRecoveryError("failed_capture_timestamp_missing_timezone")
    return value.astimezone(timezone.utc).isoformat()


def _verify_failed_row(storage, row: Mapping[str, Any]) -> str:
    expected_fields = {
        "capture_id": FAILED_CAPTURE_ID,
        "game_id": FAILED_GAME_ID,
        "learning_run_id": FAILED_LEARNING_RUN_ID,
        "environment": "dev",
        "capture_status": "captured",
        "payload_sha256": FAILED_STORED_PAYLOAD_SHA256,
        "model_version": FAILED_MODEL_VERSION,
        "ruleset_version": FAILED_RULESET_VERSION,
    }
    for field, expected in expected_fields.items():
        if row.get(field) != expected:
            raise SnapshotRecoveryError(
                f"failed_capture_{field}_mismatch:"
                f"expected={expected}:actual={row.get(field)}"
            )

    if _normalized_timestamp(row.get("captured_at")) != (
        FAILED_CAPTURED_AT.isoformat()
    ):
        raise SnapshotRecoveryError("failed_capture_captured_at_mismatch")
    if len(row.get("lens_tags") or []) != EXPECTED_LENS_TAG_COUNT:
        raise SnapshotRecoveryError("failed_capture_lens_tag_count_mismatch")

    payload = row.get("response_payload")
    if not isinstance(payload, Mapping):
        raise SnapshotRecoveryError("failed_capture_payload_not_json_object")
    require_eligible_pregame_payload(
        payload=payload,
        game_id=row.get("game_id"),
        captured_at=row.get("captured_at"),
        scheduled_kickoff=row.get("scheduled_kickoff"),
    )
    fresh_hash = payload_sha256(payload)
    if fresh_hash != CORRECTED_READBACK_PAYLOAD_SHA256:
        raise SnapshotRecoveryError(
            "failed_capture_corrected_hash_mismatch:"
            f"expected={CORRECTED_READBACK_PAYLOAD_SHA256}:"
            f"actual={fresh_hash}"
        )
    if storage.table_id != EXPECTED_TABLE_ID:
        raise SnapshotRecoveryError(
            f"failed_capture_table_mismatch:{storage.table_id}"
        )
    return fresh_hash


def _evidence_bytes(row: Mapping[str, Any], fresh_hash: str) -> bytes:
    evidence = {
        "artifact": "gamelens_ll3_failed_capture_evidence_v1",
        "reason": "bigquery_integral_float_json_normalization",
        "table": EXPECTED_TABLE_ID,
        "failed_stored_payload_sha256": FAILED_STORED_PAYLOAD_SHA256,
        "corrected_readback_payload_sha256": fresh_hash,
        "row": _json_safe(row),
    }
    rendered = json.dumps(
        evidence,
        indent=2,
        sort_keys=True,
        ensure_ascii=True,
        allow_nan=False,
    )
    return (rendered + "\n").encode("utf-8")


def _delete_sql(table_id: str) -> str:
    return f"""
        DELETE FROM `{table_id}`
        WHERE capture_id = @capture_id
          AND game_id = @game_id
          AND learning_run_id = @learning_run_id
          AND environment = 'dev'
          AND capture_status = 'captured'
          AND payload_sha256 = @payload_sha256
          AND model_version = @model_version
          AND ruleset_version = @ruleset_version
          AND captured_at = @captured_at
          AND ARRAY_LENGTH(lens_tags) = @lens_tag_count
          AND 1 = (
              SELECT COUNT(*)
              FROM `{table_id}`
              WHERE capture_id = @capture_id
          )
    """


def _delete_job_config():
    values = (
        ("capture_id", "STRING", FAILED_CAPTURE_ID),
        ("game_id", "STRING", FAILED_GAME_ID),
        ("learning_run_id", "STRING", FAILED_LEARNING_RUN_ID),
        ("payload_sha256", "STRING", FAILED_STORED_PAYLOAD_SHA256),
        ("model_version", "STRING", FAILED_MODEL_VERSION),
        ("ruleset_version", "STRING", FAILED_RULESET_VERSION),
        ("captured_at", "TIMESTAMP", FAILED_CAPTURED_AT),
        ("lens_tag_count", "INT64", EXPECTED_LENS_TAG_COUNT),
    )
    return bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(name, type_name, value)
            for name, type_name, value in values
        ]
    )


def recover_failed_capture(
    *,
    storage,
    client,
    evidence_file: Path,
    delete: bool = False,
    expected_evidence_sha256: Optional[str] = None,
) -> dict:
    storage.verify_table_contract()
    rows = storage.read_capture_rows(FAILED_CAPTURE_ID)
    if len(rows) != 1:
        raise SnapshotRecoveryError(
            f"failed_capture_row_count_mismatch:{len(rows)}"
        )
    row = rows[0]
    fresh_hash = _verify_failed_row(storage, row)
    content = _evidence_bytes(row, fresh_hash)
    evidence_hash = hashlib.sha256(content).hexdigest()
    evidence_path = Path(evidence_file).expanduser().resolve()

    if not delete:
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        with evidence_path.open("xb") as handle:
            handle.write(content)
        return {
            "status": "evidence_exported",
            "material_change": False,
            "table": storage.table_id,
            "capture_id": FAILED_CAPTURE_ID,
            "row_count": 1,
            "evidence_file": str(evidence_path),
            "evidence_sha256": evidence_hash,
            "stored_payload_sha256": FAILED_STORED_PAYLOAD_SHA256,
            "corrected_readback_payload_sha256": fresh_hash,
        }

    if not expected_evidence_sha256:
        raise SnapshotRecoveryError("expected_evidence_sha256_is_required")
    if expected_evidence_sha256 != evidence_hash:
        raise SnapshotRecoveryError("expected_evidence_sha256_mismatch")
    if not evidence_path.is_file():
        raise SnapshotRecoveryError("evidence_file_missing")
    saved_content = evidence_path.read_bytes()
    if saved_content != content:
        raise SnapshotRecoveryError("evidence_file_does_not_match_live_row")
    saved_hash = hashlib.sha256(saved_content).hexdigest()
    if saved_hash != expected_evidence_sha256:
        raise SnapshotRecoveryError("evidence_file_sha256_mismatch")

    job = client.query(
        _delete_sql(storage.table_id),
        job_config=_delete_job_config(),
    )
    job.result()
    affected = getattr(job, "num_dml_affected_rows", None)
    if affected != 1:
        raise SnapshotRecoveryError(
            f"failed_capture_delete_count_mismatch:{affected}"
        )
    remaining = storage.read_capture_rows(FAILED_CAPTURE_ID)
    if remaining:
        raise SnapshotRecoveryError(
            f"failed_capture_still_present:{len(remaining)}"
        )
    return {
        "status": "failed_proof_deleted",
        "material_change": True,
        "table": storage.table_id,
        "capture_id": FAILED_CAPTURE_ID,
        "affected_rows": affected,
        "row_count_after": 0,
        "evidence_file": str(evidence_path),
        "evidence_sha256": evidence_hash,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export, then optionally delete, the one approved failed LL-3 "
            "development proof capture."
        )
    )
    parser.add_argument("--evidence-file", required=True)
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete only after verifying the unchanged evidence export.",
    )
    parser.add_argument("--expected-evidence-sha256", default=None)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.delete and not args.expected_evidence_sha256:
        raise ValueError("--delete requires --expected-evidence-sha256")
    if not args.delete and args.expected_evidence_sha256:
        raise ValueError(
            "--expected-evidence-sha256 is valid only with --delete"
        )

    runtime_config = load_runtime_config()
    if not runtime_config.is_dev:
        raise ValueError("Set GAMELENS_ENVIRONMENT=dev before LL-3 recovery")
    client = bigquery.Client(project=runtime_config.project_id)
    storage = BigQuerySnapshotStorage(
        client=client,
        runtime_config=runtime_config,
    )
    result = recover_failed_capture(
        storage=storage,
        client=client,
        evidence_file=Path(args.evidence_file),
        delete=args.delete,
        expected_evidence_sha256=args.expected_evidence_sha256,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
