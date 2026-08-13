"""Backfill verified Packet 2 per-game audit rows without rerunning capture."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from google.cloud import bigquery

from runtime_config import load_runtime_config
from services.gamelens_snapshot_storage import (
    GAMELENS_DEV_DATASET,
    STAGE_RUNS_TABLE,
    BigQuerySnapshotStorage,
)


STAGE_NAME = "snapshot_capture"
BACKFILL_SOURCE = "packet_2_verified_evidence_2026-08-13"
LEARNING_RUN_ID = "gamelens_2026_preseason_v1"

VERIFIED_ATTEMPTS = (
    {
        "attempt_id": "snapshot_20260812T184849Z_1830177d",
        "expected_stage_status": "failure",
        "results": (
            {
                "game_id": "20260813_DET@CIN",
                "capture_id": "capture_080e1b4f3af8317bfb216ce0",
                "status": "failure",
                "reason": (
                    "snapshot_insert_failed:"
                    "response_payload_is_not_a_record"
                ),
                "rebuilt": False,
            },
        ),
    },
    {
        "attempt_id": "snapshot_20260812T191038Z_4f4725f5",
        "expected_stage_status": "success",
        "results": (
            {
                "game_id": "20260813_DET@CIN",
                "capture_id": "capture_080e1b4f3af8317bfb216ce0",
                "status": "success",
                "reason": None,
                "rebuilt": True,
            },
        ),
    },
    {
        "attempt_id": "snapshot_20260812T213819Z_83ef2f7e",
        "expected_stage_status": "no_op",
        "results": (
            {
                "game_id": "20260813_DET@CIN",
                "capture_id": "capture_080e1b4f3af8317bfb216ce0",
                "status": "no_op",
                "reason": "canonical_capture_exists",
                "rebuilt": False,
            },
        ),
    },
    {
        "attempt_id": "snapshot_20260813T140739Z_e7a26844",
        "expected_stage_status": "success",
        "results": (
            {
                "game_id": "20260813_DET@CIN",
                "capture_id": "capture_080e1b4f3af8317bfb216ce0",
                "status": "no_op",
                "reason": "canonical_capture_exists",
                "rebuilt": False,
            },
            {
                "game_id": "20260813_GB@PIT",
                "capture_id": "capture_98bca4de1e28583c67c25204",
                "status": "success",
                "reason": None,
                "rebuilt": True,
            },
            {
                "game_id": "20260813_IND@NE",
                "capture_id": "capture_c1effcb077007166b4ccd2bd",
                "status": "success",
                "reason": None,
                "rebuilt": True,
            },
            {
                "game_id": "20260813_ARI@LV",
                "capture_id": "capture_3a04ea363187904257e2afa3",
                "status": "success",
                "reason": None,
                "rebuilt": True,
            },
            {
                "game_id": "20260813_LAC@HOU",
                "capture_id": "capture_a868adbaa49a7f50aeaf3818",
                "status": "success",
                "reason": None,
                "rebuilt": True,
            },
            {
                "game_id": "20260813_TEN@SF",
                "capture_id": "capture_80967deb4c5a4eec7454051d",
                "status": "success",
                "reason": None,
                "rebuilt": True,
            },
        ),
    },
)


def verified_attempt_ids() -> list:
    return [attempt["attempt_id"] for attempt in VERIFIED_ATTEMPTS]


def load_verified_attempt_receipts(
    *,
    client: bigquery.Client,
    table_id: str,
) -> dict:
    """Load exactly the four attempt summaries that prove the backfill."""
    query = f"""
        SELECT
            attempt_id, stage_name, status, season, season_type,
            upstream_run_id
        FROM `{table_id}`
        WHERE attempt_id IN UNNEST(@attempt_ids)
    """
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "attempt_ids",
                "STRING",
                verified_attempt_ids(),
            )
        ]
    )
    rows = [dict(row) for row in client.query(
        query,
        job_config=config,
    ).result()]
    receipts = {}
    for row in rows:
        attempt_id = str(row.get("attempt_id") or "")
        if attempt_id in receipts:
            raise ValueError(
                f"duplicate stage receipt for verified attempt: {attempt_id}"
            )
        receipts[attempt_id] = row
    validate_verified_attempt_receipts(receipts)
    return receipts


def validate_verified_attempt_receipts(
    receipts: Mapping[str, Mapping[str, Any]],
) -> None:
    expected_ids = set(verified_attempt_ids())
    actual_ids = set(receipts)
    missing = sorted(expected_ids - actual_ids)
    unexpected = sorted(actual_ids - expected_ids)
    if missing or unexpected:
        raise ValueError(
            "verified attempt receipts do not match: "
            f"missing={missing}, unexpected={unexpected}"
        )
    for attempt in VERIFIED_ATTEMPTS:
        attempt_id = attempt["attempt_id"]
        receipt = receipts[attempt_id]
        if receipt.get("stage_name") != STAGE_NAME:
            raise ValueError(
                f"unexpected stage_name for {attempt_id}: "
                f"{receipt.get('stage_name')}"
            )
        if receipt.get("status") != attempt["expected_stage_status"]:
            raise ValueError(
                f"unexpected stage status for {attempt_id}: "
                f"{receipt.get('status')}"
            )


def build_verified_backfill_rows(
    *,
    receipts: Mapping[str, Mapping[str, Any]],
    recorded_at: datetime,
) -> list:
    """Translate the verified evidence into nine provenance-marked rows."""
    validate_verified_attempt_receipts(receipts)
    if recorded_at.tzinfo is None:
        raise ValueError("recorded_at must include a timezone")
    recorded_at_text = recorded_at.astimezone(timezone.utc).isoformat()
    rows = []
    for attempt in VERIFIED_ATTEMPTS:
        receipt = receipts[attempt["attempt_id"]]
        for result in attempt["results"]:
            rows.append({
                "attempt_id": attempt["attempt_id"],
                "stage_name": STAGE_NAME,
                "game_id": result["game_id"],
                "capture_id": result["capture_id"],
                "learning_run_id": LEARNING_RUN_ID,
                "season": str(receipt.get("season") or "2026"),
                "season_type": (
                    receipt.get("season_type") or "Preseason"
                ),
                "status": result["status"],
                "reason": result["reason"],
                "eligible": True,
                "rebuilt": result["rebuilt"],
                "input_count": 1,
                "output_count": (
                    1 if result["status"] == "success" else 0
                ),
                "upstream_run_id": receipt.get("upstream_run_id"),
                "recorded_at": recorded_at_text,
                "is_backfill": True,
                "backfill_source": BACKFILL_SOURCE,
            })
    return rows


def verify_canonical_references(
    *,
    storage: BigQuerySnapshotStorage,
    rows: Sequence[Mapping[str, Any]],
) -> dict:
    """Require every referenced canonical capture to match its game/cohort."""
    expected = {}
    for row in rows:
        capture_id = row["capture_id"]
        identity = (row["game_id"], row["learning_run_id"])
        previous = expected.setdefault(capture_id, identity)
        if previous != identity:
            raise ValueError(
                f"conflicting canonical identity for {capture_id}"
            )

    for capture_id, (game_id, learning_run_id) in expected.items():
        snapshot = storage.find_capture(capture_id)
        if snapshot is None:
            raise ValueError(
                f"canonical snapshot missing for backfill: {capture_id}"
            )
        if str(snapshot.get("game_id") or "") != game_id:
            raise ValueError(
                f"canonical game mismatch for backfill: {capture_id}"
            )
        if str(snapshot.get("learning_run_id") or "") != learning_run_id:
            raise ValueError(
                f"canonical learning run mismatch for backfill: {capture_id}"
            )
    return {
        "canonical_capture_count": len(expected),
        "canonical_references_verified": True,
    }


def _group_rows_by_attempt(rows: Sequence[Mapping[str, Any]]) -> list:
    grouped = []
    for attempt in VERIFIED_ATTEMPTS:
        attempt_rows = [
            row for row in rows
            if row["attempt_id"] == attempt["attempt_id"]
        ]
        grouped.append((attempt["attempt_id"], attempt_rows))
    return grouped


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run or write the nine verified Packet 2 per-game "
            "audit rows. Snapshot Capture is never invoked."
        )
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write missing rows to GameLens_dev.stage_game_results",
    )
    args = parser.parse_args(argv)

    runtime_config = load_runtime_config()
    if not runtime_config.is_dev:
        raise ValueError("Packet 2 audit backfill is allowed only in dev")

    client = bigquery.Client(project=runtime_config.project_id)
    storage = BigQuerySnapshotStorage(
        client=client,
        runtime_config=runtime_config,
    )
    stage_runs_table = (
        f"{runtime_config.project_id}.{GAMELENS_DEV_DATASET}."
        f"{STAGE_RUNS_TABLE}"
    )
    receipts = load_verified_attempt_receipts(
        client=client,
        table_id=stage_runs_table,
    )
    rows = build_verified_backfill_rows(
        receipts=receipts,
        recorded_at=datetime.now(timezone.utc),
    )
    canonical_proof = verify_canonical_references(
        storage=storage,
        rows=rows,
    )

    output = {
        "status": "READY_TO_WRITE" if not args.write else "SUCCESS",
        "write_requested": args.write,
        "stage_runs_verified": len(receipts),
        "per_game_rows_expected": len(rows),
        "logical_key_count": len({
            (
                row["attempt_id"],
                row["stage_name"],
                row["game_id"],
            )
            for row in rows
        }),
        "status_counts": dict(Counter(row["status"] for row in rows)),
        "backfill_source": BACKFILL_SOURCE,
        **canonical_proof,
        "attempts": [],
    }

    if args.write:
        for attempt_id, attempt_rows in _group_rows_by_attempt(rows):
            write_result = storage.write_stage_game_results(attempt_rows)
            output["attempts"].append({
                "attempt_id": attempt_id,
                **write_result,
            })
        output["inserted_count"] = sum(
            attempt["inserted_count"] for attempt in output["attempts"]
        )
        output["existing_count"] = sum(
            attempt["existing_count"] for attempt in output["attempts"]
        )
    else:
        output["message"] = (
            "Dry run only. Re-run with --write after reviewing this proof."
        )

    print(json.dumps(output, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

