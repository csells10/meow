import unittest
from collections import Counter
from datetime import datetime, timezone

from tests._gcp_stubs import install_bigquery_stub

install_bigquery_stub()

from backfill_gamelens_stage_game_results import (
    BACKFILL_SOURCE,
    VERIFIED_ATTEMPTS,
    build_verified_backfill_rows,
    validate_verified_attempt_receipts,
    verify_canonical_references,
)


RECORDED_AT = datetime(2026, 8, 13, 17, 0, tzinfo=timezone.utc)


def receipts():
    return {
        attempt["attempt_id"]: {
            "attempt_id": attempt["attempt_id"],
            "stage_name": "snapshot_capture",
            "status": attempt["expected_stage_status"],
            "season": "2026",
            "season_type": (
                None
                if attempt["attempt_id"].startswith(
                    "snapshot_20260812T18"
                )
                else "Preseason"
            ),
            "upstream_run_id": "observed_prod_2026_asof_20260806",
        }
        for attempt in VERIFIED_ATTEMPTS
    }


class FakeSnapshotStorage:
    def __init__(self, rows):
        self.rows = rows

    def find_capture(self, capture_id):
        return self.rows.get(capture_id)


class TestStageGameResultBackfill(unittest.TestCase):
    def test_verified_evidence_builds_nine_unique_rows(self):
        rows = build_verified_backfill_rows(
            receipts=receipts(),
            recorded_at=RECORDED_AT,
        )

        self.assertEqual(len(rows), 9)
        self.assertEqual(
            len({
                (
                    row["attempt_id"],
                    row["stage_name"],
                    row["game_id"],
                )
                for row in rows
            }),
            9,
        )
        self.assertEqual(
            Counter(row["status"] for row in rows),
            Counter({"success": 6, "no_op": 2, "failure": 1}),
        )
        self.assertEqual(
            sum(row["output_count"] for row in rows),
            6,
        )
        self.assertTrue(all(row["is_backfill"] for row in rows))
        self.assertTrue(all(
            row["backfill_source"] == BACKFILL_SOURCE
            for row in rows
        ))
        self.assertTrue(all(
            row["season_type"] == "Preseason"
            for row in rows
        ))

    def test_receipt_validation_requires_exact_known_attempts(self):
        incomplete = receipts()
        incomplete.pop(next(iter(incomplete)))

        with self.assertRaisesRegex(
            ValueError,
            "verified attempt receipts do not match",
        ):
            validate_verified_attempt_receipts(incomplete)

    def test_receipt_validation_rejects_changed_stage_status(self):
        changed = receipts()
        attempt_id = VERIFIED_ATTEMPTS[0]["attempt_id"]
        changed[attempt_id]["status"] = "success"

        with self.assertRaisesRegex(
            ValueError,
            "unexpected stage status",
        ):
            validate_verified_attempt_receipts(changed)

    def test_canonical_reference_check_verifies_six_captures(self):
        rows = build_verified_backfill_rows(
            receipts=receipts(),
            recorded_at=RECORDED_AT,
        )
        snapshots = {}
        for row in rows:
            snapshots[row["capture_id"]] = {
                "capture_id": row["capture_id"],
                "game_id": row["game_id"],
                "learning_run_id": row["learning_run_id"],
            }

        result = verify_canonical_references(
            storage=FakeSnapshotStorage(snapshots),
            rows=rows,
        )

        self.assertEqual(result["canonical_capture_count"], 6)
        self.assertTrue(result["canonical_references_verified"])

    def test_canonical_reference_check_fails_closed_on_missing_row(self):
        rows = build_verified_backfill_rows(
            receipts=receipts(),
            recorded_at=RECORDED_AT,
        )

        with self.assertRaisesRegex(
            ValueError,
            "canonical snapshot missing",
        ):
            verify_canonical_references(
                storage=FakeSnapshotStorage({}),
                rows=rows,
            )


if __name__ == "__main__":
    unittest.main()

