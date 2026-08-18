import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from services import gamelens_packet4_coordinator as coordinator


def _grade(game_id, **overrides):
    result = {
        "game_id": game_id,
        "capture": {
            "learning_run_id": "cohort-1",
            "capture_id": f"capture-{game_id}",
            "pipeline_run_id": "pipeline-1",
            "source_payload_sha256": "hash-1",
        },
        "source_counts": {"canonical_snapshot_rows": 1, "final_score_rows": 2},
        "reconciliation": {
            "grades_out": 1,
            "inserted": 0,
            "unchanged": 1,
            "conflicts": 0,
        },
        "write_performed": False,
    }
    result.update(overrides)
    return result


def _level2(game_id, **overrides):
    result = {
        "game_id": game_id,
        "learning_run_id": "cohort-1",
        "capture_id": f"capture-{game_id}",
        "validation_status": "no_op",
        "validation_reason": "zero_claims",
        "source_counts": {"claims": 0},
        "by_validation_result": {},
        "reconciliation": {
            "validations_in": 0,
            "updated": 0,
            "unchanged": 0,
            "conflicts": 0,
            "rejected": 0,
        },
        "write_performed": False,
    }
    result.update(overrides)
    return result


def _level3(game_id, **overrides):
    result = {
        "game_id": game_id,
        "learning_run_id": "cohort-1",
        "capture_id": f"capture-{game_id}",
        "feature_status": "no_op",
        "feature_reason": "zero_claims",
        "source_counts": {"claims": 0, "level2_unavailable": 0},
        "reconciliation": {
            "features_in": 0,
            "updated": 0,
            "unchanged": 0,
            "conflicts": 0,
            "rejected": 0,
        },
        "write_performed": False,
    }
    result.update(overrides)
    return result


class _ReceiptStorage:
    def __init__(self, fail=False):
        self.fail = fail
        self.rows = []

    def store_receipts(self, *, rows, attempt_id):
        if self.fail:
            raise RuntimeError("receipt unavailable")
        self.attempt_id = attempt_id
        self.rows = list(rows)
        return {
            "status": "matched",
            "receipts_in": len(rows),
            "inserted": len(rows),
            "unchanged": 0,
            "conflicts": 0,
            "write_performed": True,
        }


def _run(games, storage=None, grade=None, level2=None, level3=None):
    storage = storage or _ReceiptStorage()
    result = coordinator.run_packet4_coordinator(
        client=object(),
        bigquery=object(),
        runtime_config=SimpleNamespace(is_dev=True, project_id="project"),
        game_ids=games,
        attempt_id="attempt-1",
        receipt_storage=storage,
        grade_executor=grade or (lambda game_id, **_: _grade(game_id)),
        level2_executor=level2 or (lambda game_id, **_: _level2(game_id)),
        level3_executor=level3 or (lambda game_id, **_: _level3(game_id)),
        now=lambda: datetime(2026, 8, 18, tzinfo=timezone.utc),
    )
    return result, storage


class Packet4CoordinatorTests(unittest.TestCase):
    def test_zero_claim_game_runs_all_stages_and_writes_four_receipts(self):
        result, storage = _run(["game-1"])
        self.assertEqual(result["status"], "success")
        self.assertEqual(
            [stage["status"] for stage in result["game_results"][0]["stages"]],
            ["success", "no_op", "no_op"],
        )
        self.assertEqual(result["funnel"][0]["claims"], 0)
        self.assertEqual(result["funnel"][0]["reason"], "zero_claims")
        self.assertFalse(result["learning_write_performed"])
        self.assertEqual(result["receipt_reconciliation"]["receipts_in"], 4)
        self.assertEqual(len(storage.rows), 4)

    def test_grade_failure_skips_downstream_stages(self):
        calls = []

        def fail_grade(**_):
            raise RuntimeError("score unavailable")

        def forbidden(**_):
            calls.append("called")
            raise AssertionError("downstream worker must not run")

        result, _ = _run(
            ["game-1"], grade=fail_grade, level2=forbidden, level3=forbidden
        )
        self.assertEqual(result["status"], "failure")
        self.assertEqual(
            [stage["status"] for stage in result["game_results"][0]["stages"]],
            ["failed", "skipped", "skipped"],
        )
        self.assertEqual(calls, [])

    def test_sibling_game_is_preserved_when_one_game_fails(self):
        def grade(game_id, **_):
            if game_id == "bad-game":
                raise RuntimeError("broken score")
            return _grade(game_id)

        result, _ = _run(["good-game", "bad-game"], grade=grade)
        self.assertEqual(result["status"], "partial_failure")
        self.assertEqual(result["counts"]["games_completed"], 1)
        self.assertEqual(result["counts"]["games_failed"], 1)
        self.assertEqual(result["game_results"][0]["status"], "success")
        self.assertEqual(result["game_results"][1]["status"], "failed")

    def test_level2_identity_mismatch_blocks_level3(self):
        calls = []

        def mismatch(game_id, **_):
            return _level2(game_id, capture_id="other-capture")

        def level3(**_):
            calls.append("called")
            return {}

        result, _ = _run(["game-1"], level2=mismatch, level3=level3)
        stages = result["game_results"][0]["stages"]
        self.assertEqual(stages[1]["status"], "failed")
        self.assertEqual(stages[1]["reason"], "level2_identity_mismatch")
        self.assertEqual(stages[2]["status"], "skipped")
        self.assertEqual(calls, [])

    def test_receipt_failure_makes_successful_learning_partial_failure(self):
        result, _ = _run(["game-1"], storage=_ReceiptStorage(fail=True))
        self.assertEqual(result["status"], "partial_failure")
        self.assertEqual(result["reason"], "receipt_storage_failed")
        self.assertEqual(result["game_results"][0]["status"], "success")

    def test_production_refused_before_workers(self):
        with self.assertRaisesRegex(ValueError, "only in dev"):
            coordinator.run_packet4_coordinator(
                client=object(),
                bigquery=object(),
                runtime_config=SimpleNamespace(is_dev=False),
                game_ids=["game-1"],
                attempt_id="attempt-1",
                receipt_storage=_ReceiptStorage(),
                grade_executor=lambda **_: self.fail("must not run"),
            )


if __name__ == "__main__":
    unittest.main()
