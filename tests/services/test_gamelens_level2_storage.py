import unittest
from types import SimpleNamespace

from services import gamelens_level2_storage as storage


def _validation(key="claim-1", **overrides):
    row = {
        "run_id": "cohort-1",
        "claim_key": key,
        "feature_build_stage": "validated",
        "feature_status": "validated",
        "feature_notes": "level2",
        "actual_team": "DAL",
        "actual_side": "away",
        "validation_result": "validated",
        "validated_flag": True,
        "elevated_deserved_flag": True,
        "actual_gap": 100.0,
        "actual_rank_gap": None,
        "actual_percentile_gap": None,
        "actual_gap_bucket": "dominant_edge",
        "qa_read_v2": "good_reasoning_correct_outcome",
        "headline_claim_validation_rate": 1.0,
        "unique_claim_validation_rate": 1.0,
        "updated_at": "ignored",
    }
    row.update(overrides)
    return row


def _stored(key="claim-1", **overrides):
    row = {
        "learning_run_id": "cohort-1",
        "capture_id": "capture-1",
        "game_id": "game-1",
        "run_id": "cohort-1",
        "claim_key": key,
        **{field: None for field in storage.LEVEL2_UPDATE_FIELDS},
    }
    row.update(overrides)
    return row


def _plan(validations, existing):
    return storage.plan_level2_validation_update(
        learning_run_id="cohort-1",
        capture_id="capture-1",
        game_id="game-1",
        validation_rows=validations,
        existing_claim_rows=existing,
    )


class _MemoryStorage(storage.BigQueryLevel2ValidationStorage):
    def __init__(self, rows):
        self.rows = [dict(row) for row in rows]
        self.table = "project.GameLens_dev.claim_training_examples"

    def _read_capture_claims(self, **_):
        return [dict(row) for row in self.rows]

    def _apply_pending_rows(self, *, rows, attempt_id, capture_id):
        self.last_attempt = attempt_id
        self.last_capture = capture_id
        by_key = {row["claim_key"]: row for row in self.rows}
        for incoming in rows:
            target = by_key[incoming["claim_key"]]
            for field in storage.LEVEL2_UPDATE_FIELDS:
                target[field] = incoming.get(field)
        return len(rows)


class Level2StorageTests(unittest.TestCase):
    def test_zero_claims_is_update_free_no_op(self):
        boundary = _MemoryStorage([])
        result = boundary.store_validations(
            learning_run_id="cohort-1",
            capture_id="capture-1",
            game_id="game-1",
            validation_rows=[],
            attempt_id="zero",
        )
        self.assertEqual(result["status"], "no_op")
        self.assertEqual(result["reason"], "zero_claims")
        self.assertEqual(result["updated"], 0)
        self.assertFalse(result["write_performed"])

    def test_first_update_and_identical_retry(self):
        boundary = _MemoryStorage([_stored()])
        first = boundary.store_validations(
            learning_run_id="cohort-1",
            capture_id="capture-1",
            game_id="game-1",
            validation_rows=[_validation()],
            attempt_id="first",
        )
        retry = boundary.store_validations(
            learning_run_id="cohort-1",
            capture_id="capture-1",
            game_id="game-1",
            validation_rows=[_validation()],
            attempt_id="retry",
        )
        self.assertEqual(first["updated"], 1)
        self.assertEqual(first["unchanged"], 0)
        self.assertTrue(first["write_performed"])
        self.assertEqual(retry["updated"], 0)
        self.assertEqual(retry["unchanged"], 1)
        self.assertFalse(retry["write_performed"])

    def test_conflicting_existing_validation_is_quarantined(self):
        boundary = _MemoryStorage(
            [_stored(validation_result="not_validated", actual_team="SEA")]
        )
        with self.assertRaises(storage.Level2ValidationStorageConflictError):
            boundary.store_validations(
                learning_run_id="cohort-1",
                capture_id="capture-1",
                game_id="game-1",
                validation_rows=[_validation()],
                attempt_id="conflict",
            )

    def test_missing_validation_for_selected_claim_is_rejected(self):
        plan = _plan([], [_stored()])
        self.assertEqual(plan["rejected_count"], 1)
        self.assertEqual(plan["rejected"][0]["reason"], "missing_validation")

    def test_later_level3_stage_metadata_does_not_break_level2_retry(self):
        validation = _validation()
        current = _stored(
            **{
                field: validation.get(field)
                for field in storage.LEVEL2_EVIDENCE_FIELDS
            },
            feature_build_stage="features_built",
            feature_status="complete",
            feature_notes="level3",
        )
        plan = _plan([validation], [current])
        self.assertEqual(plan["status"], "matched")
        self.assertEqual(plan["expected_updated"], 0)
        self.assertEqual(plan["expected_unchanged"], 1)
        self.assertEqual(plan["conflict_count"], 0)

    def test_constructor_refuses_production_before_client_access(self):
        class Client:
            def get_table(self, _):
                raise AssertionError("client must not be touched")

        with self.assertRaisesRegex(ValueError, "only in dev"):
            storage.BigQueryLevel2ValidationStorage(
                client=Client(),
                runtime_config=SimpleNamespace(is_dev=False),
                bigquery_module=object(),
            )

    def test_merge_sql_is_capture_game_scoped_and_update_only(self):
        boundary = _MemoryStorage([])
        sql = boundary._merge_sql("project.GameLens_dev.stage")
        self.assertIn(
            "target.learning_run_id = source.learning_run_id", sql
        )
        self.assertIn("target.claim_key = source.claim_key", sql)
        self.assertIn("target.capture_id = source.capture_id", sql)
        self.assertIn("target.game_id = source.game_id", sql)
        self.assertIn("target.validation_result IS NULL", sql)
        self.assertNotIn("WHEN NOT MATCHED", sql)


if __name__ == "__main__":
    unittest.main()
