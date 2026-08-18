import unittest
from types import SimpleNamespace

from services import gamelens_level3_storage as storage


def _feature(key="claim-1", **overrides):
    row = {
        "run_id": "cohort-1",
        "claim_key": key,
        "game_id": "game-1",
        "registry_core_area": "offense",
        "registry_category": "efficiency",
        "registry_metric_label": "EPA per play",
        "registry_signal_strength": "strong",
        "registry_ranking_usage": "primary",
        "clean_hierarchy_path": "offense/efficiency/epa_per_play",
        "clean_hierarchy_status": "complete",
        "clean_hierarchy_path_flag": True,
        "missing_hierarchy_parent_flag": False,
        "offensive_efficiency_support_score": 0.8,
        "offensive_efficiency_support_bucket": "strong",
        "offensive_efficiency_support_strength": "high",
        "offensive_efficiency_support_reason": "three supporting metrics",
        "offensive_efficiency_support_metrics": ["epa_per_play"],
        "offense_finish_score": 0.7,
        "defensive_suppression_score": 0.2,
        "two_way_edge_score": 0.5,
        "two_way_context": "offensive advantage",
        "feature_formula_version": "packet4_level3_v1",
        "feature_notes": "level3",
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
        **{field: None for field in storage.LEVEL3_UPDATE_FIELDS},
    }
    row.update(overrides)
    return row


def _plan(features, existing):
    return storage.plan_level3_feature_update(
        learning_run_id="cohort-1",
        capture_id="capture-1",
        game_id="game-1",
        feature_rows=features,
        existing_claim_rows=existing,
    )


class _MemoryStorage(storage.BigQueryLevel3FeatureStorage):
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
            for field in storage.LEVEL3_UPDATE_FIELDS:
                target[field] = incoming.get(field)
        return len(rows)


class Level3StorageTests(unittest.TestCase):
    def test_zero_claims_is_update_free_no_op(self):
        boundary = _MemoryStorage([])
        result = boundary.store_features(
            learning_run_id="cohort-1",
            capture_id="capture-1",
            game_id="game-1",
            feature_rows=[],
            attempt_id="zero",
        )
        self.assertEqual(result["status"], "no_op")
        self.assertEqual(result["reason"], "zero_claims")
        self.assertEqual(result["updated"], 0)
        self.assertFalse(result["write_performed"])

    def test_first_update_and_identical_retry(self):
        boundary = _MemoryStorage([_stored()])
        first = boundary.store_features(
            learning_run_id="cohort-1",
            capture_id="capture-1",
            game_id="game-1",
            feature_rows=[_feature()],
            attempt_id="first",
        )
        retry = boundary.store_features(
            learning_run_id="cohort-1",
            capture_id="capture-1",
            game_id="game-1",
            feature_rows=[_feature()],
            attempt_id="retry",
        )
        self.assertEqual(first["updated"], 1)
        self.assertEqual(first["unchanged"], 0)
        self.assertTrue(first["write_performed"])
        self.assertEqual(retry["updated"], 0)
        self.assertEqual(retry["unchanged"], 1)
        self.assertFalse(retry["write_performed"])

    def test_conflicting_existing_features_are_quarantined(self):
        values = {
            field: _feature().get(field)
            for field in storage.LEVEL3_EVIDENCE_FIELDS
        }
        values["two_way_edge_score"] = 0.9
        boundary = _MemoryStorage([_stored(**values)])
        with self.assertRaises(storage.Level3FeatureStorageConflictError):
            boundary.store_features(
                learning_run_id="cohort-1",
                capture_id="capture-1",
                game_id="game-1",
                feature_rows=[_feature()],
                attempt_id="conflict",
            )

    def test_missing_feature_for_selected_claim_is_rejected(self):
        plan = _plan([], [_stored()])
        self.assertEqual(plan["rejected_count"], 1)
        self.assertEqual(plan["rejected"][0]["reason"], "missing_feature")

    def test_constructor_refuses_production_before_client_access(self):
        class Client:
            def get_table(self, _):
                raise AssertionError("client must not be touched")

        with self.assertRaisesRegex(ValueError, "only in dev"):
            storage.BigQueryLevel3FeatureStorage(
                client=Client(),
                runtime_config=SimpleNamespace(is_dev=False),
                bigquery_module=object(),
            )

    def test_merge_sql_is_capture_game_scoped_and_update_only(self):
        boundary = _MemoryStorage([])
        sql = boundary._merge_sql("project.GameLens_dev.stage")
        self.assertIn("target.learning_run_id = source.learning_run_id", sql)
        self.assertIn("target.claim_key = source.claim_key", sql)
        self.assertIn("target.capture_id = source.capture_id", sql)
        self.assertIn("target.game_id = source.game_id", sql)
        self.assertIn("target.feature_formula_version IS NULL", sql)
        self.assertNotIn("WHEN NOT MATCHED", sql)


if __name__ == "__main__":
    unittest.main()
