"""Development-only update boundary for Packet 4 Level 2 evidence."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
from importlib import import_module
from typing import Any, Mapping, Sequence

LEVEL2_CALCULATION_MODULE = (
    "agg.gamelens_training.update_claim_training_validation"
)
GAMELENS_DEV_DATASET = "GameLens_dev"
CLAIM_TRAINING_EXAMPLES_TABLE = "claim_training_examples"
LEVEL2_EVIDENCE_FIELDS = (
    "actual_team",
    "actual_side",
    "validation_result",
    "validated_flag",
    "elevated_deserved_flag",
    "actual_gap",
    "actual_rank_gap",
    "actual_percentile_gap",
    "actual_gap_bucket",
    "qa_read_v2",
    "headline_claim_validation_rate",
    "unique_claim_validation_rate",
)
LEVEL2_UPDATE_FIELDS = (
    "feature_build_stage",
    "feature_status",
    "feature_notes",
    *LEVEL2_EVIDENCE_FIELDS,
    "updated_at",
)
LEVEL2_IDENTITY_FIELDS = (
    "learning_run_id",
    "capture_id",
    "game_id",
    "run_id",
    "claim_key",
)


class Level2ValidationStorageConflictError(RuntimeError):
    """Stored Level 2 evidence disagrees with the bounded validation result."""


def _required(value: Any, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} is required")
    return normalized


def _comparable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def normalize_level2_validation_rows(
    *,
    learning_run_id: str,
    capture_id: str,
    game_id: str,
    validation_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    cohort = _required(learning_run_id, "learning_run_id")
    capture = _required(capture_id, "capture_id")
    game = _required(game_id, "game_id")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source in validation_rows:
        row = dict(source)
        if row.get("run_id") != cohort:
            raise ValueError("Level 2 validation run_id disagrees")
        claim_key = _required(row.get("claim_key"), "claim_key")
        if claim_key in seen:
            raise ValueError("duplicate Level 2 validation claim_key")
        seen.add(claim_key)
        validation_result = _required(
            row.get("validation_result"), "validation_result"
        )
        normalized.append(
            {
                "learning_run_id": cohort,
                "capture_id": capture,
                "game_id": game,
                "run_id": cohort,
                "claim_key": claim_key,
                **{field: row.get(field) for field in LEVEL2_UPDATE_FIELDS},
                "validation_result": validation_result,
            }
        )
    return normalized


def plan_level2_validation_update(
    *,
    learning_run_id: str,
    capture_id: str,
    game_id: str,
    validation_rows: Sequence[Mapping[str, Any]],
    existing_claim_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    incoming = normalize_level2_validation_rows(
        learning_run_id=learning_run_id,
        capture_id=capture_id,
        game_id=game_id,
        validation_rows=validation_rows,
    )
    existing_by_key: dict[str, dict[str, Any]] = {}
    for source in existing_claim_rows:
        row = dict(source)
        if row.get("learning_run_id") != learning_run_id:
            raise ValueError("stored Level 2 learning_run_id disagrees")
        if row.get("capture_id") != capture_id:
            raise ValueError("stored Level 2 capture_id disagrees")
        if row.get("game_id") != game_id:
            raise ValueError("stored Level 2 game_id disagrees")
        if row.get("run_id") not in {None, learning_run_id}:
            raise ValueError("stored Level 2 run_id alias disagrees")
        key = _required(row.get("claim_key"), "stored claim_key")
        if key in existing_by_key:
            raise ValueError("duplicate stored Level 2 claim_key")
        existing_by_key[key] = row

    incoming_by_key = {row["claim_key"]: row for row in incoming}
    rejected = []
    for key in sorted(set(existing_by_key) - set(incoming_by_key)):
        rejected.append({"claim_key": key, "reason": "missing_validation"})
    for key in sorted(set(incoming_by_key) - set(existing_by_key)):
        rejected.append({"claim_key": key, "reason": "missing_target_claim"})

    pending = []
    unchanged = []
    conflicts = []
    for key in sorted(set(existing_by_key) & set(incoming_by_key)):
        stored = existing_by_key[key]
        candidate = incoming_by_key[key]
        if stored.get("validation_result") in {None, ""}:
            pending.append(candidate)
            continue
        differing = [
            field
            for field in LEVEL2_EVIDENCE_FIELDS
            if _comparable(stored.get(field)) != _comparable(candidate.get(field))
        ]
        if differing:
            conflicts.append({"claim_key": key, "fields": differing})
        else:
            unchanged.append(key)

    return {
        "status": (
            "conflict"
            if conflicts or rejected
            else "no_op"
            if not incoming
            else "ready"
            if pending
            else "matched"
        ),
        "reason": "zero_claims" if not incoming else None,
        "validations_in": len(incoming),
        "selected_claim_rows": len(existing_by_key),
        "expected_updated": len(pending),
        "expected_unchanged": len(unchanged),
        "conflict_count": len(conflicts),
        "rejected_count": len(rejected),
        "conflicts": conflicts,
        "rejected": rejected,
        "pending_rows": pending,
    }


def level2_staging_schema(*, bigquery: Any, calculation_module: Any = None):
    calculation = calculation_module or import_module(LEVEL2_CALCULATION_MODULE)
    schema = list(calculation.validation_update_schema(bigquery))
    schema.extend(
        [
            bigquery.SchemaField("learning_run_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("capture_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("game_id", "STRING", mode="REQUIRED"),
        ]
    )
    names = [field.name for field in schema]
    if len(names) != len(set(names)):
        raise ValueError("Packet 4 Level 2 staging schema has duplicate fields")
    return schema


class BigQueryLevel2ValidationStorage:
    """Capture-scoped, update-only storage for existing development claims."""

    def __init__(
        self,
        *,
        client: Any,
        runtime_config: Any,
        bigquery_module: Any,
        calculation_module: Any = None,
    ) -> None:
        if not runtime_config.is_dev:
            raise ValueError("Packet 4 Level 2 writes are allowed only in dev")
        self.client = client
        self.runtime_config = runtime_config
        self.bigquery = bigquery_module
        self.schema = level2_staging_schema(
            bigquery=bigquery_module,
            calculation_module=calculation_module,
        )
        self.table = (
            f"{runtime_config.project_id}.{GAMELENS_DEV_DATASET}."
            f"{CLAIM_TRAINING_EXAMPLES_TABLE}"
        )
        table = client.get_table(self.table)
        available = {field.name for field in table.schema}
        required = set(LEVEL2_IDENTITY_FIELDS) | set(LEVEL2_UPDATE_FIELDS)
        missing = sorted(required - available)
        if missing:
            raise ValueError(
                "Packet 4 Level 2 target is missing fields: "
                + ",".join(missing)
            )

    def plan_validations(
        self,
        *,
        learning_run_id: str,
        capture_id: str,
        game_id: str,
        validation_rows: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        stored = self._read_capture_claims(
            learning_run_id=learning_run_id,
            capture_id=capture_id,
            game_id=game_id,
        )
        plan = plan_level2_validation_update(
            learning_run_id=learning_run_id,
            capture_id=capture_id,
            game_id=game_id,
            validation_rows=validation_rows,
            existing_claim_rows=stored,
        )
        return {
            key: value for key, value in plan.items() if key != "pending_rows"
        } | {
            "target_table": self.table,
            "merge_key": ["learning_run_id", "claim_key"],
            "write_performed": False,
        }

    def store_validations(
        self,
        *,
        learning_run_id: str,
        capture_id: str,
        game_id: str,
        validation_rows: Sequence[Mapping[str, Any]],
        attempt_id: str,
    ) -> dict[str, Any]:
        attempt = _required(attempt_id, "attempt_id")
        stored = self._read_capture_claims(
            learning_run_id=learning_run_id,
            capture_id=capture_id,
            game_id=game_id,
        )
        plan = plan_level2_validation_update(
            learning_run_id=learning_run_id,
            capture_id=capture_id,
            game_id=game_id,
            validation_rows=validation_rows,
            existing_claim_rows=stored,
        )
        if plan["conflict_count"] or plan["rejected_count"]:
            raise Level2ValidationStorageConflictError(
                "Packet 4 Level 2 update conflicts with stored claim evidence"
            )

        updated = 0
        pending = plan["pending_rows"]
        if pending:
            timestamp = datetime.now(timezone.utc).isoformat()
            rows = [{**row, "updated_at": timestamp} for row in pending]
            updated = self._apply_pending_rows(
                rows=rows,
                attempt_id=attempt,
                capture_id=capture_id,
            )
            if updated != len(rows):
                raise RuntimeError(
                    "Packet 4 Level 2 MERGE update count disagrees"
                )

        after = self._read_capture_claims(
            learning_run_id=learning_run_id,
            capture_id=capture_id,
            game_id=game_id,
        )
        replay = plan_level2_validation_update(
            learning_run_id=learning_run_id,
            capture_id=capture_id,
            game_id=game_id,
            validation_rows=validation_rows,
            existing_claim_rows=after,
        )
        if (
            replay["conflict_count"]
            or replay["rejected_count"]
            or replay["expected_updated"]
        ):
            raise RuntimeError("Packet 4 Level 2 read-back does not reconcile")
        return {
            "status": "no_op" if not validation_rows else "matched",
            "reason": "zero_claims" if not validation_rows else None,
            "validations_in": len(validation_rows),
            "selected_claim_rows": len(after),
            "updated": updated,
            "unchanged": plan["expected_unchanged"],
            "conflicts": 0,
            "rejected": 0,
            "target_table": self.table,
            "merge_key": ["learning_run_id", "claim_key"],
            "write_performed": bool(updated),
        }

    def _read_capture_claims(
        self, *, learning_run_id: str, capture_id: str, game_id: str
    ) -> list[dict[str, Any]]:
        fields = ", ".join(
            f"`{field}`" for field in (*LEVEL2_IDENTITY_FIELDS, *LEVEL2_UPDATE_FIELDS)
        )
        query = f"""
            SELECT {fields}
            FROM `{self.table}`
            WHERE learning_run_id = @learning_run_id
              AND capture_id = @capture_id
              AND game_id = @game_id
            ORDER BY claim_key
        """
        config = self.bigquery.QueryJobConfig(
            query_parameters=[
                self.bigquery.ScalarQueryParameter(
                    "learning_run_id", "STRING", learning_run_id
                ),
                self.bigquery.ScalarQueryParameter(
                    "capture_id", "STRING", capture_id
                ),
                self.bigquery.ScalarQueryParameter("game_id", "STRING", game_id),
            ]
        )
        return [
            dict(row)
            for row in self.client.query(query, job_config=config).result()
        ]

    def _apply_pending_rows(
        self, *, rows: list[dict[str, Any]], attempt_id: str, capture_id: str
    ) -> int:
        suffix = sha256(
            f"{attempt_id}|{capture_id}".encode("utf-8")
        ).hexdigest()[:20]
        staging = (
            f"{self.runtime_config.project_id}.{GAMELENS_DEV_DATASET}."
            f"_packet4_level2_stage_{suffix}"
        )
        try:
            config = self.bigquery.LoadJobConfig(
                schema=self.schema,
                write_disposition=self.bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            self.client.load_table_from_json(
                rows, staging, job_config=config
            ).result()
            job = self.client.query(self._merge_sql(staging))
            job.result()
            return int(job.num_dml_affected_rows or 0)
        finally:
            self.client.delete_table(staging, not_found_ok=True)

    def _merge_sql(self, staging_table: str) -> str:
        assignments = ",\n".join(
            f"              `{field}` = source.`{field}`"
            for field in LEVEL2_UPDATE_FIELDS
        )
        return f"""
            MERGE `{self.table}` AS target
            USING `{staging_table}` AS source
            ON target.learning_run_id = source.learning_run_id
               AND target.claim_key = source.claim_key
               AND target.capture_id = source.capture_id
               AND target.game_id = source.game_id
            WHEN MATCHED AND target.validation_result IS NULL THEN
              UPDATE SET
{assignments}
        """
