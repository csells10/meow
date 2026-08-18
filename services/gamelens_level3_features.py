"""Bounded, leakage-safe Packet 4 adapter for existing Level 3 features."""

from __future__ import annotations

from importlib import import_module
from typing import Any, Mapping, Sequence


LEVEL3_CALCULATION_MODULE = (
    "agg.gamelens_training.update_claim_training_features"
)

# These are the only fields the existing Level 3 calculation may receive.
# feature_status/notes are stage audit pass-through fields; no numeric or label
# target from Level 2 or Model Outcome is admitted.
LEVEL3_CALCULATION_INPUT_FIELDS = (
    "run_id",
    "claim_key",
    "game_id",
    "season",
    "claimed_team",
    "claimed_side",
    "opponent_team",
    "opponent_side",
    "claim_type",
    "claim_layer",
    "claim_name",
    "core_area",
    "category",
    "metric",
    "pregame_raw_gap",
    "pregame_percentile_gap",
    "pregame_abs_percentile_gap",
    "claimed_team_value",
    "opponent_team_value",
    "core_area_agreement_rate",
    "feature_status",
    "feature_notes",
    "feature_formula_version",
)

POSTGAME_TARGET_FIELDS = frozenset(
    {
        "actual_team",
        "actual_side",
        "validation_result",
        "validated_flag",
        "elevated_deserved_flag",
        "actual_gap",
        "actual_rank_gap",
        "actual_percentile_gap",
        "actual_gap_bucket",
        "predicted_team",
        "actual_winner",
        "model_result",
        "model_outcome",
        "model_trust",
        "is_tie",
        "final_score",
        "final_away_total",
        "final_home_total",
        "final_margin_abs",
        "final_margin_bucket",
        "qa_read_v2",
        "headline_claim_validation_rate",
        "unique_claim_validation_rate",
    }
)


def _required(value: Any, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} is required")
    return normalized


def project_level3_calculation_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return the explicit Level 3 allowlist and nothing else."""
    projected = {
        field: row.get(field) for field in LEVEL3_CALCULATION_INPUT_FIELDS
    }
    leaked = POSTGAME_TARGET_FIELDS.intersection(projected)
    if leaked:
        raise ValueError(
            "Packet 4 Level 3 projection admitted postgame targets: "
            + ",".join(sorted(leaked))
        )
    return projected


def _assert_claim_identity(
    rows: Sequence[Mapping[str, Any]],
    *,
    learning_run_id: str,
    capture_id: str,
    game_id: str,
) -> None:
    seen: set[str] = set()
    for row in rows:
        if row.get("learning_run_id") != learning_run_id:
            raise ValueError("Level 3 claim learning_run_id disagrees")
        if row.get("run_id") != learning_run_id:
            raise ValueError("Level 3 claim run_id alias disagrees")
        if row.get("capture_id") != capture_id:
            raise ValueError("Level 3 claim capture_id disagrees")
        if row.get("game_id") != game_id:
            raise ValueError("Level 3 claim game_id disagrees")
        key = _required(row.get("claim_key"), "claim_key")
        if key in seen:
            raise ValueError("duplicate bounded Level 3 claim_key")
        seen.add(key)


def run_bounded_level3_features(
    *,
    learning_run_id: str,
    capture_id: str,
    game_id: str,
    claim_rows: Sequence[Mapping[str, Any]],
    level2_status: str,
    formula_version: str | None = None,
    calculation_module: Any = None,
) -> dict[str, Any]:
    """Build Level 3 rows for one capture without performing a write."""
    cohort = _required(learning_run_id, "learning_run_id")
    capture = _required(capture_id, "capture_id")
    game = _required(game_id, "game_id")
    claims = [dict(row) for row in claim_rows]
    _assert_claim_identity(
        claims,
        learning_run_id=cohort,
        capture_id=capture,
        game_id=game,
    )

    base = {
        "learning_run_id": cohort,
        "capture_id": capture,
        "game_id": game,
        "source_counts": {
            "claims": len(claims),
            "level2_unavailable": sum(
                row.get("validation_result") == "unavailable"
                for row in claims
            ),
        },
        "write_performed": False,
    }
    if level2_status not in {"completed", "no_op"}:
        return {
            **base,
            "status": "blocked",
            "reason": (
                "level2_failed"
                if level2_status == "failed"
                else "level2_not_complete"
            ),
            "feature_rows": [],
            "reconciliation": {
                "claims_in": len(claims),
                "features_out": 0,
                "rejected": 0,
                "postgame_fields_admitted": 0,
                "write_performed": False,
            },
        }
    if not claims:
        return {
            **base,
            "status": "no_op",
            "reason": "zero_claims",
            "formula_version": formula_version,
            "feature_rows": [],
            "summary": {},
            "reconciliation": {
                "claims_in": 0,
                "features_out": 0,
                "rejected": 0,
                "postgame_fields_admitted": 0,
                "write_performed": False,
            },
        }
    if level2_status != "completed":
        raise ValueError("populated Level 3 claims require completed Level 2")
    unvalidated = [
        row["claim_key"]
        for row in claims
        if row.get("validation_result") in {None, ""}
    ]
    if unvalidated:
        raise ValueError("Level 3 claims are missing Level 2 validation state")

    calculation = calculation_module or import_module(LEVEL3_CALCULATION_MODULE)
    effective_formula = formula_version or calculation.DEFAULT_FORMULA_VERSION
    projected = [project_level3_calculation_row(row) for row in claims]
    for row in projected:
        admitted = POSTGAME_TARGET_FIELDS.intersection(row)
        if admitted:
            raise ValueError("postgame target entered Level 3 calculation")
    feature_rows = calculation.build_feature_updates(
        training_rows=projected,
        formula_version=effective_formula,
    )
    if len(feature_rows) != len(claims):
        raise ValueError("Level 3 claims-in and features-out do not reconcile")
    expected_keys = {row["claim_key"] for row in claims}
    output_keys = {_required(row.get("claim_key"), "feature claim_key") for row in feature_rows}
    if output_keys != expected_keys or len(output_keys) != len(feature_rows):
        raise ValueError("Level 3 feature claim keys do not reconcile")
    if any(row.get("run_id") != cohort for row in feature_rows):
        raise ValueError("Level 3 feature run_id disagrees")
    if any(row.get("game_id") != game for row in feature_rows):
        raise ValueError("Level 3 feature game_id disagrees")

    return {
        **base,
        "status": "completed",
        "reason": None,
        "formula_version": effective_formula,
        "feature_rows": feature_rows,
        "summary": calculation.build_summary(feature_rows),
        "reconciliation": {
            "claims_in": len(claims),
            "features_out": len(feature_rows),
            "rejected": 0,
            "postgame_fields_admitted": 0,
            "write_performed": False,
        },
    }
