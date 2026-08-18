"""Bounded Packet 4 adapter for the existing Level 2 validation semantics."""

from __future__ import annotations

from importlib import import_module
from typing import Any, Mapping, Sequence


LEVEL2_CALCULATION_MODULE = (
    "agg.gamelens_training.update_claim_training_validation"
)


def _required_identity(value: str, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} is required")
    return normalized


def _assert_bounded_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    learning_run_id: str,
    capture_id: str,
    game_id: str,
) -> None:
    seen_claim_keys: set[str] = set()
    for row in rows:
        if row.get("learning_run_id") != learning_run_id:
            raise ValueError("Level 2 claim learning_run_id disagrees")
        if row.get("run_id") not in {None, learning_run_id}:
            raise ValueError("Level 2 claim run_id alias disagrees")
        if row.get("capture_id") != capture_id:
            raise ValueError("Level 2 claim capture_id disagrees")
        if row.get("game_id") != game_id:
            raise ValueError("Level 2 claim game_id disagrees")

        claim_key = str(row.get("claim_key") or "").strip()
        if not claim_key:
            raise ValueError("Level 2 claim_key is required")
        if claim_key in seen_claim_keys:
            raise ValueError("duplicate bounded Level 2 claim_key")
        seen_claim_keys.add(claim_key)


def _assert_game_facts(
    rows: Sequence[Mapping[str, Any]], *, game_id: str
) -> None:
    for row in rows:
        if row.get("game_id") != game_id:
            raise ValueError("Level 2 actual fact game_id disagrees")


def _count_by(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get(field) if row.get(field) is not None else "NULL")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def run_bounded_level2_validation(
    *,
    learning_run_id: str,
    capture_id: str,
    game_id: str,
    claim_rows: Sequence[Mapping[str, Any]],
    actual_fact_rows: Sequence[Mapping[str, Any]],
    facts_accepted: bool,
    accepted_fact_row_count: int | None = None,
    abs_tol: float = 0.0001,
    pct_tol: float = 0.02,
    reasoning_valid_threshold: float = 0.60,
    reasoning_weak_threshold: float = 0.40,
    calculation_module: Any = None,
) -> dict[str, Any]:
    """Validate exactly one canonical capture without performing a write.

    The adapter owns identity, readiness, and reconciliation only. The existing
    Level 2 module remains the single owner of metric comparison and scoring.
    """
    cohort = _required_identity(learning_run_id, "learning_run_id")
    capture = _required_identity(capture_id, "capture_id")
    game = _required_identity(game_id, "game_id")
    claims = [dict(row) for row in claim_rows]
    actuals = [dict(row) for row in actual_fact_rows]
    accepted_count = (
        len(actuals)
        if accepted_fact_row_count is None
        else int(accepted_fact_row_count)
    )
    if accepted_count < len(actuals):
        raise ValueError(
            "accepted fact row count cannot be smaller than eligible facts"
        )
    if facts_accepted != (accepted_count > 0):
        raise ValueError("facts_accepted disagrees with accepted fact row count")

    _assert_bounded_rows(
        claims,
        learning_run_id=cohort,
        capture_id=capture,
        game_id=game,
    )
    _assert_game_facts(actuals, game_id=game)

    base = {
        "learning_run_id": cohort,
        "capture_id": capture,
        "game_id": game,
        "source_counts": {
            "claims": len(claims),
            "accepted_fact_rows_total": accepted_count,
            "eligible_actual_fact_rows": len(actuals),
        },
        "write_performed": False,
    }

    if not facts_accepted:
        return {
            **base,
            "status": "deferred",
            "reason": "waiting_accepted_facts",
            "validation_rows": [],
            "reconciliation": {
                "claims_in": len(claims),
                "validations_out": 0,
                "unavailable": 0,
                "rejected": 0,
                "conflicts": 0,
                "write_performed": False,
            },
        }

    if not claims:
        return {
            **base,
            "status": "no_op",
            "reason": "zero_claims",
            "validation_rows": [],
            "by_validation_result": {},
            "by_feature_status": {},
            "reconciliation": {
                "claims_in": 0,
                "validations_out": 0,
                "unavailable": 0,
                "rejected": 0,
                "conflicts": 0,
                "write_performed": False,
            },
        }

    calculation = calculation_module or import_module(LEVEL2_CALCULATION_MODULE)
    actual_index = calculation.build_actual_index(actuals)
    validation_rows = [
        calculation.validate_training_row(
            row=row,
            actual_index=actual_index,
            abs_tol=abs_tol,
            pct_tol=pct_tol,
        )
        for row in claims
    ]
    calculation.add_game_level_scores(
        validation_rows=validation_rows,
        training_rows=claims,
        valid_threshold=reasoning_valid_threshold,
        weak_threshold=reasoning_weak_threshold,
    )

    if len(validation_rows) != len(claims):
        raise ValueError("Level 2 claims-in and validations-out do not reconcile")
    unavailable = sum(
        row.get("validation_result") == "unavailable"
        for row in validation_rows
    )
    return {
        **base,
        "status": "completed",
        "reason": None,
        "validation_rows": validation_rows,
        "by_validation_result": _count_by(
            validation_rows, "validation_result"
        ),
        "by_feature_status": _count_by(validation_rows, "feature_status"),
        "reconciliation": {
            "claims_in": len(claims),
            "validations_out": len(validation_rows),
            "unavailable": unavailable,
            "rejected": 0,
            "conflicts": 0,
            "write_performed": False,
        },
    }
