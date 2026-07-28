"""
Build GameLens Level 4 claim-language calibration summaries.

Recommended repo location:
    agg/gamelens_training/build_claim_language_calibration.py

Purpose:
    Level 4 worker for GameLens claim-language calibration.

    Levels 1-3 already create and enrich one row per pregame GameLens claim:

        Level 1:
            build_claim_training_examples.py
            -> extracts one row per pregame claim

        Level 2:
            update_claim_training_validation.py
            -> adds postgame validation labels

        Level 3:
            update_claim_training_features.py
            -> adds engineered pregame features:
                clean_hierarchy_context_v1 metadata
                offensive_efficiency_support_v1 metadata
                offense_finish_score
                defensive_suppression_score
                two_way_edge_score
                two_way_context

    This Level 4 worker reads those rows AFTER Level 3 completes and summarizes
    which claim/metric/surface combinations deserve stronger, softer, blocked,
    or normal language.

Important boundary:
    This script does NOT:
        - pick winners
        - change matchup_lean
        - change outcome_confidence
        - change Model Trust
        - update the /game API response directly
        - change frontend behavior

    It only produces calibration summaries that help answer:

        "When GameLens makes this type of claim, did postgame data usually agree?"

Current v0.1 role:
    This first version is intentionally conservative.

    It compares historical claim validation results against the current runtime
    registry in:

        services/claim_language_support_registry.py

    That registry currently controls:
        - strong allowlist metrics
        - watch metrics
        - conditional-disabled metrics
        - blocked metrics
        - exact claim_type + claim_layer + metric surfaces

Output:
    Dry-run outputs:
        qa/gamelens_calibration_runs/<calibration_run_id>/calibration_summary.csv
        qa/gamelens_calibration_runs/<calibration_run_id>/calibration_summary.json
        qa/gamelens_calibration_runs/<calibration_run_id>/run_metadata.json

    Optional BigQuery output:
        Analytics.gamelens_claim_language_calibration

Example dry run:
    python -m agg.gamelens_training.build_claim_language_calibration \
      --run-id larger_240_level3_qa_20260517 \
      --dry-run

Example BigQuery write:
    python -m agg.gamelens_training.build_claim_language_calibration \
      --run-id larger_240_level3_qa_20260517 \
      --write-bigquery \
      --replace-run

Recommended workflow:
    Run this after Level 3 completes:

        Level 1 -> Level 2 -> Level 3 -> Level 4

    Do dry-run first. Only write to BigQuery after reviewing the local outputs.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


PROJECT_ID = "nfl-stream-406420"
DATASET_ID = "Analytics"
TRAINING_TABLE = "gamelens_claim_training_examples"
OUTPUT_TABLE = "gamelens_claim_language_calibration"

DEFAULT_OUTPUT_ROOT = Path("qa/gamelens_calibration_runs")
DEFAULT_CALIBRATION_VERSION = "level4_v0_2_offensive_efficiency_language_calibration"

# These thresholds are not final model rules.
# They are simple review helpers for deciding whether a pattern deserves more study.
MIN_SAMPLE_ROWS = 30
STRONG_REVIEW_RATE = 0.58
WATCH_REVIEW_RATE = 0.54
MEANINGFUL_LIFT = 0.04


# ---------------------------------------------------------------------------
# Runtime registry import
# ---------------------------------------------------------------------------

try:
    from services.claim_language_support_registry import (
        get_claim_language_decision,
        STRONG_ALLOWLIST_METRICS,
        WATCH_ALLOWLIST_METRICS,
        CONDITIONAL_DISABLED_METRICS,
        BLOCKED_METRICS,
    )
except Exception as exc:  # pragma: no cover - fail clearly at runtime
    raise ImportError(
        "Could not import services.claim_language_support_registry. "
        "Run this from the repo root and confirm the Level 4 registry exists."
    ) from exc


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_identifier(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_")[:100] or "run"


def as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return bool(value)

    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "t", "yes", "y", "1"}:
            return True
        if lowered in {"false", "f", "no", "n", "0"}:
            return False

    return None


def round_rate(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), 4)


def write_json(data: Any, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def write_csv(rows: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        output_path.write_text("", encoding="utf-8")
        return

    preferred = [
        "calibration_run_id",
        "source_run_id",
        "calibration_version",
        "feature_formula_version",
        "claim_type",
        "claim_layer",
        "metric",
        "core_area",
        "category",
        "two_way_context",
        "offensive_efficiency_support_bucket",
        "offensive_efficiency_support_strength",
        "offensive_efficiency_support_score_avg",
        "offensive_efficiency_support_score_min",
        "offensive_efficiency_support_score_max",
        "offensive_efficiency_language_signal",
        "row_count",
        "validated_count",
        "validation_rate",
        "surface_baseline_validation_rate",
        "lift_vs_surface_baseline",
        "overall_baseline_validation_rate",
        "lift_vs_overall_baseline",
        "current_rule_status",
        "current_support_level",
        "current_language_boost_allowed",
        "recommendation",
        "language_modifier",
        "api_use_allowed_v0_1",
        "notes",
        "created_at",
    ]

    all_keys = sorted({key for row in rows for key in row.keys()})
    fieldnames = [key for key in preferred if key in all_keys] + [
        key for key in all_keys if key not in preferred
    ]

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def import_bigquery():
    from google.cloud import bigquery
    return bigquery


# ---------------------------------------------------------------------------
# BigQuery loading
# ---------------------------------------------------------------------------

def load_training_rows(
    *,
    client: Any,
    bigquery: Any,
    project_id: str,
    dataset_id: str,
    training_table: str,
    run_id: str,
    limit: Optional[int],
    include_missing_metric: bool,
) -> List[Dict[str, Any]]:
    table_ref = f"{project_id}.{dataset_id}.{training_table}"
    limit_clause = "LIMIT @limit" if limit else ""
    metric_filter = "" if include_missing_metric else "AND metric IS NOT NULL"

    query = f"""
        SELECT
            claim_key,
            run_id,
            game_id,
            season,
            claim_type,
            claim_layer,
            claim_name,
            group_name,
            core_area,
            category,
            metric,
            metric_label,

            validation_result,
            validated_flag,

            offense_finish_score,
            defensive_suppression_score,
            two_way_edge_score,
            two_way_context,
            offensive_efficiency_support_score,
            offensive_efficiency_support_bucket,
            offensive_efficiency_support_strength,
            offensive_efficiency_support_reason,
            offensive_efficiency_support_metrics,
            feature_formula_version,
            feature_status,
            feature_notes
        FROM `{table_ref}`
        WHERE run_id = @run_id
          {metric_filter}
        ORDER BY season, game_id, claim_type, claim_layer, metric
        {limit_clause}
    """

    params = [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]

    if limit:
        params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return [dict(row) for row in client.query(query, job_config=job_config).result()]


# ---------------------------------------------------------------------------
# Calibration math
# ---------------------------------------------------------------------------

def is_validated(row: Dict[str, Any]) -> bool:
    validated_flag = as_bool(row.get("validated_flag"))
    if validated_flag is not None:
        return validated_flag

    return str(row.get("validation_result") or "").strip().lower() == "validated"


def validation_bucket(row: Dict[str, Any]) -> str:
    result = str(row.get("validation_result") or "").strip().lower()

    if result == "validated":
        return "validated"

    if result == "not_validated":
        return "not_validated"

    if result == "actual_neutral_or_mixed":
        return "actual_neutral_or_mixed"

    if result == "unavailable":
        return "unavailable"

    return result or "unknown"


def group_key(row: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        row.get("feature_formula_version") or "unknown_formula",
        row.get("claim_type") or "unknown_claim_type",
        row.get("claim_layer") or "unknown_claim_layer",
        row.get("metric") or "missing_metric",
        row.get("core_area") or None,
        row.get("category") or None,
        row.get("two_way_context") or "unknown",
        row.get("offensive_efficiency_support_bucket") or "not_available",
        row.get("offensive_efficiency_support_strength") or "not_available",
    )


def surface_key(row: Dict[str, Any]) -> Tuple[str, str]:
    return (
        str(row.get("claim_type") or "unknown_claim_type"),
        str(row.get("claim_layer") or "unknown_claim_layer"),
    )


def calculate_rate(validated_count: int, row_count: int) -> Optional[float]:
    if row_count <= 0:
        return None
    return validated_count / row_count


def build_baselines(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    overall_count = len(rows)
    overall_validated = sum(1 for row in rows if is_validated(row))

    surface_counts: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(
        lambda: {"row_count": 0, "validated_count": 0}
    )

    for row in rows:
        key = surface_key(row)
        surface_counts[key]["row_count"] += 1
        if is_validated(row):
            surface_counts[key]["validated_count"] += 1

    surface_rates = {
        key: calculate_rate(vals["validated_count"], vals["row_count"])
        for key, vals in surface_counts.items()
    }

    return {
        "overall_row_count": overall_count,
        "overall_validated_count": overall_validated,
        "overall_validation_rate": calculate_rate(overall_validated, overall_count),
        "surface_rates": surface_rates,
        "surface_counts": surface_counts,
    }


def recommend_language_action(
    *,
    metric: str,
    two_way_context: str,
    row_count: int,
    validation_rate: Optional[float],
    surface_baseline_rate: Optional[float],
    decision: Any,
) -> Tuple[str, str, str]:
    """
    Return:
        recommendation, language_modifier, notes

    This is intentionally conservative.

    The runtime registry remains the source of truth for v0.1.
    This function helps review whether future versions should change the registry.
    """

    if metric in BLOCKED_METRICS:
        return (
            "keep_blocked",
            "block_stronger_language",
            "Metric is blocked from automatic stronger language in v0.1.",
        )

    if metric in CONDITIONAL_DISABLED_METRICS:
        return (
            "review_conditional_candidate",
            "normal_language_for_now",
            "Metric is a conditional candidate but remains disabled in v0.1.",
        )

    if two_way_context != "supportive":
        return (
            "no_boost_context_not_supportive",
            "normal_or_cautious_language",
            "two_way_context is not supportive, so v0.1 should not boost this claim.",
        )

    if decision.language_boost_allowed:
        if decision.support_level == "strong":
            return (
                "keep_v0_1_strong_allow",
                "stronger_language_allowed",
                "Current registry allows strong claim-language support.",
            )

        if decision.support_level == "watch":
            return (
                "keep_v0_1_watch_allow",
                "measured_support_language",
                "Current registry allows measured/watch-level support.",
            )

        return (
            "keep_v0_1_allow",
            "support_language_allowed",
            "Current registry allows support language.",
        )

    if row_count < MIN_SAMPLE_ROWS:
        return (
            "insufficient_sample",
            "normal_language",
            f"Sample below review threshold of {MIN_SAMPLE_ROWS} rows.",
        )

    if validation_rate is None:
        return (
            "insufficient_validation_data",
            "normal_language",
            "Validation rate could not be calculated.",
        )

    lift = None
    if surface_baseline_rate is not None:
        lift = validation_rate - surface_baseline_rate

    if (
        validation_rate >= STRONG_REVIEW_RATE
        and lift is not None
        and lift >= MEANINGFUL_LIFT
    ):
        return (
            "future_allowlist_candidate",
            "review_for_stronger_language",
            "Pattern beat threshold and surface baseline; review before enabling.",
        )

    if (
        validation_rate >= WATCH_REVIEW_RATE
        and lift is not None
        and lift >= 0
    ):
        return (
            "future_watch_candidate",
            "review_for_measured_language",
            "Pattern is interesting but not enough for automatic strong language.",
        )

    return (
        "keep_normal_language",
        "normal_language",
        "No v0.1 allowlist rule and no strong review signal.",
    )




def numeric_summary(values: List[Optional[float]]) -> Dict[str, Optional[float]]:
    clean_values = [float(value) for value in values if value is not None]
    if not clean_values:
        return {"avg": None, "min": None, "max": None}

    return {
        "avg": round(sum(clean_values) / len(clean_values), 4),
        "min": round(min(clean_values), 4),
        "max": round(max(clean_values), 4),
    }


def first_non_empty(values: List[Any]) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def offensive_efficiency_language_signal(
    *,
    bucket: str,
    strength: str,
    row_count: int,
    validation_rate: Optional[float],
    surface_baseline_rate: Optional[float],
) -> Tuple[str, str]:
    """
    Metadata-only Level 4 signal for offensive_efficiency_support_v1.

    This does not override the current runtime registry or two_way_context rule.
    It simply marks whether the new feature deserves review/exposure.
    """
    if bucket in {"caution_only", "negative_caution", "opposing_efficiency_signal"}:
        return (
            "caution_metadata",
            "Feature bucket is caution-oriented; do not use for stronger language.",
        )

    if bucket in {"context_only", "not_relevant", "anchor_unavailable", "not_available"}:
        return (
            "normal_metadata",
            "Feature bucket is context-only, unavailable, or outside feature scope.",
        )

    if row_count < MIN_SAMPLE_ROWS:
        return (
            "insufficient_sample",
            f"Sample below review threshold of {MIN_SAMPLE_ROWS} rows.",
        )

    if validation_rate is None:
        return (
            "insufficient_validation_data",
            "Validation rate could not be calculated.",
        )

    lift = None
    if surface_baseline_rate is not None:
        lift = validation_rate - surface_baseline_rate

    if strength == "strong_support" and validation_rate >= STRONG_REVIEW_RATE and (lift is None or lift >= 0):
        return (
            "strong_support_metadata",
            "Strong offensive-efficiency support; expose for Level 4 review but do not auto-boost yet.",
        )

    if strength == "measured_support" and validation_rate >= WATCH_REVIEW_RATE and (lift is None or lift >= 0):
        return (
            "measured_support_metadata",
            "Measured offensive-efficiency support; expose for Level 4 review with cautious wording.",
        )

    if strength == "mixed":
        return (
            "mixed_metadata",
            "Mixed offensive-efficiency support; no broad boost, but metric-specific pockets may deserve review.",
        )

    return (
        "normal_metadata",
        "No offensive-efficiency language signal beyond normal metadata exposure.",
    )

def summarize_groups(
    *,
    rows: List[Dict[str, Any]],
    source_run_id: str,
    calibration_run_id: str,
    calibration_version: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    baselines = build_baselines(rows)
    overall_rate = baselines["overall_validation_rate"]

    grouped: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[group_key(row)].append(row)

    created_at = utc_now_iso()
    output_rows: List[Dict[str, Any]] = []

    for key, group_rows in grouped.items():
        (
            feature_formula_version,
            claim_type,
            claim_layer,
            metric,
            core_area,
            category,
            two_way_context,
            offensive_efficiency_support_bucket,
            offensive_efficiency_support_strength,
        ) = key

        row_count = len(group_rows)
        validated_count = sum(1 for row in group_rows if is_validated(row))

        bucket_counts = defaultdict(int)
        for row in group_rows:
            bucket_counts[validation_bucket(row)] += 1

        validation_rate = calculate_rate(validated_count, row_count)

        s_key = (str(claim_type), str(claim_layer))
        surface_baseline_rate = baselines["surface_rates"].get(s_key)

        lift_vs_surface = (
            validation_rate - surface_baseline_rate
            if validation_rate is not None and surface_baseline_rate is not None
            else None
        )

        lift_vs_overall = (
            validation_rate - overall_rate
            if validation_rate is not None and overall_rate is not None
            else None
        )

        offensive_score_summary = numeric_summary([
            as_float(row.get("offensive_efficiency_support_score"))
            for row in group_rows
        ])
        offensive_reason_sample = first_non_empty([
            row.get("offensive_efficiency_support_reason")
            for row in group_rows
        ])
        offensive_metrics_sample = first_non_empty([
            row.get("offensive_efficiency_support_metrics")
            for row in group_rows
        ])
        oe_signal, oe_notes = offensive_efficiency_language_signal(
            bucket=str(offensive_efficiency_support_bucket),
            strength=str(offensive_efficiency_support_strength),
            row_count=row_count,
            validation_rate=validation_rate,
            surface_baseline_rate=surface_baseline_rate,
        )

        decision = get_claim_language_decision(
            claim_type=str(claim_type),
            claim_layer=str(claim_layer),
            metric=str(metric),
            two_way_context=str(two_way_context),
        )

        recommendation, language_modifier, notes = recommend_language_action(
            metric=str(metric),
            two_way_context=str(two_way_context),
            row_count=row_count,
            validation_rate=validation_rate,
            surface_baseline_rate=surface_baseline_rate,
            decision=decision,
        )

        output_rows.append({
            "calibration_run_id": calibration_run_id,
            "source_run_id": source_run_id,
            "calibration_version": calibration_version,
            "feature_formula_version": feature_formula_version,

            "claim_type": claim_type,
            "claim_layer": claim_layer,
            "metric": metric,
            "core_area": core_area,
            "category": category,
            "two_way_context": two_way_context,
            "offensive_efficiency_support_bucket": offensive_efficiency_support_bucket,
            "offensive_efficiency_support_strength": offensive_efficiency_support_strength,
            "offensive_efficiency_support_score_avg": offensive_score_summary["avg"],
            "offensive_efficiency_support_score_min": offensive_score_summary["min"],
            "offensive_efficiency_support_score_max": offensive_score_summary["max"],
            "offensive_efficiency_support_reason_sample": offensive_reason_sample,
            "offensive_efficiency_support_metrics": offensive_metrics_sample,
            "offensive_efficiency_language_signal": oe_signal,
            "offensive_efficiency_notes": oe_notes,

            "row_count": row_count,
            "validated_count": validated_count,
            "not_validated_count": bucket_counts.get("not_validated", 0),
            "actual_neutral_or_mixed_count": bucket_counts.get("actual_neutral_or_mixed", 0),
            "unavailable_count": bucket_counts.get("unavailable", 0),
            "unknown_validation_count": bucket_counts.get("unknown", 0),

            "validation_rate": round_rate(validation_rate),
            "surface_baseline_validation_rate": round_rate(surface_baseline_rate),
            "lift_vs_surface_baseline": round_rate(lift_vs_surface),
            "overall_baseline_validation_rate": round_rate(overall_rate),
            "lift_vs_overall_baseline": round_rate(lift_vs_overall),

            "current_language_boost_allowed": decision.language_boost_allowed,
            "current_support_level": decision.support_level,
            "current_rule_status": decision.rule_status,
            "current_rule_reason": decision.reason,

            "recommendation": recommendation,
            "language_modifier": language_modifier,
            "api_use_allowed_v0_1": decision.language_boost_allowed,
            "api_exposure_allowed_v0_2": True,

            "min_sample_rows": MIN_SAMPLE_ROWS,
            "meets_min_sample": row_count >= MIN_SAMPLE_ROWS,
            "notes": notes,
            "created_at": created_at,
        })

    output_rows.sort(
        key=lambda row: (
            str(row.get("two_way_context")),
            str(row.get("claim_type")),
            str(row.get("claim_layer")),
            str(row.get("metric")),
            -(row.get("row_count") or 0),
        )
    )

    metadata = {
        "calibration_run_id": calibration_run_id,
        "source_run_id": source_run_id,
        "calibration_version": calibration_version,
        "created_at": created_at,

        "input_row_count": len(rows),
        "summary_row_count": len(output_rows),

        "overall_row_count": baselines["overall_row_count"],
        "overall_validated_count": baselines["overall_validated_count"],
        "overall_validation_rate": round_rate(overall_rate),

        "offensive_efficiency_signal_counts": dict(
            sorted(
                {
                    signal: sum(
                        1
                        for row in output_rows
                        if row.get("offensive_efficiency_language_signal") == signal
                    )
                    for signal in {
                        row.get("offensive_efficiency_language_signal")
                        for row in output_rows
                    }
                }.items()
            )
        ),

        "feature_formula_versions": sorted({
            str(row.get("feature_formula_version") or "unknown_formula")
            for row in rows
        }),

        "registry_snapshot": {
            "strong_allowlist_metrics": sorted(STRONG_ALLOWLIST_METRICS),
            "watch_allowlist_metrics": sorted(WATCH_ALLOWLIST_METRICS),
            "conditional_disabled_metrics": sorted(CONDITIONAL_DISABLED_METRICS),
            "blocked_metrics": sorted(BLOCKED_METRICS),
        },

        "recommendation_counts": dict(
            sorted(
                {
                    rec: sum(1 for row in output_rows if row["recommendation"] == rec)
                    for rec in {row["recommendation"] for row in output_rows}
                }.items()
            )
        ),
    }

    return output_rows, metadata


# ---------------------------------------------------------------------------
# BigQuery write
# ---------------------------------------------------------------------------

def output_schema(bigquery: Any) -> List[Any]:
    return [
        bigquery.SchemaField("calibration_run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("calibration_version", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("feature_formula_version", "STRING", mode="NULLABLE"),

        bigquery.SchemaField("claim_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("claim_layer", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("metric", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("core_area", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("two_way_context", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("offensive_efficiency_support_bucket", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("offensive_efficiency_support_strength", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("offensive_efficiency_support_score_avg", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("offensive_efficiency_support_score_min", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("offensive_efficiency_support_score_max", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("offensive_efficiency_support_reason_sample", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("offensive_efficiency_support_metrics", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("offensive_efficiency_language_signal", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("offensive_efficiency_notes", "STRING", mode="NULLABLE"),

        bigquery.SchemaField("row_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("validated_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("not_validated_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("actual_neutral_or_mixed_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("unavailable_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("unknown_validation_count", "INTEGER", mode="NULLABLE"),

        bigquery.SchemaField("validation_rate", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("surface_baseline_validation_rate", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("lift_vs_surface_baseline", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("overall_baseline_validation_rate", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("lift_vs_overall_baseline", "FLOAT", mode="NULLABLE"),

        bigquery.SchemaField("current_language_boost_allowed", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("current_support_level", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("current_rule_status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("current_rule_reason", "STRING", mode="NULLABLE"),

        bigquery.SchemaField("recommendation", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("language_modifier", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("api_use_allowed_v0_1", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("api_exposure_allowed_v0_2", "BOOLEAN", mode="NULLABLE"),

        bigquery.SchemaField("min_sample_rows", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("meets_min_sample", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("notes", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    ]


def ensure_output_table(
    *,
    client: Any,
    bigquery: Any,
    table_ref: str,
) -> None:
    from google.api_core.exceptions import NotFound

    desired_schema = output_schema(bigquery)

    try:
        table = client.get_table(table_ref)
        existing_names = {field.name for field in table.schema}
        missing_fields = [
            field for field in desired_schema
            if field.name not in existing_names
        ]

        if missing_fields:
            table.schema = list(table.schema) + missing_fields
            client.update_table(table, ["schema"])
            print(
                f"Added {len(missing_fields)} missing columns to {table_ref}: "
                + ", ".join(field.name for field in missing_fields)
            )

        return
    except NotFound:
        pass

    table = bigquery.Table(table_ref, schema=desired_schema)
    table.description = (
        "GameLens Level 4 claim-language calibration summary. "
        "One row per source run + feature formula + claim surface + metric + two_way_context."
    )
    table.clustering_fields = [
        "source_run_id",
        "two_way_context",
        "offensive_efficiency_support_strength",
        "claim_type",
        "metric",
    ]

    client.create_table(table)
    print(f"Created BigQuery table: {table_ref}")


def delete_existing_output_rows(
    *,
    client: Any,
    bigquery: Any,
    table_ref: str,
    calibration_run_id: str,
    source_run_id: str,
) -> None:
    query = f"""
        DELETE FROM `{table_ref}`
        WHERE calibration_run_id = @calibration_run_id
          AND source_run_id = @source_run_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("calibration_run_id", "STRING", calibration_run_id),
            bigquery.ScalarQueryParameter("source_run_id", "STRING", source_run_id),
        ]
    )

    client.query(query, job_config=job_config).result()


def write_to_bigquery(
    *,
    client: Any,
    bigquery: Any,
    project_id: str,
    dataset_id: str,
    output_table: str,
    rows: List[Dict[str, Any]],
    calibration_run_id: str,
    source_run_id: str,
    replace_run: bool,
) -> None:
    if not rows:
        print("No rows to write to BigQuery.")
        return

    table_ref = f"{project_id}.{dataset_id}.{output_table}"

    ensure_output_table(
        client=client,
        bigquery=bigquery,
        table_ref=table_ref,
    )

    if replace_run:
        delete_existing_output_rows(
            client=client,
            bigquery=bigquery,
            table_ref=table_ref,
            calibration_run_id=calibration_run_id,
            source_run_id=source_run_id,
        )

    job_config = bigquery.LoadJobConfig(
        schema=output_schema(bigquery),
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()

    print(f"Wrote {len(rows)} rows to {table_ref}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build GameLens Level 4 claim-language calibration summaries."
    )

    parser.add_argument(
        "--run-id",
        required=True,
        help="Source run_id in Analytics.gamelens_claim_training_examples.",
    )
    parser.add_argument(
        "--calibration-run-id",
        default=None,
        help=(
            "Optional output calibration run id. "
            "Default: <run-id>__level4_v0_2"
        ),
    )
    parser.add_argument(
        "--calibration-version",
        default=DEFAULT_CALIBRATION_VERSION,
        help="Calibration version label.",
    )
    parser.add_argument(
        "--project-id",
        default=PROJECT_ID,
        help="BigQuery project id.",
    )
    parser.add_argument(
        "--dataset-id",
        default=DATASET_ID,
        help="BigQuery dataset id.",
    )
    parser.add_argument(
        "--training-table",
        default=TRAINING_TABLE,
        help="Input claim training examples table name.",
    )
    parser.add_argument(
        "--output-table",
        default=OUTPUT_TABLE,
        help="Output BigQuery calibration table name.",
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Local output root for dry-run files.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row limit for debugging.",
    )
    parser.add_argument(
        "--include-missing-metric",
        action="store_true",
        help="Include claim rows with metric IS NULL. Default excludes them.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write local CSV/JSON only. This is the recommended first run.",
    )
    parser.add_argument(
        "--write-bigquery",
        action="store_true",
        help="Write calibration summary rows to BigQuery.",
    )
    parser.add_argument(
        "--replace-run",
        action="store_true",
        help="Delete existing rows for this calibration_run_id/source_run_id before writing.",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.dry_run and not args.write_bigquery:
        print("No output mode selected. Defaulting to --dry-run behavior.")
        args.dry_run = True

    calibration_run_id = (
        args.calibration_run_id
        or f"{sanitize_identifier(args.run_id)}__level4_v0_2"
    )

    bigquery = import_bigquery()
    client = bigquery.Client(project=args.project_id)

    print("GameLens Level 4 claim-language calibration")
    print("------------------------------------------------")
    print(f"Source run id:        {args.run_id}")
    print(f"Calibration run id:   {calibration_run_id}")
    print(f"Calibration version:  {args.calibration_version}")
    print(f"Write BigQuery:       {args.write_bigquery}")
    print(f"Dry run:              {args.dry_run}")

    rows = load_training_rows(
        client=client,
        bigquery=bigquery,
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        training_table=args.training_table,
        run_id=args.run_id,
        limit=args.limit,
        include_missing_metric=args.include_missing_metric,
    )

    if not rows:
        print("No input rows found. Nothing to calibrate.")
        return 1

    summary_rows, metadata = summarize_groups(
        rows=rows,
        source_run_id=args.run_id,
        calibration_run_id=calibration_run_id,
        calibration_version=args.calibration_version,
    )

    output_dir = Path(args.output_root) / calibration_run_id
    csv_path = output_dir / "calibration_summary.csv"
    json_path = output_dir / "calibration_summary.json"
    metadata_path = output_dir / "run_metadata.json"

    write_csv(summary_rows, csv_path)
    write_json(summary_rows, json_path)
    write_json(metadata, metadata_path)

    print("")
    print("Local outputs written:")
    print(f"  {csv_path}")
    print(f"  {json_path}")
    print(f"  {metadata_path}")

    print("")
    print("Summary:")
    print(f"  Input rows:    {metadata['input_row_count']}")
    print(f"  Summary rows:  {metadata['summary_row_count']}")
    print(f"  Overall rate:  {metadata['overall_validation_rate']}")
    print(f"  Recommendations: {metadata['recommendation_counts']}")

    if args.write_bigquery:
        write_to_bigquery(
            client=client,
            bigquery=bigquery,
            project_id=args.project_id,
            dataset_id=args.dataset_id,
            output_table=args.output_table,
            rows=summary_rows,
            calibration_run_id=calibration_run_id,
            source_run_id=args.run_id,
            replace_run=args.replace_run,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())