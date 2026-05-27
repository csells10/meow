"""
Build GameLens Expected Claim Quality dry-run outputs.

Recommended repo location:
    agg/gamelens_training/build_expected_claim_quality.py

Purpose
-------
This is an offline calibration experiment, not runtime model logic.

It estimates pregame Expected Claim Quality from historical claim signatures,
then measures whether that score creates lift against the existing 2025 Admin
baselines for:

    - overall claim validation
    - outcome confidence
    - profile strength
    - Core Area alignment / profile_type
    - confidence x profile intersections

Main question
-------------
Inside High Confidence games, does expected claim quality split the good High
reads from the fragile High reads?

Important boundaries
--------------------
This script does NOT:
    - change winner prediction
    - change matchup_lean
    - change outcome_confidence
    - change Model Trust
    - update /game
    - update the Admin API
    - write to BigQuery
    - create hard-coded runtime gates

It only writes local dry-run CSV/JSON artifacts so the pattern can be reviewed
before any product or model behavior changes are considered.

Anti-overfitting guardrail
--------------------------
Scores use leave-one-game-out logic:

    When scoring Game A, all rows from Game A are excluded from the historical
    validation-rate lookup. If the exact claim signature is too thin, the worker
    falls back to broader signatures.

That means the score asks:

    "Before this game happened, how reliable had similar claims been?"

not:

    "Did this exact claim validate in this exact game?"

Example BigQuery dry run
------------------------
    python -m agg.gamelens_training.build_expected_claim_quality \
      --run-id full_2025_reg_post_claim_matrix_pilot \
      --season 2025 \
      --dry-run

Example local CSV dry run
-------------------------
    python -m agg.gamelens_training.build_expected_claim_quality \
      --input-csv qa/example_claim_training_rows.csv \
      --run-id example_run \
      --dry-run

Outputs
-------
    qa/gamelens_expected_claim_quality_runs/<run_id>/
        expected_claim_quality_by_claim.csv
        expected_claim_quality_by_game.csv
        expected_claim_quality_lift_summary.csv
        high_confidence_expected_quality_split.csv
        high_confidence_expected_quality_ranked_split.csv
        high_confidence_expected_quality_half_split.csv
        expected_quality_outcome_lift_summary.csv
        confidence_relabel_audit.csv
        profile_conditioned_confidence_audit.csv
        confidence_label_integrity_audit.csv
        expected_quality_monotonicity_audit.csv
        high_confidence_miss_review.csv
        high_confidence_driver_profile.csv

Additional v0.3.2 cleanup
-------------------------
This version keeps v0.3.1 behavior but tightens QA outputs:
    - removes duplicated .1-style columns from high_confidence_miss_review.csv
    - adds graded_games, tie_or_push_games, and Wilson CI fields to
      high_confidence_expected_quality_ranked_split.csv
    - adds explicit use_for_confidence_relabeling flags to the monotonicity audit
      so non-monotonic segments cannot be over-read as relabeling evidence

        run_metadata.json
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple


PROJECT_ID = "nfl-stream-406420"
DATASET_ID = "Analytics"
TRAINING_TABLE = "gamelens_claim_training_examples"
DEFAULT_OUTPUT_ROOT = Path("qa/gamelens_expected_claim_quality_runs")
DEFAULT_VERSION = "expected_claim_quality_v0_3_2_clean_diagnostics_guardrails"

# Admin-style comparability: Level 4 / Admin-style summaries historically use
# validated rows divided by row_count. Keep unavailable/neutral rows in the
# denominator by default so lift can be compared to the 2025 Admin dashboard.
DEFAULT_EXCLUDE_UNAVAILABLE = False

# Conservative review thresholds. These are NOT runtime confidence rules.
DEFAULT_MIN_SAMPLE_ROWS = 30
DEFAULT_STRONG_LIFT = 0.06
DEFAULT_SUPPORTIVE_LIFT = 0.02
DEFAULT_FRAGILE_LIFT = -0.06

VALIDATED_RESULT = "validated"
UNAVAILABLE_RESULTS = {"unavailable", "unknown", ""}
NO_PICK_RESULTS = {"no pick", "no_pick", "no decision", "no_decision"}

DESIRED_COLUMNS = [
    # identity / run
    "claim_key",
    "run_id",
    "game_id",
    "season",
    "game_date",
    "game_week",
    "season_type",

    # game-level model context
    "outcome_confidence_code",
    "outcome_confidence_label",
    "profile_strength_code",
    "profile_strength_label",
    "profile_type",
    "matchup_label",
    "model_result",
    "is_tie",
    "final_margin_abs",
    "final_margin_bucket",
    "predicted_team",
    "actual_winner",
    "qa_read_v2",
    "confidence_cap_reason",
    "signal_gap",
    "core_gap",
    "core_area_split",
    "team_comp_edge_score",
    "team_comp_away_count",
    "team_comp_home_count",
    "team_comp_neutral_count",
    "team_comp_total_visible",

    # claim surface / hierarchy
    "claim_type",
    "claim_layer",
    "claim_name",
    "group_name",
    "source_section",
    "claim_rank",
    "claimed_side",
    "claimed_team",
    "core_area",
    "category",
    "metric",
    "metric_label",
    "registry_core_area",
    "registry_category",
    "registry_metric_label",
    "registry_signal_strength",
    "registry_ranking_usage",
    "clean_hierarchy_status",
    "clean_hierarchy_path_flag",
    "missing_hierarchy_parent_flag",

    # validation / Level 3 features
    "validation_result",
    "validated_flag",
    "qa_read_v2",
    "two_way_context",
    "two_way_edge_score",
    "offense_finish_score",
    "defensive_suppression_score",
    "offensive_efficiency_support_score",
    "offensive_efficiency_support_bucket",
    "offensive_efficiency_support_strength",
    "offensive_efficiency_support_reason",
    "claim_strength_bucket",
    "claim_strength_context",
    "claim_strength_language_signal",
    "feature_formula_version",
]

REQUIRED_COLUMNS = {"game_id", "validation_result"}

# Signature fallbacks. Each level should be pregame-safe metadata only.
SIGNATURE_LEVELS: List[Tuple[str, Tuple[str, ...]]] = [
    (
        "full_signature",
        (
            "claim_type",
            "claim_layer",
            "resolved_core_area",
            "resolved_category",
            "metric",
            "two_way_context",
            "offensive_efficiency_support_bucket",
            "claim_strength_bucket",
        ),
    ),
    (
        "metric_support_signature",
        (
            "claim_type",
            "claim_layer",
            "metric",
            "two_way_context",
            "offensive_efficiency_support_bucket",
        ),
    ),
    (
        "metric_signature",
        (
            "claim_type",
            "claim_layer",
            "metric",
        ),
    ),
    (
        "category_support_signature",
        (
            "claim_type",
            "claim_layer",
            "resolved_core_area",
            "resolved_category",
            "two_way_context",
        ),
    ),
    (
        "category_signature",
        (
            "claim_type",
            "claim_layer",
            "resolved_core_area",
            "resolved_category",
        ),
    ),
    (
        "core_area_signature",
        (
            "claim_type",
            "claim_layer",
            "resolved_core_area",
        ),
    ),
    (
        "surface_signature",
        (
            "claim_type",
            "claim_layer",
        ),
    ),
]


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_identifier(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_@.-]+", "_", str(value or "")).strip("_")[:120] or "run"


def clean_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_token(value: Any) -> str:
    text = clean_string(value)
    if not text:
        return "missing"
    return text.strip().lower()


def as_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if math.isnan(value) if isinstance(value, float) else False:
            return None
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "t", "yes", "y", "1"}:
            return True
        if lowered in {"false", "f", "no", "n", "0"}:
            return False
    return None


def as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        number = float(value)
        if math.isnan(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def as_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def round_rate(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), 4)


def pct_points(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value) * 100.0, 2)


def safe_divide(numerator: int, denominator: int) -> Optional[float]:
    if denominator <= 0:
        return None
    return numerator / denominator


def write_json(data: Any, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def write_csv(rows: List[Dict[str, Any]], output_path: Path, preferred: Sequence[str]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        output_path.write_text("", encoding="utf-8")
        return

    all_keys = sorted({key for row in rows for key in row.keys()})
    fieldnames = [key for key in preferred if key in all_keys] + [
        key for key in all_keys if key not in preferred
    ]

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def validation_result(row: Dict[str, Any]) -> str:
    return normalize_token(row.get("validation_result"))


def is_validated(row: Dict[str, Any]) -> bool:
    flag = as_bool(row.get("validated_flag"))
    if flag is not None:
        return flag
    return validation_result(row) == VALIDATED_RESULT


def is_denominator_eligible(row: Dict[str, Any], *, exclude_unavailable: bool) -> bool:
    result = validation_result(row)
    if exclude_unavailable and result in UNAVAILABLE_RESULTS:
        return False
    return bool(row.get("game_id"))


def model_result_bucket(row: Dict[str, Any]) -> str:
    result = normalize_token(row.get("model_result"))
    if result in {"correct", "win", "won"}:
        return "correct"
    if result in {"incorrect", "loss", "lost"}:
        return "incorrect"
    if result in NO_PICK_RESULTS:
        return "no_pick"
    if result in {"tie", "push"}:
        return "tie_or_push"
    return result or "unknown"


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def import_bigquery():
    from google.cloud import bigquery
    return bigquery


def get_available_columns(*, client: Any, bigquery: Any, project_id: str, dataset_id: str, table: str) -> set[str]:
    query = f"""
        SELECT column_name
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = @table
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("table", "STRING", table)]
    )
    return {str(row["column_name"]) for row in client.query(query, job_config=job_config).result()}


def load_rows_from_bigquery(
    *,
    run_id: str,
    season: Optional[str],
    project_id: str,
    dataset_id: str,
    training_table: str,
    limit: Optional[int],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    bigquery = import_bigquery()
    client = bigquery.Client(project=project_id)
    available = get_available_columns(
        client=client,
        bigquery=bigquery,
        project_id=project_id,
        dataset_id=dataset_id,
        table=training_table,
    )

    missing_required = REQUIRED_COLUMNS - available
    if missing_required:
        raise RuntimeError(
            f"Training table is missing required columns: {sorted(missing_required)}"
        )

    selected = [col for col in DESIRED_COLUMNS if col in available]
    missing_desired = [col for col in DESIRED_COLUMNS if col not in available]

    filters = ["run_id = @run_id"] if "run_id" in available else []
    params = []
    if "run_id" in available:
        params.append(bigquery.ScalarQueryParameter("run_id", "STRING", run_id))

    if season and "season" in available:
        filters.append("CAST(season AS STRING) = @season")
        params.append(bigquery.ScalarQueryParameter("season", "STRING", str(season)))

    where_clause = "WHERE " + " AND ".join(filters) if filters else ""

    order_candidates = [
        "season",
        "game_date",
        "game_id",
        "claim_type",
        "claim_layer",
        "claim_rank",
        "claim_key",
    ]
    order_by = ", ".join(col for col in order_candidates if col in available)
    order_clause = f"ORDER BY {order_by}" if order_by else ""
    limit_clause = "LIMIT @limit" if limit else ""
    if limit:
        params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))

    select_clause = ",\n            ".join(selected)
    table_ref = f"{project_id}.{dataset_id}.{training_table}"
    query = f"""
        SELECT
            {select_clause}
        FROM `{table_ref}`
        {where_clause}
        {order_clause}
        {limit_clause}
    """

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = [dict(row) for row in client.query(query, job_config=job_config).result()]

    for row in rows:
        for col in missing_desired:
            row[col] = None

    return rows, {
        "source": "bigquery",
        "table_ref": table_ref,
        "available_columns": sorted(available),
        "missing_desired_columns": missing_desired,
    }


def load_rows_from_csv(path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = [dict(row) for row in reader]

    columns = set(rows[0].keys()) if rows else set()
    missing_required = REQUIRED_COLUMNS - columns
    if missing_required:
        raise RuntimeError(f"Input CSV is missing required columns: {sorted(missing_required)}")

    for row in rows:
        for col in DESIRED_COLUMNS:
            row.setdefault(col, None)

    return rows, {
        "source": "csv",
        "input_csv": str(path),
        "available_columns": sorted(columns),
        "missing_desired_columns": [col for col in DESIRED_COLUMNS if col not in columns],
    }


# ---------------------------------------------------------------------------
# Row enrichment / signatures
# ---------------------------------------------------------------------------


def enrich_rows(rows: List[Dict[str, Any]], *, exclude_unavailable: bool) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []

    for ix, row in enumerate(rows, 1):
        out = dict(row)
        out["row_index"] = ix
        out["resolved_core_area"] = clean_string(row.get("registry_core_area")) or clean_string(row.get("core_area"))
        out["resolved_category"] = clean_string(row.get("registry_category")) or clean_string(row.get("category"))
        out["validated_bool"] = is_validated(row)
        out["validation_bucket"] = validation_result(row)
        out["denominator_eligible"] = is_denominator_eligible(row, exclude_unavailable=exclude_unavailable)
        out["model_result_bucket"] = model_result_bucket(row)
        enriched.append(out)

    return enriched


def signature_key(row: Dict[str, Any], fields: Sequence[str]) -> Tuple[str, ...]:
    return tuple(normalize_token(row.get(field)) for field in fields)


def build_signature_indexes(
    rows: List[Dict[str, Any]],
    *,
    exclude_unavailable: bool,
) -> Dict[str, Dict[Tuple[str, ...], Dict[str, Any]]]:
    indexes: Dict[str, Dict[Tuple[str, ...], Dict[str, Any]]] = {
        level_name: defaultdict(lambda: {"row_count": 0, "validated_count": 0, "games": set()})
        for level_name, _ in SIGNATURE_LEVELS
    }

    for row in rows:
        if not is_denominator_eligible(row, exclude_unavailable=exclude_unavailable):
            continue

        game_id = str(row.get("game_id") or "")
        for level_name, fields in SIGNATURE_LEVELS:
            key = signature_key(row, fields)
            bucket = indexes[level_name][key]
            bucket["row_count"] += 1
            bucket["validated_count"] += 1 if row.get("validated_bool") else 0
            if game_id:
                bucket["games"].add(game_id)

    return indexes


def build_game_adjustment_indexes(
    rows: List[Dict[str, Any]],
    *,
    exclude_unavailable: bool,
) -> Dict[str, Dict[Tuple[str, Tuple[str, ...]], Dict[str, int]]]:
    """Count each game's own contribution per signature so LOO can subtract it."""
    adjustments: Dict[str, Dict[Tuple[str, Tuple[str, ...]], Dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: {"row_count": 0, "validated_count": 0})
    )

    for row in rows:
        if not is_denominator_eligible(row, exclude_unavailable=exclude_unavailable):
            continue

        game_id = str(row.get("game_id") or "")
        if not game_id:
            continue

        for level_name, fields in SIGNATURE_LEVELS:
            key = signature_key(row, fields)
            adj = adjustments[game_id][(level_name, key)]
            adj["row_count"] += 1
            adj["validated_count"] += 1 if row.get("validated_bool") else 0

    return adjustments


def validation_rate_from_counts(validated_count: int, row_count: int) -> Optional[float]:
    return safe_divide(validated_count, row_count)


def overall_counts(
    rows: List[Dict[str, Any]],
    *,
    exclude_game_id: Optional[str],
    exclude_unavailable: bool,
) -> Dict[str, int]:
    row_count = 0
    validated_count = 0
    games = set()
    for row in rows:
        if exclude_game_id and str(row.get("game_id")) == str(exclude_game_id):
            continue
        if not is_denominator_eligible(row, exclude_unavailable=exclude_unavailable):
            continue
        row_count += 1
        validated_count += 1 if row.get("validated_bool") else 0
        if row.get("game_id"):
            games.add(str(row.get("game_id")))
    return {"row_count": row_count, "validated_count": validated_count, "game_count": len(games)}


def lookup_expected_rate_for_claim(
    row: Dict[str, Any],
    *,
    all_rows: List[Dict[str, Any]],
    indexes: Dict[str, Dict[Tuple[str, ...], Dict[str, Any]]],
    adjustments: Dict[str, Dict[Tuple[str, Tuple[str, ...]], Dict[str, int]]],
    min_sample_rows: int,
    exclude_unavailable: bool,
) -> Dict[str, Any]:
    game_id = str(row.get("game_id") or "")

    # Diagnostic: exact full signature count after leave-one-game-out.
    full_name, full_fields = SIGNATURE_LEVELS[0]
    full_key = signature_key(row, full_fields)
    full_bucket = indexes[full_name].get(full_key, {"row_count": 0, "validated_count": 0, "games": set()})
    full_adj = adjustments.get(game_id, {}).get((full_name, full_key), {"row_count": 0, "validated_count": 0})
    full_loo_rows = int(full_bucket.get("row_count", 0)) - int(full_adj.get("row_count", 0))
    full_loo_validated = int(full_bucket.get("validated_count", 0)) - int(full_adj.get("validated_count", 0))

    for level_name, fields in SIGNATURE_LEVELS:
        key = signature_key(row, fields)
        bucket = indexes[level_name].get(key)
        if not bucket:
            continue

        adj = adjustments.get(game_id, {}).get((level_name, key), {"row_count": 0, "validated_count": 0})
        loo_rows = int(bucket.get("row_count", 0)) - int(adj.get("row_count", 0))
        loo_validated = int(bucket.get("validated_count", 0)) - int(adj.get("validated_count", 0))

        if loo_rows >= min_sample_rows:
            return {
                "expected_claim_quality_score": round_rate(validation_rate_from_counts(loo_validated, loo_rows)),
                "signature_level_used": level_name,
                "signature_key_used": "|".join(key),
                "signature_rows_available": loo_rows,
                "signature_validated_rows": loo_validated,
                "signature_games_available": max(len(bucket.get("games", set())) - (1 if game_id in bucket.get("games", set()) else 0), 0),
                "full_signature_rows_available": max(full_loo_rows, 0),
                "full_signature_validated_rows": max(full_loo_validated, 0),
                "low_sample_signature_flag": full_loo_rows < min_sample_rows,
                "fallback_used_flag": level_name != "full_signature",
                "global_baseline_used_flag": False,
                "leave_one_game_out_flag": True,
            }

    # Final fallback: global leave-one-game-out baseline.
    global_counts = overall_counts(
        all_rows,
        exclude_game_id=game_id,
        exclude_unavailable=exclude_unavailable,
    )
    score = validation_rate_from_counts(global_counts["validated_count"], global_counts["row_count"])
    return {
        "expected_claim_quality_score": round_rate(score),
        "signature_level_used": "global_baseline",
        "signature_key_used": "global_baseline",
        "signature_rows_available": global_counts["row_count"],
        "signature_validated_rows": global_counts["validated_count"],
        "signature_games_available": global_counts["game_count"],
        "full_signature_rows_available": max(full_loo_rows, 0),
        "full_signature_validated_rows": max(full_loo_validated, 0),
        "low_sample_signature_flag": True,
        "fallback_used_flag": True,
        "global_baseline_used_flag": True,
        "leave_one_game_out_flag": True,
    }


# ---------------------------------------------------------------------------
# Scoring and aggregation
# ---------------------------------------------------------------------------


def expected_quality_bucket(
    score: Optional[float],
    *,
    baseline_rate: Optional[float],
    strong_lift: float,
    supportive_lift: float,
    fragile_lift: float,
) -> str:
    if score is None:
        return "insufficient_sample"

    baseline = baseline_rate if baseline_rate is not None else 0.50

    if score >= max(0.60, baseline + strong_lift):
        return "strong_expected_quality"

    if score >= baseline + supportive_lift:
        return "supportive_expected_quality"

    if score <= baseline + fragile_lift:
        return "fragile_expected_quality"

    return "mixed_expected_quality"


def classify_layer(row: Dict[str, Any]) -> str:
    layer = normalize_token(row.get("claim_layer"))
    source = normalize_token(row.get("source_section"))
    claim_type = normalize_token(row.get("claim_type"))

    combined = " ".join([layer, source, claim_type])
    if any(token in combined for token in ["headline", "summary", "primary", "top"]):
        return "headline"
    return "supporting"


def average(values: Iterable[Optional[float]]) -> Optional[float]:
    nums = [float(v) for v in values if v is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def score_claim_rows(
    rows: List[Dict[str, Any]],
    *,
    min_sample_rows: int,
    exclude_unavailable: bool,
    overall_baseline_rate: Optional[float],
    strong_lift: float,
    supportive_lift: float,
    fragile_lift: float,
) -> List[Dict[str, Any]]:
    indexes = build_signature_indexes(rows, exclude_unavailable=exclude_unavailable)
    adjustments = build_game_adjustment_indexes(rows, exclude_unavailable=exclude_unavailable)

    scored: List[Dict[str, Any]] = []
    for row in rows:
        out = dict(row)
        out["claim_quality_layer"] = classify_layer(row)

        if not is_denominator_eligible(row, exclude_unavailable=exclude_unavailable):
            out.update({
                "expected_claim_quality_score": None,
                "expected_claim_quality_bucket": "not_denominator_eligible",
                "signature_level_used": None,
                "signature_key_used": None,
                "signature_rows_available": 0,
                "signature_validated_rows": 0,
                "signature_games_available": 0,
                "full_signature_rows_available": 0,
                "low_sample_signature_flag": None,
                "fallback_used_flag": None,
                "global_baseline_used_flag": None,
                "leave_one_game_out_flag": True,
            })
            scored.append(out)
            continue

        lookup = lookup_expected_rate_for_claim(
            row,
            all_rows=rows,
            indexes=indexes,
            adjustments=adjustments,
            min_sample_rows=min_sample_rows,
            exclude_unavailable=exclude_unavailable,
        )
        out.update(lookup)
        out["expected_claim_quality_bucket"] = expected_quality_bucket(
            out.get("expected_claim_quality_score"),
            baseline_rate=overall_baseline_rate,
            strong_lift=strong_lift,
            supportive_lift=supportive_lift,
            fragile_lift=fragile_lift,
        )
        scored.append(out)

    return scored


def aggregate_by_game(
    claim_rows: List[Dict[str, Any]],
    *,
    overall_baseline_rate: Optional[float],
    strong_lift: float,
    supportive_lift: float,
    fragile_lift: float,
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in claim_rows:
        game_id = str(row.get("game_id") or "")
        if game_id:
            grouped[game_id].append(row)

    game_rows: List[Dict[str, Any]] = []
    for game_id, rows in sorted(grouped.items()):
        denominator_rows = [r for r in rows if r.get("denominator_eligible")]
        scored_rows = [r for r in denominator_rows if r.get("expected_claim_quality_score") is not None]
        validated_count = sum(1 for r in denominator_rows if r.get("validated_bool"))
        claim_row_count = len(denominator_rows)

        expected_score = average(r.get("expected_claim_quality_score") for r in scored_rows)
        headline_score = average(
            r.get("expected_claim_quality_score")
            for r in scored_rows
            if r.get("claim_quality_layer") == "headline"
        )
        supporting_score = average(
            r.get("expected_claim_quality_score")
            for r in scored_rows
            if r.get("claim_quality_layer") == "supporting"
        )

        sample_rows = [as_int(r.get("signature_rows_available")) or 0 for r in scored_rows]
        low_sample_count = sum(1 for r in scored_rows if r.get("low_sample_signature_flag"))
        fallback_count = sum(1 for r in scored_rows if r.get("fallback_used_flag"))
        global_count = sum(1 for r in scored_rows if r.get("global_baseline_used_flag"))
        missing_count = global_count

        first = rows[0]
        actual_validation_rate = validation_rate_from_counts(validated_count, claim_row_count)
        bucket = expected_quality_bucket(
            expected_score,
            baseline_rate=overall_baseline_rate,
            strong_lift=strong_lift,
            supportive_lift=supportive_lift,
            fragile_lift=fragile_lift,
        )

        coverage_rate = safe_divide(len(scored_rows) - global_count, len(scored_rows))

        game_rows.append({
            "game_id": game_id,
            "season": first.get("season"),
            "game_date": first.get("game_date"),
            "game_week": first.get("game_week"),
            "outcome_confidence_label": first.get("outcome_confidence_label"),
            "profile_strength_label": first.get("profile_strength_label"),
            "profile_type": first.get("profile_type"),
            "matchup_label": first.get("matchup_label"),
            "model_result": first.get("model_result"),
            "model_result_bucket": first.get("model_result_bucket"),
            "final_margin_abs": first.get("final_margin_abs"),
            "final_margin_bucket": first.get("final_margin_bucket"),
            "predicted_team": first.get("predicted_team"),
            "actual_winner": first.get("actual_winner"),
            "qa_read_v2": first.get("qa_read_v2"),
            "confidence_cap_reason": first.get("confidence_cap_reason"),
            "signal_gap": first.get("signal_gap"),
            "core_gap": first.get("core_gap"),
            "core_area_split": first.get("core_area_split"),
            "team_comp_edge_score": first.get("team_comp_edge_score"),
            "team_comp_away_count": first.get("team_comp_away_count"),
            "team_comp_home_count": first.get("team_comp_home_count"),
            "team_comp_neutral_count": first.get("team_comp_neutral_count"),
            "team_comp_total_visible": first.get("team_comp_total_visible"),
            "actual_claim_validation_rate": round_rate(actual_validation_rate),
            "actual_claim_validation_pct": pct_points(actual_validation_rate),
            "validated_claim_rows": validated_count,
            "claim_rows": claim_row_count,
            "claim_rows_scored": len(scored_rows),
            "expected_claim_quality_score": round_rate(expected_score),
            "expected_claim_quality_pct": pct_points(expected_score),
            "expected_claim_quality_bucket": bucket,
            "headline_expected_claim_quality_score": round_rate(headline_score),
            "supporting_expected_claim_quality_score": round_rate(supporting_score),
            "expected_quality_coverage_rate": round_rate(coverage_rate),
            "expected_quality_coverage_pct": pct_points(coverage_rate),
            "avg_signature_rows_available": round_rate(average(sample_rows)),
            "min_signature_rows_available": min(sample_rows) if sample_rows else None,
            "low_sample_signature_rows": low_sample_count,
            "fallback_rows_used": fallback_count,
            "missing_signature_rows": missing_count,
            "global_baseline_rows_used": global_count,
            "global_baseline_used_flag": global_count > 0,
            "leave_one_game_out_flag": True,
        })

    return game_rows


# ---------------------------------------------------------------------------
# Baselines and lift summaries
# ---------------------------------------------------------------------------


def calculate_baseline(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    eligible = [r for r in rows if r.get("denominator_eligible")]
    games = {str(r.get("game_id")) for r in eligible if r.get("game_id")}
    validated = sum(1 for r in eligible if r.get("validated_bool"))
    rate = validation_rate_from_counts(validated, len(eligible))
    return {
        "games": len(games),
        "claim_rows": len(eligible),
        "validated_claim_rows": validated,
        "claim_validation_rate": round_rate(rate),
        "claim_validation_pct": pct_points(rate),
    }


def segment_specs() -> List[Tuple[str, str, Callable[[Dict[str, Any]], bool]]]:
    """Return dynamic segment predicates built from row values later."""
    # Placeholder. Actual specs are created from data in build_segment_specs.
    return []


def distinct_values(rows: List[Dict[str, Any]], field: str) -> List[str]:
    return sorted({str(r.get(field)) for r in rows if clean_string(r.get(field))})


def build_segment_predicates(rows: List[Dict[str, Any]]) -> List[Tuple[str, str, Callable[[Dict[str, Any]], bool]]]:
    specs: List[Tuple[str, str, Callable[[Dict[str, Any]], bool]]] = []

    specs.append(("overall", "All Games", lambda r: True))

    for value in distinct_values(rows, "outcome_confidence_label"):
        specs.append((
            "outcome_confidence",
            value,
            lambda r, value=value: str(r.get("outcome_confidence_label")) == value,
        ))

    for value in distinct_values(rows, "profile_strength_label"):
        specs.append((
            "profile_strength",
            value,
            lambda r, value=value: str(r.get("profile_strength_label")) == value,
        ))

    for value in distinct_values(rows, "profile_type"):
        specs.append((
            "core_area_alignment",
            value,
            lambda r, value=value: str(r.get("profile_type")) == value,
        ))

    # Intersections that directly answer the Admin-page questions.
    confidence_values = distinct_values(rows, "outcome_confidence_label")
    strength_values = distinct_values(rows, "profile_strength_label")
    profile_values = distinct_values(rows, "profile_type")

    for confidence in confidence_values:
        for strength in strength_values:
            label = f"{confidence} × {strength}"
            specs.append((
                "confidence_x_profile_strength",
                label,
                lambda r, confidence=confidence, strength=strength: (
                    str(r.get("outcome_confidence_label")) == confidence
                    and str(r.get("profile_strength_label")) == strength
                ),
            ))

    for confidence in confidence_values:
        for profile_type in profile_values:
            label = f"{confidence} × {profile_type}"
            specs.append((
                "confidence_x_core_area_alignment",
                label,
                lambda r, confidence=confidence, profile_type=profile_type: (
                    str(r.get("outcome_confidence_label")) == confidence
                    and str(r.get("profile_type")) == profile_type
                ),
            ))

    for confidence in confidence_values:
        for strength in strength_values:
            for profile_type in profile_values:
                label = f"{confidence} × {strength} × {profile_type}"
                specs.append((
                    "confidence_x_profile_strength_x_core_area_alignment",
                    label,
                    lambda r, confidence=confidence, strength=strength, profile_type=profile_type: (
                        str(r.get("outcome_confidence_label")) == confidence
                        and str(r.get("profile_strength_label")) == strength
                        and str(r.get("profile_type")) == profile_type
                    ),
                ))

    return specs


def build_claim_rows_with_game_bucket(
    claim_rows: List[Dict[str, Any]],
    game_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    game_lookup = {row["game_id"]: row for row in game_rows}
    joined: List[Dict[str, Any]] = []

    for row in claim_rows:
        game = game_lookup.get(str(row.get("game_id") or ""), {})
        out = dict(row)
        out["game_expected_claim_quality_score"] = game.get("expected_claim_quality_score")
        out["game_expected_claim_quality_bucket"] = game.get("expected_claim_quality_bucket")
        out["game_actual_claim_validation_rate"] = game.get("actual_claim_validation_rate")
        joined.append(out)

    return joined


def summarize_lift(
    claim_rows: List[Dict[str, Any]],
    game_rows: List[Dict[str, Any]],
    *,
    minimum_segment_claim_rows: int,
) -> List[Dict[str, Any]]:
    rows_with_bucket = build_claim_rows_with_game_bucket(claim_rows, game_rows)
    specs = build_segment_predicates(rows_with_bucket)
    buckets = [
        "strong_expected_quality",
        "supportive_expected_quality",
        "mixed_expected_quality",
        "fragile_expected_quality",
        "insufficient_sample",
    ]

    summaries: List[Dict[str, Any]] = []

    for segment_type, segment_name, predicate in specs:
        segment_rows = [r for r in rows_with_bucket if r.get("denominator_eligible") and predicate(r)]
        if not segment_rows:
            continue

        baseline = calculate_baseline(segment_rows)
        if baseline["claim_rows"] < minimum_segment_claim_rows:
            # Avoid emitting a huge amount of meaningless sparse intersections.
            continue

        baseline_rate = baseline["claim_validation_rate"]

        for bucket in buckets:
            bucket_rows = [
                r for r in segment_rows if r.get("game_expected_claim_quality_bucket") == bucket
            ]
            if not bucket_rows:
                continue

            bucket_base = calculate_baseline(bucket_rows)
            bucket_rate = bucket_base["claim_validation_rate"]
            lift = None
            if bucket_rate is not None and baseline_rate is not None:
                lift = bucket_rate - baseline_rate

            game_ids = {str(r.get("game_id")) for r in bucket_rows if r.get("game_id")}
            bucket_game_rows = [g for g in game_rows if str(g.get("game_id")) in game_ids]

            summaries.append({
                "segment_type": segment_type,
                "segment_name": segment_name,
                "expected_quality_bucket": bucket,
                "games": len(game_ids),
                "claim_rows": bucket_base["claim_rows"],
                "validated_claim_rows": bucket_base["validated_claim_rows"],
                "baseline_claim_validation_rate": baseline_rate,
                "baseline_claim_validation_pct": baseline["claim_validation_pct"],
                "bucket_claim_validation_rate": bucket_rate,
                "bucket_claim_validation_pct": bucket_base["claim_validation_pct"],
                "lift_vs_baseline": round_rate(lift),
                "lift_vs_baseline_pct_points": pct_points(lift),
                "avg_expected_claim_quality_score": round_rate(
                    average(g.get("expected_claim_quality_score") for g in bucket_game_rows)
                ),
                "avg_actual_claim_validation_rate": round_rate(
                    average(g.get("actual_claim_validation_rate") for g in bucket_game_rows)
                ),
                "sample_warning": sample_warning(bucket_base["claim_rows"], len(game_ids)),
            })

    return sorted(
        summaries,
        key=lambda r: (
            r["segment_type"],
            r["segment_name"],
            bucket_sort_key(str(r["expected_quality_bucket"])),
        ),
    )


def bucket_sort_key(bucket: str) -> int:
    order = {
        "strong_expected_quality": 1,
        "supportive_expected_quality": 2,
        "mixed_expected_quality": 3,
        "fragile_expected_quality": 4,
        "insufficient_sample": 5,
    }
    return order.get(bucket, 99)


def sample_warning(claim_rows: int, games: int) -> str:
    if games < 5 or claim_rows < 50:
        return "low_sample_review_only"
    if games < 10 or claim_rows < 100:
        return "moderate_sample_use_caution"
    return "sufficient_sample"


# ---------------------------------------------------------------------------
# v0.3 outcome / confidence calibration helpers
# ---------------------------------------------------------------------------


def wilson_interval(successes: int, total: int, z: float = 1.96) -> Tuple[Optional[float], Optional[float]]:
    """Approximate 95% Wilson interval for binomial rates.

    This is review-only uncertainty context. It prevents tiny game groups from
    looking more precise than they are.
    """
    if total <= 0:
        return None, None

    phat = successes / total
    denom = 1 + (z * z / total)
    centre = phat + (z * z / (2 * total))
    margin = z * math.sqrt((phat * (1 - phat) + (z * z / (4 * total))) / total)
    low = (centre - margin) / denom
    high = (centre + margin) / denom
    return max(0.0, low), min(1.0, high)


def annotate_confidence_rank_groups(game_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Add within-confidence quality rank fields to game rows.

    Why this exists:
    - The 2025 High Confidence sample is naturally small.
    - Fixed global buckets can leave a tiny High/Mixed group.
    - Within-confidence rank groups let us test whether lower-quality High games
      perform worse than higher-quality High games without inventing runtime
      thresholds.
    """
    out = [dict(row) for row in game_rows]
    by_confidence: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for row in out:
        score = as_float(row.get("expected_claim_quality_score"))
        confidence = normalize_token(row.get("outcome_confidence_label"))
        if confidence and score is not None:
            by_confidence[confidence].append(row)

    for confidence, rows in by_confidence.items():
        rows.sort(key=lambda r: (as_float(r.get("expected_claim_quality_score")) or -1.0, str(r.get("game_id") or "")))
        n = len(rows)
        if n <= 0:
            continue

        for ix, row in enumerate(rows):
            rank = ix + 1
            rank_pct = 1.0 if n == 1 else ix / (n - 1)
            row["expected_quality_confidence_rank"] = rank
            row["expected_quality_confidence_rank_pct"] = round_rate(rank_pct)

            # Bottom/top half uses all available games and is the most stable
            # small-sample view.
            midpoint = n / 2.0
            row["expected_quality_confidence_half"] = (
                "bottom_half_expected_quality" if ix < midpoint else "top_half_expected_quality"
            )

            # Tertile is useful for shape. Keep group sizes reasonable without
            # using it as a hard runtime rule.
            if n >= 6:
                lower_end = n // 3
                upper_start = n - (n // 3)
                if ix < lower_end:
                    tertile = "bottom_third_expected_quality"
                elif ix >= upper_start:
                    tertile = "top_third_expected_quality"
                else:
                    tertile = "middle_third_expected_quality"
            else:
                tertile = "insufficient_games_for_tertile"
            row["expected_quality_confidence_tertile"] = tertile

    return out


def summarize_game_outcomes(games: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Summarize outcome calibration and claim validation for game groups."""
    games = [g for g in games if g]
    correct = sum(1 for g in games if normalize_token(g.get("model_result_bucket")) == "correct")
    incorrect = sum(1 for g in games if normalize_token(g.get("model_result_bucket")) == "incorrect")
    no_pick = sum(1 for g in games if normalize_token(g.get("model_result_bucket")) == "no_pick")
    tie_or_push = sum(1 for g in games if normalize_token(g.get("model_result_bucket")) == "tie_or_push")
    graded = correct + incorrect

    correct_rate = safe_divide(correct, graded)
    correct_low, correct_high = wilson_interval(correct, graded)

    claim_rows = sum(as_int(g.get("claim_rows")) or 0 for g in games)
    validated_claim_rows = sum(as_int(g.get("validated_claim_rows")) or 0 for g in games)
    claim_validation_rate = safe_divide(validated_claim_rows, claim_rows)
    claim_low, claim_high = wilson_interval(validated_claim_rows, claim_rows)

    material_or_severe = sum(
        1 for g in games
        if normalize_token(g.get("final_margin_bucket")) in {"material", "severe"}
    )
    close_or_one_score = sum(
        1 for g in games
        if normalize_token(g.get("final_margin_bucket")) in {"close", "one_score"}
    )

    return {
        "games": len(games),
        "graded_games": graded,
        "correct_games": correct,
        "incorrect_games": incorrect,
        "no_pick_games": no_pick,
        "tie_or_push_games": tie_or_push,
        "correct_game_rate": round_rate(correct_rate),
        "correct_game_rate_pct": pct_points(correct_rate),
        "correct_game_rate_ci_low": round_rate(correct_low),
        "correct_game_rate_ci_high": round_rate(correct_high),
        "correct_game_rate_ci_low_pct": pct_points(correct_low),
        "correct_game_rate_ci_high_pct": pct_points(correct_high),
        "claim_rows": claim_rows,
        "validated_claim_rows": validated_claim_rows,
        "claim_validation_rate": round_rate(claim_validation_rate),
        "claim_validation_pct": pct_points(claim_validation_rate),
        "claim_validation_ci_low": round_rate(claim_low),
        "claim_validation_ci_high": round_rate(claim_high),
        "claim_validation_ci_low_pct": pct_points(claim_low),
        "claim_validation_ci_high_pct": pct_points(claim_high),
        "avg_expected_claim_quality_score": round_rate(average(g.get("expected_claim_quality_score") for g in games)),
        "avg_actual_claim_validation_rate": round_rate(average(g.get("actual_claim_validation_rate") for g in games)),
        "avg_final_margin_abs": round_rate(average(as_float(g.get("final_margin_abs")) for g in games)),
        "material_or_severe_margin_games": material_or_severe,
        "close_or_one_score_games": close_or_one_score,
        "sample_warning": sample_warning(claim_rows, len(games)),
    }


def summarize_high_confidence_half_split(
    claim_rows: List[Dict[str, Any]],
    game_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Stable top-half vs bottom-half High Confidence diagnostic."""
    high_games = [
        g for g in game_rows
        if normalize_token(g.get("outcome_confidence_label")) == "high"
        and clean_string(g.get("expected_quality_confidence_half"))
    ]
    if not high_games:
        return []

    high_summary = summarize_game_outcomes(high_games)
    high_correct_rate = high_summary["correct_game_rate"]
    high_claim_rate = high_summary["claim_validation_rate"]

    rows: List[Dict[str, Any]] = []
    for group in ["bottom_half_expected_quality", "top_half_expected_quality"]:
        games = [g for g in high_games if g.get("expected_quality_confidence_half") == group]
        if not games:
            continue
        summary = summarize_game_outcomes(games)
        rows.append({
            "outcome_confidence_label": "High",
            "ranked_quality_group": group,
            **summary,
            "high_confidence_baseline_correct_game_rate": high_correct_rate,
            "high_confidence_baseline_claim_validation_rate": high_claim_rate,
            "lift_correct_rate_vs_high_baseline": round_rate(
                (summary["correct_game_rate"] - high_correct_rate)
                if summary["correct_game_rate"] is not None and high_correct_rate is not None
                else None
            ),
            "lift_claim_validation_vs_high_baseline": round_rate(
                (summary["claim_validation_rate"] - high_claim_rate)
                if summary["claim_validation_rate"] is not None and high_claim_rate is not None
                else None
            ),
            "lift_correct_rate_vs_high_baseline_pct_points": pct_points(
                (summary["correct_game_rate"] - high_correct_rate)
                if summary["correct_game_rate"] is not None and high_correct_rate is not None
                else None
            ),
            "lift_claim_validation_vs_high_baseline_pct_points": pct_points(
                (summary["claim_validation_rate"] - high_claim_rate)
                if summary["claim_validation_rate"] is not None and high_claim_rate is not None
                else None
            ),
        })
    return rows


def summarize_expected_quality_outcome_lift(
    game_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Measure whether expected quality improves outcome calibration too.

    v0.1 primarily measured claim-validation lift. v0.3 adds outcome lift because
    the product question is whether High/Medium labels are calibrated, not only
    whether explanation rows validate.
    """
    specs = build_segment_predicates(game_rows)
    buckets = [
        "strong_expected_quality",
        "supportive_expected_quality",
        "mixed_expected_quality",
        "fragile_expected_quality",
        "insufficient_sample",
    ]

    rows: List[Dict[str, Any]] = []
    for segment_type, segment_name, predicate in specs:
        segment_games = [g for g in game_rows if predicate(g)]
        if not segment_games:
            continue

        baseline = summarize_game_outcomes(segment_games)
        # Avoid an unreadable file full of tiny empty intersections.
        if baseline["games"] < 3 and baseline["claim_rows"] < 50:
            continue

        for bucket in buckets:
            bucket_games = [g for g in segment_games if g.get("expected_claim_quality_bucket") == bucket]
            if not bucket_games:
                continue
            summary = summarize_game_outcomes(bucket_games)

            correct_lift = None
            if summary["correct_game_rate"] is not None and baseline["correct_game_rate"] is not None:
                correct_lift = summary["correct_game_rate"] - baseline["correct_game_rate"]

            claim_lift = None
            if summary["claim_validation_rate"] is not None and baseline["claim_validation_rate"] is not None:
                claim_lift = summary["claim_validation_rate"] - baseline["claim_validation_rate"]

            rows.append({
                "segment_type": segment_type,
                "segment_name": segment_name,
                "expected_quality_bucket": bucket,
                "baseline_games": baseline["games"],
                "baseline_graded_games": baseline["graded_games"],
                "baseline_correct_game_rate": baseline["correct_game_rate"],
                "baseline_correct_game_rate_pct": baseline["correct_game_rate_pct"],
                "baseline_claim_validation_rate": baseline["claim_validation_rate"],
                "baseline_claim_validation_pct": baseline["claim_validation_pct"],
                **summary,
                "lift_correct_rate_vs_segment_baseline": round_rate(correct_lift),
                "lift_correct_rate_vs_segment_baseline_pct_points": pct_points(correct_lift),
                "lift_claim_validation_vs_segment_baseline": round_rate(claim_lift),
                "lift_claim_validation_vs_segment_baseline_pct_points": pct_points(claim_lift),
            })

    return sorted(
        rows,
        key=lambda r: (
            r["segment_type"],
            r["segment_name"],
            bucket_sort_key(str(r["expected_quality_bucket"])),
        ),
    )


def summarize_confidence_relabel_audit(game_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Review-only simulated relabel groups.

    This does NOT recommend production changes. It asks whether Expected Claim
    Quality can identify:
    - current High games that look like keep candidates
    - current High games that look like downgrade candidates
    - current Medium games that might have deserved stronger consideration
    """
    def conf(row: Dict[str, Any], value: str) -> bool:
        return normalize_token(row.get("outcome_confidence_label")) == value

    def supportive(row: Dict[str, Any]) -> bool:
        return row.get("expected_claim_quality_bucket") in {
            "strong_expected_quality",
            "supportive_expected_quality",
        }

    def mixed_or_fragile(row: Dict[str, Any]) -> bool:
        return row.get("expected_claim_quality_bucket") in {
            "mixed_expected_quality",
            "fragile_expected_quality",
        }

    def strong_shape(row: Dict[str, Any]) -> bool:
        return (
            normalize_token(row.get("profile_strength_label")) == "strong profile"
            or normalize_token(row.get("profile_type")) == "confirmed_edge"
        )

    group_defs: List[Tuple[str, str, Callable[[Dict[str, Any]], bool]]] = [
        ("current_high_all", "All current High Confidence games", lambda g: conf(g, "high")),
        (
            "current_high_keep_candidate_supportive_quality",
            "Current High + supportive/strong expected claim quality",
            lambda g: conf(g, "high") and supportive(g),
        ),
        (
            "current_high_downgrade_candidate_mixed_quality",
            "Current High + mixed/fragile expected claim quality",
            lambda g: conf(g, "high") and mixed_or_fragile(g),
        ),
        (
            "current_high_top_half_quality",
            "Current High + top-half expected quality within High",
            lambda g: conf(g, "high") and g.get("expected_quality_confidence_half") == "top_half_expected_quality",
        ),
        (
            "current_high_bottom_half_quality",
            "Current High + bottom-half expected quality within High",
            lambda g: conf(g, "high") and g.get("expected_quality_confidence_half") == "bottom_half_expected_quality",
        ),
        ("current_medium_all", "All current Medium Confidence games", lambda g: conf(g, "medium")),
        (
            "medium_promotion_candidate_supportive_quality_strong_shape",
            "Current Medium + supportive quality + Strong Profile or Confirmed Edge",
            lambda g: conf(g, "medium") and supportive(g) and strong_shape(g),
        ),
        (
            "medium_top_half_quality",
            "Current Medium + top-half expected quality within Medium",
            lambda g: conf(g, "medium") and g.get("expected_quality_confidence_half") == "top_half_expected_quality",
        ),
        (
            "medium_bottom_half_quality",
            "Current Medium + bottom-half expected quality within Medium",
            lambda g: conf(g, "medium") and g.get("expected_quality_confidence_half") == "bottom_half_expected_quality",
        ),
    ]

    rows: List[Dict[str, Any]] = []
    for group_key, description, predicate in group_defs:
        games = [g for g in game_rows if predicate(g)]
        if not games:
            continue
        rows.append({
            "audit_group": group_key,
            "description": description,
            **summarize_game_outcomes(games),
        })
    return rows


def summarize_profile_conditioned_confidence_audit(game_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Compare Medium vs High inside profile families.

    This directly answers the user's calibration standard:
    inside Confirmed Edge or Strong Profile, High should be at least close to
    Medium and preferably better. If retained High improves while downgraded
    High weakens, Expected Claim Quality may be a useful calibration tool.
    """
    dimensions: List[Tuple[str, str]] = [
        ("profile_type", "Core Area Alignment"),
        ("profile_strength_label", "Profile Strength"),
    ]

    segment_defs: List[Tuple[str, Callable[[Dict[str, Any]], bool]]] = [
        ("current_low", lambda g: normalize_token(g.get("outcome_confidence_label")) == "low"),
        ("current_medium", lambda g: normalize_token(g.get("outcome_confidence_label")) == "medium"),
        ("current_high", lambda g: normalize_token(g.get("outcome_confidence_label")) == "high"),
        (
            "retained_high_supportive_quality",
            lambda g: normalize_token(g.get("outcome_confidence_label")) == "high"
            and g.get("expected_claim_quality_bucket") in {"strong_expected_quality", "supportive_expected_quality"},
        ),
        (
            "downgrade_candidate_high_mixed_quality",
            lambda g: normalize_token(g.get("outcome_confidence_label")) == "high"
            and g.get("expected_claim_quality_bucket") in {"mixed_expected_quality", "fragile_expected_quality"},
        ),
        (
            "retained_high_top_half_quality",
            lambda g: normalize_token(g.get("outcome_confidence_label")) == "high"
            and g.get("expected_quality_confidence_half") == "top_half_expected_quality",
        ),
        (
            "downgrade_candidate_high_bottom_half_quality",
            lambda g: normalize_token(g.get("outcome_confidence_label")) == "high"
            and g.get("expected_quality_confidence_half") == "bottom_half_expected_quality",
        ),
        (
            "medium_promotion_candidate_supportive_quality",
            lambda g: normalize_token(g.get("outcome_confidence_label")) == "medium"
            and g.get("expected_claim_quality_bucket") in {"strong_expected_quality", "supportive_expected_quality"},
        ),
        (
            "medium_top_half_quality",
            lambda g: normalize_token(g.get("outcome_confidence_label")) == "medium"
            and g.get("expected_quality_confidence_half") == "top_half_expected_quality",
        ),
    ]

    rows: List[Dict[str, Any]] = []

    for field, dimension_label in dimensions:
        for value in distinct_values(game_rows, field):
            profile_games = [g for g in game_rows if str(g.get(field)) == value]
            if not profile_games:
                continue

            # Baselines inside the profile family.
            medium_summary = summarize_game_outcomes([
                g for g in profile_games
                if normalize_token(g.get("outcome_confidence_label")) == "medium"
            ])
            high_summary = summarize_game_outcomes([
                g for g in profile_games
                if normalize_token(g.get("outcome_confidence_label")) == "high"
            ])

            medium_rate = medium_summary.get("correct_game_rate")
            high_rate = high_summary.get("correct_game_rate")

            for segment_name, predicate in segment_defs:
                games = [g for g in profile_games if predicate(g)]
                if not games:
                    continue
                summary = summarize_game_outcomes(games)
                current_rate = summary.get("correct_game_rate")
                rows.append({
                    "profile_dimension": field,
                    "profile_dimension_label": dimension_label,
                    "profile_value": value,
                    "audit_segment": segment_name,
                    "profile_medium_games": medium_summary.get("games"),
                    "profile_medium_correct_rate": medium_rate,
                    "profile_high_games": high_summary.get("games"),
                    "profile_high_correct_rate": high_rate,
                    "current_high_minus_medium_correct_rate": round_rate(
                        (high_rate - medium_rate)
                        if high_rate is not None and medium_rate is not None
                        else None
                    ),
                    "segment_minus_medium_correct_rate": round_rate(
                        (current_rate - medium_rate)
                        if current_rate is not None and medium_rate is not None
                        else None
                    ),
                    "segment_minus_current_high_correct_rate": round_rate(
                        (current_rate - high_rate)
                        if current_rate is not None and high_rate is not None
                        else None
                    ),
                    **summary,
                })

    return rows


def summarize_high_confidence_split(
    claim_rows: List[Dict[str, Any]],
    game_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    high_games = [
        g for g in game_rows if normalize_token(g.get("outcome_confidence_label")) == "high"
    ]
    high_game_ids = {str(g.get("game_id")) for g in high_games}
    high_claim_rows = [
        r for r in claim_rows
        if r.get("denominator_eligible") and str(r.get("game_id")) in high_game_ids
    ]

    high_baseline = calculate_baseline(high_claim_rows)
    high_baseline_rate = high_baseline["claim_validation_rate"]

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for game in high_games:
        grouped[str(game.get("expected_claim_quality_bucket"))].append(game)

    rows: List[Dict[str, Any]] = []
    for bucket, games in sorted(grouped.items(), key=lambda item: bucket_sort_key(item[0])):
        game_ids = {str(g.get("game_id")) for g in games}
        bucket_claims = [r for r in high_claim_rows if str(r.get("game_id")) in game_ids]
        baseline = calculate_baseline(bucket_claims)
        rate = baseline["claim_validation_rate"]
        lift = rate - high_baseline_rate if rate is not None and high_baseline_rate is not None else None

        correct_games = sum(1 for g in games if normalize_token(g.get("model_result_bucket")) == "correct")
        incorrect_games = sum(1 for g in games if normalize_token(g.get("model_result_bucket")) == "incorrect")
        no_pick_games = sum(1 for g in games if normalize_token(g.get("model_result_bucket")) == "no_pick")
        severe_or_material = sum(
            1 for g in games
            if normalize_token(g.get("final_margin_bucket")) in {"severe", "material"}
        )

        rows.append({
            "outcome_confidence_label": "High",
            "expected_quality_bucket": bucket,
            "games": len(games),
            "correct_games": correct_games,
            "incorrect_games": incorrect_games,
            "no_pick_games": no_pick_games,
            "correct_game_rate": round_rate(safe_divide(correct_games, correct_games + incorrect_games)),
            "claim_rows": baseline["claim_rows"],
            "validated_claim_rows": baseline["validated_claim_rows"],
            "high_confidence_baseline_claim_validation_rate": high_baseline_rate,
            "bucket_claim_validation_rate": rate,
            "lift_vs_high_confidence_baseline": round_rate(lift),
            "lift_vs_high_confidence_baseline_pct_points": pct_points(lift),
            "avg_expected_claim_quality_score": round_rate(
                average(g.get("expected_claim_quality_score") for g in games)
            ),
            "avg_actual_claim_validation_rate": round_rate(
                average(g.get("actual_claim_validation_rate") for g in games)
            ),
            "avg_final_margin_abs": round_rate(average(as_float(g.get("final_margin_abs")) for g in games)),
            "material_or_severe_margin_games": severe_or_material,
            "sample_warning": sample_warning(baseline["claim_rows"], len(games)),
        })

    return rows




def summarize_high_confidence_ranked_split(
    claim_rows: List[Dict[str, Any]],
    game_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Diagnostic-only split for the core question:

        Inside High Confidence games, does Expected Claim Quality separate
        stronger High reads from fragile High reads?

    This intentionally does NOT replace the global expected_quality_bucket.
    The global bucket is useful for Admin-style comparability, but High
    Confidence has a small sample. A within-High ranked split gives a clearer
    diagnostic without forcing new runtime thresholds.
    """
    high_games = [
        g for g in game_rows
        if normalize_token(g.get("outcome_confidence_label")) == "high"
        and as_float(g.get("expected_claim_quality_score")) is not None
    ]

    high_games = sorted(
        high_games,
        key=lambda g: (
            as_float(g.get("expected_claim_quality_score")) or -1.0,
            str(g.get("game_id") or ""),
        ),
    )

    if not high_games:
        return []

    high_game_ids = {str(g.get("game_id")) for g in high_games}
    high_claim_rows = [
        r for r in claim_rows
        if r.get("denominator_eligible") and str(r.get("game_id")) in high_game_ids
    ]

    high_baseline = calculate_baseline(high_claim_rows)
    high_baseline_rate = high_baseline["claim_validation_rate"]

    n = len(high_games)
    groups: List[Tuple[str, List[Dict[str, Any]]]] = []

    if n >= 15:
        # Tertiles make the "fragile/middle/stronger" shape more visible for
        # the 2025 High Confidence sample while keeping group sizes reasonable.
        lower_end = n // 3
        upper_start = n - (n // 3)
        groups = [
            ("bottom_third_expected_quality", high_games[:lower_end]),
            ("middle_third_expected_quality", high_games[lower_end:upper_start]),
            ("top_third_expected_quality", high_games[upper_start:]),
        ]
    else:
        midpoint = n // 2
        groups = [
            ("bottom_half_expected_quality", high_games[:midpoint]),
            ("top_half_expected_quality", high_games[midpoint:]),
        ]

    rows: List[Dict[str, Any]] = []

    for group_name, games in groups:
        if not games:
            continue

        game_ids = {str(g.get("game_id")) for g in games}
        bucket_claims = [r for r in high_claim_rows if str(r.get("game_id")) in game_ids]
        baseline = calculate_baseline(bucket_claims)
        rate = baseline["claim_validation_rate"]
        lift = rate - high_baseline_rate if rate is not None and high_baseline_rate is not None else None

        correct_games = sum(1 for g in games if normalize_token(g.get("model_result_bucket")) == "correct")
        incorrect_games = sum(1 for g in games if normalize_token(g.get("model_result_bucket")) == "incorrect")
        no_pick_games = sum(1 for g in games if normalize_token(g.get("model_result_bucket")) == "no_pick")
        severe_or_material = sum(
            1 for g in games
            if normalize_token(g.get("final_margin_bucket")) in {"severe", "material"}
        )

        scores = [as_float(g.get("expected_claim_quality_score")) for g in games]
        scores = [s for s in scores if s is not None]

        graded_games = correct_games + incorrect_games
        tie_or_push_games = sum(1 for g in games if normalize_token(g.get("model_result_bucket")) == "tie_or_push")
        correct_low, correct_high = wilson_interval(correct_games, graded_games)
        claim_low, claim_high = wilson_interval(baseline["validated_claim_rows"], baseline["claim_rows"])
        correct_rate = safe_divide(correct_games, graded_games)

        rows.append({
            "outcome_confidence_label": "High",
            "ranked_quality_group": group_name,
            "games": len(games),
            "graded_games": graded_games,
            "correct_games": correct_games,
            "incorrect_games": incorrect_games,
            "no_pick_games": no_pick_games,
            "tie_or_push_games": tie_or_push_games,
            "correct_game_rate": round_rate(correct_rate),
            "correct_game_rate_pct": pct_points(correct_rate),
            "correct_game_rate_ci_low": round_rate(correct_low),
            "correct_game_rate_ci_high": round_rate(correct_high),
            "correct_game_rate_ci_low_pct": pct_points(correct_low),
            "correct_game_rate_ci_high_pct": pct_points(correct_high),
            "claim_rows": baseline["claim_rows"],
            "validated_claim_rows": baseline["validated_claim_rows"],
            "high_confidence_baseline_claim_validation_rate": high_baseline_rate,
            "group_claim_validation_rate": rate,
            "claim_validation_ci_low": round_rate(claim_low),
            "claim_validation_ci_high": round_rate(claim_high),
            "claim_validation_ci_low_pct": pct_points(claim_low),
            "claim_validation_ci_high_pct": pct_points(claim_high),
            "lift_vs_high_confidence_baseline": round_rate(lift),
            "lift_vs_high_confidence_baseline_pct_points": pct_points(lift),
            "avg_expected_claim_quality_score": round_rate(average(scores)),
            "min_expected_claim_quality_score": round_rate(min(scores) if scores else None),
            "max_expected_claim_quality_score": round_rate(max(scores) if scores else None),
            "avg_actual_claim_validation_rate": round_rate(
                average(g.get("actual_claim_validation_rate") for g in games)
            ),
            "avg_final_margin_abs": round_rate(average(as_float(g.get("final_margin_abs")) for g in games)),
            "material_or_severe_margin_games": severe_or_material,
            "sample_warning": sample_warning(baseline["claim_rows"], len(games)),
        })

    return rows




# ---------------------------------------------------------------------------
# v0.3.1 diagnostics / trash-check helpers
# ---------------------------------------------------------------------------


def numeric_values(values: Iterable[Any]) -> List[float]:
    out: List[float] = []
    for value in values:
        number = as_float(value)
        if number is not None:
            out.append(number)
    return out


def stddev(values: Iterable[Any]) -> Optional[float]:
    nums = numeric_values(values)
    if len(nums) < 2:
        return None
    mean = sum(nums) / len(nums)
    variance = sum((x - mean) ** 2 for x in nums) / (len(nums) - 1)
    return math.sqrt(variance)


def pearson_correlation(xs: List[float], ys: List[float]) -> Optional[float]:
    if len(xs) != len(ys) or len(xs) < 3:
        return None
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


def rank_values(values: List[float]) -> List[float]:
    """Average ranks for ties, 1-indexed."""
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i
        while j + 1 < len(indexed) and indexed[j + 1][1] == indexed[i][1]:
            j += 1
        avg_rank = (i + 1 + j + 1) / 2.0
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = avg_rank
        i = j + 1
    return ranks


def spearman_correlation(xs: List[float], ys: List[float]) -> Optional[float]:
    if len(xs) != len(ys) or len(xs) < 3:
        return None
    return pearson_correlation(rank_values(xs), rank_values(ys))


def summarize_driver_context(games: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "avg_signal_gap": round_rate(average(as_float(g.get("signal_gap")) for g in games)),
        "avg_core_gap": round_rate(average(as_float(g.get("core_gap")) for g in games)),
        "avg_team_comp_edge_score": round_rate(average(as_float(g.get("team_comp_edge_score")) for g in games)),
        "avg_team_comp_total_visible": round_rate(average(as_float(g.get("team_comp_total_visible")) for g in games)),
        "avg_team_comp_neutral_count": round_rate(average(as_float(g.get("team_comp_neutral_count")) for g in games)),
        "most_common_core_area_split": most_common_value(g.get("core_area_split") for g in games),
        "most_common_final_margin_bucket": most_common_value(g.get("final_margin_bucket") for g in games),
        "most_common_profile_type": most_common_value(g.get("profile_type") for g in games),
        "most_common_profile_strength_label": most_common_value(g.get("profile_strength_label") for g in games),
    }


def most_common_value(values: Iterable[Any]) -> Optional[str]:
    counts: Dict[str, int] = defaultdict(int)
    for value in values:
        text = clean_string(value)
        if text:
            counts[text] += 1
    if not counts:
        return None
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def summarize_confidence_label_integrity_audit(game_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Expose odd confidence/result combinations before analysis.

    Example: High Confidence + No Pick / No Decision should not silently flow
    through calibration review without being visible.
    """
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for game in game_rows:
        confidence = clean_string(game.get("outcome_confidence_label")) or "missing"
        result = clean_string(game.get("model_result_bucket")) or "missing"
        grouped[(confidence, result)].append(game)

    rows: List[Dict[str, Any]] = []
    for (confidence, result), games in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1])):
        summary = summarize_game_outcomes(games)
        rows.append({
            "outcome_confidence_label": confidence,
            "model_result_bucket": result,
            "integrity_flag": (
                "review_high_non_graded" if normalize_token(confidence) == "high" and result in {"no_pick", "tie_or_push"}
                else "review_missing_confidence" if normalize_token(confidence) == "missing"
                else "ok"
            ),
            **summary,
            **summarize_driver_context(games),
        })
    return rows


def monotonicity_read(*, games: int, spearman_claim: Optional[float], spearman_correct: Optional[float]) -> str:
    if games < 10:
        return "low_sample_review_only"
    values = [v for v in [spearman_claim, spearman_correct] if v is not None]
    if not values:
        return "insufficient_variation"
    if any(v <= -0.10 for v in values):
        return "non_monotonic_caution"
    if all(v >= 0.10 for v in values):
        return "directionally_monotonic"
    return "weak_or_mixed_monotonicity"


def monotonicity_use_flag(read: str, games: int) -> Tuple[bool, str]:
    """Guardrail for whether a segment should influence confidence relabeling.

    This is not runtime logic. It is an analysis safety label so we do not
    accidentally treat a non-monotonic score as evidence for promotions or
    downgrades.
    """
    if games < 10:
        return False, "Too few games for confidence relabeling evidence."
    if read == "directionally_monotonic":
        return True, "Directionally monotonic; can be reviewed as supporting evidence, not a rule."
    if read == "non_monotonic_caution":
        return False, "Non-monotonic; do not use raw expected-quality rank for relabeling."
    if read == "weak_or_mixed_monotonicity":
        return False, "Weak or mixed monotonicity; review only."
    return False, "Insufficient variation or unsupported monotonicity evidence."


def monotonicity_row(segment_type: str, segment_name: str, games: List[Dict[str, Any]]) -> Dict[str, Any]:
    scored = [g for g in games if as_float(g.get("expected_claim_quality_score")) is not None]
    claim_pairs: List[Tuple[float, float]] = []
    correct_pairs: List[Tuple[float, float]] = []

    for g in scored:
        score = as_float(g.get("expected_claim_quality_score"))
        claim_rate = as_float(g.get("actual_claim_validation_rate"))
        result = normalize_token(g.get("model_result_bucket"))
        if score is not None and claim_rate is not None:
            claim_pairs.append((score, claim_rate))
        if score is not None and result in {"correct", "incorrect"}:
            correct_pairs.append((score, 1.0 if result == "correct" else 0.0))

    claim_x = [x for x, _ in claim_pairs]
    claim_y = [y for _, y in claim_pairs]
    correct_x = [x for x, _ in correct_pairs]
    correct_y = [y for _, y in correct_pairs]

    spearman_claim = spearman_correlation(claim_x, claim_y)
    spearman_correct = spearman_correlation(correct_x, correct_y)
    read = monotonicity_read(
        games=len(games),
        spearman_claim=spearman_claim,
        spearman_correct=spearman_correct,
    )
    use_for_relabeling, use_note = monotonicity_use_flag(read, len(games))

    return {
        "segment_type": segment_type,
        "segment_name": segment_name,
        "games": len(games),
        "scored_games": len(scored),
        "claim_validation_pairs": len(claim_pairs),
        "correctness_pairs": len(correct_pairs),
        "score_min": round_rate(min(claim_x) if claim_x else None),
        "score_max": round_rate(max(claim_x) if claim_x else None),
        "score_avg": round_rate(average(claim_x)),
        "score_stddev": round_rate(stddev(claim_x)),
        "avg_actual_claim_validation_rate": round_rate(average(claim_y)),
        "pearson_score_vs_claim_validation": round_rate(pearson_correlation(claim_x, claim_y)),
        "spearman_score_vs_claim_validation": round_rate(spearman_claim),
        "pearson_score_vs_correct": round_rate(pearson_correlation(correct_x, correct_y)),
        "spearman_score_vs_correct": round_rate(spearman_correct),
        "monotonicity_read": read,
        "use_for_confidence_relabeling": use_for_relabeling,
        "confidence_relabeling_note": use_note,
    }


def summarize_expected_quality_monotonicity_audit(game_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    specs: List[Tuple[str, str, Callable[[Dict[str, Any]], bool]]] = [
        ("overall", "all_games", lambda g: True),
    ]

    for value in distinct_values(game_rows, "outcome_confidence_label"):
        specs.append(("confidence", value, lambda g, value=value: str(g.get("outcome_confidence_label")) == value))

    for value in distinct_values(game_rows, "profile_type"):
        specs.append(("profile_type", value, lambda g, value=value: str(g.get("profile_type")) == value))

    for value in distinct_values(game_rows, "profile_strength_label"):
        specs.append(("profile_strength", value, lambda g, value=value: str(g.get("profile_strength_label")) == value))

    # Explicit high-confidence profile intersections because that is the key question.
    for profile_type in distinct_values(game_rows, "profile_type"):
        specs.append((
            "high_confidence_x_profile_type",
            f"High | {profile_type}",
            lambda g, profile_type=profile_type: normalize_token(g.get("outcome_confidence_label")) == "high" and str(g.get("profile_type")) == profile_type,
        ))

    rows: List[Dict[str, Any]] = []
    seen = set()
    for segment_type, segment_name, predicate in specs:
        games = [g for g in game_rows if predicate(g)]
        key = (segment_type, segment_name)
        if key in seen or not games:
            continue
        seen.add(key)
        rows.append(monotonicity_row(segment_type, segment_name, games))
    return rows


def summarize_high_confidence_miss_review(game_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = [
        g for g in game_rows
        if normalize_token(g.get("outcome_confidence_label")) == "high"
        and normalize_token(g.get("model_result_bucket")) != "correct"
    ]
    out: List[Dict[str, Any]] = []
    for g in sorted(rows, key=lambda r: (normalize_token(r.get("model_result_bucket")), -(as_float(r.get("expected_claim_quality_score")) or -1.0), str(r.get("game_id") or ""))):
        out.append({
            "game_id": g.get("game_id"),
            "game_date": g.get("game_date"),
            "game_week": g.get("game_week"),
            "outcome_confidence_label": g.get("outcome_confidence_label"),
            "profile_strength_label": g.get("profile_strength_label"),
            "profile_type": g.get("profile_type"),
            "model_result": g.get("model_result"),
            "model_result_bucket": g.get("model_result_bucket"),
            "predicted_team": g.get("predicted_team"),
            "actual_winner": g.get("actual_winner"),
            "final_margin_abs": g.get("final_margin_abs"),
            "final_margin_bucket": g.get("final_margin_bucket"),
            "actual_claim_validation_rate": g.get("actual_claim_validation_rate"),
            "expected_claim_quality_score": g.get("expected_claim_quality_score"),
            "expected_claim_quality_bucket": g.get("expected_claim_quality_bucket"),
            "expected_quality_confidence_rank": g.get("expected_quality_confidence_rank"),
            "expected_quality_confidence_rank_pct": g.get("expected_quality_confidence_rank_pct"),
            "expected_quality_confidence_half": g.get("expected_quality_confidence_half"),
            "expected_quality_confidence_tertile": g.get("expected_quality_confidence_tertile"),
            "claim_rows": g.get("claim_rows"),
            "validated_claim_rows": g.get("validated_claim_rows"),
            "signal_gap": g.get("signal_gap"),
            "core_gap": g.get("core_gap"),
            "core_area_split": g.get("core_area_split"),
            "team_comp_edge_score": g.get("team_comp_edge_score"),
            "team_comp_away_count": g.get("team_comp_away_count"),
            "team_comp_home_count": g.get("team_comp_home_count"),
            "team_comp_neutral_count": g.get("team_comp_neutral_count"),
            "team_comp_total_visible": g.get("team_comp_total_visible"),
            "qa_read_v2": g.get("qa_read_v2"),
            "confidence_cap_reason": g.get("confidence_cap_reason"),
            "diagnostic_note": "High Confidence non-correct/no-decision game; review drivers before changing confidence logic.",
        })
    return out


def summarize_high_confidence_driver_profile(game_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    high_games = [g for g in game_rows if normalize_token(g.get("outcome_confidence_label")) == "high"]
    groups: List[Tuple[str, str, Callable[[Dict[str, Any]], bool]]] = [
        ("all_high", "all_current_high", lambda g: True),
        ("quality_bucket", "supportive_or_strong", lambda g: g.get("expected_claim_quality_bucket") in {"strong_expected_quality", "supportive_expected_quality"}),
        ("quality_bucket", "mixed_or_fragile", lambda g: g.get("expected_claim_quality_bucket") in {"mixed_expected_quality", "fragile_expected_quality"}),
        ("rank_half", "bottom_half", lambda g: g.get("expected_quality_confidence_half") == "bottom_half_expected_quality"),
        ("rank_half", "top_half", lambda g: g.get("expected_quality_confidence_half") == "top_half_expected_quality"),
        ("rank_tertile", "bottom_third", lambda g: g.get("expected_quality_confidence_tertile") == "bottom_third_expected_quality"),
        ("rank_tertile", "middle_third", lambda g: g.get("expected_quality_confidence_tertile") == "middle_third_expected_quality"),
        ("rank_tertile", "top_third", lambda g: g.get("expected_quality_confidence_tertile") == "top_third_expected_quality"),
    ]
    rows: List[Dict[str, Any]] = []
    for group_type, group_name, predicate in groups:
        games = [g for g in high_games if predicate(g)]
        if not games:
            continue
        rows.append({
            "group_type": group_type,
            "group_name": group_name,
            **summarize_game_outcomes(games),
            **summarize_driver_context(games),
        })
    return rows


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


def run_expected_claim_quality(
    *,
    run_id: str,
    season: Optional[str],
    input_csv: Optional[Path],
    output_root: Path,
    project_id: str,
    dataset_id: str,
    training_table: str,
    limit: Optional[int],
    min_sample_rows: int,
    minimum_segment_claim_rows: int,
    exclude_unavailable: bool,
    strong_lift: float,
    supportive_lift: float,
    fragile_lift: float,
    version: str,
) -> Dict[str, Any]:
    if input_csv:
        raw_rows, source_meta = load_rows_from_csv(input_csv)
        if run_id and "run_id" in (raw_rows[0].keys() if raw_rows else set()):
            raw_rows = [r for r in raw_rows if not clean_string(r.get("run_id")) or str(r.get("run_id")) == run_id]
        if season:
            raw_rows = [r for r in raw_rows if not clean_string(r.get("season")) or str(r.get("season")) == str(season)]
        if limit:
            raw_rows = raw_rows[:limit]
    else:
        raw_rows, source_meta = load_rows_from_bigquery(
            run_id=run_id,
            season=season,
            project_id=project_id,
            dataset_id=dataset_id,
            training_table=training_table,
            limit=limit,
        )

    if not raw_rows:
        raise RuntimeError("No claim-training rows were loaded. Check run_id, season, and source table/input CSV.")

    rows = enrich_rows(raw_rows, exclude_unavailable=exclude_unavailable)
    overall_baseline = calculate_baseline(rows)
    overall_rate = overall_baseline["claim_validation_rate"]

    scored_claims = score_claim_rows(
        rows,
        min_sample_rows=min_sample_rows,
        exclude_unavailable=exclude_unavailable,
        overall_baseline_rate=overall_rate,
        strong_lift=strong_lift,
        supportive_lift=supportive_lift,
        fragile_lift=fragile_lift,
    )

    game_rows = aggregate_by_game(
        scored_claims,
        overall_baseline_rate=overall_rate,
        strong_lift=strong_lift,
        supportive_lift=supportive_lift,
        fragile_lift=fragile_lift,
    )
    game_rows = annotate_confidence_rank_groups(game_rows)

    lift_summary = summarize_lift(
        scored_claims,
        game_rows,
        minimum_segment_claim_rows=minimum_segment_claim_rows,
    )

    high_split = summarize_high_confidence_split(scored_claims, game_rows)
    high_ranked_split = summarize_high_confidence_ranked_split(scored_claims, game_rows)
    high_half_split = summarize_high_confidence_half_split(scored_claims, game_rows)
    outcome_lift_summary = summarize_expected_quality_outcome_lift(game_rows)
    confidence_relabel_audit = summarize_confidence_relabel_audit(game_rows)
    profile_conditioned_confidence_audit = summarize_profile_conditioned_confidence_audit(game_rows)
    high_game_rows = [
        row for row in game_rows
        if normalize_token(row.get("outcome_confidence_label")) == "high"
    ]
    confidence_label_integrity_audit = summarize_confidence_label_integrity_audit(game_rows)
    expected_quality_monotonicity_audit = summarize_expected_quality_monotonicity_audit(game_rows)
    high_confidence_miss_review = summarize_high_confidence_miss_review(game_rows)
    high_confidence_driver_profile = summarize_high_confidence_driver_profile(game_rows)

    output_dir = output_root / sanitize_identifier(run_id or "expected_claim_quality")
    output_dir.mkdir(parents=True, exist_ok=True)

    claim_preferred = [
        "run_id",
        "game_id",
        "claim_key",
        "claim_type",
        "claim_layer",
        "claim_name",
        "resolved_core_area",
        "resolved_category",
        "metric",
        "validation_result",
        "validated_bool",
        "outcome_confidence_label",
        "profile_strength_label",
        "profile_type",
        "model_result",
        "expected_claim_quality_score",
        "expected_claim_quality_bucket",
        "signature_level_used",
        "signature_rows_available",
        "signature_games_available",
        "full_signature_rows_available",
        "low_sample_signature_flag",
        "fallback_used_flag",
        "global_baseline_used_flag",
        "leave_one_game_out_flag",
        "signature_key_used",
    ]

    game_preferred = [
        "game_id",
        "season",
        "game_date",
        "game_week",
        "outcome_confidence_label",
        "profile_strength_label",
        "profile_type",
        "model_result",
        "model_result_bucket",
        "final_margin_abs",
        "final_margin_bucket",
        "predicted_team",
        "actual_winner",
        "qa_read_v2",
        "confidence_cap_reason",
        "signal_gap",
        "core_gap",
        "core_area_split",
        "team_comp_edge_score",
        "team_comp_away_count",
        "team_comp_home_count",
        "team_comp_neutral_count",
        "team_comp_total_visible",
        "actual_claim_validation_rate",
        "actual_claim_validation_pct",
        "validated_claim_rows",
        "claim_rows",
        "claim_rows_scored",
        "expected_claim_quality_score",
        "expected_claim_quality_pct",
        "expected_claim_quality_bucket",
        "expected_quality_confidence_rank",
        "expected_quality_confidence_rank_pct",
        "expected_quality_confidence_half",
        "expected_quality_confidence_tertile",
        "headline_expected_claim_quality_score",
        "supporting_expected_claim_quality_score",
        "expected_quality_coverage_rate",
        "low_sample_signature_rows",
        "fallback_rows_used",
        "missing_signature_rows",
        "global_baseline_rows_used",
        "leave_one_game_out_flag",
    ]

    lift_preferred = [
        "segment_type",
        "segment_name",
        "expected_quality_bucket",
        "games",
        "claim_rows",
        "validated_claim_rows",
        "baseline_claim_validation_rate",
        "baseline_claim_validation_pct",
        "bucket_claim_validation_rate",
        "bucket_claim_validation_pct",
        "lift_vs_baseline",
        "lift_vs_baseline_pct_points",
        "avg_expected_claim_quality_score",
        "avg_actual_claim_validation_rate",
        "sample_warning",
    ]

    high_preferred = [
        "outcome_confidence_label",
        "expected_quality_bucket",
        "games",
        "correct_games",
        "incorrect_games",
        "no_pick_games",
        "correct_game_rate",
        "claim_rows",
        "validated_claim_rows",
        "high_confidence_baseline_claim_validation_rate",
        "bucket_claim_validation_rate",
        "lift_vs_high_confidence_baseline",
        "lift_vs_high_confidence_baseline_pct_points",
        "avg_expected_claim_quality_score",
        "avg_actual_claim_validation_rate",
        "avg_final_margin_abs",
        "material_or_severe_margin_games",
        "sample_warning",
    ]

    high_ranked_preferred = [
        "outcome_confidence_label",
        "ranked_quality_group",
        "games",
        "graded_games",
        "correct_games",
        "incorrect_games",
        "no_pick_games",
        "tie_or_push_games",
        "correct_game_rate",
        "correct_game_rate_pct",
        "correct_game_rate_ci_low",
        "correct_game_rate_ci_high",
        "correct_game_rate_ci_low_pct",
        "correct_game_rate_ci_high_pct",
        "claim_rows",
        "validated_claim_rows",
        "high_confidence_baseline_claim_validation_rate",
        "group_claim_validation_rate",
        "claim_validation_ci_low",
        "claim_validation_ci_high",
        "claim_validation_ci_low_pct",
        "claim_validation_ci_high_pct",
        "lift_vs_high_confidence_baseline",
        "lift_vs_high_confidence_baseline_pct_points",
        "avg_expected_claim_quality_score",
        "min_expected_claim_quality_score",
        "max_expected_claim_quality_score",
        "avg_actual_claim_validation_rate",
        "avg_final_margin_abs",
        "material_or_severe_margin_games",
        "sample_warning",
    ]

    high_half_preferred = [
        "outcome_confidence_label",
        "ranked_quality_group",
        "games",
        "graded_games",
        "correct_games",
        "incorrect_games",
        "no_pick_games",
        "correct_game_rate",
        "correct_game_rate_ci_low",
        "correct_game_rate_ci_high",
        "claim_rows",
        "validated_claim_rows",
        "claim_validation_rate",
        "claim_validation_ci_low",
        "claim_validation_ci_high",
        "high_confidence_baseline_correct_game_rate",
        "high_confidence_baseline_claim_validation_rate",
        "lift_correct_rate_vs_high_baseline",
        "lift_claim_validation_vs_high_baseline",
        "lift_correct_rate_vs_high_baseline_pct_points",
        "lift_claim_validation_vs_high_baseline_pct_points",
        "avg_expected_claim_quality_score",
        "avg_actual_claim_validation_rate",
        "avg_final_margin_abs",
        "material_or_severe_margin_games",
        "sample_warning",
    ]

    outcome_lift_preferred = [
        "segment_type",
        "segment_name",
        "expected_quality_bucket",
        "baseline_games",
        "baseline_graded_games",
        "baseline_correct_game_rate",
        "baseline_correct_game_rate_pct",
        "baseline_claim_validation_rate",
        "baseline_claim_validation_pct",
        "games",
        "graded_games",
        "correct_games",
        "incorrect_games",
        "no_pick_games",
        "correct_game_rate",
        "correct_game_rate_ci_low",
        "correct_game_rate_ci_high",
        "claim_rows",
        "validated_claim_rows",
        "claim_validation_rate",
        "claim_validation_ci_low",
        "claim_validation_ci_high",
        "lift_correct_rate_vs_segment_baseline",
        "lift_correct_rate_vs_segment_baseline_pct_points",
        "lift_claim_validation_vs_segment_baseline",
        "lift_claim_validation_vs_segment_baseline_pct_points",
        "avg_expected_claim_quality_score",
        "avg_actual_claim_validation_rate",
        "avg_final_margin_abs",
        "sample_warning",
    ]

    relabel_preferred = [
        "audit_group",
        "description",
        "games",
        "graded_games",
        "correct_games",
        "incorrect_games",
        "no_pick_games",
        "correct_game_rate",
        "correct_game_rate_ci_low",
        "correct_game_rate_ci_high",
        "claim_rows",
        "validated_claim_rows",
        "claim_validation_rate",
        "claim_validation_ci_low",
        "claim_validation_ci_high",
        "avg_expected_claim_quality_score",
        "avg_actual_claim_validation_rate",
        "avg_final_margin_abs",
        "material_or_severe_margin_games",
        "sample_warning",
    ]

    profile_audit_preferred = [
        "profile_dimension",
        "profile_dimension_label",
        "profile_value",
        "audit_segment",
        "profile_medium_games",
        "profile_medium_correct_rate",
        "profile_high_games",
        "profile_high_correct_rate",
        "current_high_minus_medium_correct_rate",
        "segment_minus_medium_correct_rate",
        "segment_minus_current_high_correct_rate",
        "games",
        "graded_games",
        "correct_games",
        "incorrect_games",
        "no_pick_games",
        "correct_game_rate",
        "correct_game_rate_ci_low",
        "correct_game_rate_ci_high",
        "claim_rows",
        "validated_claim_rows",
        "claim_validation_rate",
        "claim_validation_ci_low",
        "claim_validation_ci_high",
        "avg_expected_claim_quality_score",
        "avg_actual_claim_validation_rate",
        "avg_final_margin_abs",
        "sample_warning",
    ]


    integrity_preferred = [
        "outcome_confidence_label",
        "model_result_bucket",
        "integrity_flag",
        "games",
        "graded_games",
        "correct_games",
        "incorrect_games",
        "no_pick_games",
        "tie_or_push_games",
        "correct_game_rate",
        "correct_game_rate_ci_low",
        "correct_game_rate_ci_high",
        "claim_rows",
        "validated_claim_rows",
        "claim_validation_rate",
        "claim_validation_ci_low",
        "claim_validation_ci_high",
        "avg_signal_gap",
        "avg_core_gap",
        "avg_team_comp_edge_score",
        "most_common_core_area_split",
        "sample_warning",
    ]

    monotonicity_preferred = [
        "segment_type",
        "segment_name",
        "games",
        "scored_games",
        "claim_validation_pairs",
        "correctness_pairs",
        "score_min",
        "score_max",
        "score_avg",
        "score_stddev",
        "avg_actual_claim_validation_rate",
        "pearson_score_vs_claim_validation",
        "spearman_score_vs_claim_validation",
        "pearson_score_vs_correct",
        "spearman_score_vs_correct",
        "monotonicity_read",
        "use_for_confidence_relabeling",
        "confidence_relabeling_note",
    ]

    high_miss_preferred = [
        "game_id",
        "game_date",
        "game_week",
        "outcome_confidence_label",
        "profile_strength_label",
        "profile_type",
        "model_result",
        "model_result_bucket",
        "predicted_team",
        "actual_winner",
        "final_margin_abs",
        "final_margin_bucket",
        "qa_read_v2",
        "confidence_cap_reason",
        "signal_gap",
        "core_gap",
        "core_area_split",
        "team_comp_edge_score",
        "team_comp_away_count",
        "team_comp_home_count",
        "team_comp_neutral_count",
        "team_comp_total_visible",
        "actual_claim_validation_rate",
        "expected_claim_quality_score",
        "expected_claim_quality_bucket",
        "expected_quality_confidence_rank",
        "expected_quality_confidence_rank_pct",
        "expected_quality_confidence_half",
        "expected_quality_confidence_tertile",
        "claim_rows",
        "validated_claim_rows",
        "diagnostic_note",
    ]

    high_driver_preferred = [
        "group_type",
        "group_name",
        "games",
        "graded_games",
        "correct_games",
        "incorrect_games",
        "no_pick_games",
        "correct_game_rate",
        "claim_rows",
        "validated_claim_rows",
        "claim_validation_rate",
        "avg_expected_claim_quality_score",
        "avg_actual_claim_validation_rate",
        "avg_final_margin_abs",
        "avg_signal_gap",
        "avg_core_gap",
        "avg_team_comp_edge_score",
        "avg_team_comp_total_visible",
        "avg_team_comp_neutral_count",
        "most_common_core_area_split",
        "most_common_final_margin_bucket",
        "sample_warning",
    ]

    write_csv(scored_claims, output_dir / "expected_claim_quality_by_claim.csv", claim_preferred)
    write_csv(game_rows, output_dir / "expected_claim_quality_by_game.csv", game_preferred)
    write_csv(lift_summary, output_dir / "expected_claim_quality_lift_summary.csv", lift_preferred)
    write_csv(high_split, output_dir / "high_confidence_expected_quality_split.csv", high_preferred)
    write_csv(high_ranked_split, output_dir / "high_confidence_expected_quality_ranked_split.csv", high_ranked_preferred)
    write_csv(high_half_split, output_dir / "high_confidence_expected_quality_half_split.csv", high_half_preferred)
    write_csv(outcome_lift_summary, output_dir / "expected_quality_outcome_lift_summary.csv", outcome_lift_preferred)
    write_csv(confidence_relabel_audit, output_dir / "confidence_relabel_audit.csv", relabel_preferred)
    write_csv(profile_conditioned_confidence_audit, output_dir / "profile_conditioned_confidence_audit.csv", profile_audit_preferred)
    write_csv(high_game_rows, output_dir / "high_confidence_expected_quality_games.csv", game_preferred)
    write_csv(confidence_label_integrity_audit, output_dir / "confidence_label_integrity_audit.csv", integrity_preferred)
    write_csv(expected_quality_monotonicity_audit, output_dir / "expected_quality_monotonicity_audit.csv", monotonicity_preferred)
    write_csv(high_confidence_miss_review, output_dir / "high_confidence_miss_review.csv", high_miss_preferred)
    write_csv(high_confidence_driver_profile, output_dir / "high_confidence_driver_profile.csv", high_driver_preferred)

    metadata = {
        "available": True,
        "version": version,
        "created_at": utc_now_iso(),
        "run_id": run_id,
        "season": season,
        "dry_run_only": True,
        "no_runtime_behavior_changed": True,
        "anti_overfitting_guardrail": "leave_one_game_out_by_game_id",
        "source": source_meta,
        "parameters": {
            "min_sample_rows": min_sample_rows,
            "minimum_segment_claim_rows": minimum_segment_claim_rows,
            "exclude_unavailable": exclude_unavailable,
            "strong_lift": strong_lift,
            "supportive_lift": supportive_lift,
            "fragile_lift": fragile_lift,
        },
        "outputs": {
            "output_dir": str(output_dir),
            "expected_claim_quality_by_claim": str(output_dir / "expected_claim_quality_by_claim.csv"),
            "expected_claim_quality_by_game": str(output_dir / "expected_claim_quality_by_game.csv"),
            "expected_claim_quality_lift_summary": str(output_dir / "expected_claim_quality_lift_summary.csv"),
            "high_confidence_expected_quality_split": str(output_dir / "high_confidence_expected_quality_split.csv"),
            "high_confidence_expected_quality_ranked_split": str(output_dir / "high_confidence_expected_quality_ranked_split.csv"),
            "high_confidence_expected_quality_half_split": str(output_dir / "high_confidence_expected_quality_half_split.csv"),
            "expected_quality_outcome_lift_summary": str(output_dir / "expected_quality_outcome_lift_summary.csv"),
            "confidence_relabel_audit": str(output_dir / "confidence_relabel_audit.csv"),
            "profile_conditioned_confidence_audit": str(output_dir / "profile_conditioned_confidence_audit.csv"),
            "high_confidence_expected_quality_games": str(output_dir / "high_confidence_expected_quality_games.csv"),
            "confidence_label_integrity_audit": str(output_dir / "confidence_label_integrity_audit.csv"),
            "expected_quality_monotonicity_audit": str(output_dir / "expected_quality_monotonicity_audit.csv"),
            "high_confidence_miss_review": str(output_dir / "high_confidence_miss_review.csv"),
            "high_confidence_driver_profile": str(output_dir / "high_confidence_driver_profile.csv"),
            "run_metadata": str(output_dir / "run_metadata.json"),
        },
        "overall_baseline": overall_baseline,
        "row_counts": {
            "raw_rows_loaded": len(raw_rows),
            "denominator_eligible_claim_rows": overall_baseline["claim_rows"],
            "games_scored": len(game_rows),
            "lift_summary_rows": len(lift_summary),
            "high_confidence_split_rows": len(high_split),
            "high_confidence_ranked_split_rows": len(high_ranked_split),
            "high_confidence_half_split_rows": len(high_half_split),
            "outcome_lift_summary_rows": len(outcome_lift_summary),
            "confidence_relabel_audit_rows": len(confidence_relabel_audit),
            "profile_conditioned_confidence_audit_rows": len(profile_conditioned_confidence_audit),
            "high_confidence_games": len(high_game_rows),
            "confidence_label_integrity_audit_rows": len(confidence_label_integrity_audit),
            "expected_quality_monotonicity_audit_rows": len(expected_quality_monotonicity_audit),
            "high_confidence_miss_review_rows": len(high_confidence_miss_review),
            "high_confidence_driver_profile_rows": len(high_confidence_driver_profile),
        },
        "bucket_distribution": count_by(game_rows, "expected_claim_quality_bucket"),
        "signature_level_distribution": count_by(scored_claims, "signature_level_used"),
        "answer_targets": {
            "high_confidence_split_question": "Review high_confidence_expected_quality_split.csv, high_confidence_expected_quality_ranked_split.csv, and high_confidence_expected_quality_half_split.csv",
            "confidence_calibration_question": "Review profile_conditioned_confidence_audit.csv and confidence_relabel_audit.csv",
            "outcome_lift_against_2025_admin_baselines": "Review expected_quality_outcome_lift_summary.csv",
            "claim_lift_against_2025_admin_baselines": "Review expected_claim_quality_lift_summary.csv",
            "game_level_details": "Review expected_claim_quality_by_game.csv",
            "claim_signature_diagnostics": "Review expected_claim_quality_by_claim.csv",
            "trash_in_trash_out_checks": "Review confidence_label_integrity_audit.csv and expected_quality_monotonicity_audit.csv before drawing conclusions",
            "high_confidence_driver_review": "Review high_confidence_miss_review.csv and high_confidence_driver_profile.csv",
        },
    }

    write_json(metadata, output_dir / "run_metadata.json")
    return metadata


def count_by(rows: List[Dict[str, Any]], field: str) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)
    for row in rows:
        counts[str(row.get(field) or "missing")] += 1
    return dict(sorted(counts.items(), key=lambda item: item[0]))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build offline Expected Claim Quality dry-run outputs for GameLens."
    )
    parser.add_argument("--run-id", required=True, help="Source run_id from gamelens_claim_training_examples.")
    parser.add_argument("--season", default="2025", help="Season to analyze. Default: 2025.")
    parser.add_argument("--input-csv", type=Path, default=None, help="Optional local claim-training CSV instead of BigQuery.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--project-id", default=PROJECT_ID)
    parser.add_argument("--dataset-id", default=DATASET_ID)
    parser.add_argument("--training-table", default=TRAINING_TABLE)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--min-sample-rows", type=int, default=DEFAULT_MIN_SAMPLE_ROWS)
    parser.add_argument(
        "--minimum-segment-claim-rows",
        type=int,
        default=50,
        help="Suppress lift-summary segments smaller than this many claim rows. Default: 50.",
    )
    parser.add_argument(
        "--exclude-unavailable",
        action="store_true",
        default=DEFAULT_EXCLUDE_UNAVAILABLE,
        help=(
            "Exclude unavailable/unknown validation rows from denominators. "
            "Default keeps them for Admin-style comparability."
        ),
    )
    parser.add_argument("--strong-lift", type=float, default=DEFAULT_STRONG_LIFT)
    parser.add_argument("--supportive-lift", type=float, default=DEFAULT_SUPPORTIVE_LIFT)
    parser.add_argument("--fragile-lift", type=float, default=DEFAULT_FRAGILE_LIFT)
    parser.add_argument("--version", default=DEFAULT_VERSION)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Document intent. This worker is dry-run only and never writes to BigQuery.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    metadata = run_expected_claim_quality(
        run_id=args.run_id,
        season=args.season,
        input_csv=args.input_csv,
        output_root=args.output_root,
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        training_table=args.training_table,
        limit=args.limit,
        min_sample_rows=args.min_sample_rows,
        minimum_segment_claim_rows=args.minimum_segment_claim_rows,
        exclude_unavailable=args.exclude_unavailable,
        strong_lift=args.strong_lift,
        supportive_lift=args.supportive_lift,
        fragile_lift=args.fragile_lift,
        version=args.version,
    )

    print(json.dumps({
        "available": True,
        "dry_run_only": True,
        "output_dir": metadata["outputs"]["output_dir"],
        "overall_baseline": metadata["overall_baseline"],
        "row_counts": metadata["row_counts"],
        "bucket_distribution": metadata["bucket_distribution"],
        "signature_level_distribution": metadata["signature_level_distribution"],
    }, indent=2))


if __name__ == "__main__":
    main()
