"""
Build GameLens confidence calibration audit summaries.

Recommended repo location:
    agg/gamelens_training/build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim.py

Purpose
-------
This is an offline, dry-run-only audit worker for GameLens confidence labels.

It asks a different question than Expected Claim Quality:

    When GameLens labels a game High Confidence, does that label deserve to be
    higher than Medium inside similar profile families?

This script does NOT:
    - change /game API behavior
    - change matchup_lean
    - change outcome_confidence
    - change Model Trust
    - write to BigQuery
    - create production gates

It DOES:
    - materialize one row per game from claim-training rows
    - compare High vs Medium confidence calibration
    - audit High misses for driver patterns such as signal_gap, core_gap,
      Team Comparison strength, profile shape, claim validation, and season phase
    - surface Medium groups that may be acting more like what High should mean
    - produce review-only CSV/JSON outputs
    - add core_area_durability_band/core_area_durability_sort
    - simulate audit-only High-to-Medium softening when High lacks Core Area durability

Example BigQuery dry run:
    python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_2_high_retention_simulation \
      --run-id full_2025_reg_post_claim_matrix_pilot \
      --season 2025 \
      --dry-run \
      --output-root qa/gamelens_confidence_calibration_audit_v0_1_runs

Example local CSV dry run:
    python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_2_high_retention_simulation \
      --run-id full_2025_reg_post_claim_matrix_pilot \
      --input-csv qa/some_claim_rows.csv \
      --dry-run
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


PROJECT_ID = "nfl-stream-406420"
DATASET_ID = "Analytics"
TRAINING_TABLE = "gamelens_claim_training_examples"
DEFAULT_OUTPUT_ROOT = Path("qa/gamelens_confidence_calibration_audit_v0_1_3_runs")
VERSION = "confidence_calibration_audit_v0_1_3_core_durability_confidence_sim"
DEFAULT_CORE_GAP_FLOORS = [0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55]
DEFAULT_CALIBRATION_CORE_GAP_FLOOR = 0.45

# Fields we would like to use. The BigQuery loader will select only the fields
# that exist and will add missing desired fields as nulls.
DESIRED_COLUMNS = [
    "claim_key",
    "run_id",
    "season",
    "game_id",
    "game_date",
    "game_week",
    "season_type",
    "bucket",
    "away_team",
    "home_team",
    "predicted_team",
    "actual_winner",
    "model_result",
    "is_tie",
    "final_margin_abs",
    "final_margin_bucket",
    "outcome_confidence_code",
    "outcome_confidence_label",
    "profile_strength_code",
    "profile_strength_label",
    "profile_type",
    "matchup_label",
    "core_area_split",
    "core_gap",
    "signal_gap",
    "team_comp_edge_score",
    "team_comp_away_count",
    "team_comp_home_count",
    "team_comp_neutral_count",
    "team_comp_total_visible",
    "qa_read_v2",
    "confidence_cap_reason",
    "claim_type",
    "claim_layer",
    "claim_name",
    "core_area",
    "category",
    "metric",
    "registry_core_area",
    "registry_category",
    "two_way_context",
    "claim_strength_bucket",
    "claim_strength_language_signal",
    "offensive_efficiency_support_bucket",
    "offensive_efficiency_support_strength",
    "validation_result",
    "validated_flag",
    "actual_gap_bucket",
    "headline_claim_validation_rate",
    "unique_claim_validation_rate",
]

REQUIRED_COLUMNS = ["game_id"]

HIGH_LABELS = {"high", "high confidence"}
MEDIUM_LABELS = {"medium", "medium confidence"}
LOW_LABELS = {"low", "low confidence"}
CORRECT_RESULTS = {"correct", "win", "model aligned with outcome"}
INCORRECT_RESULTS = {"incorrect", "loss", "model miss", "model miss — learning opportunity logged"}
NO_PICK_RESULTS = {"no pick", "no_pick", "no decision", "no_decision", "tie", "push", "neutral"}


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_missing(value: Any) -> bool:
    """Return True for None, pandas NA/NaN/NaT, and blank strings.

    This helper exists because pandas.NA cannot be compared with == or used in
    boolean contexts. Without this guard, fields like validated_flag can raise
    "TypeError: boolean value of NA is ambiguous" when loaded from BigQuery.
    """
    if value is None:
        return True

    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass

    if isinstance(value, str) and not value.strip():
        return True

    return False


def sanitize_identifier(value: str) -> str:
    if is_missing(value):
        text = ""
    else:
        text = str(value)
    return re.sub(r"[^A-Za-z0-9_]+", "_", text).strip("_")[:100] or "run"


def as_float(value: Any) -> Optional[float]:
    if is_missing(value):
        return None
    try:
        number = float(value)
        if math.isnan(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def as_int(value: Any) -> Optional[int]:
    if is_missing(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def as_bool(value: Any) -> Optional[bool]:
    if is_missing(value):
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


def clean_text(value: Any) -> Optional[str]:
    if is_missing(value):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "<na>", "nat"}:
        return None
    return text


def normalized_text(value: Any) -> str:
    text = clean_text(value)
    return (text or "").strip().lower()


def first_non_null(values: Iterable[Any]) -> Any:
    for value in values:
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except TypeError:
            pass
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def mean_safe(values: Iterable[Any]) -> Optional[float]:
    nums = [as_float(v) for v in values]
    nums = [v for v in nums if v is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def median_safe(values: Iterable[Any]) -> Optional[float]:
    nums = sorted(v for v in (as_float(x) for x in values) if v is not None)
    if not nums:
        return None
    mid = len(nums) // 2
    if len(nums) % 2:
        return nums[mid]
    return (nums[mid - 1] + nums[mid]) / 2


def round_rate(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), 4)


def pct(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value) * 100, 2)


def wilson_interval(successes: int, n: int, z: float = 1.96) -> Tuple[Optional[float], Optional[float]]:
    if n <= 0:
        return None, None
    phat = successes / n
    denom = 1 + z * z / n
    center = (phat + z * z / (2 * n)) / denom
    half = z * math.sqrt((phat * (1 - phat) + z * z / (4 * n)) / n) / denom
    return max(0.0, center - half), min(1.0, center + half)


def write_json(data: Any, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def write_csv(rows: List[Dict[str, Any]], output_path: Path, preferred: Optional[List[str]] = None) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return

    all_keys = sorted({k for row in rows for k in row.keys()})
    if preferred:
        fieldnames = [k for k in preferred if k in all_keys] + [k for k in all_keys if k not in preferred]
    else:
        fieldnames = all_keys

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def import_bigquery():
    from google.cloud import bigquery
    return bigquery


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

def get_available_columns(
    *,
    client: Any,
    bigquery: Any,
    project_id: str,
    dataset_id: str,
    table_name: str,
) -> List[str]:
    query = f"""
        SELECT column_name
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = @table_name
        ORDER BY ordinal_position
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("table_name", "STRING", table_name)]
    )
    return [str(row["column_name"]) for row in client.query(query, job_config=job_config).result()]


def load_from_bigquery(
    *,
    run_id: str,
    season: Optional[str],
    project_id: str,
    dataset_id: str,
    training_table: str,
    limit: Optional[int],
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    bigquery = import_bigquery()
    client = bigquery.Client(project=project_id)

    table_ref = f"{project_id}.{dataset_id}.{training_table}"
    available_columns = get_available_columns(
        client=client,
        bigquery=bigquery,
        project_id=project_id,
        dataset_id=dataset_id,
        table_name=training_table,
    )
    available_set = set(available_columns)

    missing_required = [c for c in REQUIRED_COLUMNS if c not in available_set]
    if missing_required:
        raise RuntimeError(f"Missing required columns in {table_ref}: {missing_required}")

    selected_columns = [c for c in DESIRED_COLUMNS if c in available_set]
    missing_desired = [c for c in DESIRED_COLUMNS if c not in available_set]
    select_clause = ",\n            ".join(selected_columns)
    season_filter = "AND CAST(season AS STRING) = @season" if season and "season" in available_set else ""
    limit_clause = "LIMIT @limit" if limit else ""

    query = f"""
        SELECT
            {select_clause}
        FROM `{table_ref}`
        WHERE run_id = @run_id
          {season_filter}
        ORDER BY game_id, claim_type, claim_layer, claim_key
        {limit_clause}
    """

    params = [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
    if season and "season" in available_set:
        params.append(bigquery.ScalarQueryParameter("season", "STRING", str(season)))
    if limit:
        params.append(bigquery.ScalarQueryParameter("limit", "INT64", int(limit)))

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    df = client.query(query, job_config=job_config).to_dataframe()

    for col in DESIRED_COLUMNS:
        if col not in df.columns:
            df[col] = None

    metadata = {
        "source": "bigquery",
        "table_ref": table_ref,
        "available_columns": available_columns,
        "selected_columns": selected_columns,
        "missing_desired_columns": missing_desired,
    }
    return df, metadata


def load_from_csv(input_csv: Path, run_id: str, season: Optional[str], limit: Optional[int]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    df = pd.read_csv(input_csv)
    if "run_id" in df.columns:
        df = df[df["run_id"].astype(str) == str(run_id)]
    if season and "season" in df.columns:
        df = df[df["season"].astype(str) == str(season)]
    if limit:
        df = df.head(int(limit)).copy()
    for col in DESIRED_COLUMNS:
        if col not in df.columns:
            df[col] = None
    missing_required = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_required:
        raise RuntimeError(f"Input CSV missing required columns: {missing_required}")
    return df, {
        "source": "csv",
        "input_csv": str(input_csv),
        "available_columns": sorted(df.columns.tolist()),
        "missing_desired_columns": [c for c in DESIRED_COLUMNS if c not in df.columns],
    }


# ---------------------------------------------------------------------------
# Game-level building
# ---------------------------------------------------------------------------

def is_validated(row: pd.Series) -> bool:
    flag = as_bool(row.get("validated_flag"))
    if flag is not None:
        return flag
    return normalized_text(row.get("validation_result")) == "validated"


def normalized_result(value: Any) -> str:
    text = normalized_text(value)
    if text in CORRECT_RESULTS:
        return "correct"
    if text in INCORRECT_RESULTS:
        return "incorrect"
    if text in NO_PICK_RESULTS:
        return "no_pick"
    if "correct" == text:
        return "correct"
    if "incorrect" == text:
        return "incorrect"
    if "no" in text and "pick" in text:
        return "no_pick"
    if "tie" in text or "push" in text:
        return "no_pick"
    return text or "unknown"


def confidence_group(value: Any) -> str:
    text = normalized_text(value)
    if text in HIGH_LABELS:
        return "High"
    if text in MEDIUM_LABELS:
        return "Medium"
    if text in LOW_LABELS:
        return "Low"
    if "high" in text:
        return "High"
    if "medium" in text:
        return "Medium"
    if "low" in text:
        return "Low"
    return clean_text(value) or "Unknown"


def margin_bucket(value: Any, existing: Any = None) -> str:
    existing_text = normalized_text(existing)
    if existing_text and existing_text not in {"unknown", "nan", "none", "null"}:
        return existing_text
    margin = as_float(value)
    if margin is None:
        return "unknown"
    if margin == 0:
        return "tie"
    if margin <= 3:
        return "close"
    if margin <= 8:
        return "one_score"
    if margin <= 16:
        return "material"
    return "severe"


def phase_group_from_row(row: Dict[str, Any]) -> str:
    bucket = normalized_text(row.get("bucket"))
    if bucket in {"early", "early_season"}:
        return "early"
    if bucket in {"mid", "midseason", "mid_season"}:
        return "mid"
    if bucket in {"late", "late_season"}:
        return "late"
    if bucket in {"postseason", "playoff", "playoffs"}:
        return "postseason"

    season_type = normalized_text(row.get("season_type"))
    game_week = normalized_text(row.get("game_week"))
    if "post" in season_type or game_week in {"wild card", "wildcard", "divisional round", "conference championship", "super bowl"}:
        return "postseason"

    m = re.search(r"week\s+(\d+)", game_week)
    if m:
        week = int(m.group(1))
        if week <= 6:
            return "early"
        if week <= 13:
            return "mid"
        return "late"

    return bucket or "unknown"




# ---------------------------------------------------------------------------
# Core Area durability helper
# ---------------------------------------------------------------------------

CORE_AREA_DURABILITY_SORT = {
    "unknown": 0,
    "weak": 10,
    "borderline": 20,
    "durable": 30,
    "strong_durable": 40,
    "very_strong": 50,
}


def core_area_durability_band(value: Any) -> str:
    """Bucket core_gap into review-only durability bands.

    These bands are diagnostic. They do not define a production confidence rule.
    """
    gap = as_float(value)
    if gap is None:
        return "unknown"
    if gap < 0.35:
        return "weak"
    if gap < 0.40:
        return "borderline"
    if gap < 0.45:
        return "durable"
    if gap < 0.50:
        return "strong_durable"
    return "very_strong"


def core_area_durability_sort(value: Any) -> int:
    return CORE_AREA_DURABILITY_SORT.get(core_area_durability_band(value), 0)


def add_core_area_durability_fields(game_df: pd.DataFrame) -> pd.DataFrame:
    out = game_df.copy()
    if "core_gap" not in out.columns:
        out["core_gap"] = None
    out["core_area_durability_band"] = out["core_gap"].apply(core_area_durability_band)
    out["core_area_durability_sort"] = out["core_gap"].apply(core_area_durability_sort)
    return out

def build_game_rows(df: pd.DataFrame, *, exclude_unavailable: bool) -> pd.DataFrame:
    work = df.copy()
    work["_validation_bucket"] = work.apply(lambda r: normalized_text(r.get("validation_result")) or "unknown", axis=1)
    if exclude_unavailable:
        work = work[work["_validation_bucket"] != "unavailable"].copy()

    work["_validated_bool"] = work.apply(is_validated, axis=1)

    game_rows: List[Dict[str, Any]] = []
    game_fields = [
        "run_id",
        "season",
        "game_id",
        "game_date",
        "game_week",
        "season_type",
        "bucket",
        "away_team",
        "home_team",
        "predicted_team",
        "actual_winner",
        "model_result",
        "is_tie",
        "final_margin_abs",
        "final_margin_bucket",
        "outcome_confidence_code",
        "outcome_confidence_label",
        "profile_strength_code",
        "profile_strength_label",
        "profile_type",
        "matchup_label",
        "core_area_split",
        "core_gap",
        "signal_gap",
        "team_comp_edge_score",
        "team_comp_away_count",
        "team_comp_home_count",
        "team_comp_neutral_count",
        "team_comp_total_visible",
        "qa_read_v2",
        "confidence_cap_reason",
        "headline_claim_validation_rate",
        "unique_claim_validation_rate",
    ]

    for game_id, group in work.groupby("game_id", dropna=False):
        row: Dict[str, Any] = {}
        for field in game_fields:
            row[field] = first_non_null(group[field].tolist()) if field in group.columns else None

        claim_rows = len(group)
        validated_claim_rows = int(group["_validated_bool"].sum())
        not_validated_rows = int((group["_validation_bucket"] == "not_validated").sum())
        neutral_or_mixed_rows = int((group["_validation_bucket"] == "actual_neutral_or_mixed").sum())
        unavailable_rows = int((group["_validation_bucket"] == "unavailable").sum())
        claim_validation_rate = validated_claim_rows / claim_rows if claim_rows else None

        row.update({
            "claim_rows": claim_rows,
            "validated_claim_rows": validated_claim_rows,
            "not_validated_claim_rows": not_validated_rows,
            "neutral_or_mixed_claim_rows": neutral_or_mixed_rows,
            "unavailable_claim_rows": unavailable_rows,
            "actual_claim_validation_rate": round_rate(claim_validation_rate),
            "actual_claim_validation_pct": pct(claim_validation_rate),
            "confidence_group": confidence_group(row.get("outcome_confidence_label") or row.get("outcome_confidence_code")),
            "model_result_normalized": normalized_result(row.get("model_result")),
        })

        row["is_correct"] = row["model_result_normalized"] == "correct"
        row["is_incorrect"] = row["model_result_normalized"] == "incorrect"
        row["is_no_pick_or_tie"] = row["model_result_normalized"] == "no_pick"
        row["is_graded"] = row["is_correct"] or row["is_incorrect"]
        row["final_margin_abs"] = as_float(row.get("final_margin_abs"))
        row["final_margin_bucket_normalized"] = margin_bucket(row.get("final_margin_abs"), row.get("final_margin_bucket"))
        row["phase_group"] = phase_group_from_row(row)

        row["is_close_miss"] = bool(row["is_incorrect"] and row["final_margin_abs"] is not None and row["final_margin_abs"] <= 8)
        row["is_severe_miss"] = bool(row["is_incorrect"] and row["final_margin_abs"] is not None and row["final_margin_abs"] >= 17)
        row["is_late_or_postseason"] = row["phase_group"] in {"late", "postseason"}
        row["is_strong_profile"] = normalized_text(row.get("profile_strength_label")) == "strong profile"
        row["is_confirmed_edge"] = normalized_text(row.get("profile_type")) == "confirmed_edge"
        row["core_area_durability_band"] = core_area_durability_band(row.get("core_gap"))
        row["core_area_durability_sort"] = core_area_durability_sort(row.get("core_gap"))

        game_rows.append(row)

    return pd.DataFrame(game_rows)





def prepare_game_audit_csv_rows(df: pd.DataFrame, *, season: Optional[str], limit: Optional[int]) -> pd.DataFrame:
    """Prepare a previously generated game_level_confidence_audit.csv.

    This lets us test new audit-only simulations against existing CSV outputs
    without re-querying BigQuery or rebuilding claim rows. It intentionally
    derives only pregame-safe or already-present game-level audit fields.
    """
    out = df.copy()
    if season and "season" in out.columns:
        out = out[out["season"].astype(str) == str(season)].copy()
    if limit:
        out = out.head(int(limit)).copy()

    for col in [
        "game_id", "game_date", "game_week", "season_type", "bucket",
        "away_team", "home_team", "predicted_team", "actual_winner", "model_result",
        "outcome_confidence_label", "outcome_confidence_code", "profile_strength_label",
        "profile_type", "matchup_label", "core_area_split", "final_margin_bucket",
        "team_comp_away_count", "team_comp_home_count", "team_comp_neutral_count",
        "team_comp_total_visible", "qa_read_v2", "confidence_cap_reason",
    ]:
        if col not in out.columns:
            out[col] = None

    for col in [
        "final_margin_abs", "core_gap", "signal_gap", "team_comp_edge_score",
        "actual_claim_validation_rate", "actual_claim_validation_pct",
        "claim_rows", "validated_claim_rows", "not_validated_claim_rows",
        "neutral_or_mixed_claim_rows", "unavailable_claim_rows",
    ]:
        if col not in out.columns:
            out[col] = None

    # Numeric normalization.
    for col in ["final_margin_abs", "core_gap", "signal_gap", "team_comp_edge_score", "actual_claim_validation_rate"]:
        out[col] = out[col].apply(as_float)

    if out["actual_claim_validation_rate"].isna().all() and "actual_claim_validation_pct" in out.columns:
        out["actual_claim_validation_rate"] = out["actual_claim_validation_pct"].apply(lambda v: as_float(v) / 100 if as_float(v) is not None else None)

    out["confidence_group"] = out.apply(
        lambda r: clean_text(r.get("confidence_group")) or confidence_group(r.get("outcome_confidence_label") or r.get("outcome_confidence_code")),
        axis=1,
    )
    out["model_result_normalized"] = out.apply(
        lambda r: clean_text(r.get("model_result_normalized")) or normalized_result(r.get("model_result")),
        axis=1,
    )

    out["is_correct"] = out["model_result_normalized"] == "correct"
    out["is_incorrect"] = out["model_result_normalized"] == "incorrect"
    out["is_no_pick_or_tie"] = out["model_result_normalized"] == "no_pick"
    out["is_graded"] = out["is_correct"] | out["is_incorrect"]
    out["final_margin_bucket_normalized"] = out.apply(
        lambda r: margin_bucket(r.get("final_margin_abs"), r.get("final_margin_bucket")),
        axis=1,
    )
    out["phase_group"] = out.apply(lambda r: phase_group_from_row(r.to_dict()), axis=1)
    out["is_close_miss"] = out.apply(
        lambda r: bool(r.get("is_incorrect") and as_float(r.get("final_margin_abs")) is not None and as_float(r.get("final_margin_abs")) <= 8),
        axis=1,
    )
    out["is_severe_miss"] = out.apply(
        lambda r: bool(r.get("is_incorrect") and as_float(r.get("final_margin_abs")) is not None and as_float(r.get("final_margin_abs")) >= 17),
        axis=1,
    )
    out["is_late_or_postseason"] = out["phase_group"].isin(["late", "postseason"])
    out["is_strong_profile"] = out["profile_strength_label"].apply(lambda v: normalized_text(v) == "strong profile")
    out["is_confirmed_edge"] = out["profile_type"].apply(lambda v: normalized_text(v) == "confirmed_edge")
    out["strong_confirmed_shape_flag"] = out["is_strong_profile"] & out["is_confirmed_edge"]
    out = add_core_area_durability_fields(out)

    # Keep count fields usable for summarize_games even if CSV source omitted them.
    for col in ["claim_rows", "validated_claim_rows"]:
        out[col] = out[col].apply(lambda v: as_int(v) or 0)

    return out

# ---------------------------------------------------------------------------
# Audit thresholds / flags
# ---------------------------------------------------------------------------

def percentile(values: Iterable[Any], q: float) -> Optional[float]:
    nums = sorted(v for v in (as_float(x) for x in values) if v is not None)
    if not nums:
        return None
    if len(nums) == 1:
        return nums[0]
    pos = (len(nums) - 1) * q
    lower = math.floor(pos)
    upper = math.ceil(pos)
    if lower == upper:
        return nums[int(pos)]
    return nums[lower] * (upper - pos) + nums[upper] * (pos - lower)


def build_thresholds(game_df: pd.DataFrame) -> Dict[str, Any]:
    graded = game_df[game_df["is_graded"]].copy()
    return {
        "signal_gap_p75_all_graded": percentile(graded.get("signal_gap", []), 0.75),
        "signal_gap_p90_all_graded": percentile(graded.get("signal_gap", []), 0.90),
        "core_gap_p75_all_graded": percentile(graded.get("core_gap", []), 0.75),
        "core_gap_p90_all_graded": percentile(graded.get("core_gap", []), 0.90),
        "team_comp_edge_score_p75_all_graded": percentile(graded.get("team_comp_edge_score", []), 0.75),
        "team_comp_edge_score_p90_all_graded": percentile(graded.get("team_comp_edge_score", []), 0.90),
        "claim_validation_p25_all_games": percentile(game_df.get("actual_claim_validation_rate", []), 0.25),
        "claim_validation_p10_all_games": percentile(game_df.get("actual_claim_validation_rate", []), 0.10),
        "overall_game_claim_validation_rate": mean_safe(game_df.get("actual_claim_validation_rate", [])),
    }


def add_driver_flags(game_df: pd.DataFrame, thresholds: Dict[str, Any]) -> pd.DataFrame:
    out = game_df.copy()

    signal_high = thresholds.get("signal_gap_p75_all_graded")
    signal_very_high = thresholds.get("signal_gap_p90_all_graded")
    core_high = thresholds.get("core_gap_p75_all_graded")
    core_very_high = thresholds.get("core_gap_p90_all_graded")
    team_high = thresholds.get("team_comp_edge_score_p75_all_graded")
    team_very_high = thresholds.get("team_comp_edge_score_p90_all_graded")
    claim_low = thresholds.get("claim_validation_p25_all_games")
    claim_very_low = thresholds.get("claim_validation_p10_all_games")

    def ge(value: Any, threshold: Any) -> bool:
        v = as_float(value)
        t = as_float(threshold)
        return bool(v is not None and t is not None and v >= t)

    def le(value: Any, threshold: Any) -> bool:
        v = as_float(value)
        t = as_float(threshold)
        return bool(v is not None and t is not None and v <= t)

    out["huge_signal_gap_flag"] = out["signal_gap"].apply(lambda v: ge(v, signal_high))
    out["extreme_signal_gap_flag"] = out["signal_gap"].apply(lambda v: ge(v, signal_very_high))
    out["big_core_gap_flag"] = out["core_gap"].apply(lambda v: ge(v, core_high))
    out["extreme_core_gap_flag"] = out["core_gap"].apply(lambda v: ge(v, core_very_high))
    out["loud_team_comp_flag"] = out["team_comp_edge_score"].apply(lambda v: ge(v, team_high))
    out["extreme_team_comp_flag"] = out["team_comp_edge_score"].apply(lambda v: ge(v, team_very_high))
    out["weak_claim_validation_flag"] = out["actual_claim_validation_rate"].apply(lambda v: le(v, claim_low))
    out["very_weak_claim_validation_flag"] = out["actual_claim_validation_rate"].apply(lambda v: le(v, claim_very_low))

    out["big_core_gap_weak_claims_flag"] = out["big_core_gap_flag"] & out["weak_claim_validation_flag"]
    out["huge_signal_gap_weak_claims_flag"] = out["huge_signal_gap_flag"] & out["weak_claim_validation_flag"]
    out["loud_team_comp_weak_claims_flag"] = out["loud_team_comp_flag"] & out["weak_claim_validation_flag"]
    out["strong_confirmed_shape_flag"] = out["is_strong_profile"] & out["is_confirmed_edge"]
    out["strong_confirmed_but_poor_result_flag"] = out["strong_confirmed_shape_flag"] & out["is_incorrect"]

    def failure_tags(row: pd.Series) -> str:
        tags = []
        if row.get("is_no_pick_or_tie"):
            tags.append("tie_or_no_decision")
        if row.get("is_severe_miss"):
            tags.append("severe_miss")
        elif row.get("is_close_miss"):
            tags.append("close_miss")
        elif row.get("is_incorrect"):
            tags.append("material_miss")
        if row.get("huge_signal_gap_weak_claims_flag"):
            tags.append("huge_signal_gap_with_weak_claims")
        elif row.get("huge_signal_gap_flag"):
            tags.append("huge_signal_gap")
        if row.get("big_core_gap_weak_claims_flag"):
            tags.append("big_core_gap_with_weak_claims")
        elif row.get("big_core_gap_flag"):
            tags.append("big_core_gap")
        if row.get("loud_team_comp_weak_claims_flag"):
            tags.append("loud_team_comp_with_weak_claims")
        elif row.get("loud_team_comp_flag"):
            tags.append("loud_team_comp")
        if row.get("is_late_or_postseason"):
            tags.append("late_or_postseason")
        if row.get("strong_confirmed_but_poor_result_flag"):
            tags.append("strong_confirmed_poor_result")
        if row.get("weak_claim_validation_flag"):
            tags.append("weak_claim_validation")
        return ";".join(tags) if tags else "no_review_flag"

    out["audit_failure_tags"] = out.apply(failure_tags, axis=1)
    return out


# ---------------------------------------------------------------------------
# Statistics builders
# ---------------------------------------------------------------------------

def summarize_games(rows: pd.DataFrame, segment_name: str, segment_type: str = "segment") -> Dict[str, Any]:
    """Summarize a filtered game-level DataFrame.

    Robustness note:
    Some audit slices can be empty, especially during --limit smoke runs or
    simulation tests from CSV. This function returns a valid zero-row summary
    instead of failing when expected columns are missing.
    """
    games = len(rows)

    def bool_series(column: str) -> pd.Series:
        if column not in rows.columns:
            return pd.Series(False, index=rows.index, dtype=bool)
        return rows[column].apply(lambda v: bool(as_bool(v)) if as_bool(v) is not None else bool(v)).fillna(False).astype(bool)

    def numeric_sum(column: str) -> int:
        if column not in rows.columns or not games:
            return 0
        return int(pd.to_numeric(rows[column], errors="coerce").fillna(0).sum())

    def safe_values(column: str) -> Iterable[Any]:
        if column not in rows.columns:
            return []
        return rows[column]

    is_graded = bool_series("is_graded")
    is_correct = bool_series("is_correct")
    is_incorrect = bool_series("is_incorrect")
    is_no_pick_or_tie = bool_series("is_no_pick_or_tie")
    is_severe_miss = bool_series("is_severe_miss")
    is_close_miss = bool_series("is_close_miss")

    graded = rows[is_graded] if games else rows.iloc[0:0]
    correct = int(is_correct.sum()) if games else 0
    incorrect = int(is_incorrect.sum()) if games else 0
    no_pick = int(is_no_pick_or_tie.sum()) if games else 0
    graded_games = len(graded)

    correct_rate = correct / graded_games if graded_games else None
    correct_ci_low, correct_ci_high = wilson_interval(correct, graded_games) if graded_games else (None, None)

    claim_rows = numeric_sum("claim_rows")
    validated_claim_rows = numeric_sum("validated_claim_rows")
    claim_validation_rate = validated_claim_rows / claim_rows if claim_rows else None
    claim_ci_low, claim_ci_high = wilson_interval(validated_claim_rows, claim_rows) if claim_rows else (None, None)

    severe_misses = int(is_severe_miss.sum()) if games else 0
    close_misses = int(is_close_miss.sum()) if games else 0
    severe_miss_rate = severe_misses / incorrect if incorrect else None
    close_miss_rate = close_misses / incorrect if incorrect else None

    sample_warning = "sufficient_review_sample"
    if games < 10 or graded_games < 10:
        sample_warning = "very_low_game_sample_review_only"
    elif games < 30 or graded_games < 30:
        sample_warning = "low_game_sample_review_only"

    return {
        "segment_type": segment_type,
        "segment_name": segment_name,
        "games": games,
        "graded_games": graded_games,
        "correct_games": correct,
        "incorrect_games": incorrect,
        "no_pick_or_tie_games": no_pick,
        "correct_game_rate": round_rate(correct_rate),
        "correct_game_pct": pct(correct_rate),
        "correct_game_rate_ci_low": round_rate(correct_ci_low),
        "correct_game_rate_ci_high": round_rate(correct_ci_high),
        "claim_rows": claim_rows,
        "validated_claim_rows": validated_claim_rows,
        "claim_validation_rate": round_rate(claim_validation_rate),
        "claim_validation_pct": pct(claim_validation_rate),
        "claim_validation_ci_low": round_rate(claim_ci_low),
        "claim_validation_ci_high": round_rate(claim_ci_high),
        "avg_game_claim_validation_rate": round_rate(mean_safe(safe_values("actual_claim_validation_rate"))),
        "avg_final_margin_abs": round_rate(mean_safe(safe_values("final_margin_abs"))),
        "median_final_margin_abs": round_rate(median_safe(safe_values("final_margin_abs"))),
        "avg_signal_gap": round_rate(mean_safe(safe_values("signal_gap"))),
        "median_signal_gap": round_rate(median_safe(safe_values("signal_gap"))),
        "avg_core_gap": round_rate(mean_safe(safe_values("core_gap"))),
        "median_core_gap": round_rate(median_safe(safe_values("core_gap"))),
        "avg_team_comp_edge_score": round_rate(mean_safe(safe_values("team_comp_edge_score"))),
        "median_team_comp_edge_score": round_rate(median_safe(safe_values("team_comp_edge_score"))),
        "severe_misses": severe_misses,
        "close_misses": close_misses,
        "severe_miss_rate_among_incorrect": round_rate(severe_miss_rate),
        "close_miss_rate_among_incorrect": round_rate(close_miss_rate),
        "sample_warning": sample_warning,
    }

def build_confidence_calibration_summary(game_df: pd.DataFrame) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    rows.append(summarize_games(game_df, "overall", "overall"))

    for conf, group in game_df.groupby("confidence_group", dropna=False):
        rows.append(summarize_games(group, str(conf), "confidence"))

    for profile, group in game_df.groupby("profile_strength_label", dropna=False):
        rows.append(summarize_games(group, clean_text(profile) or "missing_profile_strength", "profile_strength"))

    for profile_type, group in game_df.groupby("profile_type", dropna=False):
        rows.append(summarize_games(group, clean_text(profile_type) or "missing_profile_type", "profile_type"))

    for keys, group in game_df.groupby(["confidence_group", "profile_strength_label"], dropna=False):
        conf, profile = keys
        rows.append(summarize_games(group, f"{conf} | {clean_text(profile) or 'missing_profile_strength'}", "confidence_x_profile_strength"))

    for keys, group in game_df.groupby(["confidence_group", "profile_type"], dropna=False):
        conf, profile_type = keys
        rows.append(summarize_games(group, f"{conf} | {clean_text(profile_type) or 'missing_profile_type'}", "confidence_x_profile_type"))

    if "core_area_durability_band" in game_df.columns:
        durability_order = CORE_AREA_DURABILITY_SORT
        for band, group in sorted(
            game_df.groupby("core_area_durability_band", dropna=False),
            key=lambda item: durability_order.get(str(item[0]), 999),
        ):
            rows.append(summarize_games(group, clean_text(band) or "missing_core_area_durability", "core_area_durability"))

        for keys, group in sorted(
            game_df.groupby(["confidence_group", "core_area_durability_band"], dropna=False),
            key=lambda item: (str(item[0][0]), durability_order.get(str(item[0][1]), 999)),
        ):
            conf, band = keys
            rows.append(summarize_games(group, f"{conf} | {clean_text(band) or 'missing_core_area_durability'}", "confidence_x_core_area_durability"))

    # Key product comparison group: Strong Profile + confirmed_edge, split by confidence.
    strong_confirmed = game_df[game_df["strong_confirmed_shape_flag"]].copy()
    rows.append(summarize_games(strong_confirmed, "Strong Profile + confirmed_edge", "profile_shape"))
    for conf, group in strong_confirmed.groupby("confidence_group", dropna=False):
        rows.append(summarize_games(group, f"{conf} | Strong Profile + confirmed_edge", "confidence_x_strong_confirmed"))

    return rows


def build_high_confidence_game_audit(game_df: pd.DataFrame) -> List[Dict[str, Any]]:
    high = game_df[game_df["confidence_group"] == "High"].copy()
    high = high.sort_values(["is_incorrect", "final_margin_abs", "game_date"], ascending=[False, False, True])
    preferred = [
        "game_id", "game_date", "game_week", "phase_group", "season_type", "bucket",
        "away_team", "home_team", "predicted_team", "actual_winner", "model_result", "model_result_normalized",
        "final_margin_abs", "final_margin_bucket_normalized", "is_correct", "is_incorrect", "is_close_miss", "is_severe_miss",
        "outcome_confidence_label", "profile_strength_label", "profile_type", "matchup_label", "core_area_split",
        "signal_gap", "core_gap", "core_area_durability_band", "core_area_durability_sort", "team_comp_edge_score", "team_comp_away_count", "team_comp_home_count", "team_comp_neutral_count", "team_comp_total_visible",
        "claim_rows", "validated_claim_rows", "actual_claim_validation_rate", "actual_claim_validation_pct",
        "huge_signal_gap_flag", "extreme_signal_gap_flag", "big_core_gap_flag", "extreme_core_gap_flag",
        "loud_team_comp_flag", "extreme_team_comp_flag", "weak_claim_validation_flag", "very_weak_claim_validation_flag",
        "big_core_gap_weak_claims_flag", "huge_signal_gap_weak_claims_flag", "loud_team_comp_weak_claims_flag",
        "is_late_or_postseason", "strong_confirmed_shape_flag", "strong_confirmed_but_poor_result_flag",
        "audit_failure_tags", "qa_read_v2", "confidence_cap_reason",
    ]
    return [{k: row.get(k) for k in preferred if k in row.index} for _, row in high.iterrows()]


def build_medium_promotion_candidate_audit(game_df: pd.DataFrame) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Build summary and game rows for Medium groups that may deserve High review.

    This is NOT a production promotion rule. It is an audit of pregame-shape groups
    that performed better than current High in the 2025 pilot.
    """
    medium = game_df[game_df["confidence_group"] == "Medium"].copy()

    candidate_masks = {
        "medium_strong_profile_confirmed_edge": medium["strong_confirmed_shape_flag"],
        "medium_strong_profile": medium["is_strong_profile"],
        "medium_confirmed_edge": medium["is_confirmed_edge"],
        "medium_strong_or_confirmed": medium["is_strong_profile"] | medium["is_confirmed_edge"],
    }

    summary_rows: List[Dict[str, Any]] = []
    game_rows: List[Dict[str, Any]] = []

    for name, mask in candidate_masks.items():
        subset = medium[mask].copy()
        summary = summarize_games(subset, name, "medium_promotion_candidate_group")
        summary["candidate_logic"] = name.replace("medium_", "Medium + ").replace("_", " ")
        summary["production_use_allowed"] = False
        summary["notes"] = "Review-only candidate group; do not promote confidence from this output alone."
        summary_rows.append(summary)

        subset["candidate_group"] = name
        for _, row in subset.iterrows():
            game_rows.append({
                "candidate_group": name,
                "game_id": row.get("game_id"),
                "game_date": row.get("game_date"),
                "game_week": row.get("game_week"),
                "phase_group": row.get("phase_group"),
                "away_team": row.get("away_team"),
                "home_team": row.get("home_team"),
                "predicted_team": row.get("predicted_team"),
                "actual_winner": row.get("actual_winner"),
                "model_result": row.get("model_result"),
                "model_result_normalized": row.get("model_result_normalized"),
                "final_margin_abs": row.get("final_margin_abs"),
                "final_margin_bucket_normalized": row.get("final_margin_bucket_normalized"),
                "profile_strength_label": row.get("profile_strength_label"),
                "profile_type": row.get("profile_type"),
                "core_area_split": row.get("core_area_split"),
                "signal_gap": row.get("signal_gap"),
                "core_gap": row.get("core_gap"),
                "team_comp_edge_score": row.get("team_comp_edge_score"),
                "actual_claim_validation_rate": row.get("actual_claim_validation_rate"),
                "actual_claim_validation_pct": row.get("actual_claim_validation_pct"),
                "audit_failure_tags": row.get("audit_failure_tags"),
            })

    return summary_rows, game_rows


def build_confidence_failure_reason_summary(game_df: pd.DataFrame) -> List[Dict[str, Any]]:
    high = game_df[game_df["confidence_group"] == "High"].copy()
    high_bad = high[high["is_incorrect"] | high["is_no_pick_or_tie"]].copy()

    flags = [
        ("huge_signal_gap_flag", "High miss/no-decision had signal_gap at or above all-graded p75."),
        ("extreme_signal_gap_flag", "High miss/no-decision had signal_gap at or above all-graded p90."),
        ("big_core_gap_flag", "High miss/no-decision had core_gap at or above all-graded p75."),
        ("extreme_core_gap_flag", "High miss/no-decision had core_gap at or above all-graded p90."),
        ("weak_claim_validation_flag", "High miss/no-decision had game claim validation at or below all-game p25."),
        ("very_weak_claim_validation_flag", "High miss/no-decision had game claim validation at or below all-game p10."),
        ("big_core_gap_weak_claims_flag", "Big core gap paired with weak claim validation."),
        ("huge_signal_gap_weak_claims_flag", "Huge signal gap paired with weak claim validation."),
        ("loud_team_comp_flag", "Team Comparison edge score at or above all-graded p75."),
        ("extreme_team_comp_flag", "Team Comparison edge score at or above all-graded p90."),
        ("loud_team_comp_weak_claims_flag", "Loud Team Comparison paired with weak claim validation."),
        ("is_late_or_postseason", "Game occurred in late season or postseason audit bucket."),
        ("strong_confirmed_but_poor_result_flag", "High game had Strong Profile + confirmed_edge but poor result."),
        ("is_close_miss", "Incorrect High game missed by one score or less."),
        ("is_severe_miss", "Incorrect High game missed by 17+ points."),
    ]

    rows: List[Dict[str, Any]] = []
    total_high = len(high)
    total_bad = len(high_bad)

    for flag, description in flags:
        flagged_bad = high_bad[high_bad[flag].fillna(False)] if flag in high_bad.columns else high_bad.iloc[0:0]
        flagged_high = high[high[flag].fillna(False)] if flag in high.columns else high.iloc[0:0]
        row = summarize_games(flagged_bad, flag, "high_bad_failure_flag")
        row.update({
            "flag": flag,
            "description": description,
            "high_bad_games_with_flag": len(flagged_bad),
            "all_high_games_with_flag": len(flagged_high),
            "share_of_high_bad_games": round_rate(len(flagged_bad) / total_bad) if total_bad else None,
            "share_of_all_high_games": round_rate(len(flagged_high) / total_high) if total_high else None,
            "total_high_bad_games": total_bad,
            "total_high_games": total_high,
        })
        rows.append(row)

    # Add raw tag counts for easier human review.
    tag_counter: Counter[str] = Counter()
    for tags in high_bad.get("audit_failure_tags", []):
        clean_tags = clean_text(tags) or ""
        for tag in clean_tags.split(";"):
            if tag:
                tag_counter[tag] += 1
    for tag, count in sorted(tag_counter.items(), key=lambda kv: (-kv[1], kv[0])):
        rows.append({
            "segment_type": "high_bad_failure_tag",
            "segment_name": tag,
            "flag": tag,
            "description": "Derived semicolon failure tag from high_confidence_game_audit.",
            "high_bad_games_with_flag": count,
            "share_of_high_bad_games": round_rate(count / total_bad) if total_bad else None,
            "total_high_bad_games": total_bad,
            "production_use_allowed": False,
        })

    return rows


def build_high_driver_comparison(game_df: pd.DataFrame) -> List[Dict[str, Any]]:
    high = game_df[game_df["confidence_group"] == "High"].copy()
    rows = [summarize_games(high, "current_high_all", "high_driver_comparison")]
    rows.append(summarize_games(high[high["is_correct"]], "current_high_correct", "high_driver_comparison"))
    rows.append(summarize_games(high[high["is_incorrect"]], "current_high_incorrect", "high_driver_comparison"))
    rows.append(summarize_games(high[high["is_no_pick_or_tie"]], "current_high_no_pick_or_tie", "high_driver_comparison"))

    for phase, group in high.groupby("phase_group", dropna=False):
        rows.append(summarize_games(group, f"current_high_phase_{phase}", "high_phase"))

    return rows


def parse_core_gap_floors(value: Optional[str]) -> List[float]:
    """Parse comma-separated core-gap floors for high-retention simulation."""
    if not value:
        return list(DEFAULT_CORE_GAP_FLOORS)

    floors: List[float] = []
    for part in str(value).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            floors.append(round(float(part), 4))
        except ValueError as exc:
            raise ValueError(f"Invalid --core-gap-floors value: {part!r}") from exc

    # Preserve order while removing duplicates.
    seen = set()
    out: List[float] = []
    for floor in floors:
        if floor not in seen:
            out.append(floor)
            seen.add(floor)
    return out or list(DEFAULT_CORE_GAP_FLOORS)


def _summary_subset_fields(summary: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    """Flatten the most important summarize_games fields under a prefix."""
    wanted = [
        "games",
        "graded_games",
        "correct_games",
        "incorrect_games",
        "no_pick_or_tie_games",
        "correct_game_rate",
        "correct_game_pct",
        "correct_game_rate_ci_low",
        "correct_game_rate_ci_high",
        "claim_rows",
        "validated_claim_rows",
        "claim_validation_rate",
        "claim_validation_pct",
        "claim_validation_ci_low",
        "claim_validation_ci_high",
        "avg_game_claim_validation_rate",
        "avg_final_margin_abs",
        "median_final_margin_abs",
        "avg_signal_gap",
        "median_signal_gap",
        "avg_core_gap",
        "median_core_gap",
        "avg_team_comp_edge_score",
        "median_team_comp_edge_score",
        "severe_misses",
        "close_misses",
        "severe_miss_rate_among_incorrect",
        "close_miss_rate_among_incorrect",
        "sample_warning",
    ]
    return {f"{prefix}_{key}": summary.get(key) for key in wanted}


def build_high_retention_simulation(
    game_df: pd.DataFrame,
    *,
    core_gap_floors: List[float],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Simulate making High harder to keep based on broad Core Area separation.

    Important boundary:
    - This uses pregame-available core_gap only for the simulated decision.
    - Postgame correctness and claim validation are used only to evaluate the
      simulation after the fact.
    - This does not recommend a production rule by itself. It only tests whether
      the core_gap suspect is stable across several floors instead of lucky at
      one cutoff.
    """
    high = game_df[game_df["confidence_group"] == "High"].copy()
    current_high_summary = summarize_games(high, "current_high_all", "current_high_baseline")

    summary_rows: List[Dict[str, Any]] = []
    ladder_rows: List[Dict[str, Any]] = []
    assignment_rows: List[Dict[str, Any]] = []

    for floor in core_gap_floors:
        scenario_name = f"retain_high_core_gap_ge_{floor:.2f}"

        def keep_high(value: Any) -> bool:
            num = as_float(value)
            return num is not None and num >= floor

        retained = high[high["core_gap"].apply(keep_high)].copy()
        downgraded = high[~high["core_gap"].apply(keep_high)].copy()

        retained_summary = summarize_games(retained, f"{scenario_name} | retained_high", "high_retention")
        downgraded_summary = summarize_games(downgraded, f"{scenario_name} | downgraded_high_to_medium", "high_retention")

        row: Dict[str, Any] = {
            "scenario_name": scenario_name,
            "scenario_type": "high_retention_core_gap_floor",
            "core_gap_floor": floor,
            "decision_input_fields": "core_gap_only",
            "pregame_safe_decision": True,
            "production_use_allowed": False,
            "interpretation_note": (
                "Review-only simulation. Tests whether current High games below this core_gap floor looked fragile; "
                "does not define a production confidence rule."
            ),
        }
        row.update(_summary_subset_fields(current_high_summary, "current_high"))
        row.update(_summary_subset_fields(retained_summary, "retained_high"))
        row.update(_summary_subset_fields(downgraded_summary, "downgraded_high"))

        current_rate = as_float(current_high_summary.get("correct_game_rate"))
        retained_rate = as_float(retained_summary.get("correct_game_rate"))
        downgraded_rate = as_float(downgraded_summary.get("correct_game_rate"))
        current_claim = as_float(current_high_summary.get("claim_validation_rate"))
        retained_claim = as_float(retained_summary.get("claim_validation_rate"))
        downgraded_claim = as_float(downgraded_summary.get("claim_validation_rate"))

        row.update({
            "retained_high_lift_vs_current_high_correct_rate": round_rate(retained_rate - current_rate) if retained_rate is not None and current_rate is not None else None,
            "retained_high_lift_vs_current_high_claim_validation": round_rate(retained_claim - current_claim) if retained_claim is not None and current_claim is not None else None,
            "downgraded_high_lift_vs_current_high_correct_rate": round_rate(downgraded_rate - current_rate) if downgraded_rate is not None and current_rate is not None else None,
            "downgraded_high_lift_vs_current_high_claim_validation": round_rate(downgraded_claim - current_claim) if downgraded_claim is not None and current_claim is not None else None,
            "retained_high_share_of_current_high": round_rate(len(retained) / len(high)) if len(high) else None,
            "downgraded_high_share_of_current_high": round_rate(len(downgraded) / len(high)) if len(high) else None,
            "simulation_warning": (
                "very_low_high_sample_review_only" if len(retained) < 10 or len(downgraded) < 5 else
                "low_high_sample_review_only" if len(retained) < 20 else
                "review_sample_ok"
            ),
        })
        summary_rows.append(row)

        # Simulated confidence ladder: current High below the floor becomes Medium.
        sim_df = game_df.copy()
        sim_df["simulated_confidence_group"] = sim_df["confidence_group"]
        high_downgrade_ids = set(downgraded["game_id"].astype(str).tolist()) if not downgraded.empty else set()
        sim_df.loc[sim_df["game_id"].astype(str).isin(high_downgrade_ids), "simulated_confidence_group"] = "Medium"

        for conf_group, group in sim_df.groupby("simulated_confidence_group", dropna=False):
            ladder = summarize_games(group, f"{scenario_name} | simulated_{conf_group}", "simulated_confidence_ladder")
            ladder.update({
                "scenario_name": scenario_name,
                "core_gap_floor": floor,
                "simulated_confidence_group": conf_group,
                "pregame_safe_decision": True,
                "production_use_allowed": False,
            })
            ladder_rows.append(ladder)

        # Per-game assignment output for current High games.
        for _, r in high.sort_values(["core_gap", "game_date"], na_position="last").iterrows():
            core_gap = as_float(r.get("core_gap"))
            retained_flag = bool(core_gap is not None and core_gap >= floor)
            assignment_rows.append({
                "scenario_name": scenario_name,
                "core_gap_floor": floor,
                "game_id": r.get("game_id"),
                "game_date": r.get("game_date"),
                "game_week": r.get("game_week"),
                "phase_group": r.get("phase_group"),
                "away_team": r.get("away_team"),
                "home_team": r.get("home_team"),
                "predicted_team": r.get("predicted_team"),
                "actual_winner": r.get("actual_winner"),
                "model_result": r.get("model_result"),
                "model_result_normalized": r.get("model_result_normalized"),
                "is_correct": r.get("is_correct"),
                "is_incorrect": r.get("is_incorrect"),
                "is_no_pick_or_tie": r.get("is_no_pick_or_tie"),
                "is_close_miss": r.get("is_close_miss"),
                "is_severe_miss": r.get("is_severe_miss"),
                "final_margin_abs": r.get("final_margin_abs"),
                "final_margin_bucket_normalized": r.get("final_margin_bucket_normalized"),
                "profile_strength_label": r.get("profile_strength_label"),
                "profile_type": r.get("profile_type"),
                "signal_gap": r.get("signal_gap"),
                "core_gap": core_gap,
                "core_area_split": r.get("core_area_split"),
                "team_comp_edge_score": r.get("team_comp_edge_score"),
                "actual_claim_validation_rate": r.get("actual_claim_validation_rate"),
                "actual_claim_validation_pct": r.get("actual_claim_validation_pct"),
                "retained_high_flag": retained_flag,
                "simulated_confidence_group": "High" if retained_flag else "Medium",
                "simulated_action": "retain_high" if retained_flag else "downgrade_high_to_medium",
                "pregame_safe_decision": True,
                "production_use_allowed": False,
                "audit_failure_tags": r.get("audit_failure_tags"),
            })

    return summary_rows, ladder_rows, assignment_rows




# ---------------------------------------------------------------------------
# Audit-only core-area durability confidence simulation
# ---------------------------------------------------------------------------

def apply_core_durability_confidence_simulation(
    game_df: pd.DataFrame,
    *,
    core_gap_floor: float,
) -> pd.DataFrame:
    """Simulate softening current High Confidence labels when durability is weak.

    Boundary:
    - This does not change the predicted team or matchup lean.
    - This does not write to BigQuery or alter /game behavior.
    - It only tests whether the High label becomes more meaningful if it must
      be backed by durable Core Area separation.
    """
    out = add_core_area_durability_fields(game_df)

    def should_soften(row: pd.Series) -> bool:
        if row.get("confidence_group") != "High":
            return False
        gap = as_float(row.get("core_gap"))
        return gap is None or gap < core_gap_floor

    soften_mask = out.apply(should_soften, axis=1)
    out["simulated_confidence_group_v0"] = out["confidence_group"]
    out.loc[soften_mask, "simulated_confidence_group_v0"] = "Medium"
    out["confidence_calibration_action_v0"] = "keep_current"
    out.loc[soften_mask, "confidence_calibration_action_v0"] = "soften_high_to_medium"
    out["confidence_calibration_reason_v0"] = "no_calibration_change"
    out.loc[soften_mask, "confidence_calibration_reason_v0"] = "high_signal_but_insufficient_core_area_durability"
    out["confidence_calibration_core_gap_floor_v0"] = core_gap_floor
    out["confidence_calibration_feature_v0"] = "core_area_durability_context_v0"
    out["confidence_calibration_production_use_allowed_v0"] = False
    return out


def build_core_durability_confidence_simulation(
    game_df: pd.DataFrame,
    *,
    core_gap_floor: float,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]], List[Dict[str, Any]]]:
    sim_df = apply_core_durability_confidence_simulation(game_df, core_gap_floor=core_gap_floor)

    ladder_rows: List[Dict[str, Any]] = []
    for conf, group in sim_df.groupby("confidence_group", dropna=False):
        row = summarize_games(group, f"current_{conf}", "current_confidence_ladder")
        row.update({
            "ladder_version": "current",
            "confidence_group": conf,
            "core_gap_floor": core_gap_floor,
            "production_use_allowed": False,
        })
        ladder_rows.append(row)

    for conf, group in sim_df.groupby("simulated_confidence_group_v0", dropna=False):
        row = summarize_games(group, f"simulated_{conf}", "core_durability_simulated_confidence_ladder")
        row.update({
            "ladder_version": "simulated_core_area_durability_v0",
            "confidence_group": conf,
            "core_gap_floor": core_gap_floor,
            "production_use_allowed": False,
        })
        ladder_rows.append(row)

    action_rows: List[Dict[str, Any]] = []
    group_cols = ["confidence_calibration_action_v0"]
    if "season" in sim_df.columns:
        group_cols = ["season", "confidence_calibration_action_v0"]

    for keys, group in sim_df.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (None, keys) if "season" in group_cols else (keys,)
        if "season" in group_cols:
            season_value, action = keys
            segment_name = f"{season_value} | {action}"
        else:
            season_value = None
            action = keys[0]
            segment_name = str(action)

        row = summarize_games(group, segment_name, "core_durability_calibration_action")
        row.update({
            "season": season_value,
            "calibration_action": action,
            "core_gap_floor": core_gap_floor,
            "production_use_allowed": False,
            "decision_input_fields": "confidence_group,core_gap",
            "calibration_feature": "core_area_durability_context_v0",
        })
        action_rows.append(row)

    return sim_df, ladder_rows, action_rows

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_audit(
    *,
    run_id: str,
    season: Optional[str],
    input_csv: Optional[Path],
    input_game_csv: Optional[Path],
    output_root: Path,
    project_id: str,
    dataset_id: str,
    training_table: str,
    limit: Optional[int],
    exclude_unavailable: bool,
    core_gap_floors: List[float],
    calibration_core_gap_floor: float,
    dry_run: bool,
) -> Dict[str, Any]:
    if input_game_csv:
        raw_game_df = pd.read_csv(input_game_csv)
        game_df = prepare_game_audit_csv_rows(raw_game_df, season=season, limit=limit)
        claim_df = pd.DataFrame()
        source_meta = {
            "source": "game_audit_csv",
            "input_game_csv": str(input_game_csv),
            "rows_loaded": int(len(game_df)),
            "note": "Loaded pre-built game_level_confidence_audit.csv; claim-row outputs are rebuilt from game-level fields only.",
        }
    else:
        if input_csv:
            claim_df, source_meta = load_from_csv(input_csv, run_id, season, limit)
        else:
            claim_df, source_meta = load_from_bigquery(
                run_id=run_id,
                season=season,
                project_id=project_id,
                dataset_id=dataset_id,
                training_table=training_table,
                limit=limit,
            )

        if claim_df.empty:
            raise RuntimeError("No claim rows loaded. Check run_id/season/input source.")

        game_df = build_game_rows(claim_df, exclude_unavailable=exclude_unavailable)

    if game_df.empty:
        raise RuntimeError("No game rows loaded. Check run_id/season/input source.")

    game_df = add_core_area_durability_fields(game_df)
    thresholds = build_thresholds(game_df)
    game_df = add_driver_flags(game_df, thresholds)

    output_dir = output_root / sanitize_identifier(run_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    summary_rows = build_confidence_calibration_summary(game_df)
    high_game_rows = build_high_confidence_game_audit(game_df)
    medium_summary_rows, medium_game_rows = build_medium_promotion_candidate_audit(game_df)
    failure_summary_rows = build_confidence_failure_reason_summary(game_df)
    high_driver_rows = build_high_driver_comparison(game_df)
    high_retention_rows, simulated_ladder_rows, high_retention_game_rows = build_high_retention_simulation(
        game_df,
        core_gap_floors=core_gap_floors,
    )
    core_durability_sim_game_df, core_durability_ladder_rows, core_durability_action_rows = build_core_durability_confidence_simulation(
        game_df,
        core_gap_floor=calibration_core_gap_floor,
    )

    # Output paths.
    paths = {
        "game_level_confidence_audit": output_dir / "game_level_confidence_audit.csv",
        "confidence_calibration_summary": output_dir / "confidence_calibration_summary.csv",
        "high_confidence_game_audit": output_dir / "high_confidence_game_audit.csv",
        "medium_promotion_candidate_summary": output_dir / "medium_promotion_candidate_summary.csv",
        "medium_promotion_candidate_games": output_dir / "medium_promotion_candidate_games.csv",
        "confidence_failure_reason_summary": output_dir / "confidence_failure_reason_summary.csv",
        "high_confidence_driver_comparison": output_dir / "high_confidence_driver_comparison.csv",
        "high_retention_simulation_summary": output_dir / "high_retention_simulation_summary.csv",
        "simulated_confidence_ladder_summary": output_dir / "simulated_confidence_ladder_summary.csv",
        "high_retention_game_assignments": output_dir / "high_retention_game_assignments.csv",
        "core_durability_confidence_simulation_games": output_dir / "core_durability_confidence_simulation_games.csv",
        "core_durability_confidence_ladder_summary": output_dir / "core_durability_confidence_ladder_summary.csv",
        "core_durability_confidence_action_summary": output_dir / "core_durability_confidence_action_summary.csv",
        "confidence_calibration_thresholds": output_dir / "confidence_calibration_thresholds.json",
        "run_metadata": output_dir / "run_metadata.json",
    }

    preferred_game_cols = [
        "game_id", "game_date", "game_week", "phase_group", "season_type", "bucket",
        "away_team", "home_team", "predicted_team", "actual_winner", "model_result", "model_result_normalized",
        "confidence_group", "simulated_confidence_group_v0", "confidence_calibration_action_v0", "confidence_calibration_reason_v0", "outcome_confidence_label", "profile_strength_label", "profile_type", "matchup_label",
        "is_correct", "is_incorrect", "is_no_pick_or_tie", "is_graded",
        "final_margin_abs", "final_margin_bucket_normalized", "is_close_miss", "is_severe_miss",
        "claim_rows", "validated_claim_rows", "actual_claim_validation_rate", "actual_claim_validation_pct",
        "signal_gap", "core_gap", "core_area_durability_band", "core_area_durability_sort", "core_area_split", "team_comp_edge_score", "team_comp_away_count", "team_comp_home_count", "team_comp_neutral_count", "team_comp_total_visible",
        "huge_signal_gap_flag", "extreme_signal_gap_flag", "big_core_gap_flag", "extreme_core_gap_flag", "loud_team_comp_flag", "extreme_team_comp_flag",
        "weak_claim_validation_flag", "very_weak_claim_validation_flag", "big_core_gap_weak_claims_flag", "huge_signal_gap_weak_claims_flag", "loud_team_comp_weak_claims_flag",
        "is_late_or_postseason", "strong_confirmed_shape_flag", "strong_confirmed_but_poor_result_flag", "audit_failure_tags", "qa_read_v2", "confidence_cap_reason",
    ]

    write_csv(game_df.to_dict(orient="records"), paths["game_level_confidence_audit"], preferred=preferred_game_cols)
    write_csv(summary_rows, paths["confidence_calibration_summary"])
    write_csv(high_game_rows, paths["high_confidence_game_audit"], preferred=preferred_game_cols)
    write_csv(medium_summary_rows, paths["medium_promotion_candidate_summary"])
    write_csv(medium_game_rows, paths["medium_promotion_candidate_games"])
    write_csv(failure_summary_rows, paths["confidence_failure_reason_summary"])
    write_csv(high_driver_rows, paths["high_confidence_driver_comparison"])
    write_csv(high_retention_rows, paths["high_retention_simulation_summary"])
    write_csv(simulated_ladder_rows, paths["simulated_confidence_ladder_summary"])
    write_csv(high_retention_game_rows, paths["high_retention_game_assignments"])
    write_csv(core_durability_sim_game_df.to_dict(orient="records"), paths["core_durability_confidence_simulation_games"], preferred=preferred_game_cols)
    write_csv(core_durability_ladder_rows, paths["core_durability_confidence_ladder_summary"])
    write_csv(core_durability_action_rows, paths["core_durability_confidence_action_summary"])
    write_json(thresholds, paths["confidence_calibration_thresholds"])

    high_count = int((game_df["confidence_group"] == "High").sum())
    medium_count = int((game_df["confidence_group"] == "Medium").sum())
    low_count = int((game_df["confidence_group"] == "Low").sum())

    metadata = {
        "available": True,
        "version": VERSION,
        "created_at": utc_now_iso(),
        "run_id": run_id,
        "season": season,
        "dry_run_only": bool(dry_run),
        "no_runtime_behavior_changed": True,
        "writes_bigquery": False,
        "source": source_meta,
        "parameters": {
            "exclude_unavailable": exclude_unavailable,
            "limit": limit,
            "core_gap_floors": core_gap_floors,
            "calibration_core_gap_floor": calibration_core_gap_floor,
            "input_game_csv": str(input_game_csv) if input_game_csv else None,
        },
        "row_counts": {
            "raw_claim_rows_loaded": int(len(claim_df)),
            "game_audit_rows_loaded": int(len(game_df)) if input_game_csv else None,
            "games_scored": int(len(game_df)),
            "high_confidence_games": high_count,
            "medium_confidence_games": medium_count,
            "low_confidence_games": low_count,
            "confidence_calibration_summary_rows": len(summary_rows),
            "high_confidence_game_audit_rows": len(high_game_rows),
            "medium_promotion_candidate_summary_rows": len(medium_summary_rows),
            "medium_promotion_candidate_games_rows": len(medium_game_rows),
            "confidence_failure_reason_summary_rows": len(failure_summary_rows),
            "high_confidence_driver_comparison_rows": len(high_driver_rows),
            "high_retention_simulation_summary_rows": len(high_retention_rows),
            "simulated_confidence_ladder_summary_rows": len(simulated_ladder_rows),
            "high_retention_game_assignments_rows": len(high_retention_game_rows),
            "core_durability_confidence_simulation_games_rows": int(len(core_durability_sim_game_df)),
            "core_durability_confidence_ladder_summary_rows": len(core_durability_ladder_rows),
            "core_durability_confidence_action_summary_rows": len(core_durability_action_rows),
        },
        "thresholds": thresholds,
        "outputs": {k: str(v) for k, v in paths.items()},
        "answer_targets": {
            "are_high_misses_huge_signal_gap_games": "Review high_confidence_game_audit.csv and confidence_failure_reason_summary.csv.",
            "are_high_misses_big_core_gap_weak_claims": "Review big_core_gap_weak_claims_flag in high_confidence_game_audit.csv and confidence_failure_reason_summary.csv.",
            "are_high_misses_strong_confirmed_but_poor_margin": "Review strong_confirmed_but_poor_result_flag, final_margin_abs, and high_confidence_driver_comparison.csv.",
            "are_high_misses_late_or_postseason": "Review phase_group/is_late_or_postseason in high_confidence_game_audit.csv.",
            "are_high_misses_loud_team_comparison": "Review loud_team_comp_flag and team_comp_edge_score fields.",
            "do_medium_candidates_look_like_better_high": "Review medium_promotion_candidate_summary.csv and medium_promotion_candidate_games.csv.",
            "would_core_gap_retention_make_high_more_deserving": "Review high_retention_simulation_summary.csv and simulated_confidence_ladder_summary.csv.",
            "which_high_games_move_under_each_core_gap_floor": "Review high_retention_game_assignments.csv.",
            "would_high_softening_make_high_more_meaningful": "Review core_durability_confidence_ladder_summary.csv.",
            "which_games_would_soften_from_high_to_medium": "Review core_durability_confidence_simulation_games.csv where confidence_calibration_action_v0 = soften_high_to_medium.",
        },
        "interpretation_guardrail": (
            "This audit is review-only. It identifies candidate failure patterns and Medium promotion populations, "
            "but it does not define production confidence rules. Core durability confidence simulation is audit-only."
        ),
    }
    write_json(metadata, paths["run_metadata"])
    return metadata


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build offline GameLens confidence calibration audit outputs.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--season", default="2025")
    parser.add_argument("--input-csv", type=Path, default=None, help="Optional claim-row CSV input instead of BigQuery.")
    parser.add_argument("--input-game-csv", type=Path, default=None, help="Optional prebuilt game_level_confidence_audit.csv input for CSV-only simulation testing.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--project-id", default=PROJECT_ID)
    parser.add_argument("--dataset-id", default=DATASET_ID)
    parser.add_argument("--training-table", default=TRAINING_TABLE)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--exclude-unavailable", action="store_true")
    parser.add_argument(
        "--core-gap-floors",
        default=",".join(str(x) for x in DEFAULT_CORE_GAP_FLOORS),
        help="Comma-separated core_gap floors to test for retaining current High Confidence games.",
    )
    parser.add_argument("--calibration-core-gap-floor", type=float, default=DEFAULT_CALIBRATION_CORE_GAP_FLOOR, help="Audit-only floor used to simulate softening current High Confidence to Medium when core_gap is below the floor.")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    metadata = build_audit(
        run_id=args.run_id,
        season=args.season,
        input_csv=args.input_csv,
        input_game_csv=args.input_game_csv,
        output_root=args.output_root,
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        training_table=args.training_table,
        limit=args.limit,
        exclude_unavailable=args.exclude_unavailable,
        core_gap_floors=parse_core_gap_floors(args.core_gap_floors),
        calibration_core_gap_floor=args.calibration_core_gap_floor,
        dry_run=args.dry_run,
    )
    print(json.dumps({
        "available": metadata["available"],
        "version": metadata["version"],
        "run_id": metadata["run_id"],
        "games_scored": metadata["row_counts"]["games_scored"],
        "high_confidence_games": metadata["row_counts"]["high_confidence_games"],
        "output_dir": str(Path(metadata["outputs"]["run_metadata"]).parent),
    }, indent=2))


if __name__ == "__main__":
    main()
