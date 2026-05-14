"""
Update GameLens claim training examples with Level 3 engineered feature scores.

V2 scopes offense_finish_score to claim families where the feature showed useful signal.

Recommended repo location:
    agg/update_gamelens_claim_training_features_v2.py

Purpose:
    Level 3 job for GameLens feature engineering.

    Level 1 built one row per pregame GameLens claim.
    Level 2 attached postgame validation labels.
    Level 3 begins adding pregame-only engineered features that can later help
    predict claim quality and calibrate language.

This v1 only computes:
    offense_finish_score

Football idea:
    Can the claimed team both move the ball and finish drives?

Important:
    offense_finish_score is pregame-only. It uses the pregame Core Area
    Comparison rows already stored in gamelens_claim_training_examples:
        - Offensive Output
        - Scoring Efficiency

    It does NOT use validation_result, actual_gap, final_score, or any other
    postgame field. That prevents leakage.

Score meaning:
    Range is roughly -1.0 to +1.0.
        Positive = claimed team had combined Offensive Output + Scoring Efficiency support.
        Near 0   = mixed/near-even offensive finish profile.
        Negative = opponent had the stronger offensive finish profile.

Example dry run:
    python -m agg.update_gamelens_claim_training_features_v2 \
      --run-id baseline_96_stage1_v2 \
      --dry-run

Example BigQuery update:
    python -m agg.update_gamelens_claim_training_features_v2 \
      --run-id baseline_96_stage1_v2 \
      --write-bigquery
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


PROJECT_ID = "nfl-stream-406420"
DATASET_ID = "Analytics"
TRAINING_TABLE = "gamelens_claim_training_examples"
DEFAULT_OUTPUT_ROOT = Path("qa/gamelens_feature_update_runs")
DEFAULT_FORMULA_VERSION = "offense_finish_v2_relevance_scoped"

REQUIRED_CORE_AREAS = ("Offensive Output", "Scoring Efficiency")

# V2 finding:
# offense_finish_score helped most for finishing-context claims, not as a
# universal offensive feature. Keep the score scoped so defensive/pressure/
# turnover rows do not accidentally inherit an offensive context feature.
OFFENSE_FINISH_RELEVANT_CORE_AREAS = {
    "Scoring Efficiency",
}

OFFENSE_FINISH_RELEVANT_CATEGORIES = {
    "Drive Conversion",
    "Red Zone Finish",
    "Rushing Game",
}

OFFENSE_FINISH_RELEVANT_METRICS = {
    "third_down_pct",
    "red_zone_efficiency",
    "yards_per_rush",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def clamp(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def write_json(data: Any, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def write_csv(rows: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return

    preferred = [
        "run_id",
        "claim_key",
        "game_id",
        "claimed_side",
        "claimed_team",
        "claim_type",
        "claim_layer",
        "claim_name",
        "core_area",
        "category",
        "metric",
        "offense_finish_score",
        "offense_finish_relevance_reason",
        "away_offense_finish_score",
        "home_offense_finish_score",
        "offensive_output_away_edge",
        "scoring_efficiency_away_edge",
        "feature_formula_version",
        "feature_status",
        "feature_notes",
        "updated_at",
    ]
    all_keys = sorted({key for row in rows for key in row.keys()})
    fieldnames = [key for key in preferred if key in all_keys] + [key for key in all_keys if key not in preferred]

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
    table_ref: str,
    run_id: str,
    limit: Optional[int],
) -> List[Dict[str, Any]]:
    limit_clause = "LIMIT @limit" if limit else ""

    query = f"""
        SELECT
            claim_key,
            run_id,
            game_id,
            season,
            claimed_team,
            claimed_side,
            opponent_team,
            opponent_side,
            claim_type,
            claim_layer,
            claim_name,
            core_area,
            category,
            metric,
            pregame_raw_gap,
            claimed_team_value,
            opponent_team_value,
            core_area_agreement_rate,
            feature_status,
            feature_notes,
            feature_formula_version
        FROM `{table_ref}`
        WHERE run_id = @run_id
        ORDER BY season, game_id, claim_type, claim_layer, claim_rank
        {limit_clause}
    """

    params = [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
    if limit:
        params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return [dict(row) for row in client.query(query, job_config=job_config).result()]


# ---------------------------------------------------------------------------
# Feature formula
# ---------------------------------------------------------------------------

def build_core_area_edges(training_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    """
    Returns:
        {
            game_id: {
                "Offensive Output": away_signed_edge,
                "Scoring Efficiency": away_signed_edge,
            }
        }

    away_signed_edge:
        Positive if away led that core area.
        Negative if home led that core area.

    Source rows:
        claim_type = core_area_comparison
        core_area in REQUIRED_CORE_AREAS

    Why core_area_comparison?
        This is the cleanest high-level pregame source for "which side led the
        Core Area" and avoids double-counting summary/detail rows.
    """
    edges: Dict[str, Dict[str, float]] = defaultdict(dict)

    for row in training_rows:
        if row.get("claim_type") != "core_area_comparison":
            continue

        core_area = row.get("core_area")
        if core_area not in REQUIRED_CORE_AREAS:
            continue

        game_id = row.get("game_id")
        claimed_side = row.get("claimed_side")
        raw_gap = as_float(row.get("pregame_raw_gap"))

        if not game_id or claimed_side not in {"away", "home"} or raw_gap is None:
            continue

        # pregame_raw_gap is leader score - opponent score from the builder.
        away_signed = raw_gap if claimed_side == "away" else -raw_gap
        edges[str(game_id)][str(core_area)] = away_signed

    return edges


def calculate_offense_finish_score(
    *,
    offensive_output_edge: Optional[float],
    scoring_efficiency_edge: Optional[float],
    side: str,
) -> Optional[float]:
    """
    Compute a side-specific offense_finish_score.

    Inputs are away-signed core area edges.
    If side is home, flip signs so the score is from that team's perspective.

    Formula v1:
        side_offense_edge = signed Offensive Output edge
        side_scoring_edge = signed Scoring Efficiency edge

        base = 0.45 * side_offense_edge + 0.55 * side_scoring_edge

        if both are positive:
            synergy bonus = 0.15 * min(side_offense_edge, side_scoring_edge)

        if one positive and one negative:
            conflict penalty = 0.10 * min(abs(side_offense_edge), abs(side_scoring_edge))

        score = clamp(base + synergy - conflict_penalty, -1, 1)

    Why heavier scoring efficiency?
        The 96-game QA showed scoring-related claims validated better than some
        noisier areas, and football-wise finishing drives matters more than
        empty yardage.

    Return:
        None if either required core area is missing.
    """
    if offensive_output_edge is None or scoring_efficiency_edge is None:
        return None

    if side == "away":
        offense = offensive_output_edge
        scoring = scoring_efficiency_edge
    elif side == "home":
        offense = -offensive_output_edge
        scoring = -scoring_efficiency_edge
    else:
        return None

    base = (0.45 * offense) + (0.55 * scoring)

    synergy_bonus = 0.0
    conflict_penalty = 0.0

    if offense > 0 and scoring > 0:
        synergy_bonus = 0.15 * min(offense, scoring)
    elif (offense > 0 > scoring) or (scoring > 0 > offense):
        conflict_penalty = 0.10 * min(abs(offense), abs(scoring))

    return round(clamp(base + synergy_bonus - conflict_penalty), 4)


def offense_finish_relevance_reason(row: Dict[str, Any]) -> Optional[str]:
    """
    V2 scoping rule.

    Only apply offense_finish_score to claim families where the first QA pass
    showed useful signal and where the football meaning is sensible.

    Included:
        - Scoring Efficiency core-area level claims
        - Drive Conversion
        - Red Zone Finish
        - Rushing Game
        - Direct metrics: third_down_pct, red_zone_efficiency, yards_per_rush

    Excluded for now:
        - Generic Offensive Output
        - Offensive Rhythm
        - Passing Game
        - Scoring Production
        - Defensive Control
        - Pressure / Turnovers

    This keeps the feature from looking noisy when evaluated against unrelated
    claim families.
    """
    core_area = row.get("core_area")
    category = row.get("category")
    metric = row.get("metric")

    # Avoid applying broad Scoring Efficiency to the weaker Scoring Production
    # family unless the direct metric is specifically relevant.
    if metric in OFFENSE_FINISH_RELEVANT_METRICS:
        return f"metric:{metric}"

    if category in OFFENSE_FINISH_RELEVANT_CATEGORIES:
        return f"category:{category}"

    if core_area in OFFENSE_FINISH_RELEVANT_CORE_AREAS and not category:
        return f"core_area:{core_area}"

    return None


def is_offense_finish_relevant(row: Dict[str, Any]) -> bool:
    return offense_finish_relevance_reason(row) is not None


def build_feature_updates(
    training_rows: List[Dict[str, Any]],
    *,
    formula_version: str,
) -> List[Dict[str, Any]]:
    core_edges = build_core_area_edges(training_rows)
    now = utc_now_iso()
    updates: List[Dict[str, Any]] = []

    for row in training_rows:
        game_id = str(row.get("game_id") or "")
        claimed_side = row.get("claimed_side")
        game_edges = core_edges.get(game_id, {})

        offensive_output_away_edge = game_edges.get("Offensive Output")
        scoring_efficiency_away_edge = game_edges.get("Scoring Efficiency")

        away_score = calculate_offense_finish_score(
            offensive_output_edge=offensive_output_away_edge,
            scoring_efficiency_edge=scoring_efficiency_away_edge,
            side="away",
        )
        home_score = calculate_offense_finish_score(
            offensive_output_edge=offensive_output_away_edge,
            scoring_efficiency_edge=scoring_efficiency_away_edge,
            side="home",
        )

        relevance_reason = offense_finish_relevance_reason(row)

        if relevance_reason is None:
            # V2 change:
            # Do not apply the score to unrelated claim families.
            row_score = None
            level3_note = (
                " | Level 3 v2: offense_finish_score intentionally left NULL because "
                "this claim family is not offense-finish relevant."
            )
        elif claimed_side == "away":
            row_score = away_score
            level3_note = (
                " | Level 3 v2: offense_finish_score applied to relevant claim family "
                f"({relevance_reason}); computed pregame-only from Core Area Comparison "
                "(Offensive Output + Scoring Efficiency)."
            )
        elif claimed_side == "home":
            row_score = home_score
            level3_note = (
                " | Level 3 v2: offense_finish_score applied to relevant claim family "
                f"({relevance_reason}); computed pregame-only from Core Area Comparison "
                "(Offensive Output + Scoring Efficiency)."
            )
        else:
            row_score = None
            level3_note = (
                " | Level 3 v2: offense_finish_score not calculated because claimed_side was missing/invalid."
            )

        if relevance_reason is not None and row_score is None:
            level3_note = (
                " | Level 3 v2: offense_finish_score not calculated for relevant claim because "
                "required Core Area comparison rows were missing."
            )

        feature_notes = (row.get("feature_notes") or "") + level3_note

        updates.append({
            "run_id": row.get("run_id"),
            "claim_key": row.get("claim_key"),
            "game_id": game_id,
            "claimed_team": row.get("claimed_team"),
            "claimed_side": claimed_side,
            "claim_type": row.get("claim_type"),
            "claim_layer": row.get("claim_layer"),
            "claim_name": row.get("claim_name"),
            "core_area": row.get("core_area"),
            "category": row.get("category"),
            "metric": row.get("metric"),
            "offense_finish_score": row_score,
            "offense_finish_relevance_reason": relevance_reason,
            "away_offense_finish_score": away_score,
            "home_offense_finish_score": home_score,
            "offensive_output_away_edge": offensive_output_away_edge,
            "scoring_efficiency_away_edge": scoring_efficiency_away_edge,
            "feature_formula_version": formula_version,
            "feature_status": row.get("feature_status"),
            "feature_notes": feature_notes,
            "updated_at": now,
        })

    return updates


# ---------------------------------------------------------------------------
# BigQuery update
# ---------------------------------------------------------------------------

def feature_update_schema(bigquery: Any) -> List[Any]:
    return [
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("claim_key", "STRING"),
        bigquery.SchemaField("offense_finish_score", "FLOAT"),
        bigquery.SchemaField("feature_formula_version", "STRING"),
        bigquery.SchemaField("feature_notes", "STRING"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]


def update_bigquery_rows(
    *,
    client: Any,
    bigquery: Any,
    project_id: str,
    target_table: str,
    update_rows: List[Dict[str, Any]],
    run_id: str,
) -> None:
    if not update_rows:
        print("No feature rows to update.")
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_run = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in run_id)[:80]
    temp_table = f"{project_id}.{DATASET_ID}._tmp_gamelens_claim_features_{safe_run}_{timestamp}"

    bq_rows = [
        {
            "run_id": row["run_id"],
            "claim_key": row["claim_key"],
            "offense_finish_score": row["offense_finish_score"],
            "feature_formula_version": row["feature_formula_version"],
            "feature_notes": row["feature_notes"],
            "updated_at": row["updated_at"],
        }
        for row in update_rows
    ]

    job_config = bigquery.LoadJobConfig(
        schema=feature_update_schema(bigquery),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = client.load_table_from_json(bq_rows, temp_table, job_config=job_config)
    load_job.result()
    print(f"Loaded {len(bq_rows)} feature rows into temp table {temp_table}")

    merge_sql = f"""
        MERGE `{target_table}` AS T
        USING `{temp_table}` AS S
        ON T.run_id = S.run_id
           AND T.claim_key = S.claim_key
        WHEN MATCHED THEN UPDATE SET
            offense_finish_score = S.offense_finish_score,
            feature_formula_version = S.feature_formula_version,
            feature_notes = S.feature_notes,
            updated_at = S.updated_at
    """
    client.query(merge_sql).result()
    print(f"✅ Updated offense_finish_score in {target_table} for run_id={run_id}")

    client.delete_table(temp_table, not_found_ok=True)
    print(f"Deleted temp table {temp_table}")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def bucket_score(score: Optional[float]) -> str:
    if score is None:
        return "missing"
    if score >= 0.40:
        return "strong_positive"
    if score >= 0.15:
        return "positive"
    if score > -0.15:
        return "mixed_near_even"
    if score > -0.40:
        return "negative"
    return "strong_negative"


def build_summary(update_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    bucket_counts: Dict[str, int] = {}
    relevance_counts: Dict[str, int] = {}
    non_null_scores = []

    for row in update_rows:
        bucket = bucket_score(row.get("offense_finish_score"))
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1

        relevance_reason = row.get("offense_finish_relevance_reason")
        relevance_key = relevance_reason or "not_relevant"
        relevance_counts[relevance_key] = relevance_counts.get(relevance_key, 0) + 1

        if row.get("offense_finish_score") is not None:
            non_null_scores.append(row["offense_finish_score"])

    relevant_rows = [r for r in update_rows if r.get("offense_finish_relevance_reason")]
    relevant_missing = [
        r for r in relevant_rows
        if r.get("offense_finish_score") is None
    ]

    return {
        "formula_version": DEFAULT_FORMULA_VERSION,
        "rows_updated": len(update_rows),
        "rows_relevant_for_offense_finish": len(relevant_rows),
        "rows_not_relevant_for_offense_finish": len(update_rows) - len(relevant_rows),
        "rows_with_offense_finish_score": len(non_null_scores),
        "rows_missing_offense_finish_score": len(update_rows) - len(non_null_scores),
        "relevant_rows_missing_offense_finish_score": len(relevant_missing),
        "min_offense_finish_score": min(non_null_scores) if non_null_scores else None,
        "max_offense_finish_score": max(non_null_scores) if non_null_scores else None,
        "avg_offense_finish_score": round(sum(non_null_scores) / len(non_null_scores), 4) if non_null_scores else None,
        "score_bucket_distribution": dict(sorted(bucket_counts.items())),
        "relevance_reason_distribution": dict(sorted(relevance_counts.items())),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update GameLens claim training examples with Level 3 engineered feature scores. V2 scopes offense_finish_score to relevant claim families.")
    parser.add_argument("--run-id", required=True, help="run_id in Analytics.gamelens_claim_training_examples.")
    parser.add_argument("--project-id", default=PROJECT_ID)
    parser.add_argument("--training-table", default=f"{PROJECT_ID}.{DATASET_ID}.{TRAINING_TABLE}")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--formula-version", default=DEFAULT_FORMULA_VERSION)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true", help="Write local preview CSV/JSON only. Recommended first.")
    parser.add_argument("--write-bigquery", action="store_true", help="MERGE feature fields back into BigQuery.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    bigquery = import_bigquery()
    client = bigquery.Client(project=args.project_id)

    output_dir = Path(args.output_root) / args.run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading training rows for run_id={args.run_id}")
    training_rows = load_training_rows(
        client=client,
        bigquery=bigquery,
        table_ref=args.training_table,
        run_id=args.run_id,
        limit=args.limit,
    )
    print(f"Training rows loaded: {len(training_rows)}")

    update_rows = build_feature_updates(
        training_rows=training_rows,
        formula_version=args.formula_version,
    )
    summary = build_summary(update_rows)

    write_csv(update_rows, output_dir / "feature_update_preview.csv")
    write_json(summary, output_dir / "summary.json")

    print("\nFeature build complete")
    print("----------------------")
    print(f"Feature rows built: {len(update_rows)}")
    print(f"Rows with offense_finish_score: {summary['rows_with_offense_finish_score']}")
    print(f"Rows missing offense_finish_score: {summary['rows_missing_offense_finish_score']}")
    print(f"Preview CSV: {output_dir / 'feature_update_preview.csv'}")
    print(f"Summary JSON: {output_dir / 'summary.json'}")

    if args.write_bigquery:
        update_bigquery_rows(
            client=client,
            bigquery=bigquery,
            project_id=args.project_id,
            target_table=args.training_table,
            update_rows=update_rows,
            run_id=args.run_id,
        )
    else:
        print("BigQuery update skipped. Use --write-bigquery to update the training table.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
