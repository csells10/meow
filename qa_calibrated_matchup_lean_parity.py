from __future__ import annotations

import argparse
from collections import Counter
from typing import Any, Iterable, Mapping

from services.confidence_calibration import (
    apply_core_area_durability_confidence_calibration,
)


DEFAULT_PROJECT_ID = "nfl-stream-406420"
DEFAULT_SOURCE_TABLE = (
    "nfl-stream-406420.Analytics.gamelens_claim_training_examples"
)
DEFAULT_RUN_ID = "full_2025_reg_post_claim_matrix_pilot"
EXPECTED_RAW = {"Low": 154, "Medium": 93, "High": 22}
EXPECTED_CALIBRATED = {"Low": 154, "Medium": 103, "High": 12}


def _value(row: Any, key: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return getattr(row, key)


def build_parity_summary(rows: Iterable[Any]) -> dict:
    raw_counts: Counter[str] = Counter()
    calibrated_counts: Counter[str] = Counter()
    changed_games = []
    conflict_games = []

    for row in rows:
        game_id = str(_value(row, "game_id"))
        raw_label = _value(row, "raw_confidence_label")
        core_gap = _value(row, "core_gap")
        label_variants = int(_value(row, "label_variant_count") or 0)
        core_gap_variants = int(_value(row, "core_gap_variant_count") or 0)

        if label_variants > 1 or core_gap_variants > 1:
            conflict_games.append(game_id)
            continue

        calibration = apply_core_area_durability_confidence_calibration(
            confidence_label=raw_label,
            core_gap=core_gap,
        )
        calibrated_label = calibration["calibrated_confidence_label"]

        raw_counts[str(raw_label or "Missing")] += 1
        calibrated_counts[str(calibrated_label or "Missing")] += 1

        if calibration["confidence_calibrated"]:
            changed_games.append(
                {
                    "game_id": game_id,
                    "raw_confidence": raw_label,
                    "calibrated_confidence": calibrated_label,
                    "core_gap": calibration["core_gap"],
                }
            )

    labels = ("Low", "Medium", "High")
    raw = {label: raw_counts[label] for label in labels}
    calibrated = {label: calibrated_counts[label] for label in labels}

    return {
        "raw": raw,
        "calibrated": calibrated,
        "game_count": sum(raw_counts.values()),
        "changed_count": len(changed_games),
        "changed_games": changed_games,
        "conflict_count": len(conflict_games),
        "conflict_games": conflict_games,
        "unexpected_raw_labels": {
            key: value
            for key, value in raw_counts.items()
            if key not in labels
        },
        "unexpected_calibrated_labels": {
            key: value
            for key, value in calibrated_counts.items()
            if key not in labels
        },
    }


def render_visual(summary: dict, *, run_id: str, source_table: str) -> str:
    rows = (
        ("Stored 2025 baseline", summary["raw"]),
        ("Shared helper result", summary["calibrated"]),
        ("Admin benchmark", EXPECTED_CALIBRATED),
    )
    lines = [
        "",
        "CALIBRATED MATCHUP LEAN — READ-ONLY PARITY QA",
        "------------------------------------------------",
        f"run_id: {run_id}",
        f"source: {source_table}",
        "",
        f"{'Source':<24}{'Low':>8}{'Medium':>10}{'High':>8}{'Total':>9}",
    ]

    for label, counts in rows:
        total = sum(counts.values())
        lines.append(
            f"{label:<24}{counts['Low']:>8}{counts['Medium']:>10}"
            f"{counts['High']:>8}{total:>9}"
        )

    passed = (
        summary["raw"] == EXPECTED_RAW
        and summary["calibrated"] == EXPECTED_CALIBRATED
        and summary["changed_count"] == 10
        and summary["conflict_count"] == 0
        and not summary["unexpected_raw_labels"]
        and not summary["unexpected_calibrated_labels"]
    )
    lines.extend(
        [
            "",
            f"High → Medium moves: {summary['changed_count']}",
            f"Conflicting game rows: {summary['conflict_count']}",
            f"RESULT: {'PASS' if passed else 'FAIL'}",
        ]
    )
    return "\n".join(lines)


def load_game_rows(*, project_id: str, source_table: str, run_id: str) -> list:
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT
      game_id,
      MIN(NULLIF(TRIM(CAST(outcome_confidence_label AS STRING)), ''))
        AS raw_confidence_label,
      MIN(SAFE_CAST(core_gap AS FLOAT64)) AS core_gap,
      COUNT(DISTINCT outcome_confidence_label) AS label_variant_count,
      COUNT(DISTINCT SAFE_CAST(core_gap AS FLOAT64))
        AS core_gap_variant_count
    FROM `{source_table}`
    WHERE run_id = @run_id
    GROUP BY game_id
    ORDER BY game_id
    """
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id)
        ]
    )
    return list(client.query(query, job_config=config).result())


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compare the shared confidence helper with the existing 2025 "
            "Admin benchmark. This command is read-only."
        )
    )
    parser.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    parser.add_argument("--source-table", default=DEFAULT_SOURCE_TABLE)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    args = parser.parse_args()

    rows = load_game_rows(
        project_id=args.project_id,
        source_table=args.source_table,
        run_id=args.run_id,
    )
    summary = build_parity_summary(rows)
    visual = render_visual(
        summary,
        run_id=args.run_id,
        source_table=args.source_table,
    )
    print(visual)
    return 0 if "RESULT: PASS" in visual else 1


if __name__ == "__main__":
    raise SystemExit(main())
