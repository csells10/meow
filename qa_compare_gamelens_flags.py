"""
GameLens before/after QA snapshot runner.

Purpose
-------
Run the same set of game IDs twice:
  1. USE_WINDOWED_METRICS_FOR_GAME=false
  2. USE_WINDOWED_METRICS_FOR_GAME=true

It saves the full /game-style payloads and creates a compact comparison summary.

Why this script imports backend code directly
--------------------------------------------
This avoids Firebase auth and avoids needing the Flask server running.
It also starts a fresh subprocess per game/flag so the env var is applied cleanly
before backend modules are imported.

Safety
------
The worker monkeypatches services.game_service.save_model_results to a no-op so
local QA reads do not insert/update model outcome audit tables.

Run from your repo root, for example:

    python qa_compare_gamelens_flags.py \
      --game-ids 20251020_TB@DET 20251207_CIN@BUF 20251013_BUF@ATL 20251009_PHI@NYG

Or with a text file:

    python qa_compare_gamelens_flags.py --game-file qa/game_ids.txt

Output defaults to:

    qa/flag_compare_runs/<timestamp>/
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_GAME_IDS = [
    "20251020_TB@DET",
    "20251207_CIN@BUF",
    "20251013_BUF@ATL",
    "20251009_PHI@NYG",
]

FLAG_FALSE = "false"
FLAG_TRUE = "true"


def json_safe_dump(data: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True, default=str)


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def clean_game_id_for_filename(game_id: str) -> str:
    return game_id.replace("@", "_at_").replace("/", "_").replace("\\", "_")


def load_game_ids(args: argparse.Namespace) -> List[str]:
    game_ids: List[str] = []

    if args.game_file:
        game_file = Path(args.game_file)
        if not game_file.exists():
            raise FileNotFoundError(f"Game file not found: {game_file}")

        for line in game_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            game_ids.append(line)

    if args.game_ids:
        game_ids.extend(args.game_ids)

    if not game_ids:
        game_ids = DEFAULT_GAME_IDS.copy()

    # Preserve order while removing duplicates.
    seen = set()
    deduped = []
    for game_id in game_ids:
        if game_id not in seen:
            deduped.append(game_id)
            seen.add(game_id)

    return deduped


def get_nested(data: Dict[str, Any], path: List[str], default: Any = None) -> Any:
    current: Any = data
    for key in path:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def extract_metric_better_counts(data: Dict[str, Any]) -> Dict[str, int]:
    counts = {"away": 0, "home": 0, "neutral": 0, "missing": 0}
    for row in data.get("team_comparison", []) or []:
        better = row.get("better")
        if better in counts:
            counts[better] += 1
        else:
            counts["missing"] += 1
    return counts


def extract_core_area_leaders(data: Dict[str, Any]) -> Dict[str, str]:
    leaders = {}
    for row in data.get("core_area_comparison", []) or []:
        core_area = row.get("core_area")
        leader = row.get("leader")
        if core_area:
            leaders[core_area] = leader
    return leaders


def extract_snapshot(data: Dict[str, Any]) -> Dict[str, Any]:
    matchup_lean = data.get("matchup_lean", {}) or {}
    model_outcome = data.get("model_outcome", {}) or {}
    model_trust = data.get("model_trust", {}) or {}
    ranking_context = data.get("ranking_context", {}) or {}
    ranking_meta = ranking_context.get("meta", {}) or {}

    core_area_context = matchup_lean.get("core_area_context", {}) or {}
    signal_score = matchup_lean.get("signal_score", {}) or {}
    edge = model_trust.get("edge", {}) or {}
    matchup_advantage = model_trust.get("matchup_advantage", {}) or {}
    signal_alignment = model_trust.get("signal_alignment", {}) or {}

    team_counts = extract_metric_better_counts(data)
    core_leaders = extract_core_area_leaders(data)

    return {
        "game_id": get_nested(data, ["header", "game_id"]),
        "game_date": get_nested(data, ["header", "game_date"]),
        "away": get_nested(data, ["header", "away_team", "abbreviation"]),
        "home": get_nested(data, ["header", "home_team", "abbreviation"]),
        "final_away_total": get_nested(data, ["final_score", "away", "total"]),
        "final_home_total": get_nested(data, ["final_score", "home", "total"]),
        "target_team": matchup_lean.get("target_team"),
        "target_side": matchup_lean.get("target_side"),
        "confidence": matchup_lean.get("confidence"),
        "profile_type": matchup_lean.get("profile_type"),
        "lean_summary": matchup_lean.get("lean_summary"),
        "confidence_context": matchup_lean.get("confidence_context"),
        "signal_away": signal_score.get("away"),
        "signal_home": signal_score.get("home"),
        "signal_gap": signal_score.get("gap"),
        "core_area_split": core_area_context.get("core_area_split"),
        "core_area_leader": core_area_context.get("core_area_leader"),
        "core_gap": core_area_context.get("core_gap"),
        "away_core_wins": core_area_context.get("away_core_wins"),
        "home_core_wins": core_area_context.get("home_core_wins"),
        "neutral_core_areas": core_area_context.get("neutral_core_areas"),
        "team_comp_away_count": matchup_advantage.get("away", team_counts["away"]),
        "team_comp_home_count": matchup_advantage.get("home", team_counts["home"]),
        "team_comp_neutral_count": matchup_advantage.get("neutral", team_counts["neutral"]),
        "team_comp_decisive_count": matchup_advantage.get("decisive"),
        "team_comp_total_visible": matchup_advantage.get("total_visible"),
        "edge_strength": edge.get("strength"),
        "edge_score": edge.get("score"),
        "signal_alignment_code": signal_alignment.get("summary_code"),
        "signal_alignment_label": signal_alignment.get("summary_label"),
        "model_result": model_outcome.get("result"),
        "predicted_team": model_outcome.get("predicted_team"),
        "actual_winner": model_outcome.get("actual_winner"),
        "learning_label": model_trust.get("learning_label"),
        "ranking_available": ranking_context.get("available"),
        "ranking_as_of_date": ranking_meta.get("as_of_date"),
        "ranking_window_type": ranking_meta.get("window_type"),
        "ranking_max_data_lag_days": ranking_meta.get("max_data_lag_days"),
        "ranking_away_metric_count": ranking_meta.get("away_metric_count"),
        "ranking_home_metric_count": ranking_meta.get("home_metric_count"),
        "ranking_source_data_dates": ",".join(ranking_meta.get("source_data_dates", []) or []),
        "core_area_leaders_json": json.dumps(core_leaders, sort_keys=True),
    }


def compare_snapshots(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "game_id": after.get("game_id") or before.get("game_id"),
        "game_date": after.get("game_date") or before.get("game_date"),
        "away": after.get("away") or before.get("away"),
        "home": after.get("home") or before.get("home"),
        "final_away_total": after.get("final_away_total") or before.get("final_away_total"),
        "final_home_total": after.get("final_home_total") or before.get("final_home_total"),
    }

    fields_to_compare = [
        "target_team",
        "target_side",
        "confidence",
        "profile_type",
        "model_result",
        "predicted_team",
        "actual_winner",
        "signal_gap",
        "core_area_split",
        "core_area_leader",
        "core_gap",
        "team_comp_away_count",
        "team_comp_home_count",
        "team_comp_neutral_count",
        "edge_strength",
        "edge_score",
        "signal_alignment_code",
    ]

    for field in fields_to_compare:
        row[f"before_{field}"] = before.get(field)
        row[f"after_{field}"] = after.get(field)
        row[f"changed_{field}"] = before.get(field) != after.get(field)

    # Include ranking QA fields from both runs. Ranking context is independent of
    # the main table flag, but keeping both helps prove it stayed stable.
    ranking_fields = [
        "ranking_available",
        "ranking_as_of_date",
        "ranking_window_type",
        "ranking_max_data_lag_days",
        "ranking_away_metric_count",
        "ranking_home_metric_count",
        "ranking_source_data_dates",
    ]
    for field in ranking_fields:
        row[f"before_{field}"] = before.get(field)
        row[f"after_{field}"] = after.get(field)

    row["before_lean_summary"] = before.get("lean_summary")
    row["after_lean_summary"] = after.get("lean_summary")
    row["before_confidence_context"] = before.get("confidence_context")
    row["after_confidence_context"] = after.get("confidence_context")
    row["before_core_area_leaders_json"] = before.get("core_area_leaders_json")
    row["after_core_area_leaders_json"] = after.get("core_area_leaders_json")

    return row


def run_worker(game_id: str, flag_value: str, output_path: Path) -> None:
    """Worker mode: import backend, call get_game_details, save full JSON."""
    os.environ["USE_WINDOWED_METRICS_FOR_GAME"] = flag_value

    # Keep repo root importable when script is run from a qa/ folder.
    repo_root = Path.cwd()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from services import game_service  # pylint: disable=import-outside-toplevel

    # Avoid inserting QA reads into model outcome/detail tables.
    game_service.save_model_results = lambda *args, **kwargs: None

    data = game_service.get_game_details(game_id)
    json_safe_dump(data, output_path)


def run_snapshot_subprocess(
    *,
    script_path: Path,
    game_id: str,
    flag_value: str,
    output_path: Path,
    verbose: bool = False,
) -> None:
    env = os.environ.copy()
    env["USE_WINDOWED_METRICS_FOR_GAME"] = flag_value

    cmd = [
        sys.executable,
        str(script_path),
        "--worker",
        "--game-id",
        game_id,
        "--flag-value",
        flag_value,
        "--output-path",
        str(output_path),
    ]

    result = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    if verbose and result.stdout:
        print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(
            f"Worker failed for game_id={game_id}, flag={flag_value}, "
            f"returncode={result.returncode}"
        )


def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    if not rows:
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_main(args: argparse.Namespace) -> None:
    game_ids = load_game_ids(args)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_root = Path(args.output_dir or "qa/flag_compare_runs") / run_id

    false_dir = output_root / "flag_false"
    true_dir = output_root / "flag_true"
    summary_dir = output_root / "summary"

    script_path = Path(__file__).resolve()

    print(f"GameLens QA flag compare run: {run_id}")
    print(f"Games: {len(game_ids)}")
    print(f"Output: {output_root}")
    print()

    comparison_rows: List[Dict[str, Any]] = []
    before_rows: List[Dict[str, Any]] = []
    after_rows: List[Dict[str, Any]] = []

    for index, game_id in enumerate(game_ids, 1):
        safe_name = clean_game_id_for_filename(game_id)
        before_path = false_dir / f"{safe_name}.json"
        after_path = true_dir / f"{safe_name}.json"

        print(f"[{index}/{len(game_ids)}] {game_id} | flag=false")
        run_snapshot_subprocess(
            script_path=script_path,
            game_id=game_id,
            flag_value=FLAG_FALSE,
            output_path=before_path,
            verbose=args.verbose,
        )

        print(f"[{index}/{len(game_ids)}] {game_id} | flag=true")
        run_snapshot_subprocess(
            script_path=script_path,
            game_id=game_id,
            flag_value=FLAG_TRUE,
            output_path=after_path,
            verbose=args.verbose,
        )

        before_data = read_json(before_path)
        after_data = read_json(after_path)

        before_snapshot = extract_snapshot(before_data)
        after_snapshot = extract_snapshot(after_data)

        before_rows.append(before_snapshot)
        after_rows.append(after_snapshot)
        comparison_rows.append(compare_snapshots(before_snapshot, after_snapshot))

    json_safe_dump(
        {
            "run_id": run_id,
            "game_count": len(game_ids),
            "game_ids": game_ids,
            "output_root": str(output_root),
            "note": (
                "Full JSON payloads are saved under flag_false/ and flag_true/. "
                "CSV/JSON summaries are saved under summary/."
            ),
        },
        output_root / "run_manifest.json",
    )

    write_csv(before_rows, summary_dir / "before_flag_false_summary.csv")
    write_csv(after_rows, summary_dir / "after_flag_true_summary.csv")
    write_csv(comparison_rows, summary_dir / "before_after_comparison.csv")

    json_safe_dump(before_rows, summary_dir / "before_flag_false_summary.json")
    json_safe_dump(after_rows, summary_dir / "after_flag_true_summary.json")
    json_safe_dump(comparison_rows, summary_dir / "before_after_comparison.json")

    print()
    print("Done.")
    print(f"Full before payloads: {false_dir}")
    print(f"Full after payloads:  {true_dir}")
    print(f"Comparison CSV:       {summary_dir / 'before_after_comparison.csv'}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run GameLens /game before/after snapshots with USE_WINDOWED_METRICS_FOR_GAME false vs true."
    )

    parser.add_argument(
        "--game-ids",
        nargs="*",
        help="Game IDs to test, e.g. 20251020_TB@DET 20251207_CIN@BUF",
    )
    parser.add_argument(
        "--game-file",
        help="Optional text file with one game_id per line. Blank lines and # comments are ignored.",
    )
    parser.add_argument(
        "--output-dir",
        default="qa/flag_compare_runs",
        help="Base output directory. A timestamped folder will be created inside it.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print worker stdout if any.",
    )

    # Internal worker args.
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--game-id", help=argparse.SUPPRESS)
    parser.add_argument("--flag-value", choices=[FLAG_FALSE, FLAG_TRUE], help=argparse.SUPPRESS)
    parser.add_argument("--output-path", help=argparse.SUPPRESS)

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.worker:
        if not args.game_id or not args.flag_value or not args.output_path:
            raise ValueError("Worker mode requires --game-id, --flag-value, and --output-path")
        run_worker(
            game_id=args.game_id,
            flag_value=args.flag_value,
            output_path=Path(args.output_path),
        )
        return

    run_main(args)


if __name__ == "__main__":
    main()
