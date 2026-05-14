"""
GameLens direct backend QA payload collector.

Purpose
-------
Collect full /game-style GameLens payloads quickly without:
  - clicking around in the UI
  - starting the local Flask API server
  - dealing with Firebase auth
  - waiting on one game at a time in the browser

This script imports the backend service directly and calls:

    services.game_service.get_game_details(game_id)

It is designed for fast local QA while you tweak model language, matchup
analysis, confidence wording, ranking summaries, or other /game output logic.

Why this does NOT call the local HTTP API
----------------------------------------
For language/model iteration, the local server adds friction. The older QA
script pattern worked better because it imported backend code directly. This
script follows that approach.

Safety
------
Each game is collected in a fresh worker subprocess. Inside the worker, the
script monkeypatches:

    services.game_service.save_model_results = lambda *args, **kwargs: None

That prevents local QA reads from inserting/updating model outcome or trust audit
rows while still building the full /game-style response.

Recommended first run
---------------------
Run this from your repo root using your normal project virtual environment:

    python qa_collect_gamelens_payloads.py

Default behavior:
  - seasons: 2023, 2024, 2025
  - games per season: 12
  - sample style: scattered across early/mid/late regular season + postseason
  - output folder: qa/gamelens_payload_runs/<timestamp>/

Good follow-up run after code/model changes
-------------------------------------------
Reuse the exact same sample so the before/after comparison is fair:

    python qa_collect_gamelens_payloads.py \
      --sample-file qa/gamelens_payload_runs/<FIRST_RUN>/sampled_games.json \
      --compare-to qa/gamelens_payload_runs/<FIRST_RUN>

Useful options
--------------
Collect specific games:

    python qa_collect_gamelens_payloads.py \
      --game-ids 20251020_TB@DET 20251207_CIN@BUF 20251013_BUF@ATL

Collect games from a text file:

    python qa_collect_gamelens_payloads.py --game-file qa/game_ids.txt

Change sample size:

    python qa_collect_gamelens_payloads.py --games-per-season 20

Collect a different repeatable random sample:

    python qa_collect_gamelens_payloads.py \
      --seasons 2023 2024 2025 \
      --games-per-season 32 \
      --sample-mode random \
      --random-seed 20260514 \
      --run-name fresh_96_features_qa_v2

Dry run the selected sample without collecting payloads:

    python qa_collect_gamelens_payloads.py --dry-run

Main outputs
------------
Inside qa/gamelens_payload_runs/<timestamp>/:

  sampled_games.json
      The selected games and their season/week/bucket metadata.

  payloads/<season>/<game_id>.json
      Full /game-style payload for each game.

  game_snapshots.csv
      Compact row-per-game QA extract for scanning changes quickly.

  qa_review.md
      Human-readable run summary with distributions and games to review.

  chatgpt_analysis_packet.json
      Compact structured packet you can upload/paste back to ChatGPT for review.

  compare_to_previous.csv
      Created only when --compare-to is provided.

Assumptions
-----------
- Run from the repo root unless you pass --repo-root.
- Your Google Application Default Credentials / BigQuery access already work.
- The repo has services.game_service and queries.game_queries importable.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import re
import subprocess
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


PROJECT_ID = "nfl-stream-406420"
SCHEDULE_TABLE = f"{PROJECT_ID}.League.schedule"
FINAL_STATUSES = {"Final", "Final/OT"}
DEFAULT_SEASONS = ["2023", "2024", "2025"]
DEFAULT_GAMES_PER_SEASON = 12
DEFAULT_OUTPUT_ROOT = Path("qa/gamelens_payload_runs")

# Fields where before/after differences are most meaningful for language/model QA.
COMPARE_FIELDS = [
    "target_team",
    "target_side",
    "confidence",
    "profile_type",
    "matchup_label",
    "profile_strength_label",
    "outcome_confidence_label",
    "model_result",
    "predicted_team",
    "actual_winner",
    "core_area_split",
    "core_area_leader",
    "edge_strength",
    "signal_alignment_code",
    "matchup_cautions_json",
    "metric_highlights_count",
    "category_summaries_count",
    "core_area_summaries_count",
    "context_notes_count",
]


@dataclass
class SampleGame:
    game_id: str
    season: str
    game_date: str = ""
    game_week: str = ""
    season_type: str = ""
    game_status: str = ""
    away: str = ""
    home: str = ""
    bucket: str = "manual"


# -----------------------------------------------------------------------------
# JSON / CSV helpers
# -----------------------------------------------------------------------------


def utc_timestamp_for_folder() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_utc")


def json_safe_default(value: Any) -> str:
    return str(value)


def write_json(data: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True, default=json_safe_default)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def read_csv_dicts(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def clean_game_id_for_filename(game_id: str) -> str:
    return (
        str(game_id)
        .replace("@", "_at_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
    )


# -----------------------------------------------------------------------------
# BigQuery sampling
# -----------------------------------------------------------------------------


def import_bigquery_client():
    try:
        from google.cloud import bigquery  # type: ignore
    except Exception as exc:  # pragma: no cover - environment-specific
        raise RuntimeError(
            "Could not import google.cloud.bigquery. Run this in your project venv "
            "or pass --game-ids / --game-file / --sample-file to avoid sampling."
        ) from exc
    return bigquery


def normalize_season(value: Any) -> str:
    return str(value or "").strip()[:4]


def parse_regular_week(game_week: str) -> Optional[int]:
    match = re.fullmatch(r"\s*Week\s+(\d+)\s*", str(game_week or ""), flags=re.I)
    if not match:
        return None
    return int(match.group(1))


def classify_bucket(game_week: str, include_preseason: bool = False) -> str:
    raw = str(game_week or "").strip()
    lowered = raw.lower()

    if lowered.startswith("preseason"):
        return "preseason" if include_preseason else "skip_preseason"

    week = parse_regular_week(raw)
    if week is not None:
        if week <= 6:
            return "early_regular"
        if week <= 12:
            return "mid_regular"
        return "late_regular"

    postseason_names = {
        "wild card",
        "wildcard",
        "divisional round",
        "conference championship",
        "super bowl",
    }
    if lowered in postseason_names:
        return "postseason"

    return "unknown"


def fetch_candidate_games_from_bigquery(
    *,
    seasons: Sequence[str],
    project_id: str,
    schedule_table: str,
    include_preseason: bool,
) -> List[SampleGame]:
    bigquery = import_bigquery_client()
    client = bigquery.Client(project=project_id)

    query = f"""
        SELECT
            CAST(season AS STRING) AS season,
            gameID AS game_id,
            CAST(gameDate AS STRING) AS game_date,
            gameWeek AS game_week,
            seasonType AS season_type,
            gameStatus AS game_status,
            away,
            home
        FROM `{schedule_table}`
        WHERE CAST(season AS STRING) IN UNNEST(@seasons)
          AND gameStatus IN UNNEST(@final_statuses)
        ORDER BY season, gameDate, gameID
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("seasons", "STRING", [str(s) for s in seasons]),
            bigquery.ArrayQueryParameter("final_statuses", "STRING", sorted(FINAL_STATUSES)),
        ]
    )

    print(f"Loading candidate games from {schedule_table} for seasons {', '.join(seasons)} ...")
    rows = list(client.query(query, job_config=job_config).result())

    games: List[SampleGame] = []
    for row in rows:
        bucket = classify_bucket(row.get("game_week"), include_preseason=include_preseason)
        if bucket in {"skip_preseason", "unknown"}:
            continue

        games.append(
            SampleGame(
                game_id=str(row.get("game_id")),
                season=normalize_season(row.get("season")),
                game_date=str(row.get("game_date") or ""),
                game_week=str(row.get("game_week") or ""),
                season_type=str(row.get("season_type") or ""),
                game_status=str(row.get("game_status") or ""),
                away=str(row.get("away") or ""),
                home=str(row.get("home") or ""),
                bucket=bucket,
            )
        )

    print(f"Candidate games loaded: {len(games)}")
    return games


def choose_evenly(items: List[SampleGame], count: int) -> List[SampleGame]:
    """Choose roughly evenly spaced games from a sorted list."""
    if count <= 0 or not items:
        return []
    if count >= len(items):
        return items[:]
    if count == 1:
        return [items[len(items) // 2]]

    # Even spacing across the list, including early and late points.
    indexes = []
    for i in range(count):
        idx = round(i * (len(items) - 1) / (count - 1))
        indexes.append(idx)

    selected = []
    seen = set()
    for idx in indexes:
        idx = max(0, min(idx, len(items) - 1))
        if idx not in seen:
            selected.append(items[idx])
            seen.add(idx)

    # Fill any gaps caused by rounding collisions.
    if len(selected) < count:
        for idx, item in enumerate(items):
            if idx not in seen:
                selected.append(item)
                seen.add(idx)
            if len(selected) >= count:
                break

    return selected[:count]


def choose_randomly(items: Sequence[SampleGame], count: int, rng: random.Random) -> List[SampleGame]:
    """Choose a random subset without replacement, preserving no original ordering guarantee."""
    if count <= 0 or not items:
        return []
    if count >= len(items):
        shuffled = list(items)
        rng.shuffle(shuffled)
        return shuffled
    return rng.sample(list(items), count)


def allocate_bucket_counts(
    *,
    available_by_bucket: Dict[str, List[SampleGame]],
    total: int,
    include_preseason: bool,
) -> Dict[str, int]:
    """Allocate a smart per-season sample across football phases."""
    if total <= 0:
        return {}

    weights = {
        "preseason": 0.10 if include_preseason else 0.0,
        "early_regular": 0.25,
        "mid_regular": 0.25,
        "late_regular": 0.30,
        "postseason": 0.20,
    }

    available_buckets = [
        bucket for bucket in weights
        if available_by_bucket.get(bucket) and weights[bucket] > 0
    ]

    if not available_buckets:
        return {}

    total_weight = sum(weights[b] for b in available_buckets)
    raw_alloc = {b: (weights[b] / total_weight) * total for b in available_buckets}
    counts = {b: int(math.floor(raw_alloc[b])) for b in available_buckets}

    # Give every available bucket at least one slot when possible.
    if total >= len(available_buckets):
        for bucket in available_buckets:
            counts[bucket] = max(1, counts[bucket])

    # Respect available counts.
    for bucket in available_buckets:
        counts[bucket] = min(counts[bucket], len(available_by_bucket[bucket]))

    # Add/remove until total is hit.
    def current_total() -> int:
        return sum(counts.values())

    # Fill remaining by largest fractional remainder / available space.
    while current_total() < total:
        candidates = [
            b for b in available_buckets
            if counts[b] < len(available_by_bucket[b])
        ]
        if not candidates:
            break
        candidates.sort(key=lambda b: (raw_alloc[b] - counts[b], weights[b]), reverse=True)
        counts[candidates[0]] += 1

    # Trim if minimum-one logic overshot.
    while current_total() > total:
        candidates = [b for b in available_buckets if counts[b] > 0]
        candidates.sort(key=lambda b: (counts[b] - raw_alloc[b], counts[b]), reverse=True)
        counts[candidates[0]] -= 1

    return counts


def smart_sample_games(
    candidates: List[SampleGame],
    *,
    seasons: Sequence[str],
    games_per_season: int,
    include_preseason: bool,
    sample_mode: str = "even",
    random_seed: Optional[int] = None,
) -> List[SampleGame]:
    """
    Select a balanced per-season sample.

    sample_mode:
        even:
            deterministic evenly spaced games inside each season/bucket.
            Good for stable baseline runs.

        random:
            random games inside each season/bucket using random_seed.
            Good for fresh QA samples while keeping season/bucket balance.
    """
    if sample_mode not in {"even", "random"}:
        raise ValueError(f"Unsupported sample_mode={sample_mode!r}; use 'even' or 'random'.")

    rng = random.Random(random_seed)

    by_season: Dict[str, List[SampleGame]] = defaultdict(list)
    for game in candidates:
        by_season[str(game.season)].append(game)

    selected: List[SampleGame] = []

    def choose(items: Sequence[SampleGame], count: int) -> List[SampleGame]:
        if sample_mode == "random":
            return choose_randomly(items, count, rng)
        return choose_evenly(items, count)

    for season in [str(s) for s in seasons]:
        season_games = sorted(
            by_season.get(season, []),
            key=lambda g: (g.game_date, g.game_id),
        )
        by_bucket: Dict[str, List[SampleGame]] = defaultdict(list)
        for game in season_games:
            by_bucket[game.bucket].append(game)

        counts = allocate_bucket_counts(
            available_by_bucket=by_bucket,
            total=games_per_season,
            include_preseason=include_preseason,
        )

        season_selected: List[SampleGame] = []
        for bucket, count in counts.items():
            bucket_games = sorted(by_bucket.get(bucket, []), key=lambda g: (g.game_date, g.game_id))
            season_selected.extend(choose(bucket_games, count))

        # If a season was short in one bucket, top off from all remaining games.
        if len(season_selected) < games_per_season:
            already = {g.game_id for g in season_selected}
            remaining = [g for g in season_games if g.game_id not in already]
            season_selected.extend(choose(remaining, games_per_season - len(season_selected)))

        season_selected = sorted(
            dedupe_sample_games(season_selected),
            key=lambda g: (g.game_date, g.game_id),
        )[:games_per_season]

        selected.extend(season_selected)

    return selected


def dedupe_sample_games(games: Iterable[SampleGame]) -> List[SampleGame]:
    seen = set()
    deduped: List[SampleGame] = []
    for game in games:
        if game.game_id in seen:
            continue
        seen.add(game.game_id)
        deduped.append(game)
    return deduped


# -----------------------------------------------------------------------------
# Manual/sample-file loading
# -----------------------------------------------------------------------------


def read_game_ids_from_file(path: Path) -> List[str]:
    ids: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        ids.append(line)
    return ids


def sample_games_from_ids(game_ids: Sequence[str]) -> List[SampleGame]:
    games: List[SampleGame] = []
    for game_id in game_ids:
        season = str(game_id)[:4] if len(str(game_id)) >= 4 else ""
        games.append(SampleGame(game_id=str(game_id), season=season, bucket="manual"))
    return dedupe_sample_games(games)


def load_sample_file(path: Path) -> List[SampleGame]:
    payload = read_json(path)
    if isinstance(payload, dict):
        rows = payload.get("games") or payload.get("sampled_games") or []
    else:
        rows = payload

    games: List[SampleGame] = []
    for row in rows:
        if isinstance(row, str):
            games.append(SampleGame(game_id=row, season=row[:4], bucket="sample_file"))
            continue
        games.append(
            SampleGame(
                game_id=str(row.get("game_id") or row.get("gameID") or ""),
                season=str(row.get("season") or str(row.get("game_id") or row.get("gameID") or "")[:4]),
                game_date=str(row.get("game_date") or row.get("gameDate") or ""),
                game_week=str(row.get("game_week") or row.get("gameWeek") or ""),
                season_type=str(row.get("season_type") or row.get("seasonType") or ""),
                game_status=str(row.get("game_status") or row.get("gameStatus") or ""),
                away=str(row.get("away") or ""),
                home=str(row.get("home") or ""),
                bucket=str(row.get("bucket") or "sample_file"),
            )
        )

    return [g for g in dedupe_sample_games(games) if g.game_id]


# -----------------------------------------------------------------------------
# Worker mode: direct backend collection
# -----------------------------------------------------------------------------


def run_worker_mode(args: argparse.Namespace) -> int:
    repo_root = Path(args.repo_root).resolve()
    os.chdir(repo_root)

    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    # Keep logs quieter unless the caller explicitly wants repo logging behavior.
    os.environ.setdefault("LOG_LEVEL", args.log_level or "WARNING")

    try:
        from services import game_service  # type: ignore  # pylint: disable=import-outside-toplevel
    except Exception as exc:
        error_payload = {
            "game_id": args.worker_game_id,
            "error": "backend_import_failed",
            "message": str(exc),
        }
        write_json(error_payload, Path(args.worker_output_path))
        print(json.dumps(error_payload), file=sys.stderr)
        return 2

    # Avoid writing QA reads into model outcome/detail tables.
    if hasattr(game_service, "save_model_results"):
        game_service.save_model_results = lambda *a, **k: None

    try:
        data = game_service.get_game_details(args.worker_game_id)
        write_json(data, Path(args.worker_output_path))
        return 0
    except Exception as exc:
        error_payload = {
            "game_id": args.worker_game_id,
            "error": "get_game_details_failed",
            "message": str(exc),
        }
        write_json(error_payload, Path(args.worker_output_path))
        print(json.dumps(error_payload), file=sys.stderr)
        return 1


# -----------------------------------------------------------------------------
# Payload extraction
# -----------------------------------------------------------------------------


def get_nested(data: Dict[str, Any], path: Sequence[str], default: Any = None) -> Any:
    current: Any = data
    for key in path:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def side_to_team(side: Optional[str], header: Dict[str, Any]) -> Optional[str]:
    if side == "away":
        return get_nested(header, ["away_team", "abbreviation"])
    if side == "home":
        return get_nested(header, ["home_team", "abbreviation"])
    return None


def extract_team_comparison_counts(payload: Dict[str, Any]) -> Dict[str, int]:
    counts = Counter()
    for row in payload.get("team_comparison", []) or []:
        better = row.get("better") or "missing"
        counts[better] += 1
        if row.get("comparison_strength") == "near_even":
            counts["near_even"] += 1
    return {
        "away": int(counts.get("away", 0)),
        "home": int(counts.get("home", 0)),
        "neutral": int(counts.get("neutral", 0)),
        "missing": int(counts.get("missing", 0)),
        "near_even": int(counts.get("near_even", 0)),
    }


def extract_core_area_leaders(payload: Dict[str, Any]) -> Dict[str, str]:
    leaders: Dict[str, str] = {}
    for row in payload.get("core_area_comparison", []) or []:
        area = row.get("core_area")
        if area:
            leaders[str(area)] = str(row.get("leader") or "")
    return leaders


def compact_game_profile(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    compact = []
    for row in payload.get("game_profile", []) or []:
        compact.append({
            "category": row.get("category"),
            "level": row.get("level"),
            "tilt_team": row.get("tilt_team"),
            "tilt_text": row.get("tilt_text") or row.get("tilt"),
        })
    return compact


def compact_summary_rows(rows: List[Dict[str, Any]], limit: int = 5) -> List[Dict[str, Any]]:
    compact = []
    for row in rows[:limit]:
        compact.append({
            "name": row.get("name") or row.get("label") or row.get("metric"),
            "metric": row.get("metric"),
            "leader": row.get("leader"),
            "leader_team": row.get("leader_team"),
            "summary_label": row.get("summary_label"),
            "summary": row.get("summary"),
            "percentile_gap": row.get("percentile_gap"),
        })
    return compact


def extract_snapshot(payload: Dict[str, Any], sample_meta: Optional[SampleGame] = None) -> Dict[str, Any]:
    header = payload.get("header", {}) or {}
    final_score = payload.get("final_score", {}) or {}
    matchup_lean = payload.get("matchup_lean", {}) or {}
    model_outcome = payload.get("model_outcome", {}) or {}
    model_trust = payload.get("model_trust", {}) or {}
    ranking_context = payload.get("ranking_context", {}) or {}
    ranking_meta = ranking_context.get("meta", {}) or {}
    breakdown = payload.get("matchup_breakdown", {}) or {}
    summary_counts = breakdown.get("summary_counts", {}) or {}

    core_context = matchup_lean.get("core_area_context", {}) or {}
    signal_score = matchup_lean.get("signal_score", {}) or {}
    profile_strength = matchup_lean.get("profile_strength", {}) or {}
    outcome_confidence = matchup_lean.get("outcome_confidence", {}) or {}
    matchup_advantage = model_trust.get("matchup_advantage", {}) or {}
    edge = model_trust.get("edge", {}) or {}
    signal_alignment = model_trust.get("signal_alignment", {}) or {}
    reasoning = model_trust.get("reasoning", {}) or {}

    away = get_nested(header, ["away_team", "abbreviation"])
    home = get_nested(header, ["home_team", "abbreviation"])
    away_total = get_nested(final_score, ["away", "total"])
    home_total = get_nested(final_score, ["home", "total"])

    actual_winner_from_score = None
    margin = None
    try:
        if away_total is not None and home_total is not None:
            margin = int(home_total) - int(away_total)
            if int(away_total) > int(home_total):
                actual_winner_from_score = away
            elif int(home_total) > int(away_total):
                actual_winner_from_score = home
            else:
                actual_winner_from_score = "tie"
    except Exception:
        pass

    team_counts = extract_team_comparison_counts(payload)
    core_leaders = extract_core_area_leaders(payload)
    game_profile_compact = compact_game_profile(payload)

    team_comparison = payload.get("team_comparison", []) or []
    has_turnover_margin_per_game = any(
        row.get("metric") == "turnover_margin_per_game" for row in team_comparison
    )

    metric_highlights = breakdown.get("metric_highlights", []) or []
    category_summaries = breakdown.get("category_summaries", []) or []
    core_area_summaries = breakdown.get("core_area_summaries", []) or []
    context_notes = breakdown.get("context_notes", []) or []

    error = payload.get("error")

    row = {
        "game_id": get_nested(header, ["game_id"]) or payload.get("game_id") or (sample_meta.game_id if sample_meta else None),
        "season": str(get_nested(header, ["season"]) or (sample_meta.season if sample_meta else ""))[:4],
        "game_date": get_nested(header, ["game_date"]) or (sample_meta.game_date if sample_meta else ""),
        "game_week": get_nested(header, ["game_week"]) or (sample_meta.game_week if sample_meta else ""),
        "game_status": get_nested(header, ["game_status"]) or (sample_meta.game_status if sample_meta else ""),
        "bucket": sample_meta.bucket if sample_meta else "",
        "away": away or (sample_meta.away if sample_meta else ""),
        "home": home or (sample_meta.home if sample_meta else ""),
        "final_away_total": away_total,
        "final_home_total": home_total,
        "home_margin": margin,
        "actual_winner_from_score": actual_winner_from_score,

        "target_team": matchup_lean.get("target_team"),
        "target_side": matchup_lean.get("target_side"),
        "confidence": matchup_lean.get("confidence"),
        "profile_type": matchup_lean.get("profile_type"),
        "lean_summary": matchup_lean.get("lean_summary"),
        "confidence_context": matchup_lean.get("confidence_context"),
        "matchup_label": matchup_lean.get("matchup_label"),
        "matchup_cautions_json": json.dumps(matchup_lean.get("matchup_cautions") or [], sort_keys=True),
        "profile_strength_code": profile_strength.get("code"),
        "profile_strength_label": profile_strength.get("label"),
        "profile_strength_summary": profile_strength.get("summary"),
        "outcome_confidence_code": outcome_confidence.get("code"),
        "outcome_confidence_label": outcome_confidence.get("label"),
        "outcome_confidence_summary": outcome_confidence.get("summary"),
        "signal_away": signal_score.get("away"),
        "signal_home": signal_score.get("home"),
        "signal_gap": signal_score.get("gap"),

        "core_area_split": core_context.get("core_area_split"),
        "core_area_leader": core_context.get("core_area_leader"),
        "core_gap": core_context.get("core_gap"),
        "away_core_wins": core_context.get("away_core_wins"),
        "home_core_wins": core_context.get("home_core_wins"),
        "neutral_core_areas": core_context.get("neutral_core_areas"),
        "total_core_areas": core_context.get("total_core_areas"),
        "core_area_leaders_json": json.dumps(core_leaders, sort_keys=True),

        "team_comp_away_count": matchup_advantage.get("away", team_counts["away"]),
        "team_comp_home_count": matchup_advantage.get("home", team_counts["home"]),
        "team_comp_neutral_count": matchup_advantage.get("neutral", team_counts["neutral"]),
        "team_comp_near_even_count": team_counts["near_even"],
        "team_comp_decisive_count": matchup_advantage.get("decisive"),
        "team_comp_total_visible": matchup_advantage.get("total_visible"),
        "edge_strength": edge.get("strength"),
        "edge_score": edge.get("score"),

        "model_result": model_outcome.get("result"),
        "predicted_team": model_outcome.get("predicted_team"),
        "actual_winner": model_outcome.get("actual_winner") or actual_winner_from_score,
        "learning_label": model_trust.get("learning_label"),
        "reasoning_headline": reasoning.get("headline"),
        "reasoning_summary": reasoning.get("summary"),
        "signal_alignment_code": signal_alignment.get("summary_code"),
        "signal_alignment_label": signal_alignment.get("summary_label"),
        "signal_alignment_aligned_count": signal_alignment.get("aligned_count"),
        "signal_alignment_total_count": signal_alignment.get("total_count"),

        "ranking_available": ranking_context.get("available"),
        "ranking_reason": ranking_context.get("reason"),
        "ranking_as_of_date": ranking_meta.get("as_of_date"),
        "ranking_window_type": ranking_meta.get("window_type"),
        "ranking_max_data_lag_days": ranking_meta.get("max_data_lag_days"),
        "ranking_away_metric_count": ranking_meta.get("away_metric_count"),
        "ranking_home_metric_count": ranking_meta.get("home_metric_count"),
        "ranking_source_data_dates_json": json.dumps(ranking_meta.get("source_data_dates") or [], sort_keys=True),

        "matchup_breakdown_available": breakdown.get("available"),
        "metric_highlights_count": len(metric_highlights),
        "category_summaries_count": len(category_summaries),
        "core_area_summaries_count": len(core_area_summaries),
        "context_notes_count": len(context_notes),
        "summary_counts_json": json.dumps(summary_counts, sort_keys=True),
        "top_metric_highlights_json": json.dumps(compact_summary_rows(metric_highlights, limit=5), sort_keys=True),
        "top_category_summaries_json": json.dumps(compact_summary_rows(category_summaries, limit=5), sort_keys=True),
        "top_core_area_summaries_json": json.dumps(compact_summary_rows(core_area_summaries, limit=5), sort_keys=True),

        "game_profile_json": json.dumps(game_profile_compact, sort_keys=True),
        "has_turnover_margin_per_game": has_turnover_margin_per_game,
        "payload_error": error,
    }

    return row


# -----------------------------------------------------------------------------
# Collection orchestration
# -----------------------------------------------------------------------------


def payload_path_for_game(output_dir: Path, game: SampleGame) -> Path:
    season = game.season or str(game.game_id)[:4] or "unknown_season"
    return output_dir / "payloads" / season / f"{clean_game_id_for_filename(game.game_id)}.json"


def log_path_for_game(output_dir: Path, game: SampleGame) -> Path:
    season = game.season or str(game.game_id)[:4] or "unknown_season"
    return output_dir / "worker_logs" / season / f"{clean_game_id_for_filename(game.game_id)}.log"


def run_game_worker(
    *,
    script_path: Path,
    repo_root: Path,
    game: SampleGame,
    output_path: Path,
    log_path: Path,
    timeout_seconds: int,
    log_level: str,
    verbose: bool,
) -> Tuple[bool, Optional[str], float]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        str(script_path),
        "--worker",
        "--worker-game-id",
        game.game_id,
        "--worker-output-path",
        str(output_path),
        "--repo-root",
        str(repo_root),
        "--log-level",
        log_level,
    ]

    started = time.time()
    try:
        result = subprocess.run(
            cmd,
            cwd=str(repo_root),
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
        elapsed = time.time() - started
    except subprocess.TimeoutExpired as exc:
        elapsed = time.time() - started
        message = f"Timed out after {timeout_seconds}s"
        log_path.write_text(
            f"COMMAND: {' '.join(cmd)}\n\nTIMEOUT: {message}\nSTDOUT:\n{exc.stdout or ''}\n\nSTDERR:\n{exc.stderr or ''}",
            encoding="utf-8",
        )
        return False, message, elapsed

    log_text = (
        f"COMMAND: {' '.join(cmd)}\n"
        f"RETURN_CODE: {result.returncode}\n"
        f"ELAPSED_SECONDS: {elapsed:.2f}\n\n"
        f"STDOUT:\n{result.stdout}\n\n"
        f"STDERR:\n{result.stderr}\n"
    )
    log_path.write_text(log_text, encoding="utf-8")

    if verbose and (result.stdout or result.stderr):
        print(log_text)

    if result.returncode != 0:
        message = (result.stderr or result.stdout or f"worker return code {result.returncode}").strip()
        return False, message[-1000:], elapsed

    if not output_path.exists():
        return False, "worker succeeded but payload file was not created", elapsed

    return True, None, elapsed


# -----------------------------------------------------------------------------
# Summary / reporting
# -----------------------------------------------------------------------------


def count_by(rows: List[Dict[str, Any]], field: str) -> Dict[str, int]:
    counts = Counter(str(row.get(field) or "missing") for row in rows)
    return dict(sorted(counts.items(), key=lambda item: item[0]))


def markdown_table(rows: List[Dict[str, Any]], columns: List[str], max_rows: int = 30) -> str:
    if not rows:
        return "_None._\n"

    clipped = rows[:max_rows]
    lines = []
    lines.append("| " + " | ".join(columns) + " |")
    lines.append("| " + " | ".join(["---"] * len(columns)) + " |")
    for row in clipped:
        values = []
        for col in columns:
            value = row.get(col, "")
            text = str(value if value is not None else "")
            text = text.replace("\n", " ").replace("|", "\\|")
            if len(text) > 120:
                text = text[:117] + "..."
            values.append(text)
        lines.append("| " + " | ".join(values) + " |")

    if len(rows) > max_rows:
        lines.append(f"\n_Showing {max_rows} of {len(rows)} rows._")

    return "\n".join(lines) + "\n"


def build_review_markdown(
    *,
    output_dir: Path,
    sampled_games: List[SampleGame],
    snapshots: List[Dict[str, Any]],
    errors: List[Dict[str, Any]],
    compare_rows: Optional[List[Dict[str, Any]]],
    run_metadata: Dict[str, Any],
) -> str:
    total = len(sampled_games)
    ok = len(snapshots)
    failed = len(errors)

    high_conf_incorrect = [
        r for r in snapshots
        if str(r.get("confidence") or "").lower() == "high"
        and str(r.get("model_result") or "").lower() == "incorrect"
    ]
    no_pick_games = [r for r in snapshots if str(r.get("model_result") or "").lower() == "no pick"]
    low_conf_edges = [
        r for r in snapshots
        if str(r.get("confidence") or "").lower() == "low"
        and r.get("target_team")
        and str(r.get("target_team")) not in {"None", "No Pick", "No strong directional edge"}
    ]
    missing_rankings = [r for r in snapshots if str(r.get("ranking_available")).lower() not in {"true", "1"}]
    missing_breakdown = [r for r in snapshots if str(r.get("matchup_breakdown_available")).lower() not in {"true", "1"}]
    missing_turnover = [r for r in snapshots if str(r.get("has_turnover_margin_per_game")).lower() not in {"true", "1"}]

    lines: List[str] = []
    lines.append("# GameLens QA Payload Run")
    lines.append("")
    lines.append(f"Generated: `{run_metadata.get('created_at')}`")
    lines.append(f"Output folder: `{output_dir}`")
    lines.append("")

    lines.append("## Run Summary")
    lines.append("")
    lines.append(f"- Games selected: **{total}**")
    lines.append(f"- Payloads collected: **{ok}**")
    lines.append(f"- Failed games: **{failed}**")
    lines.append(f"- Seasons: `{', '.join(run_metadata.get('seasons') or [])}`")
    lines.append(f"- Games per season target: `{run_metadata.get('games_per_season')}`")
    lines.append("")

    lines.append("## Files to Give ChatGPT")
    lines.append("")
    lines.append("Start with these:")
    lines.append("")
    lines.append("```text")
    lines.append(str(output_dir / "qa_review.md"))
    lines.append(str(output_dir / "game_snapshots.csv"))
    lines.append(str(output_dir / "chatgpt_analysis_packet.json"))
    lines.append("```")
    lines.append("")
    lines.append("For deeper review, also upload specific full payloads from:")
    lines.append("")
    lines.append("```text")
    lines.append(str(output_dir / "payloads"))
    lines.append("```")
    lines.append("")

    lines.append("## Sample Distribution")
    lines.append("")
    sample_rows = [asdict(g) for g in sampled_games]
    dist_rows = []
    for season, count in count_by(sample_rows, "season").items():
        dist_rows.append({"field": "season", "value": season, "count": count})
    for bucket, count in count_by(sample_rows, "bucket").items():
        dist_rows.append({"field": "bucket", "value": bucket, "count": count})
    lines.append(markdown_table(dist_rows, ["field", "value", "count"], max_rows=50))
    lines.append("")

    lines.append("## Output Distributions")
    lines.append("")
    output_dist_rows = []
    for field in [
        "confidence",
        "model_result",
        "profile_type",
        "matchup_label",
        "profile_strength_label",
        "outcome_confidence_label",
        "edge_strength",
        "signal_alignment_code",
    ]:
        for value, count in count_by(snapshots, field).items():
            output_dist_rows.append({"field": field, "value": value, "count": count})
    lines.append(markdown_table(output_dist_rows, ["field", "value", "count"], max_rows=100))
    lines.append("")

    lines.append("## Games Selected")
    lines.append("")
    lines.append(markdown_table(sample_rows, ["season", "bucket", "game_date", "game_week", "game_id", "away", "home"], max_rows=100))
    lines.append("")

    lines.append("## Review Queues")
    lines.append("")

    lines.append("### High Confidence Incorrect")
    lines.append("")
    lines.append(markdown_table(
        high_conf_incorrect,
        ["game_id", "away", "home", "final_away_total", "final_home_total", "target_team", "confidence", "matchup_label", "profile_type", "reasoning_headline"],
        max_rows=30,
    ))
    lines.append("")

    lines.append("### No Pick Games")
    lines.append("")
    lines.append(markdown_table(
        no_pick_games,
        ["game_id", "away", "home", "final_away_total", "final_home_total", "confidence", "profile_type", "matchup_label", "reasoning_headline"],
        max_rows=40,
    ))
    lines.append("")

    lines.append("### Low Confidence But Directional Lean")
    lines.append("")
    lines.append(markdown_table(
        low_conf_edges,
        ["game_id", "away", "home", "target_team", "confidence", "profile_type", "matchup_label", "matchup_cautions_json"],
        max_rows=40,
    ))
    lines.append("")

    lines.append("### Missing Ranking Context")
    lines.append("")
    lines.append(markdown_table(
        missing_rankings,
        ["game_id", "season", "game_week", "ranking_available", "ranking_reason"],
        max_rows=40,
    ))
    lines.append("")

    lines.append("### Missing Matchup Breakdown")
    lines.append("")
    lines.append(markdown_table(
        missing_breakdown,
        ["game_id", "season", "game_week", "matchup_breakdown_available"],
        max_rows=40,
    ))
    lines.append("")

    lines.append("### Missing Turnover Margin / Game In Visible Team Comparison")
    lines.append("")
    lines.append(markdown_table(
        missing_turnover,
        ["game_id", "season", "game_week", "has_turnover_margin_per_game"],
        max_rows=40,
    ))
    lines.append("")

    if errors:
        lines.append("## Errors")
        lines.append("")
        lines.append(markdown_table(errors, ["game_id", "season", "bucket", "error", "elapsed_seconds"], max_rows=100))
        lines.append("")

    if compare_rows is not None:
        changed = [r for r in compare_rows if str(r.get("changed_any")).lower() == "true"]
        lines.append("## Compare To Previous Run")
        lines.append("")
        lines.append(f"- Games compared: **{len(compare_rows)}**")
        lines.append(f"- Games with important changes: **{len(changed)}**")
        lines.append("")
        lines.append(markdown_table(
            changed,
            ["game_id", "changed_fields", "before_confidence", "after_confidence", "before_matchup_label", "after_matchup_label", "before_model_result", "after_model_result"],
            max_rows=60,
        ))
        lines.append("")

    lines.append("## Suggested Next Review Questions")
    lines.append("")
    lines.append("Use this run to ask:")
    lines.append("")
    lines.append("- Are confidence labels too loud, too cautious, or about right?")
    lines.append("- Do `matchup_label`, `profile_strength`, and `outcome_confidence` agree with each other?")
    lines.append("- Are No Pick games showing useful restraint?")
    lines.append("- Are high-confidence misses close/variance misses or true calibration failures?")
    lines.append("- Are `matchup_breakdown` summaries explaining the matchup, or just repeating counts?")
    lines.append("- Are context-only metrics staying out of headline language?")
    lines.append("")

    return "\n".join(lines)


def build_analysis_packet(
    *,
    run_metadata: Dict[str, Any],
    sampled_games: List[SampleGame],
    snapshots: List[Dict[str, Any]],
    errors: List[Dict[str, Any]],
    compare_rows: Optional[List[Dict[str, Any]]],
) -> Dict[str, Any]:
    return {
        "run_metadata": run_metadata,
        "sampled_games": [asdict(g) for g in sampled_games],
        "summary": {
            "total_selected": len(sampled_games),
            "payloads_collected": len(snapshots),
            "failed_games": len(errors),
            "by_season": count_by([asdict(g) for g in sampled_games], "season"),
            "by_bucket": count_by([asdict(g) for g in sampled_games], "bucket"),
            "confidence_distribution": count_by(snapshots, "confidence"),
            "model_result_distribution": count_by(snapshots, "model_result"),
            "profile_type_distribution": count_by(snapshots, "profile_type"),
            "matchup_label_distribution": count_by(snapshots, "matchup_label"),
        },
        "review_flags": {
            "high_confidence_incorrect": [
                r for r in snapshots
                if str(r.get("confidence") or "").lower() == "high"
                and str(r.get("model_result") or "").lower() == "incorrect"
            ],
            "no_pick_games": [
                r for r in snapshots
                if str(r.get("model_result") or "").lower() == "no pick"
            ],
            "missing_ranking_context": [
                r for r in snapshots
                if str(r.get("ranking_available")).lower() not in {"true", "1"}
            ],
            "missing_matchup_breakdown": [
                r for r in snapshots
                if str(r.get("matchup_breakdown_available")).lower() not in {"true", "1"}
            ],
        },
        "snapshots": snapshots,
        "errors": errors,
        "compare_to_previous": compare_rows or [],
    }


# -----------------------------------------------------------------------------
# Compare mode
# -----------------------------------------------------------------------------


def compare_to_previous_run(current_rows: List[Dict[str, Any]], previous_dir: Path) -> List[Dict[str, Any]]:
    previous_csv = previous_dir / "game_snapshots.csv"
    previous_rows = read_csv_dicts(previous_csv)
    previous_by_game = {row.get("game_id"): row for row in previous_rows if row.get("game_id")}

    compare_rows: List[Dict[str, Any]] = []

    for current in current_rows:
        game_id = current.get("game_id")
        previous = previous_by_game.get(str(game_id))
        if not previous:
            compare_rows.append({
                "game_id": game_id,
                "changed_any": True,
                "changed_fields": "new_game_not_in_previous_run",
            })
            continue

        row: Dict[str, Any] = {"game_id": game_id}
        changed_fields = []

        for field in COMPARE_FIELDS:
            before = previous.get(field)
            after = current.get(field)
            if str(before) != str(after):
                changed_fields.append(field)
            row[f"before_{field}"] = before
            row[f"after_{field}"] = after

        row["changed_any"] = bool(changed_fields)
        row["changed_fields"] = ",".join(changed_fields)
        compare_rows.append(row)

    return compare_rows


# -----------------------------------------------------------------------------
# Main CLI
# -----------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect GameLens /game-style payloads directly from backend code for local QA.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--seasons", nargs="+", default=DEFAULT_SEASONS, help="Seasons to sample when no explicit game list is provided.")
    parser.add_argument("--games-per-season", type=int, default=DEFAULT_GAMES_PER_SEASON, help="Number of games to sample per season.")
    parser.add_argument("--include-preseason", action="store_true", help="Include preseason games as a small part of the sample.")
    parser.add_argument(
        "--sample-mode",
        choices=["even", "random"],
        default="even",
        help="Sampling mode when selecting from BigQuery schedule. 'even' preserves old deterministic behavior; 'random' uses --random-seed.",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=None,
        help="Seed for --sample-mode random. Use this for repeatable fresh samples.",
    )
    parser.add_argument("--project-id", default=PROJECT_ID, help="Google Cloud project id for BigQuery sampling.")
    parser.add_argument("--schedule-table", default=SCHEDULE_TABLE, help="Fully qualified BigQuery schedule table.")

    parser.add_argument("--game-ids", nargs="*", help="Specific game IDs to collect.")
    parser.add_argument("--game-file", help="Text file of game IDs, one per line.")
    parser.add_argument("--sample-file", help="Previously saved sampled_games.json to reuse exact sample.")

    parser.add_argument("--repo-root", default=".", help="Repo root where services/ and queries/ are importable.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Folder where timestamped QA runs are written.")
    parser.add_argument("--run-name", help="Optional output folder name. Defaults to timestamp.")
    parser.add_argument("--compare-to", help="Previous run folder containing game_snapshots.csv.")
    parser.add_argument("--timeout-seconds", type=int, default=180, help="Timeout per game worker subprocess.")
    parser.add_argument("--log-level", default="WARNING", help="Log level passed into worker process.")
    parser.add_argument("--verbose", action="store_true", help="Print worker stdout/stderr logs.")
    parser.add_argument("--dry-run", action="store_true", help="Select and write the sample, but do not collect payloads.")

    # Internal worker args.
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--worker-game-id", help=argparse.SUPPRESS)
    parser.add_argument("--worker-output-path", help=argparse.SUPPRESS)

    return parser


def select_games(args: argparse.Namespace) -> List[SampleGame]:
    if args.sample_file:
        return load_sample_file(Path(args.sample_file))

    game_ids: List[str] = []
    if args.game_file:
        game_ids.extend(read_game_ids_from_file(Path(args.game_file)))
    if args.game_ids:
        game_ids.extend(args.game_ids)

    if game_ids:
        return sample_games_from_ids(game_ids)

    candidates = fetch_candidate_games_from_bigquery(
        seasons=[str(s) for s in args.seasons],
        project_id=args.project_id,
        schedule_table=args.schedule_table,
        include_preseason=args.include_preseason,
    )
    return smart_sample_games(
        candidates,
        seasons=[str(s) for s in args.seasons],
        games_per_season=args.games_per_season,
        include_preseason=args.include_preseason,
        sample_mode=args.sample_mode,
        random_seed=args.random_seed,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.worker:
        return run_worker_mode(args)

    repo_root = Path(args.repo_root).resolve()
    script_path = Path(__file__).resolve()
    run_name = args.run_name or utc_timestamp_for_folder()
    output_dir = Path(args.output_root) / run_name
    output_dir.mkdir(parents=True, exist_ok=True)

    sampled_games = select_games(args)
    sampled_games = sorted(
        sampled_games,
        key=lambda g: (str(g.season), str(g.game_date), str(g.game_id)),
    )

    run_metadata: Dict[str, Any] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(repo_root),
        "script_path": str(script_path),
        "output_dir": str(output_dir),
        "seasons": [str(s) for s in args.seasons],
        "games_per_season": args.games_per_season,
        "include_preseason": args.include_preseason,
        "sample_mode": args.sample_mode,
        "random_seed": args.random_seed,
        "project_id": args.project_id,
        "schedule_table": args.schedule_table,
        "sample_file": args.sample_file,
        "game_file": args.game_file,
        "explicit_game_ids": args.game_ids or [],
        "compare_to": args.compare_to,
        "timeout_seconds": args.timeout_seconds,
        "dry_run": args.dry_run,
    }

    write_json(run_metadata, output_dir / "run_metadata.json")
    write_json({"games": [asdict(g) for g in sampled_games]}, output_dir / "sampled_games.json")

    print("\nGameLens QA payload collector")
    print("--------------------------------")
    print(f"Output folder: {output_dir}")
    print(f"Sample mode: {args.sample_mode}")
    print(f"Random seed: {args.random_seed}")
    print(f"Games selected: {len(sampled_games)}")
    print("Sample distribution:")
    for key, count in count_by([asdict(g) for g in sampled_games], "season").items():
        print(f"  season {key}: {count}")
    for key, count in count_by([asdict(g) for g in sampled_games], "bucket").items():
        print(f"  bucket {key}: {count}")
    print("")

    if args.dry_run:
        print("Dry run complete. No payloads collected.")
        return 0

    snapshots: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    total = len(sampled_games)
    for idx, game in enumerate(sampled_games, start=1):
        output_path = payload_path_for_game(output_dir, game)
        log_path = log_path_for_game(output_dir, game)
        label = f"[{idx}/{total}] {game.game_id} ({game.season} | {game.bucket} | {game.game_week})"
        print(f"Collecting {label} ...", flush=True)

        success, error, elapsed = run_game_worker(
            script_path=script_path,
            repo_root=repo_root,
            game=game,
            output_path=output_path,
            log_path=log_path,
            timeout_seconds=args.timeout_seconds,
            log_level=args.log_level,
            verbose=args.verbose,
        )

        if success:
            payload = read_json(output_path)
            snapshot = extract_snapshot(payload, sample_meta=game)
            snapshot["payload_path"] = str(output_path)
            snapshot["worker_log_path"] = str(log_path)
            snapshot["elapsed_seconds"] = round(elapsed, 2)
            snapshots.append(snapshot)
            print(f"  ✅ saved payload in {elapsed:.1f}s")
        else:
            error_row = {
                "game_id": game.game_id,
                "season": game.season,
                "bucket": game.bucket,
                "game_week": game.game_week,
                "game_date": game.game_date,
                "error": error,
                "payload_path": str(output_path),
                "worker_log_path": str(log_path),
                "elapsed_seconds": round(elapsed, 2),
            }
            errors.append(error_row)
            print(f"  ❌ failed: {error}")

    snapshots = sorted(snapshots, key=lambda r: (str(r.get("season")), str(r.get("game_date")), str(r.get("game_id"))))

    write_csv(snapshots, output_dir / "game_snapshots.csv")
    write_json(errors, output_dir / "errors.json")

    compare_rows: Optional[List[Dict[str, Any]]] = None
    if args.compare_to:
        compare_rows = compare_to_previous_run(snapshots, Path(args.compare_to))
        write_csv(compare_rows, output_dir / "compare_to_previous.csv")

    review_md = build_review_markdown(
        output_dir=output_dir,
        sampled_games=sampled_games,
        snapshots=snapshots,
        errors=errors,
        compare_rows=compare_rows,
        run_metadata=run_metadata,
    )
    (output_dir / "qa_review.md").write_text(review_md, encoding="utf-8")

    analysis_packet = build_analysis_packet(
        run_metadata=run_metadata,
        sampled_games=sampled_games,
        snapshots=snapshots,
        errors=errors,
        compare_rows=compare_rows,
    )
    write_json(analysis_packet, output_dir / "chatgpt_analysis_packet.json")

    print("\nRun complete")
    print("------------")
    print(f"Payloads collected: {len(snapshots)} / {len(sampled_games)}")
    print(f"Errors: {len(errors)}")
    print(f"Snapshots CSV: {output_dir / 'game_snapshots.csv'}")
    print(f"Review Markdown: {output_dir / 'qa_review.md'}")
    print(f"ChatGPT packet: {output_dir / 'chatgpt_analysis_packet.json'}")
    if compare_rows is not None:
        changed = [r for r in compare_rows if r.get("changed_any")]
        print(f"Compared to previous run: {len(changed)} games changed")

    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
