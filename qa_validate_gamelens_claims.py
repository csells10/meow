"""
GameLens Level 2 QA v2: contextual postgame claim validation.

Purpose
-------
The normal QA payload collector answers:

    Did GameLens pick the winner?
    Was confidence too loud or too cautious?

This script asks the better Level 2 question:

    Did the matchup claims GameLens made before the game actually validate
    in the postgame metrics?

Example:
    If GameLens said MIN had the Defensive Control edge, did MIN actually win
    the postgame Defensive Control metrics?

    If GameLens said MIN was generating more pressure, did MIN actually show
    the better postgame Pressure profile?

This is intentionally not a replacement for winner/pick QA. It is a second
layer that separates:

    - good reasoning, bad outcome
    - bad reasoning, bad outcome
    - cautious/no-pick games where the game became lopsided anyway
    - specific claim types that are reliable or noisy

Recommended first run
---------------------
Run against an existing QA payload run folder:

    python qa_validate_gamelens_claims.py \
      --payload-run qa/gamelens_payload_runs/baseline_32_v2 \
      --sample-size 32 \
      --run-name claims_32_test

For a full run using every payload in that folder:

    python qa_validate_gamelens_claims.py \
      --payload-run qa/gamelens_payload_runs/baseline_32_v2

Main outputs
------------
Inside qa/gamelens_claim_validation_runs/<timestamp-or-run-name>/:

    claim_validations.csv
        One row per claim being validated.

    game_claim_summary.csv
        One row per game, summarizing whether the reasoning held up.

    qa_claim_review.md
        Human-readable review file.

    chatgpt_claim_validation_packet.json
        Compact structured packet to upload back to ChatGPT.

Assumptions
-----------
- You already ran qa_collect_gamelens_payloads_bucket_v2.py or similar.
- The payload run folder contains payloads/<season>/<game_id>.json.
- Postgame actual metrics are available in BigQuery, preferably:

      nfl-stream-406420.Analytics.game_team_metric_facts_{season}

  This script can optionally fall back to Analytics.game_metrics_flat if the
  cleaned facts table is not available, but the facts table is preferred because
  it already has registry-backed category/core_area/comparison_direction fields.

Design note
-----------
This v2 QA tool stays transparent and CSV-first, but adds the key next layer:

    - separates headline claims from supporting/detail claims
    - de-duplicates repeated claims before game-level scoring
    - keeps full claim rows for debugging
    - adds context summaries such as claim type by profile/outcome/bucket

It is not trying to perfectly judge football film. It gives us a repeatable
way to find where GameLens explanations validated and where they did not.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


PROJECT_ID = "nfl-stream-406420"
FACTS_TABLE_TEMPLATE = "Analytics.game_team_metric_facts_{season}"
FLAT_METRICS_TABLE = "Analytics.game_metrics_flat"
SCHEDULE_TABLE = "League.schedule"
DEFAULT_OUTPUT_ROOT = Path("qa/gamelens_claim_validation_runs")

DIRECTIONAL_DIRECTIONS = {"higher", "lower"}
SIDE_VALUES = {"away", "home", "neutral"}

# Game Profile labels do not always map 1:1 to registry categories.
# This lets us validate human-facing claims against the appropriate metric group.
CLAIM_GROUP_ALIASES = {
    "Pressure": {"group_kind": "category", "group_name": "Pressure"},
    "Turnover Risk": {"group_kind": "category", "group_name": "Turnovers"},
    "Scoring Efficiency": {"group_kind": "core_area", "group_name": "Scoring Efficiency"},
    "Defensive Control": {"group_kind": "core_area", "group_name": "Defensive Control"},
    "Offensive Output": {"group_kind": "core_area", "group_name": "Offensive Output"},
    "Disruption and Turnovers": {"group_kind": "core_area", "group_name": "Disruption and Turnovers"},
    "Field Control (Special Teams)": {"group_kind": "core_area", "group_name": "Field Control (Special Teams)"},
}

# Claim types that should be treated as directional edge statements.
DIRECTIONAL_CLAIM_TYPES = {
    "game_profile",
    "core_area_comparison",
    "core_area_summary",
    "category_summary",
    "metric_highlight",
    "team_comparison_metric",
}

# v2: claim layers let us separate the app's main story from supporting detail.
# Headline claims should answer: "Did the main thing GameLens told the user happen?"
# Supporting claims remain useful for debugging, but should not drown out headline QA.
HEADLINE_CLAIM_TYPES = {"game_profile", "core_area_comparison"}
SUPPORTING_CLAIM_TYPES = {"core_area_summary", "category_summary", "team_comparison_metric"}
CLAIM_TYPE_PRIORITY = {
    "game_profile": 1,
    "core_area_comparison": 2,
    "metric_highlight": 3,
    "team_comparison_metric": 4,
    "core_area_summary": 5,
    "category_summary": 6,
}


@dataclass
class PayloadRef:
    game_id: str
    season: str
    payload_path: str
    game_date: str = ""
    game_week: str = ""
    bucket: str = ""
    away: str = ""
    home: str = ""


# -----------------------------------------------------------------------------
# Basic helpers
# -----------------------------------------------------------------------------


def utc_timestamp_for_folder() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_utc")


def json_safe_default(value: Any) -> str:
    return str(value)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(data: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True, default=json_safe_default)


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
                seen.add(key)
                fieldnames.append(key)

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


def get_nested(data: Dict[str, Any], path: Sequence[str], default: Any = None) -> Any:
    current: Any = data
    for key in path:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def normalize_boolish(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def side_to_team(side: Optional[str], header: Dict[str, Any]) -> Optional[str]:
    if side == "away":
        return get_nested(header, ["away_team", "abbreviation"])
    if side == "home":
        return get_nested(header, ["home_team", "abbreviation"])
    if side == "neutral":
        return "neutral"
    return None


def team_to_side(team: Optional[str], header: Dict[str, Any]) -> Optional[str]:
    if not team:
        return None
    away = get_nested(header, ["away_team", "abbreviation"])
    home = get_nested(header, ["home_team", "abbreviation"])
    cleaned = str(team).replace(" edge", "").strip()
    if cleaned == away:
        return "away"
    if cleaned == home:
        return "home"
    if cleaned.lower() in {"none", "neutral", "no pick"}:
        return "neutral"
    return None


def count_by(rows: List[Dict[str, Any]], field: str) -> Dict[str, int]:
    counts = Counter(str(row.get(field) or "missing") for row in rows)
    return dict(sorted(counts.items(), key=lambda item: item[0]))


def choose_evenly(items: List[Any], count: int) -> List[Any]:
    if count <= 0 or not items:
        return []
    if count >= len(items):
        return items[:]
    if count == 1:
        return [items[len(items) // 2]]

    selected = []
    seen = set()
    for i in range(count):
        idx = round(i * (len(items) - 1) / (count - 1))
        if idx not in seen:
            selected.append(items[idx])
            seen.add(idx)

    if len(selected) < count:
        for idx, item in enumerate(items):
            if idx not in seen:
                selected.append(item)
                seen.add(idx)
            if len(selected) >= count:
                break

    return selected[:count]


def markdown_table(rows: List[Dict[str, Any]], columns: List[str], max_rows: int = 40) -> str:
    if not rows:
        return "_None._\n"

    clipped = rows[:max_rows]
    lines = ["| " + " | ".join(columns) + " |"]
    lines.append("| " + " | ".join(["---"] * len(columns)) + " |")

    for row in clipped:
        values = []
        for col in columns:
            value = row.get(col, "")
            text = str(value if value is not None else "")
            text = text.replace("\n", " ").replace("|", "\\|")
            if len(text) > 110:
                text = text[:107] + "..."
            values.append(text)
        lines.append("| " + " | ".join(values) + " |")

    if len(rows) > max_rows:
        lines.append(f"\n_Showing {max_rows} of {len(rows)} rows._")

    return "\n".join(lines) + "\n"


# -----------------------------------------------------------------------------
# Payload loading
# -----------------------------------------------------------------------------


def clean_game_id_for_filename(game_id: str) -> str:
    return (
        str(game_id)
        .replace("@", "_at_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
    )


def discover_payloads_from_run(payload_run: Path) -> List[PayloadRef]:
    sampled_path = payload_run / "sampled_games.json"
    snapshot_path = payload_run / "game_snapshots.csv"
    snapshots = {r.get("game_id"): r for r in read_csv_dicts(snapshot_path) if r.get("game_id")}

    refs: List[PayloadRef] = []

    if sampled_path.exists():
        payload = read_json(sampled_path)
        rows = payload.get("games", payload) if isinstance(payload, dict) else payload
        for row in rows:
            if isinstance(row, str):
                game_id = row
                season = row[:4]
                meta = snapshots.get(game_id, {})
            else:
                game_id = str(row.get("game_id") or row.get("gameID") or "")
                season = str(row.get("season") or game_id[:4])[:4]
                meta = {**row, **snapshots.get(game_id, {})}
            if not game_id:
                continue
            path = payload_run / "payloads" / season / f"{clean_game_id_for_filename(game_id)}.json"
            if not path.exists():
                # Fall back to glob search in case the naming scheme changed.
                matches = list((payload_run / "payloads").glob(f"**/*{clean_game_id_for_filename(game_id)}*.json"))
                if matches:
                    path = matches[0]
            refs.append(
                PayloadRef(
                    game_id=game_id,
                    season=season,
                    payload_path=str(path),
                    game_date=str(meta.get("game_date") or meta.get("gameDate") or ""),
                    game_week=str(meta.get("game_week") or meta.get("gameWeek") or ""),
                    bucket=str(meta.get("bucket") or ""),
                    away=str(meta.get("away") or ""),
                    home=str(meta.get("home") or ""),
                )
            )
    else:
        for path in sorted((payload_run / "payloads").glob("**/*.json")):
            game_id = path.stem.replace("_at_", "@")
            season = str(path.parent.name)[:4]
            meta = snapshots.get(game_id, {})
            refs.append(
                PayloadRef(
                    game_id=game_id,
                    season=season,
                    payload_path=str(path),
                    game_date=str(meta.get("game_date") or ""),
                    game_week=str(meta.get("game_week") or ""),
                    bucket=str(meta.get("bucket") or ""),
                    away=str(meta.get("away") or ""),
                    home=str(meta.get("home") or ""),
                )
            )

    # Keep only existing payloads and remove duplicates.
    seen = set()
    final_refs: List[PayloadRef] = []
    for ref in refs:
        if ref.game_id in seen:
            continue
        if not Path(ref.payload_path).exists():
            continue
        seen.add(ref.game_id)
        final_refs.append(ref)

    return sorted(final_refs, key=lambda r: (r.season, r.game_date, r.game_id))


def filter_payload_refs(
    refs: List[PayloadRef],
    *,
    game_ids: Optional[Sequence[str]],
    game_file: Optional[str],
    sample_size: Optional[int],
    random_sample: bool,
    random_seed: int,
) -> List[PayloadRef]:
    wanted: List[str] = []
    if game_file:
        for line in Path(game_file).read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                wanted.append(line)
    if game_ids:
        wanted.extend(game_ids)

    if wanted:
        wanted_set = set(wanted)
        refs = [r for r in refs if r.game_id in wanted_set]
        order = {game_id: idx for idx, game_id in enumerate(wanted)}
        refs = sorted(refs, key=lambda r: order.get(r.game_id, 999999))

    if sample_size and sample_size > 0 and sample_size < len(refs):
        if random_sample:
            rng = random.Random(random_seed)
            refs = sorted(rng.sample(refs, sample_size), key=lambda r: (r.season, r.game_date, r.game_id))
        else:
            refs = choose_evenly(refs, sample_size)

    return refs


# -----------------------------------------------------------------------------
# BigQuery actual metrics loading
# -----------------------------------------------------------------------------


def import_bigquery_client():
    try:
        from google.cloud import bigquery  # type: ignore
    except Exception as exc:  # pragma: no cover - environment-specific
        raise RuntimeError(
            "Could not import google.cloud.bigquery. Run this in your project venv."
        ) from exc
    return bigquery


def try_import_metric_registry() -> Dict[str, Dict[str, Any]]:
    """Best-effort import for flat-table fallback metadata."""
    candidates = [
        "analytics.metric_registry",
        "metric_registry",
    ]
    for module_name in candidates:
        try:
            module = __import__(module_name, fromlist=["METRIC_REGISTRY"])
            registry = getattr(module, "METRIC_REGISTRY", {})
            if registry:
                return registry
        except Exception:
            continue
    return {}


def metric_meta_to_dict(meta: Any) -> Dict[str, Any]:
    if isinstance(meta, dict):
        return meta
    result = {}
    for key in [
        "label",
        "category",
        "core_area",
        "comparison_direction",
        "higher_is_better",
        "raw_or_derived",
        "aggregation_method",
    ]:
        result[key] = getattr(meta, key, None)
    return result


def load_actual_metrics_from_facts(
    *,
    client: Any,
    bigquery: Any,
    project_id: str,
    facts_table_template: str,
    game_ids_by_season: Dict[str, List[str]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for season, game_ids in sorted(game_ids_by_season.items()):
        if not game_ids:
            continue
        table = f"{project_id}.{facts_table_template.format(season=season)}"
        query = f"""
            SELECT
                CAST(season AS STRING) AS season,
                game_id,
                game_date,
                game_week,
                team_id,
                team_abv,
                team_type,
                metric,
                value,
                label,
                category,
                core_area,
                comparison_direction
            FROM `{table}`
            WHERE game_id IN UNNEST(@game_ids)
              AND value IS NOT NULL
              AND comparison_direction IN ('higher', 'lower')
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("game_ids", "STRING", list(game_ids)),
            ]
        )
        try:
            season_rows = [dict(r) for r in client.query(query, job_config=job_config).result()]
            rows.extend(season_rows)
        except Exception as exc:
            errors.append({"season": season, "source": "facts", "table": table, "error": str(exc)})

    return rows, errors


def load_actual_metrics_from_flat(
    *,
    client: Any,
    bigquery: Any,
    project_id: str,
    flat_metrics_table: str,
    schedule_table: str,
    game_ids_by_season: Dict[str, List[str]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Fallback if cleaned fact tables are missing.

    This attaches local registry metadata after loading game_metrics_flat.
    It is less ideal than the facts table but makes this QA script easier to test.
    """
    registry = try_import_metric_registry()
    if not registry:
        return [], [{"source": "flat", "error": "could_not_import_metric_registry_for_flat_fallback"}]

    all_game_ids = [gid for ids in game_ids_by_season.values() for gid in ids]
    if not all_game_ids:
        return [], []

    query = f"""
        SELECT
            CAST(s.season AS STRING) AS season,
            m.gameID AS game_id,
            DATE(s.gameDate) AS game_date,
            s.gameWeek AS game_week,
            CAST(s.teamIDAway AS STRING) AS away_team_id,
            CAST(s.teamIDHome AS STRING) AS home_team_id,
            CAST(m.team_id AS STRING) AS team_id,
            m.team_abv AS team_abv,
            m.metric AS metric,
            SAFE_CAST(m.value AS FLOAT64) AS value
        FROM `{project_id}.{flat_metrics_table}` AS m
        INNER JOIN `{project_id}.{schedule_table}` AS s
            ON m.gameID = s.gameID
        WHERE m.gameID IN UNNEST(@game_ids)
          AND m.metric IS NOT NULL
          AND m.value IS NOT NULL
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("game_ids", "STRING", all_game_ids)]
    )

    try:
        source_rows = [dict(r) for r in client.query(query, job_config=job_config).result()]
    except Exception as exc:
        return [], [{"source": "flat", "error": str(exc)}]

    rows: List[Dict[str, Any]] = []
    for row in source_rows:
        metric = row.get("metric")
        meta = metric_meta_to_dict(registry.get(metric))
        direction = meta.get("comparison_direction")
        if direction not in DIRECTIONAL_DIRECTIONS:
            continue

        team_id = str(row.get("team_id"))
        team_type = None
        if team_id == str(row.get("away_team_id")):
            team_type = "away"
        elif team_id == str(row.get("home_team_id")):
            team_type = "home"

        if team_type not in {"away", "home"}:
            continue

        rows.append({
            "season": str(row.get("season") or ""),
            "game_id": row.get("game_id"),
            "game_date": str(row.get("game_date") or ""),
            "game_week": row.get("game_week"),
            "team_id": row.get("team_id"),
            "team_abv": row.get("team_abv"),
            "team_type": team_type,
            "metric": metric,
            "value": row.get("value"),
            "label": meta.get("label") or metric,
            "category": meta.get("category"),
            "core_area": meta.get("core_area"),
            "comparison_direction": direction,
        })

    return rows, []


def load_actual_metrics(
    *,
    payload_refs: List[PayloadRef],
    project_id: str,
    actual_source: str,
    facts_table_template: str,
    flat_metrics_table: str,
    schedule_table: str,
    fallback_flat: bool,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], str]:
    bigquery = import_bigquery_client()
    client = bigquery.Client(project=project_id)

    game_ids_by_season: Dict[str, List[str]] = defaultdict(list)
    for ref in payload_refs:
        game_ids_by_season[str(ref.season)].append(ref.game_id)

    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    used_source = actual_source

    if actual_source == "facts":
        rows, errors = load_actual_metrics_from_facts(
            client=client,
            bigquery=bigquery,
            project_id=project_id,
            facts_table_template=facts_table_template,
            game_ids_by_season=game_ids_by_season,
        )
        if rows or not fallback_flat:
            return rows, errors, used_source

        flat_rows, flat_errors = load_actual_metrics_from_flat(
            client=client,
            bigquery=bigquery,
            project_id=project_id,
            flat_metrics_table=flat_metrics_table,
            schedule_table=schedule_table,
            game_ids_by_season=game_ids_by_season,
        )
        if flat_rows:
            return flat_rows, errors + flat_errors, "flat_fallback"
        return rows, errors + flat_errors, used_source

    if actual_source == "flat":
        rows, errors = load_actual_metrics_from_flat(
            client=client,
            bigquery=bigquery,
            project_id=project_id,
            flat_metrics_table=flat_metrics_table,
            schedule_table=schedule_table,
            game_ids_by_season=game_ids_by_season,
        )
        return rows, errors, used_source

    raise ValueError(f"Unsupported actual_source: {actual_source}")


# -----------------------------------------------------------------------------
# Actual metric model
# -----------------------------------------------------------------------------


def build_actual_index(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Build game -> side -> metric and game -> all metric rows lookup."""
    index: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        game_id = str(row.get("game_id") or "")
        side = str(row.get("team_type") or "")
        metric = str(row.get("metric") or "")
        if not game_id or side not in {"away", "home"} or not metric:
            continue

        game = index.setdefault(game_id, {"away": {}, "home": {}, "rows": []})
        game[side][metric] = row
        game["rows"].append(row)
    return index


def is_near_even(a: float, b: float, *, abs_tol: float, pct_tol: float) -> bool:
    diff = abs(a - b)
    if diff <= abs_tol:
        return True
    scale = max(abs(a), abs(b), 1.0)
    return (diff / scale) <= pct_tol


def compare_metric(
    game_actuals: Dict[str, Any],
    metric: str,
    *,
    abs_tol: float,
    pct_tol: float,
) -> Dict[str, Any]:
    away_row = game_actuals.get("away", {}).get(metric)
    home_row = game_actuals.get("home", {}).get(metric)

    if not away_row or not home_row:
        return {
            "actual_side": "unavailable",
            "actual_team": None,
            "actual_away_value": None,
            "actual_home_value": None,
            "comparison_direction": None,
            "actual_label": "missing_metric_actuals",
        }

    direction = str(away_row.get("comparison_direction") or home_row.get("comparison_direction") or "").lower()
    if direction not in DIRECTIONAL_DIRECTIONS:
        return {
            "actual_side": "unavailable",
            "actual_team": None,
            "actual_away_value": away_row.get("value"),
            "actual_home_value": home_row.get("value"),
            "comparison_direction": direction,
            "actual_label": "metric_not_directional",
        }

    try:
        away_val = float(away_row.get("value"))
        home_val = float(home_row.get("value"))
    except Exception:
        return {
            "actual_side": "unavailable",
            "actual_team": None,
            "actual_away_value": away_row.get("value"),
            "actual_home_value": home_row.get("value"),
            "comparison_direction": direction,
            "actual_label": "metric_value_not_numeric",
        }

    if is_near_even(away_val, home_val, abs_tol=abs_tol, pct_tol=pct_tol):
        leader = "neutral"
    elif direction == "higher":
        leader = "away" if away_val > home_val else "home"
    else:  # lower is better
        leader = "away" if away_val < home_val else "home"

    team_abv = None
    if leader == "away":
        team_abv = away_row.get("team_abv")
    elif leader == "home":
        team_abv = home_row.get("team_abv")
    elif leader == "neutral":
        team_abv = "neutral"

    return {
        "actual_side": leader,
        "actual_team": team_abv,
        "actual_away_value": away_val,
        "actual_home_value": home_val,
        "comparison_direction": direction,
        "actual_label": away_row.get("label") or metric,
        "metric_category": away_row.get("category"),
        "metric_core_area": away_row.get("core_area"),
    }


def compare_group(
    game_actuals: Dict[str, Any],
    *,
    group_kind: str,
    group_name: str,
    abs_tol: float,
    pct_tol: float,
) -> Dict[str, Any]:
    rows = game_actuals.get("rows", []) or []
    if group_kind == "core_area":
        metrics = sorted({r.get("metric") for r in rows if r.get("core_area") == group_name and r.get("metric")})
    elif group_kind == "category":
        metrics = sorted({r.get("metric") for r in rows if r.get("category") == group_name and r.get("metric")})
    else:
        metrics = []

    if not metrics:
        return {
            "actual_side": "unavailable",
            "actual_team": None,
            "actual_group_kind": group_kind,
            "actual_group_name": group_name,
            "actual_away_metric_wins": 0,
            "actual_home_metric_wins": 0,
            "actual_neutral_metrics": 0,
            "actual_metrics_checked": 0,
            "actual_metrics_json": "[]",
        }

    away_wins = 0
    home_wins = 0
    neutral = 0
    unavailable = 0
    metric_results = []

    for metric in metrics:
        result = compare_metric(game_actuals, metric, abs_tol=abs_tol, pct_tol=pct_tol)
        side = result.get("actual_side")
        if side == "away":
            away_wins += 1
        elif side == "home":
            home_wins += 1
        elif side == "neutral":
            neutral += 1
        else:
            unavailable += 1
        metric_results.append({
            "metric": metric,
            "label": result.get("actual_label"),
            "leader": side,
            "away_value": result.get("actual_away_value"),
            "home_value": result.get("actual_home_value"),
            "direction": result.get("comparison_direction"),
        })

    if away_wins > home_wins:
        leader = "away"
    elif home_wins > away_wins:
        leader = "home"
    else:
        leader = "neutral"

    # Use a representative row to return the team abbreviation.
    away_team = None
    home_team = None
    for row in rows:
        if row.get("team_type") == "away":
            away_team = row.get("team_abv")
        if row.get("team_type") == "home":
            home_team = row.get("team_abv")

    actual_team = "neutral"
    if leader == "away":
        actual_team = away_team
    elif leader == "home":
        actual_team = home_team

    return {
        "actual_side": leader,
        "actual_team": actual_team,
        "actual_group_kind": group_kind,
        "actual_group_name": group_name,
        "actual_away_metric_wins": away_wins,
        "actual_home_metric_wins": home_wins,
        "actual_neutral_metrics": neutral,
        "actual_unavailable_metrics": unavailable,
        "actual_metrics_checked": len(metrics),
        "actual_metrics_json": json.dumps(metric_results, sort_keys=True, default=json_safe_default),
    }


def validation_result_for_claim(claimed_side: Optional[str], actual_side: Optional[str]) -> str:
    claimed = claimed_side if claimed_side in SIDE_VALUES else None
    actual = actual_side if actual_side in SIDE_VALUES else actual_side

    if actual in {None, "unavailable"}:
        return "unavailable"
    if claimed is None:
        return "unavailable"

    if claimed == "neutral":
        return "validated" if actual == "neutral" else "neutral_claim_but_actual_edge"

    if actual == "neutral":
        return "actual_neutral_or_mixed"

    if claimed == actual:
        return "validated"

    return "not_validated"


# -----------------------------------------------------------------------------
# Claim extraction
# -----------------------------------------------------------------------------


def common_context_from_payload(payload: Dict[str, Any], ref: PayloadRef) -> Dict[str, Any]:
    header = payload.get("header", {}) or {}
    final_score = payload.get("final_score", {}) or {}
    matchup_lean = payload.get("matchup_lean", {}) or {}
    model_outcome = payload.get("model_outcome", {}) or {}

    away = get_nested(header, ["away_team", "abbreviation"]) or ref.away
    home = get_nested(header, ["home_team", "abbreviation"]) or ref.home
    away_total = get_nested(final_score, ["away", "total"])
    home_total = get_nested(final_score, ["home", "total"])

    final_margin_abs = None
    try:
        if away_total is not None and home_total is not None:
            final_margin_abs = abs(int(home_total) - int(away_total))
    except Exception:
        pass

    outcome_confidence = matchup_lean.get("outcome_confidence", {}) or {}
    profile_strength = matchup_lean.get("profile_strength", {}) or {}

    return {
        "game_id": ref.game_id,
        "season": ref.season,
        "game_date": get_nested(header, ["game_date"]) or ref.game_date,
        "game_week": get_nested(header, ["game_week"]) or ref.game_week,
        "bucket": ref.bucket,
        "away": away,
        "home": home,
        "final_away_total": away_total,
        "final_home_total": home_total,
        "final_margin_abs": final_margin_abs,
        "model_result": model_outcome.get("result"),
        "predicted_team": model_outcome.get("predicted_team"),
        "actual_winner": model_outcome.get("actual_winner"),
        "target_team": matchup_lean.get("target_team"),
        "target_side": matchup_lean.get("target_side"),
        "raw_confidence": matchup_lean.get("confidence"),
        "matchup_label": matchup_lean.get("matchup_label"),
        "profile_type": matchup_lean.get("profile_type"),
        "profile_strength_label": profile_strength.get("label"),
        "outcome_confidence_label": outcome_confidence.get("label"),
    }


def add_claim(
    claims: List[Dict[str, Any]],
    *,
    context: Dict[str, Any],
    header: Dict[str, Any],
    claim_type: str,
    claim_name: str,
    claimed_side: Optional[str],
    claim_source: str,
    metric: Optional[str] = None,
    group_kind: Optional[str] = None,
    group_name: Optional[str] = None,
    claim_text: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    if claimed_side not in SIDE_VALUES:
        # Keep unknowns out of claim validation; they are not useful directional claims.
        return

    claimed_team = side_to_team(claimed_side, header)
    row = {
        **context,
        "claim_type": claim_type,
        "claim_name": claim_name,
        "claim_source": claim_source,
        "claimed_side": claimed_side,
        "claimed_team": claimed_team,
        "metric": metric,
        "group_kind": group_kind,
        "group_name": group_name,
        "claim_text": claim_text,
    }
    if extra:
        row.update(extra)
    claims.append(row)


def extract_claims_from_payload(payload: Dict[str, Any], ref: PayloadRef) -> List[Dict[str, Any]]:
    header = payload.get("header", {}) or {}
    context = common_context_from_payload(payload, ref)
    claims: List[Dict[str, Any]] = []

    # 1) Game Profile claims: Pressure, Turnover Risk, Scoring Efficiency.
    for row in payload.get("game_profile", []) or []:
        category = row.get("category")
        claimed_side = row.get("tilt_team")
        if category in CLAIM_GROUP_ALIASES:
            alias = CLAIM_GROUP_ALIASES[category]
            add_claim(
                claims,
                context=context,
                header=header,
                claim_type="game_profile",
                claim_name=str(category),
                claimed_side=claimed_side,
                claim_source="game_profile",
                group_kind=alias["group_kind"],
                group_name=alias["group_name"],
                claim_text=row.get("tilt_text") or row.get("tilt"),
                extra={"profile_level": row.get("level")},
            )

    # 2) Core Area comparison claims.
    for row in payload.get("core_area_comparison", []) or []:
        core_area = row.get("core_area") or row.get("name")
        leader = row.get("leader")
        if core_area:
            add_claim(
                claims,
                context=context,
                header=header,
                claim_type="core_area_comparison",
                claim_name=str(core_area),
                claimed_side=leader,
                claim_source="core_area_comparison",
                group_kind="core_area",
                group_name=str(core_area),
                claim_text=row.get("summary") or row.get("summary_label"),
                extra={"claim_summary_label": row.get("summary_label")},
            )

    # 3) Visible Team Comparison metrics.
    for row in payload.get("team_comparison", []) or []:
        metric = row.get("metric")
        better = row.get("better")
        if metric:
            add_claim(
                claims,
                context=context,
                header=header,
                claim_type="team_comparison_metric",
                claim_name=str(row.get("label") or metric),
                claimed_side=better,
                claim_source="team_comparison",
                metric=str(metric),
                claim_text=row.get("description") or row.get("note"),
                extra={
                    "pregame_away_value": row.get("away_value"),
                    "pregame_home_value": row.get("home_value"),
                    "pregame_comparison_strength": row.get("comparison_strength"),
                },
            )

    breakdown = payload.get("matchup_breakdown", {}) or {}

    # 4) Metric highlights.
    for idx, row in enumerate(breakdown.get("metric_highlights", []) or [], start=1):
        metric = row.get("metric")
        leader = row.get("leader")
        if metric:
            add_claim(
                claims,
                context=context,
                header=header,
                claim_type="metric_highlight",
                claim_name=str(row.get("name") or row.get("label") or metric),
                claimed_side=leader,
                claim_source="matchup_breakdown.metric_highlights",
                metric=str(metric),
                claim_text=row.get("summary"),
                extra={
                    "claim_summary_label": row.get("summary_label"),
                    "pregame_percentile_gap": row.get("percentile_gap"),
                    "metric_highlight_rank": idx,
                },
            )

    # 5) Category summaries.
    for row in breakdown.get("category_summaries", []) or []:
        name = row.get("name") or row.get("category") or row.get("label")
        leader = row.get("leader")
        if not name:
            continue
        alias = CLAIM_GROUP_ALIASES.get(str(name), {"group_kind": "category", "group_name": str(name)})
        add_claim(
            claims,
            context=context,
            header=header,
            claim_type="category_summary",
            claim_name=str(name),
            claimed_side=leader,
            claim_source="matchup_breakdown.category_summaries",
            group_kind=alias["group_kind"],
            group_name=alias["group_name"],
            claim_text=row.get("summary"),
            extra={"claim_summary_label": row.get("summary_label")},
        )

    # 6) Core area summaries.
    for row in breakdown.get("core_area_summaries", []) or []:
        name = row.get("name") or row.get("core_area") or row.get("label")
        leader = row.get("leader")
        if name:
            add_claim(
                claims,
                context=context,
                header=header,
                claim_type="core_area_summary",
                claim_name=str(name),
                claimed_side=leader,
                claim_source="matchup_breakdown.core_area_summaries",
                group_kind="core_area",
                group_name=str(name),
                claim_text=row.get("summary"),
                extra={"claim_summary_label": row.get("summary_label")},
            )

    return claims


# -----------------------------------------------------------------------------
# Validation orchestration
# -----------------------------------------------------------------------------


def validate_claims(
    claims: List[Dict[str, Any]],
    actual_index: Dict[str, Dict[str, Any]],
    *,
    abs_tol: float,
    pct_tol: float,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    for claim in claims:
        game_id = str(claim.get("game_id") or "")
        game_actuals = actual_index.get(game_id)
        if not game_actuals:
            row = {
                **claim,
                "actual_side": "unavailable",
                "actual_team": None,
                "validation_result": "unavailable",
                "validation_note": "no_actual_metrics_for_game",
            }
            results.append(row)
            continue

        if claim.get("metric"):
            actual = compare_metric(
                game_actuals,
                str(claim.get("metric")),
                abs_tol=abs_tol,
                pct_tol=pct_tol,
            )
        elif claim.get("group_kind") and claim.get("group_name"):
            actual = compare_group(
                game_actuals,
                group_kind=str(claim.get("group_kind")),
                group_name=str(claim.get("group_name")),
                abs_tol=abs_tol,
                pct_tol=pct_tol,
            )
        else:
            actual = {"actual_side": "unavailable", "actual_team": None, "validation_note": "no_metric_or_group_mapping"}

        validation = validation_result_for_claim(claim.get("claimed_side"), actual.get("actual_side"))
        row = {
            **claim,
            **actual,
            "validation_result": validation,
        }
        results.append(row)

    return results



def claim_grain_key(row: Dict[str, Any]) -> str:
    """Return a cross-source key used to de-duplicate repeated claims.

    Example: Defensive Control can appear in both core_area_comparison and
    core_area_summary. We keep both rows for debugging, but game-level scoring
    should usually count that story once.
    """
    if row.get("metric"):
        return f"metric:{row.get('metric')}"
    if row.get("group_kind") and row.get("group_name"):
        return f"group:{row.get('group_kind')}:{row.get('group_name')}"
    return f"claim:{row.get('claim_type')}:{row.get('claim_name')}"


def infer_claim_layer(row: Dict[str, Any], *, headline_top_metrics: int) -> str:
    claim_type = str(row.get("claim_type") or "")
    if claim_type in HEADLINE_CLAIM_TYPES:
        return "headline"
    if claim_type == "metric_highlight":
        try:
            rank = int(row.get("metric_highlight_rank") or 999)
        except Exception:
            rank = 999
        return "headline" if rank <= headline_top_metrics else "supporting"
    return "supporting"


def prepare_claims_for_v2(claims: List[Dict[str, Any]], *, headline_top_metrics: int) -> List[Dict[str, Any]]:
    """Add v2 metadata used for scoring and context summaries.

    The important fields are:
    - claim_layer: headline vs supporting
    - claim_grain_key: metric/group identity independent of source section
    - is_primary_claim_instance: first/best instance used for de-duplicated game scoring
    """
    enriched: List[Dict[str, Any]] = []
    for idx, claim in enumerate(claims):
        row = dict(claim)
        layer = infer_claim_layer(row, headline_top_metrics=headline_top_metrics)
        grain = claim_grain_key(row)
        row["claim_layer"] = layer
        row["claim_grain_key"] = grain
        row["claim_priority"] = CLAIM_TYPE_PRIORITY.get(str(row.get("claim_type") or ""), 99)
        row["claim_row_index"] = idx
        row["unique_claim_key"] = f"{row.get('game_id')}|{grain}|claimed:{row.get('claimed_side')}"
        row["claim_context_profile"] = row.get("profile_type") or "missing"
        row["claim_context_outcome_confidence"] = row.get("outcome_confidence_label") or "missing"
        row["claim_context_bucket"] = row.get("bucket") or "missing"
        enriched.append(row)

    # Mark a single preferred row per unique claim. Priority keeps Game Profile and
    # Core Area Comparison ahead of repeated summaries when they describe the same idea.
    by_key: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in enriched:
        by_key[str(row.get("unique_claim_key"))].append(row)

    primary_ids = set()
    for rows in by_key.values():
        best = sorted(rows, key=lambda r: (int(r.get("claim_priority") or 99), int(r.get("claim_row_index") or 999999)))[0]
        primary_ids.add(id(best))

    for row in enriched:
        row["is_primary_claim_instance"] = id(row) in primary_ids

    return enriched


def rows_for_score(validations: List[Dict[str, Any]], *, mode: str) -> List[Dict[str, Any]]:
    """Select validation rows for a particular game-level scoring mode."""
    rows = [r for r in validations if r.get("claim_type") in DIRECTIONAL_CLAIM_TYPES]
    if mode == "full":
        return rows
    if mode == "unique":
        return [r for r in rows if normalize_boolish(r.get("is_primary_claim_instance"))]
    if mode == "headline":
        return [
            r for r in rows
            if normalize_boolish(r.get("is_primary_claim_instance")) and r.get("claim_layer") == "headline"
        ]
    if mode == "supporting":
        return [
            r for r in rows
            if normalize_boolish(r.get("is_primary_claim_instance")) and r.get("claim_layer") == "supporting"
        ]
    return rows


def score_validation_rows(rows: List[Dict[str, Any]], *, valid_threshold: float, weak_threshold: float) -> Dict[str, Any]:
    eligible = [r for r in rows if r.get("validation_result") != "unavailable"]
    validated = [r for r in eligible if r.get("validation_result") == "validated"]
    not_validated = [r for r in eligible if r.get("validation_result") == "not_validated"]
    neutralish = [r for r in eligible if r.get("validation_result") in {"actual_neutral_or_mixed", "neutral_claim_but_actual_edge"}]

    rate = round(len(validated) / len(eligible), 3) if eligible else None
    if not eligible:
        grade = "no_claims_to_validate"
    elif rate is not None and rate >= valid_threshold:
        grade = "reasoning_validated"
    elif rate is not None and rate <= weak_threshold:
        grade = "reasoning_not_validated"
    else:
        grade = "reasoning_mixed"

    return {
        "claims_total": len(rows),
        "claims_eligible": len(eligible),
        "claims_validated": len(validated),
        "claims_not_validated": len(not_validated),
        "claims_neutral_or_mixed": len(neutralish),
        "claim_validation_rate": rate,
        "reasoning_grade": grade,
    }


def qa_read_from(model_result: str, reasoning_grade: str) -> str:
    result = str(model_result or "").lower()
    if result == "correct" and reasoning_grade == "reasoning_validated":
        return "good_reasoning_correct_outcome"
    if result == "correct" and reasoning_grade != "reasoning_validated":
        return "outcome_correct_reasoning_mixed"
    if result == "incorrect" and reasoning_grade == "reasoning_validated":
        return "good_reasoning_bad_outcome"
    if result == "incorrect" and reasoning_grade == "reasoning_not_validated":
        return "bad_reasoning_bad_outcome"
    if result == "no pick":
        return "no_pick_claim_review"
    return "mixed_review"


def aggregate_game_summary(
    validations: List[Dict[str, Any]],
    *,
    valid_threshold: float = 0.60,
    weak_threshold: float = 0.40,
) -> List[Dict[str, Any]]:
    by_game: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in validations:
        by_game[str(row.get("game_id"))].append(row)

    summaries: List[Dict[str, Any]] = []
    for game_id, rows in sorted(by_game.items()):
        first = rows[0]
        full_score = score_validation_rows(rows_for_score(rows, mode="full"), valid_threshold=valid_threshold, weak_threshold=weak_threshold)
        unique_score = score_validation_rows(rows_for_score(rows, mode="unique"), valid_threshold=valid_threshold, weak_threshold=weak_threshold)
        headline_score = score_validation_rows(rows_for_score(rows, mode="headline"), valid_threshold=valid_threshold, weak_threshold=weak_threshold)
        supporting_score = score_validation_rows(rows_for_score(rows, mode="supporting"), valid_threshold=valid_threshold, weak_threshold=weak_threshold)

        # v2 uses headline score first. If no headline claims are available, fall back to unique score.
        primary_score = headline_score if headline_score["claims_eligible"] else unique_score
        model_result = str(first.get("model_result") or "")
        qa_read = qa_read_from(model_result, str(primary_score.get("reasoning_grade")))

        summaries.append({
            "game_id": game_id,
            "season": first.get("season"),
            "game_date": first.get("game_date"),
            "game_week": first.get("game_week"),
            "bucket": first.get("bucket"),
            "away": first.get("away"),
            "home": first.get("home"),
            "final_away_total": first.get("final_away_total"),
            "final_home_total": first.get("final_home_total"),
            "final_margin_abs": first.get("final_margin_abs"),
            "model_result": first.get("model_result"),
            "predicted_team": first.get("predicted_team"),
            "actual_winner": first.get("actual_winner"),
            "target_team": first.get("target_team"),
            "raw_confidence": first.get("raw_confidence"),
            "outcome_confidence_label": first.get("outcome_confidence_label"),
            "matchup_label": first.get("matchup_label"),
            "profile_type": first.get("profile_type"),

            # Full/original style score, useful for deep debugging.
            "full_claims_total": full_score["claims_total"],
            "full_claims_eligible": full_score["claims_eligible"],
            "full_claims_validated": full_score["claims_validated"],
            "full_claims_not_validated": full_score["claims_not_validated"],
            "full_claims_neutral_or_mixed": full_score["claims_neutral_or_mixed"],
            "full_claim_validation_rate": full_score["claim_validation_rate"],
            "full_reasoning_grade": full_score["reasoning_grade"],

            # De-duplicated score: one claim per metric/group/team direction.
            "unique_claims_total": unique_score["claims_total"],
            "unique_claims_eligible": unique_score["claims_eligible"],
            "unique_claims_validated": unique_score["claims_validated"],
            "unique_claims_not_validated": unique_score["claims_not_validated"],
            "unique_claims_neutral_or_mixed": unique_score["claims_neutral_or_mixed"],
            "unique_claim_validation_rate": unique_score["claim_validation_rate"],
            "unique_reasoning_grade": unique_score["reasoning_grade"],

            # Headline score: the user-facing story.
            "headline_claims_total": headline_score["claims_total"],
            "headline_claims_eligible": headline_score["claims_eligible"],
            "headline_claims_validated": headline_score["claims_validated"],
            "headline_claims_not_validated": headline_score["claims_not_validated"],
            "headline_claims_neutral_or_mixed": headline_score["claims_neutral_or_mixed"],
            "headline_claim_validation_rate": headline_score["claim_validation_rate"],
            "headline_reasoning_grade": headline_score["reasoning_grade"],

            # Supporting score: useful details, but should not drown out the headline.
            "supporting_claims_total": supporting_score["claims_total"],
            "supporting_claims_eligible": supporting_score["claims_eligible"],
            "supporting_claims_validated": supporting_score["claims_validated"],
            "supporting_claims_not_validated": supporting_score["claims_not_validated"],
            "supporting_claim_validation_rate": supporting_score["claim_validation_rate"],
            "supporting_reasoning_grade": supporting_score["reasoning_grade"],

            # Compatibility fields keep downstream review simple.
            "claims_total": unique_score["claims_total"],
            "claims_eligible": unique_score["claims_eligible"],
            "claims_validated": unique_score["claims_validated"],
            "claims_not_validated": unique_score["claims_not_validated"],
            "claims_neutral_or_mixed": unique_score["claims_neutral_or_mixed"],
            "claim_validation_rate": unique_score["claim_validation_rate"],
            "reasoning_grade": primary_score["reasoning_grade"],
            "qa_read": qa_read,
            "qa_read_v2": qa_read,
            "primary_score_used": "headline" if headline_score["claims_eligible"] else "unique",
        })

    return summaries


def summarize_claim_type(validations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    counter: Dict[Tuple[str, str], int] = Counter()
    eligible_by_type: Dict[str, int] = Counter()
    validated_by_type: Dict[str, int] = Counter()
    eligible_primary_by_type: Dict[str, int] = Counter()
    validated_primary_by_type: Dict[str, int] = Counter()

    for row in validations:
        claim_type = str(row.get("claim_type") or "missing")
        result = str(row.get("validation_result") or "missing")
        counter[(claim_type, result)] += 1
        if result != "unavailable":
            eligible_by_type[claim_type] += 1
            if normalize_boolish(row.get("is_primary_claim_instance")):
                eligible_primary_by_type[claim_type] += 1
        if result == "validated":
            validated_by_type[claim_type] += 1
            if normalize_boolish(row.get("is_primary_claim_instance")):
                validated_primary_by_type[claim_type] += 1

    rows = []
    for (claim_type, result), count in sorted(counter.items()):
        eligible = eligible_by_type.get(claim_type, 0)
        validated = validated_by_type.get(claim_type, 0)
        primary_eligible = eligible_primary_by_type.get(claim_type, 0)
        primary_validated = validated_primary_by_type.get(claim_type, 0)
        rows.append({
            "claim_type": claim_type,
            "validation_result": result,
            "count": count,
            "claim_type_validation_rate": round(validated / eligible, 3) if eligible else None,
            "primary_unique_validation_rate": round(primary_validated / primary_eligible, 3) if primary_eligible else None,
            "primary_unique_eligible": primary_eligible,
        })
    return rows


def summarize_claim_context(validations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Reliability by context: claim layer/type plus profile/outcome/bucket/model result."""
    dimensions = [
        ("claim_layer", "claim_layer"),
        ("claim_type", "claim_type"),
        ("claim_type_by_profile", "profile_type"),
        ("claim_type_by_outcome_confidence", "outcome_confidence_label"),
        ("claim_type_by_bucket", "bucket"),
        ("claim_type_by_model_result", "model_result"),
        ("group_name", "group_name"),
    ]

    rows: List[Dict[str, Any]] = []
    for summary_name, dim_field in dimensions:
        grouped: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
        for row in validations:
            result = row.get("validation_result")
            if result == "unavailable":
                continue
            claim_type = str(row.get("claim_type") or "missing")
            dim_value = str(row.get(dim_field) or "missing")
            layer = str(row.get("claim_layer") or "missing")
            key = (summary_name, claim_type if summary_name.startswith("claim_type") else layer, dim_value)
            grouped[key].append(row)

        for (name, claim_or_layer, dim_value), group_rows in sorted(grouped.items()):
            validated = [r for r in group_rows if r.get("validation_result") == "validated"]
            not_validated = [r for r in group_rows if r.get("validation_result") == "not_validated"]
            neutralish = [r for r in group_rows if r.get("validation_result") in {"actual_neutral_or_mixed", "neutral_claim_but_actual_edge"}]
            rows.append({
                "summary_name": name,
                "claim_or_layer": claim_or_layer,
                "dimension_field": dim_field,
                "dimension_value": dim_value,
                "eligible_claims": len(group_rows),
                "validated": len(validated),
                "not_validated": len(not_validated),
                "neutral_or_mixed": len(neutralish),
                "validation_rate": round(len(validated) / len(group_rows), 3) if group_rows else None,
            })
    return rows


# -----------------------------------------------------------------------------
# Markdown report
# -----------------------------------------------------------------------------



def top_context_rows(context_summary: List[Dict[str, Any]], summary_name: str, min_claims: int = 8) -> List[Dict[str, Any]]:
    return sorted(
        [r for r in context_summary if r.get("summary_name") == summary_name and int(r.get("eligible_claims") or 0) >= min_claims],
        key=lambda r: (float(r.get("validation_rate") or 0), int(r.get("eligible_claims") or 0)),
        reverse=True,
    )


def build_review_markdown(
    *,
    output_dir: Path,
    run_metadata: Dict[str, Any],
    payload_refs: List[PayloadRef],
    actual_rows: List[Dict[str, Any]],
    actual_errors: List[Dict[str, Any]],
    validations: List[Dict[str, Any]],
    game_summaries: List[Dict[str, Any]],
    claim_type_summary: List[Dict[str, Any]],
    claim_context_summary: List[Dict[str, Any]],
) -> str:
    lines: List[str] = []
    lines.append("# GameLens Level 2 QA v2 — Contextual Claim Validation")
    lines.append("")
    lines.append(f"Generated: `{run_metadata.get('created_at')}`")
    lines.append(f"Output folder: `{output_dir}`")
    lines.append("")

    lines.append("## Run Summary")
    lines.append("")
    lines.append(f"- Games selected: **{len(payload_refs)}**")
    lines.append(f"- Postgame actual metric rows loaded: **{len(actual_rows)}**")
    lines.append(f"- Claims validated: **{len(validations)}**")
    lines.append(f"- Actual source used: `{run_metadata.get('actual_source_used')}`")
    lines.append(f"- Headline top metric highlights: `{run_metadata.get('headline_top_metrics')}`")
    lines.append(f"- Reasoning thresholds: valid `>= {run_metadata.get('reasoning_valid_threshold')}`, weak `<= {run_metadata.get('reasoning_weak_threshold')}`")
    lines.append(f"- Near-even tolerance: abs `{run_metadata.get('near_even_abs')}`, pct `{run_metadata.get('near_even_pct')}`")
    lines.append("")

    lines.append("## Files to Give ChatGPT")
    lines.append("")
    lines.append("```text")
    lines.append(str(output_dir / "qa_claim_review.md"))
    lines.append(str(output_dir / "claim_validations.csv"))
    lines.append(str(output_dir / "game_claim_summary.csv"))
    lines.append(str(output_dir / "claim_type_summary.csv"))
    lines.append(str(output_dir / "claim_context_summary.csv"))
    lines.append(str(output_dir / "chatgpt_claim_validation_packet.json"))
    lines.append("```")
    lines.append("")

    ref_rows = [asdict(r) for r in payload_refs]
    dist_rows = []
    for season, count in count_by(ref_rows, "season").items():
        dist_rows.append({"field": "season", "value": season, "count": count})
    for bucket, count in count_by(ref_rows, "bucket").items():
        dist_rows.append({"field": "bucket", "value": bucket, "count": count})
    lines.append("## Game Sample Distribution")
    lines.append("")
    lines.append(markdown_table(dist_rows, ["field", "value", "count"], max_rows=20))
    lines.append("")

    validation_dist = [{"validation_result": k, "count": v} for k, v in count_by(validations, "validation_result").items()]
    lines.append("## Claim Validation Distribution")
    lines.append("")
    lines.append(markdown_table(validation_dist, ["validation_result", "count"], max_rows=20))
    lines.append("")

    layer_rows = top_context_rows(claim_context_summary, "claim_layer", min_claims=1)
    lines.append("## Headline vs Supporting Reliability")
    lines.append("")
    lines.append(markdown_table(layer_rows, ["claim_or_layer", "dimension_value", "eligible_claims", "validated", "not_validated", "neutral_or_mixed", "validation_rate"], max_rows=20))
    lines.append("")

    lines.append("## Claim Type Reliability")
    lines.append("")
    lines.append(markdown_table(
        claim_type_summary,
        ["claim_type", "validation_result", "count", "claim_type_validation_rate", "primary_unique_validation_rate", "primary_unique_eligible"],
        max_rows=80,
    ))
    lines.append("")

    lines.append("## Context Reliability — Claim Type by Profile Type")
    lines.append("")
    lines.append(markdown_table(
        top_context_rows(claim_context_summary, "claim_type_by_profile", min_claims=5),
        ["claim_or_layer", "dimension_value", "eligible_claims", "validated", "not_validated", "neutral_or_mixed", "validation_rate"],
        max_rows=50,
    ))
    lines.append("")

    lines.append("## Context Reliability — Claim Type by Outcome Confidence")
    lines.append("")
    lines.append(markdown_table(
        top_context_rows(claim_context_summary, "claim_type_by_outcome_confidence", min_claims=5),
        ["claim_or_layer", "dimension_value", "eligible_claims", "validated", "not_validated", "neutral_or_mixed", "validation_rate"],
        max_rows=50,
    ))
    lines.append("")

    lines.append("## Game-Level Reasoning Grades — v2")
    lines.append("")
    grade_rows = [{"qa_read_v2": k, "count": v} for k, v in count_by(game_summaries, "qa_read_v2").items()]
    lines.append(markdown_table(grade_rows, ["qa_read_v2", "count"], max_rows=20))
    lines.append("")

    bad = [r for r in game_summaries if r.get("qa_read_v2") == "bad_reasoning_bad_outcome"]
    good_bad = [r for r in game_summaries if r.get("qa_read_v2") == "good_reasoning_bad_outcome"]
    correct_mixed = [r for r in game_summaries if r.get("qa_read_v2") == "outcome_correct_reasoning_mixed"]
    no_pick_review = [r for r in game_summaries if r.get("qa_read_v2") == "no_pick_claim_review"]

    game_cols = [
        "game_id", "away", "home", "final_away_total", "final_home_total", "target_team",
        "raw_confidence", "outcome_confidence_label", "headline_claim_validation_rate",
        "headline_claims_validated", "headline_claims_not_validated", "unique_claim_validation_rate", "matchup_label",
    ]

    lines.append("## Priority Review — Bad Headline Reasoning + Bad Outcome")
    lines.append("")
    lines.append(markdown_table(bad, game_cols, max_rows=60))
    lines.append("")

    lines.append("## Interesting Review — Good Headline Reasoning + Bad Outcome")
    lines.append("")
    lines.append("These are games where the main claims mostly validated, but the winner still went the other way. This can point to variance or missing context.\n")
    lines.append(markdown_table(good_bad, game_cols, max_rows=40))
    lines.append("")

    lines.append("## Outcome Correct But Headline Reasoning Mixed")
    lines.append("")
    lines.append("These are sneaky: GameLens got the winner/lean right, but the main claims were not strongly validated.\n")
    lines.append(markdown_table(correct_mixed, ["game_id", "away", "home", "predicted_team", "headline_claim_validation_rate", "unique_claim_validation_rate", "full_claim_validation_rate", "matchup_label"], max_rows=60))
    lines.append("")

    lines.append("## No Pick Claim Review")
    lines.append("")
    lines.append("No Pick games can still have validated sub-claims. This shows whether restraint was appropriate or whether the app missed a cautious lean.\n")
    lines.append(markdown_table(no_pick_review, ["game_id", "away", "home", "final_margin_abs", "headline_claim_validation_rate", "unique_claim_validation_rate", "headline_claims_validated", "headline_claims_not_validated", "matchup_label"], max_rows=60))
    lines.append("")

    lines.append("## Claim Rows To Inspect First")
    lines.append("")
    not_validated_priority = [
        r for r in validations
        if r.get("validation_result") == "not_validated"
        and r.get("claim_layer") == "headline"
        and normalize_boolish(r.get("is_primary_claim_instance"))
    ]
    lines.append(markdown_table(
        not_validated_priority,
        ["game_id", "claim_type", "claim_name", "claimed_team", "actual_team", "validation_result", "model_result", "raw_confidence", "outcome_confidence_label", "claim_text"],
        max_rows=80,
    ))
    lines.append("")

    if actual_errors:
        lines.append("## Actual Metric Load Errors / Warnings")
        lines.append("")
        lines.append(markdown_table(actual_errors, ["season", "source", "table", "error"], max_rows=50))
        lines.append("")

    lines.append("## How To Interpret This")
    lines.append("")
    lines.append("- `headline_claim_validation_rate` is the main story score: Game Profile + Core Area Comparison + top metric highlights, de-duplicated.")
    lines.append("- `unique_claim_validation_rate` de-duplicates all claim rows so repeated sections do not overcount the same idea.")
    lines.append("- `full_claim_validation_rate` keeps every extracted claim row and is best for deep debugging, not headline grading.")
    lines.append("- `validated` means the postgame metric/group leader matched the pregame claim.")
    lines.append("- `not_validated` means the opposite side led postgame.")
    lines.append("- `actual_neutral_or_mixed` means the actual postgame metrics were too even or split.")
    lines.append("- `bad_reasoning_bad_outcome` is the highest-priority model-learning bucket.")
    lines.append("")

    lines.append("## Suggested Next Review Questions")
    lines.append("")
    lines.append("- Which headline claim types validate best under each profile type?")
    lines.append("- Do High Outcome Confidence misses also have weak headline validation?")
    lines.append("- Are No Pick wide-margin games hiding validated headline claims that should have produced a cautious lean?")
    lines.append("- Which Core Areas are reliable only in certain contexts?")
    lines.append("- Are supporting claims adding useful explanation, or mostly duplicating/noising up the main story?")
    lines.append("")

    return "\n".join(lines)


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate GameLens pregame claims against postgame actual metrics.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--payload-run", required=True, help="Existing QA payload run folder containing payloads/<season>/<game>.json.")
    parser.add_argument("--game-ids", nargs="*", help="Optional specific game IDs to validate.")
    parser.add_argument("--game-file", help="Optional text file of game IDs, one per line.")
    parser.add_argument("--sample-size", type=int, help="Optional number of games to select from payload run.")
    parser.add_argument("--random-sample", action="store_true", help="Use random sample instead of evenly spaced sample when --sample-size is provided.")
    parser.add_argument("--random-seed", type=int, default=42, help="Random seed for --random-sample.")

    parser.add_argument("--project-id", default=PROJECT_ID, help="Google Cloud project id.")
    parser.add_argument("--actual-source", choices=["facts", "flat"], default="facts", help="Where to load postgame actual metrics from.")
    parser.add_argument("--facts-table-template", default=FACTS_TABLE_TEMPLATE, help="Dataset.table template for cleaned facts, must include {season}.")
    parser.add_argument("--flat-metrics-table", default=FLAT_METRICS_TABLE, help="Fallback flat metrics table.")
    parser.add_argument("--schedule-table", default=SCHEDULE_TABLE, help="Schedule table used for flat fallback.")
    parser.add_argument("--no-fallback-flat", action="store_true", help="Disable fallback to game_metrics_flat if facts table fails.")

    parser.add_argument("--near-even-abs", type=float, default=0.0001, help="Absolute tolerance for treating actual metric values as neutral/even.")
    parser.add_argument("--near-even-pct", type=float, default=0.02, help="Relative tolerance for treating actual metric values as neutral/even.")
    parser.add_argument("--headline-top-metrics", type=int, default=3, help="Treat only the top N metric highlights as headline claims; the rest are supporting claims.")
    parser.add_argument("--reasoning-valid-threshold", type=float, default=0.60, help="Validation rate at or above this is considered reasoning_validated.")
    parser.add_argument("--reasoning-weak-threshold", type=float, default=0.40, help="Validation rate at or below this is considered reasoning_not_validated.")

    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Folder where claim validation runs are written.")
    parser.add_argument("--run-name", help="Optional output folder name. Defaults to timestamp.")
    parser.add_argument("--dry-run", action="store_true", help="Select payloads and write run metadata, but do not query BigQuery or validate.")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    payload_run = Path(args.payload_run)
    if not payload_run.exists():
        parser.error(f"--payload-run does not exist: {payload_run}")

    run_name = args.run_name or utc_timestamp_for_folder()
    output_dir = Path(args.output_root) / run_name
    output_dir.mkdir(parents=True, exist_ok=True)

    refs = discover_payloads_from_run(payload_run)
    refs = filter_payload_refs(
        refs,
        game_ids=args.game_ids,
        game_file=args.game_file,
        sample_size=args.sample_size,
        random_sample=args.random_sample,
        random_seed=args.random_seed,
    )

    run_metadata: Dict[str, Any] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "payload_run": str(payload_run),
        "output_dir": str(output_dir),
        "games_selected": len(refs),
        "sample_size": args.sample_size,
        "random_sample": args.random_sample,
        "random_seed": args.random_seed,
        "project_id": args.project_id,
        "actual_source_requested": args.actual_source,
        "facts_table_template": args.facts_table_template,
        "flat_metrics_table": args.flat_metrics_table,
        "schedule_table": args.schedule_table,
        "fallback_flat": not args.no_fallback_flat,
        "near_even_abs": args.near_even_abs,
        "near_even_pct": args.near_even_pct,
        "headline_top_metrics": args.headline_top_metrics,
        "reasoning_valid_threshold": args.reasoning_valid_threshold,
        "reasoning_weak_threshold": args.reasoning_weak_threshold,
        "dry_run": args.dry_run,
    }

    write_json(run_metadata, output_dir / "run_metadata.json")
    write_json({"games": [asdict(r) for r in refs]}, output_dir / "selected_games.json")

    print("\nGameLens Level 2 claim validation")
    print("----------------------------------")
    print(f"Payload run: {payload_run}")
    print(f"Output folder: {output_dir}")
    print(f"Games selected: {len(refs)}")
    for season, count in count_by([asdict(r) for r in refs], "season").items():
        print(f"  season {season}: {count}")
    print("")

    if args.dry_run:
        print("Dry run complete. No BigQuery actuals loaded and no claims validated.")
        return 0

    if not refs:
        print("No payloads selected. Nothing to validate.")
        return 1

    print("Loading postgame actual metrics from BigQuery ...", flush=True)
    actual_rows, actual_errors, actual_source_used = load_actual_metrics(
        payload_refs=refs,
        project_id=args.project_id,
        actual_source=args.actual_source,
        facts_table_template=args.facts_table_template,
        flat_metrics_table=args.flat_metrics_table,
        schedule_table=args.schedule_table,
        fallback_flat=not args.no_fallback_flat,
    )
    run_metadata["actual_source_used"] = actual_source_used
    run_metadata["actual_metric_rows_loaded"] = len(actual_rows)
    write_json(run_metadata, output_dir / "run_metadata.json")
    write_json(actual_errors, output_dir / "actual_load_errors.json")

    actual_index = build_actual_index(actual_rows)

    print(f"Actual metric rows loaded: {len(actual_rows)}")
    if actual_errors:
        print(f"Actual load warnings/errors: {len(actual_errors)}")

    print("Extracting and validating claims ...", flush=True)
    all_claims: List[Dict[str, Any]] = []
    payload_errors: List[Dict[str, Any]] = []

    for ref in refs:
        try:
            payload = read_json(Path(ref.payload_path))
            claims = extract_claims_from_payload(payload, ref)
            all_claims.extend(claims)
        except Exception as exc:
            payload_errors.append({"game_id": ref.game_id, "payload_path": ref.payload_path, "error": str(exc)})

    all_claims = prepare_claims_for_v2(all_claims, headline_top_metrics=args.headline_top_metrics)

    validations = validate_claims(
        all_claims,
        actual_index,
        abs_tol=args.near_even_abs,
        pct_tol=args.near_even_pct,
    )
    game_summaries = aggregate_game_summary(
        validations,
        valid_threshold=args.reasoning_valid_threshold,
        weak_threshold=args.reasoning_weak_threshold,
    )
    claim_type_summary = summarize_claim_type(validations)
    claim_context_summary = summarize_claim_context(validations)

    write_csv(validations, output_dir / "claim_validations.csv")
    write_csv(game_summaries, output_dir / "game_claim_summary.csv")
    write_csv(claim_type_summary, output_dir / "claim_type_summary.csv")
    write_csv(claim_context_summary, output_dir / "claim_context_summary.csv")
    write_json(payload_errors, output_dir / "payload_errors.json")

    review_md = build_review_markdown(
        output_dir=output_dir,
        run_metadata=run_metadata,
        payload_refs=refs,
        actual_rows=actual_rows,
        actual_errors=actual_errors + payload_errors,
        validations=validations,
        game_summaries=game_summaries,
        claim_type_summary=claim_type_summary,
        claim_context_summary=claim_context_summary,
    )
    (output_dir / "qa_claim_review.md").write_text(review_md, encoding="utf-8")

    packet = {
        "run_metadata": run_metadata,
        "selected_games": [asdict(r) for r in refs],
        "summary": {
            "games_selected": len(refs),
            "actual_metric_rows_loaded": len(actual_rows),
            "claims_validated": len(validations),
            "validation_distribution": count_by(validations, "validation_result"),
            "claim_type_distribution": count_by(validations, "claim_type"),
            "qa_read_distribution": count_by(game_summaries, "qa_read_v2"),
            "claim_layer_distribution": count_by(validations, "claim_layer"),
            "primary_claim_distribution": count_by(validations, "is_primary_claim_instance"),
        },
        "claim_type_summary": claim_type_summary,
        "claim_context_summary": claim_context_summary,
        "priority_games": {
            "bad_reasoning_bad_outcome": [r for r in game_summaries if r.get("qa_read_v2") == "bad_reasoning_bad_outcome"],
            "good_reasoning_bad_outcome": [r for r in game_summaries if r.get("qa_read_v2") == "good_reasoning_bad_outcome"],
            "outcome_correct_reasoning_mixed": [r for r in game_summaries if r.get("qa_read_v2") == "outcome_correct_reasoning_mixed"],
            "no_pick_claim_review": [r for r in game_summaries if r.get("qa_read_v2") == "no_pick_claim_review"],
        },
        # Keep full rows available, but the CSV is easier for broad review.
        "game_summaries": game_summaries,
        "actual_load_errors": actual_errors,
        "payload_errors": payload_errors,
    }
    write_json(packet, output_dir / "chatgpt_claim_validation_packet.json")

    print("\nRun complete")
    print("------------")
    print(f"Games selected: {len(refs)}")
    print(f"Claims extracted: {len(all_claims)}")
    print(f"Claims validated: {len(validations)}")
    print(f"Claim validations CSV: {output_dir / 'claim_validations.csv'}")
    print(f"Game summary CSV: {output_dir / 'game_claim_summary.csv'}")
    print(f"Claim context summary CSV: {output_dir / 'claim_context_summary.csv'}")
    print(f"Review Markdown: {output_dir / 'qa_claim_review.md'}")
    print(f"ChatGPT packet: {output_dir / 'chatgpt_claim_validation_packet.json'}")

    return 0 if not payload_errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
