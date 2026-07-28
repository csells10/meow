#!/usr/bin/env python
"""
collect_gamelens_api_smoke_payloads.py

Randomly samples GameLens games, pings the local /game API, saves full payloads,
and creates QA-friendly aggregate files for later analysis.

Recommended repo location:
    qa/collect_gamelens_api_smoke_payloads.py

Why this exists:
    Clicking games in the frontend is slow and incomplete.
    This script captures full API responses so GameLens can be reviewed as an
    API/product contract, not only as screenshots.

Default behavior:
    - Samples finalized games from BigQuery League.schedule
    - Balances sample across seasons when possible
    - Calls /game/<game_id> on the local API
    - Saves raw JSON payloads
    - Builds:
        payload_summary.csv
        claim_language_rows.csv
        alignment_warnings.csv
        run_summary.json
        analysis_prompt.md

Examples:
    # 30-game random sample, balanced across 2023-2025
    python qa/collect_gamelens_api_smoke_payloads.py --sample-size 30

    # 60-game sample with a stable seed
    python qa/collect_gamelens_api_smoke_payloads.py --sample-size 60 --seed 20260520

    # Use a provided file of game IDs instead of BigQuery
    python qa/collect_gamelens_api_smoke_payloads.py --game-id-file qa/game_ids.txt

    # Hit a different API base URL
    python qa/collect_gamelens_api_smoke_payloads.py --base-url http://127.0.0.1:8080

Local auth note:
    If you run app.py with:
        LOCAL_DEV_AUTH_BYPASS=true
        LOCAL_DEV_EMAIL=...
    then this script should work without an auth token.

    If you need auth, set:
        GAMELENS_AUTH_TOKEN=<token>
    or pass:
        --auth-token <token>
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote

import requests


PROJECT_ID = "nfl-stream-406420"
SCHEDULE_TABLE = f"{PROJECT_ID}.League.schedule"

FINAL_STATUSES = {"Final", "Final/OT"}
BLOCKED_METRICS = {
    "red_zone_efficiency",
    "td_rate",
    "turnover_margin_per_game",
    "yards_per_play",
}

DEFAULT_SEASONS = ["2023", "2024", "2025"]


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def utc_now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.@-]+", "_", str(value)).strip("_")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def as_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def get_nested(data: dict, path: str, default: Any = None) -> Any:
    current: Any = data
    for part in path.split("."):
        if not isinstance(current, dict):
            return default
        current = current.get(part)
    return current if current is not None else default


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def write_csv(path: Path, rows: list[dict], preferred_columns: list[str] | None = None) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    all_columns = sorted({key for row in rows for key in row.keys()})
    if preferred_columns:
        columns = [col for col in preferred_columns if col in all_columns]
        columns += [col for col in all_columns if col not in columns]
    else:
        columns = all_columns

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_game_id_file(path: Path) -> list[dict]:
    game_rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        # Accept either plain game_id or CSV-ish first token.
        game_id = stripped.split(",")[0].strip()
        game_rows.append({
            "game_id": game_id,
            "season": game_id[:4] if len(game_id) >= 4 else None,
            "source": "game_id_file",
        })
    return game_rows


# ---------------------------------------------------------------------------
# BigQuery sample loading
# ---------------------------------------------------------------------------

def import_bigquery():
    try:
        from google.cloud import bigquery
        return bigquery
    except Exception as exc:
        raise RuntimeError(
            "google-cloud-bigquery is not available in this environment. "
            "Either install it in your venv or pass --game-id-file."
        ) from exc


def load_candidate_games_from_bigquery(
    *,
    project_id: str,
    schedule_table: str,
    seasons: list[str],
    only_final: bool,
) -> list[dict]:
    bigquery = import_bigquery()
    client = bigquery.Client(project=project_id)

    status_filter = "AND gameStatus IN UNNEST(@final_statuses)" if only_final else ""

    query = f"""
        SELECT
            CAST(gameID AS STRING) AS game_id,
            CAST(season AS STRING) AS season,
            CAST(gameDate AS STRING) AS game_date,
            CAST(gameWeek AS STRING) AS game_week,
            CAST(away AS STRING) AS away,
            CAST(home AS STRING) AS home,
            CAST(gameStatus AS STRING) AS game_status
        FROM `{schedule_table}`
        WHERE CAST(season AS STRING) IN UNNEST(@seasons)
          {status_filter}
          AND gameID IS NOT NULL
        ORDER BY season, gameDate, gameID
    """

    params = [
        bigquery.ArrayQueryParameter("seasons", "STRING", seasons),
    ]
    if only_final:
        params.append(
            bigquery.ArrayQueryParameter("final_statuses", "STRING", sorted(FINAL_STATUSES))
        )

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return [dict(row) for row in client.query(query, job_config=job_config).result()]


def choose_stratified_sample(
    candidates: list[dict],
    *,
    sample_size: int,
    seasons: list[str],
    seed: int | None,
) -> list[dict]:
    rng = random.Random(seed)
    by_season: dict[str, list[dict]] = defaultdict(list)

    for row in candidates:
        season = str(row.get("season") or "unknown")
        by_season[season].append(row)

    for season_rows in by_season.values():
        rng.shuffle(season_rows)

    requested_seasons = [str(s) for s in seasons]
    base_each = sample_size // max(len(requested_seasons), 1)
    remainder = sample_size % max(len(requested_seasons), 1)

    selected: list[dict] = []
    leftovers: list[dict] = []

    for ix, season in enumerate(requested_seasons):
        target = base_each + (1 if ix < remainder else 0)
        season_rows = by_season.get(season, [])
        take = min(target, len(season_rows))
        selected.extend(season_rows[:take])
        leftovers.extend(season_rows[take:])

    # If one season did not have enough games, fill from leftovers.
    if len(selected) < sample_size:
        rng.shuffle(leftovers)
        selected_ids = {row["game_id"] for row in selected}
        for row in leftovers:
            if row["game_id"] in selected_ids:
                continue
            selected.append(row)
            selected_ids.add(row["game_id"])
            if len(selected) >= sample_size:
                break

    rng.shuffle(selected)
    return selected[:sample_size]


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

@dataclass
class FetchResult:
    game_id: str
    ok: bool
    status_code: int | None
    elapsed_seconds: float | None
    payload: dict | None
    error: str | None


def build_headers(auth_token: str | None) -> dict:
    headers = {
        "Accept": "application/json",
    }
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"
    return headers


def fetch_game_payload(
    *,
    base_url: str,
    game_id: str,
    timeout: int,
    auth_token: str | None,
) -> FetchResult:
    url = f"{base_url.rstrip('/')}/game/{quote(game_id, safe='')}"
    headers = build_headers(auth_token)
    start = time.perf_counter()

    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        elapsed = round(time.perf_counter() - start, 3)

        if response.status_code != 200:
            return FetchResult(
                game_id=game_id,
                ok=False,
                status_code=response.status_code,
                elapsed_seconds=elapsed,
                payload=None,
                error=f"HTTP {response.status_code}: {response.text[:300]}",
            )

        try:
            payload = response.json()
        except Exception as exc:
            return FetchResult(
                game_id=game_id,
                ok=False,
                status_code=response.status_code,
                elapsed_seconds=elapsed,
                payload=None,
                error=f"JSON parse failed: {exc}",
            )

        return FetchResult(
            game_id=game_id,
            ok=True,
            status_code=response.status_code,
            elapsed_seconds=elapsed,
            payload=payload,
            error=None,
        )

    except Exception as exc:
        elapsed = round(time.perf_counter() - start, 3)
        return FetchResult(
            game_id=game_id,
            ok=False,
            status_code=None,
            elapsed_seconds=elapsed,
            payload=None,
            error=str(exc),
        )


# ---------------------------------------------------------------------------
# Payload extraction
# ---------------------------------------------------------------------------

def language_support_dict(row: dict) -> dict:
    ls = row.get("language_support")
    return ls if isinstance(ls, dict) else {}


def base_claim_row(
    *,
    payload: dict,
    section: str,
    row: dict,
    claim_type: str | None = None,
    claim_layer: str | None = None,
    parent_name: str | None = None,
) -> dict:
    header = payload.get("header") or {}
    matchup_lean = payload.get("matchup_lean") or {}
    model_outcome = payload.get("model_outcome") or {}
    ranking_context = payload.get("ranking_context") or {}

    ls = language_support_dict(row)

    return {
        "game_id": header.get("game_id"),
        "season": header.get("season"),
        "game_week": header.get("game_week"),
        "game_date": header.get("game_date"),
        "away_team": get_nested(header, "away_team.abbreviation"),
        "home_team": get_nested(header, "home_team.abbreviation"),
        "section": section,
        "parent_name": parent_name,
        "claim_type": claim_type or ls.get("claim_type"),
        "claim_layer": claim_layer or ls.get("claim_layer"),
        "metric": row.get("metric") or ls.get("metric"),
        "label": row.get("label") or row.get("name"),
        "category": row.get("category"),
        "core_area": row.get("core_area"),
        "leader": row.get("leader") or row.get("better"),
        "leader_team": row.get("leader_team"),
        "summary_label": row.get("summary_label") or row.get("comparison_strength"),
        "summary": row.get("summary"),
        "percentile_gap": row.get("percentile_gap"),
        "rank_gap": row.get("rank_gap"),
        "raw_gap": row.get("raw_gap"),
        "has_language_support": bool(ls),
        "language_boost_allowed": ls.get("language_boost_allowed"),
        "support_level": ls.get("support_level"),
        "rule_status": ls.get("rule_status"),
        "language_modifier": ls.get("language_modifier"),
        "reason": ls.get("reason"),
        "claim_strength_bucket": ls.get("claim_strength_bucket"),
        "claim_strength_context": ls.get("claim_strength_context"),
        "claim_strength_language_signal": ls.get("claim_strength_language_signal"),
        "two_way_context": ls.get("two_way_context"),
        "two_way_edge_score": ls.get("two_way_edge_score"),
        "predicted_team": model_outcome.get("predicted_team"),
        "actual_winner": model_outcome.get("actual_winner"),
        "model_result": model_outcome.get("result"),
        "matchup_target_team": matchup_lean.get("target_team"),
        "matchup_confidence": matchup_lean.get("confidence"),
        "outcome_confidence": get_nested(matchup_lean, "outcome_confidence.label"),
        "profile_strength": get_nested(matchup_lean, "profile_strength.label"),
        "ranking_available": ranking_context.get("available"),
        "source_data_dates": "|".join(str(x) for x in (ranking_context.get("source_data_dates") or [])),
        "max_data_lag_days": ranking_context.get("max_data_lag_days"),
    }


def extract_claim_language_rows(payload: dict) -> list[dict]:
    rows: list[dict] = []
    matchup_breakdown = payload.get("matchup_breakdown") or {}

    for row in payload.get("team_comparison") or []:
        rows.append(
            base_claim_row(
                payload=payload,
                section="team_comparison",
                row=row,
                claim_type="team_comparison_metric",
                claim_layer="supporting",
            )
        )

    for row in matchup_breakdown.get("metric_highlights") or []:
        rows.append(
            base_claim_row(
                payload=payload,
                section="metric_highlight",
                row=row,
                claim_type="metric_highlight",
                claim_layer="headline",
            )
        )

    for summary in matchup_breakdown.get("category_summaries") or []:
        # Aggregate category row.
        rows.append(
            base_claim_row(
                payload=payload,
                section="category_summary_aggregate",
                row=summary,
                claim_type="category_summary",
                claim_layer="aggregate",
                parent_name=summary.get("name"),
            )
        )

        for driver in summary.get("drivers") or []:
            enriched_driver = dict(driver)
            # Drivers often inherit category through parent summary.
            enriched_driver.setdefault("category", summary.get("name"))
            rows.append(
                base_claim_row(
                    payload=payload,
                    section="category_summary_driver",
                    row=enriched_driver,
                    claim_type="category_summary",
                    claim_layer="supporting",
                    parent_name=summary.get("name"),
                )
            )

    # Include core-area summaries as unannotated rows so we can detect section alignment.
    for summary in matchup_breakdown.get("core_area_summaries") or []:
        rows.append(
            base_claim_row(
                payload=payload,
                section="core_area_summary_aggregate_unannotated",
                row=summary,
                claim_type="core_area_summary",
                claim_layer="aggregate",
                parent_name=summary.get("name"),
            )
        )
        for driver in summary.get("drivers") or []:
            enriched_driver = dict(driver)
            enriched_driver.setdefault("core_area", summary.get("name"))
            rows.append(
                base_claim_row(
                    payload=payload,
                    section="core_area_summary_driver_unannotated",
                    row=enriched_driver,
                    claim_type="core_area_summary",
                    claim_layer="supporting",
                    parent_name=summary.get("name"),
                )
            )

    return rows


def core_area_summary_map(payload: dict) -> dict[str, dict]:
    result = {}
    for row in get_nested(payload, "matchup_breakdown.core_area_summaries", []) or []:
        name = row.get("name")
        if name:
            result[str(name)] = row
    return result


def build_alignment_warnings(payload: dict) -> list[dict]:
    warnings: list[dict] = []
    header = payload.get("header") or {}
    game_id = header.get("game_id")

    def add_warning(code: str, severity: str, message: str, **extra: Any) -> None:
        warnings.append({
            "game_id": game_id,
            "season": header.get("season"),
            "game_week": header.get("game_week"),
            "game_date": header.get("game_date"),
            "away_team": get_nested(header, "away_team.abbreviation"),
            "home_team": get_nested(header, "home_team.abbreviation"),
            "warning_code": code,
            "severity": severity,
            "message": message,
            **extra,
        })

    # 1. Confidence contract check.
    #
    # matchup_lean.confidence is preserved as a legacy/raw signal field.
    # The frontend-facing confidence should come from:
    #   matchup_lean.user_facing_confidence.label
    #
    # This warning should only fire when the new user-facing field is missing,
    # disagrees with outcome_confidence, or the legacy/raw fields disagree.
    legacy_confidence = get_nested(payload, "matchup_lean.confidence")
    raw_signal_confidence = get_nested(payload, "matchup_lean.raw_signal_confidence")
    confidence_role = get_nested(payload, "matchup_lean.confidence_role")
    outcome_confidence = get_nested(payload, "matchup_lean.outcome_confidence.label")
    user_facing_confidence = get_nested(payload, "matchup_lean.user_facing_confidence.label")
    user_facing_source = get_nested(payload, "matchup_lean.user_facing_confidence.source")

    if outcome_confidence and not user_facing_confidence:
        add_warning(
            "user_facing_confidence_missing",
            "high",
            "matchup_lean.user_facing_confidence.label is missing even though outcome_confidence.label exists.",
            confidence=legacy_confidence,
            raw_signal_confidence=raw_signal_confidence,
            confidence_role=confidence_role,
            outcome_confidence=outcome_confidence,
            user_facing_confidence=user_facing_confidence,
            user_facing_source=user_facing_source,
            matchup_label=get_nested(payload, "matchup_lean.matchup_label"),
        )

    elif (
        outcome_confidence
        and user_facing_confidence
        and str(user_facing_confidence).lower() != str(outcome_confidence).lower()
    ):
        add_warning(
            "user_facing_confidence_mismatch",
            "high",
            "matchup_lean.user_facing_confidence.label differs from matchup_lean.outcome_confidence.label.",
            confidence=legacy_confidence,
            raw_signal_confidence=raw_signal_confidence,
            confidence_role=confidence_role,
            outcome_confidence=outcome_confidence,
            user_facing_confidence=user_facing_confidence,
            user_facing_source=user_facing_source,
            matchup_label=get_nested(payload, "matchup_lean.matchup_label"),
        )

    elif (
        legacy_confidence
        and raw_signal_confidence
        and str(legacy_confidence).lower() != str(raw_signal_confidence).lower()
    ):
        add_warning(
            "legacy_raw_confidence_mismatch",
            "medium",
            "matchup_lean.confidence differs from matchup_lean.raw_signal_confidence.",
            confidence=legacy_confidence,
            raw_signal_confidence=raw_signal_confidence,
            confidence_role=confidence_role,
            outcome_confidence=outcome_confidence,
            user_facing_confidence=user_facing_confidence,
            user_facing_source=user_facing_source,
            matchup_label=get_nested(payload, "matchup_lean.matchup_label"),
        )

    # 2. Team Comparison under-classified claim-strength.
    claim_rows = extract_claim_language_rows(payload)

    team_unclassified = [
        row for row in claim_rows
        if row["section"] == "team_comparison"
        and row.get("claim_strength_context") == "unclassified_edge"
    ]
    if team_unclassified:
        add_warning(
            "team_comparison_unclassified_claim_strength",
            "medium_high",
            "Team Comparison rows have unclassified claim-strength context; likely missing category/core_area.",
            count=len(team_unclassified),
            metrics="|".join(sorted({str(row.get("metric")) for row in team_unclassified})),
        )

    # 3. Core Area comparison vs ranking-based core area summaries.
    summary_by_name = core_area_summary_map(payload)
    for row in payload.get("core_area_comparison") or []:
        core_area = row.get("core_area")
        broad_leader = row.get("leader")
        summary = summary_by_name.get(str(core_area))

        if not summary:
            continue

        summary_leader = summary.get("leader")
        if broad_leader != summary_leader:
            add_warning(
                "core_area_layer_leader_mismatch",
                "medium_high",
                "core_area_comparison leader differs from matchup_breakdown.core_area_summaries leader.",
                core_area=core_area,
                broad_leader=broad_leader,
                summary_leader=summary_leader,
                broad_away_score=row.get("away_score"),
                broad_home_score=row.get("home_score"),
                summary_away_score=summary.get("away_score"),
                summary_home_score=summary.get("home_score"),
            )

    # 4. Model Trust tooltip wording mismatch.
    tooltip = str(get_nested(payload, "model_trust.edge.tooltip", "") or "")
    neutral = as_int(get_nested(payload, "model_trust.matchup_advantage.neutral"))

    if "several even" in tooltip.lower() and neutral is not None and neutral <= 1:
        add_warning(
            "model_trust_tooltip_even_area_mismatch",
            "medium",
            "Model Trust tooltip references several even areas, but visible neutral count is low.",
            tooltip=tooltip,
            neutral_count=neutral,
        )

    # 5. Aggregate category reason missing.
    for row in claim_rows:
        if row["section"] != "category_summary_aggregate":
            continue

        if row.get("language_boost_allowed") is False and not row.get("reason"):
            add_warning(
                "category_aggregate_missing_no_boost_reason",
                "low_medium",
                "Category summary aggregate has no boost and no reason.",
                category=row.get("parent_name"),
            )

    # 6. Hard safety: blocked metrics should not boost.
    for row in claim_rows:
        if row.get("metric") in BLOCKED_METRICS and row.get("language_boost_allowed") is True:
            add_warning(
                "blocked_metric_boosted",
                "critical",
                "Blocked metric received language_boost_allowed=true.",
                section=row.get("section"),
                metric=row.get("metric"),
                rule_status=row.get("rule_status"),
            )

    # 7. Hard safety: non-supportive two-way context should not boost.
    for row in claim_rows:
        two_way = row.get("two_way_context")
        if two_way and two_way != "supportive" and row.get("language_boost_allowed") is True:
            add_warning(
                "non_supportive_two_way_boosted",
                "critical",
                "Row boosted language despite two_way_context not being supportive.",
                section=row.get("section"),
                metric=row.get("metric"),
                two_way_context=two_way,
                rule_status=row.get("rule_status"),
            )

    # 8. Missing claim-strength metadata where language_support exists and gap exists.
    for row in claim_rows:
        if not row.get("has_language_support"):
            continue

        if row.get("percentile_gap") is None:
            continue

        missing_fields = [
            field for field in [
                "claim_strength_bucket",
                "claim_strength_context",
                "claim_strength_language_signal",
            ]
            if row.get(field) in {None, ""}
        ]

        if missing_fields:
            add_warning(
                "claim_strength_metadata_missing",
                "medium",
                "language_support exists with percentile_gap, but claim-strength metadata is missing.",
                section=row.get("section"),
                metric=row.get("metric"),
                missing_fields="|".join(missing_fields),
            )

    return warnings


def summarize_payload(payload: dict, *, elapsed_seconds: float | None = None) -> dict:
    header = payload.get("header") or {}
    matchup_lean = payload.get("matchup_lean") or {}
    model_outcome = payload.get("model_outcome") or {}
    model_trust = payload.get("model_trust") or {}
    ranking_context = payload.get("ranking_context") or {}
    claim_context = payload.get("claim_language_context") or {}

    claim_rows = extract_claim_language_rows(payload)
    warnings = build_alignment_warnings(payload)

    by_section = Counter(row["section"] for row in claim_rows)
    by_signal = Counter(str(row.get("claim_strength_language_signal") or "NULL") for row in claim_rows if row.get("has_language_support"))
    by_context = Counter(str(row.get("claim_strength_context") or "NULL") for row in claim_rows if row.get("has_language_support"))
    by_rule = Counter(str(row.get("rule_status") or "NULL") for row in claim_rows if row.get("has_language_support"))

    boosted_rows = [r for r in claim_rows if r.get("language_boost_allowed") is True]
    unclassified_team_rows = [
        r for r in claim_rows
        if r["section"] == "team_comparison"
        and r.get("claim_strength_context") == "unclassified_edge"
    ]

    category_aggregates_missing_reason = [
        r for r in claim_rows
        if r["section"] == "category_summary_aggregate"
        and r.get("language_boost_allowed") is False
        and not r.get("reason")
    ]

    return {
        "game_id": header.get("game_id"),
        "season": header.get("season"),
        "game_week": header.get("game_week"),
        "game_date": header.get("game_date"),
        "game_status": header.get("game_status"),
        "away_team": get_nested(header, "away_team.abbreviation"),
        "home_team": get_nested(header, "home_team.abbreviation"),
        "actual_winner": model_outcome.get("actual_winner"),
        "predicted_team": model_outcome.get("predicted_team"),
        "model_result": model_outcome.get("result"),
        "matchup_target_team": matchup_lean.get("target_team"),
        "matchup_target_side": matchup_lean.get("target_side"),
        "matchup_confidence": matchup_lean.get("confidence"),
        "outcome_confidence": get_nested(matchup_lean, "outcome_confidence.label"),
        "profile_strength": get_nested(matchup_lean, "profile_strength.label"),
        "matchup_label": matchup_lean.get("matchup_label"),
        "profile_type": matchup_lean.get("profile_type"),
        "signal_gap": get_nested(matchup_lean, "signal_score.gap"),
        "core_area_leader": get_nested(matchup_lean, "core_area_context.core_area_leader"),
        "core_area_split": get_nested(matchup_lean, "core_area_context.core_area_split"),
        "core_gap": get_nested(matchup_lean, "core_area_context.core_gap"),
        "model_trust_edge_strength": get_nested(model_trust, "edge.strength"),
        "model_trust_edge_score": get_nested(model_trust, "edge.score"),
        "model_trust_matchup_advantage_leader": get_nested(model_trust, "matchup_advantage.leader"),
        "model_trust_matchup_advantage_away": get_nested(model_trust, "matchup_advantage.away"),
        "model_trust_matchup_advantage_home": get_nested(model_trust, "matchup_advantage.home"),
        "model_trust_matchup_advantage_neutral": get_nested(model_trust, "matchup_advantage.neutral"),
        "signal_alignment_code": get_nested(model_trust, "signal_alignment.summary_code"),
        "ranking_available": ranking_context.get("available"),
        "ranking_window_type": ranking_context.get("window_type"),
        "ranking_max_data_lag_days": ranking_context.get("max_data_lag_days"),
        "claim_language_available": claim_context.get("available"),
        "away_two_way_context": get_nested(claim_context, "two_way_context_by_side.away.two_way_context"),
        "home_two_way_context": get_nested(claim_context, "two_way_context_by_side.home.two_way_context"),
        "away_two_way_edge_score": get_nested(claim_context, "two_way_context_by_side.away.two_way_edge_score"),
        "home_two_way_edge_score": get_nested(claim_context, "two_way_context_by_side.home.two_way_edge_score"),
        "claim_row_count": len(claim_rows),
        "language_support_row_count": sum(1 for row in claim_rows if row.get("has_language_support")),
        "boosted_language_row_count": len(boosted_rows),
        "team_comparison_unclassified_count": len(unclassified_team_rows),
        "category_aggregate_missing_reason_count": len(category_aggregates_missing_reason),
        "warning_count": len(warnings),
        "critical_warning_count": sum(1 for w in warnings if w.get("severity") == "critical"),
        "high_warning_count": sum(1 for w in warnings if str(w.get("severity", "")).startswith("high")),
        "sections_seen": json.dumps(dict(sorted(by_section.items()))),
        "claim_strength_signal_counts": json.dumps(dict(sorted(by_signal.items()))),
        "claim_strength_context_counts": json.dumps(dict(sorted(by_context.items()))),
        "rule_status_counts": json.dumps(dict(sorted(by_rule.items()))),
        "elapsed_seconds": elapsed_seconds,
    }


def build_run_summary(
    *,
    selected_games: list[dict],
    fetch_results: list[FetchResult],
    payload_summaries: list[dict],
    claim_rows: list[dict],
    warnings: list[dict],
    args: argparse.Namespace,
) -> dict:
    ok_results = [r for r in fetch_results if r.ok]
    failed_results = [r for r in fetch_results if not r.ok]

    warning_counts = Counter(w["warning_code"] for w in warnings)
    severity_counts = Counter(w["severity"] for w in warnings)
    result_counts = Counter(str(r.get("model_result") or "unknown") for r in payload_summaries)
    season_counts = Counter(str(r.get("season") or "unknown") for r in payload_summaries)
    signal_counts = Counter(str(r.get("claim_strength_language_signal") or "NULL") for r in claim_rows if r.get("has_language_support"))
    context_counts = Counter(str(r.get("claim_strength_context") or "NULL") for r in claim_rows if r.get("has_language_support"))
    rule_counts = Counter(str(r.get("rule_status") or "NULL") for r in claim_rows if r.get("has_language_support"))

    elapsed_values = [r.elapsed_seconds for r in ok_results if r.elapsed_seconds is not None]
    avg_elapsed = round(sum(elapsed_values) / len(elapsed_values), 3) if elapsed_values else None

    return {
        "run_created_at": utc_now_iso(),
        "base_url": args.base_url,
        "sample_size_requested": args.sample_size,
        "selected_game_count": len(selected_games),
        "successful_payloads": len(ok_results),
        "failed_payloads": len(failed_results),
        "seasons_requested": args.seasons,
        "only_final": args.only_final,
        "seed": args.seed,
        "avg_elapsed_seconds": avg_elapsed,
        "model_result_counts": dict(sorted(result_counts.items())),
        "season_counts": dict(sorted(season_counts.items())),
        "warning_counts": dict(sorted(warning_counts.items())),
        "warning_severity_counts": dict(sorted(severity_counts.items())),
        "claim_strength_language_signal_counts": dict(sorted(signal_counts.items())),
        "claim_strength_context_counts": dict(sorted(context_counts.items())),
        "rule_status_counts": dict(sorted(rule_counts.items())),
        "failed_games": [
            {
                "game_id": r.game_id,
                "status_code": r.status_code,
                "error": r.error,
                "elapsed_seconds": r.elapsed_seconds,
            }
            for r in failed_results
        ],
    }


def write_analysis_prompt(
    *,
    path: Path,
    run_name: str,
    run_summary: dict,
) -> None:
    content = f"""# GameLens API Payload QA Analysis Prompt

Run name: `{run_name}`

I collected a randomized batch of full `/game` API payloads for GameLens.

Please analyze these output files together:

1. `run_summary.json`
2. `payload_summary.csv`
3. `claim_language_rows.csv`
4. `alignment_warnings.csv`
5. Raw payloads in `payloads/`

## What I want you to evaluate

Please do a devil's advocate API/product smoke test:

- Does the full API response make logical sense across games?
- Are any sections contradictory?
- Is `matchup_lean.confidence` aligned with `outcome_confidence`?
- Are Team Comparison rows under-classified because `category` / `core_area` is missing?
- Do `core_area_comparison` and `matchup_breakdown.core_area_summaries` disagree often?
- Are blocked metrics ever accidentally boosted?
- Are non-supportive `two_way_context` rows ever boosted?
- Are `claim_strength_bucket`, `claim_strength_context`, and `claim_strength_language_signal` behaving consistently?
- Which warnings are true product issues vs acceptable internal differences?
- What should be fixed before frontend badges or public language changes?

## Current run summary

```json
{json.dumps(run_summary, indent=2)}
```

## Output I want back

Please return:

1. Executive verdict
2. Top 5 API alignment issues
3. Top 5 safe/working behaviors
4. Specific file/function fixes
5. Recommended next development step
6. Whether this is safe for frontend debug display yet
"""
    path.write_text(content, encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Randomly collect and aggregate GameLens /game API payloads for QA."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--sample-size", type=int, default=30)
    parser.add_argument("--seasons", nargs="+", default=DEFAULT_SEASONS)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--only-final", action="store_true", default=True)
    parser.add_argument("--include-non-final", action="store_true", help="Disable final-only filter.")
    parser.add_argument("--game-id-file", default=None, help="Optional file with one game_id per line.")
    parser.add_argument("--output-root", default="qa/api_response_runs")
    parser.add_argument("--run-name", default=None)
    parser.add_argument("--project-id", default=PROJECT_ID)
    parser.add_argument("--schedule-table", default=SCHEDULE_TABLE)
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--sleep", type=float, default=0.1)
    parser.add_argument("--auth-token", default=None)
    parser.add_argument("--dry-run", action="store_true", help="Select games but do not call the API.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.include_non_final:
        args.only_final = False

    auth_token = args.auth_token or os.environ.get("GAMELENS_AUTH_TOKEN")

    run_name = args.run_name or f"gamelens_api_smoke_{utc_now_stamp()}"
    output_dir = ensure_dir(Path(args.output_root) / run_name)
    payload_dir = ensure_dir(output_dir / "payloads")

    print("GameLens API smoke payload collector")
    print("------------------------------------")
    print(f"Run name:      {run_name}")
    print(f"Base URL:      {args.base_url}")
    print(f"Sample size:   {args.sample_size}")
    print(f"Seasons:       {args.seasons}")
    print(f"Only final:    {args.only_final}")
    print(f"Output dir:    {output_dir}")
    print(f"Dry run:       {args.dry_run}")

    if args.game_id_file:
        candidates = read_game_id_file(Path(args.game_id_file))
        selected_games = candidates[: args.sample_size]
        random.Random(args.seed).shuffle(selected_games)
    else:
        print("\nLoading candidate games from BigQuery...")
        candidates = load_candidate_games_from_bigquery(
            project_id=args.project_id,
            schedule_table=args.schedule_table,
            seasons=[str(s) for s in args.seasons],
            only_final=args.only_final,
        )
        print(f"Candidate games loaded: {len(candidates)}")

        if not candidates:
            print("No candidate games found.")
            return 1

        selected_games = choose_stratified_sample(
            candidates=candidates,
            sample_size=args.sample_size,
            seasons=[str(s) for s in args.seasons],
            seed=args.seed,
        )

    write_json(output_dir / "selected_games.json", selected_games)

    if args.dry_run:
        print("\nDry run selected games:")
        for row in selected_games:
            print(f"  {row.get('game_id')}")
        return 0

    fetch_results: list[FetchResult] = []
    payload_summaries: list[dict] = []
    all_claim_rows: list[dict] = []
    all_warnings: list[dict] = []

    print("\nFetching /game payloads...")
    for ix, row in enumerate(selected_games, 1):
        game_id = str(row.get("game_id") or row.get("gameID") or "").strip()
        if not game_id:
            continue

        result = fetch_game_payload(
            base_url=args.base_url,
            game_id=game_id,
            timeout=args.timeout,
            auth_token=auth_token,
        )
        fetch_results.append(result)

        status_text = "OK" if result.ok else "FAIL"
        print(f"[{ix:03d}/{len(selected_games):03d}] {status_text} {game_id} ({result.elapsed_seconds}s)")

        if result.ok and result.payload:
            payload_path = payload_dir / f"{safe_filename(game_id)}.json"
            write_json(payload_path, result.payload)

            summary = summarize_payload(result.payload, elapsed_seconds=result.elapsed_seconds)
            payload_summaries.append(summary)

            claim_rows = extract_claim_language_rows(result.payload)
            all_claim_rows.extend(claim_rows)

            warnings = build_alignment_warnings(result.payload)
            all_warnings.extend(warnings)

        if args.sleep:
            time.sleep(args.sleep)

    run_summary = build_run_summary(
        selected_games=selected_games,
        fetch_results=fetch_results,
        payload_summaries=payload_summaries,
        claim_rows=all_claim_rows,
        warnings=all_warnings,
        args=args,
    )

    write_json(output_dir / "run_summary.json", run_summary)

    write_csv(
        output_dir / "payload_summary.csv",
        payload_summaries,
        preferred_columns=[
            "game_id",
            "season",
            "game_week",
            "game_date",
            "away_team",
            "home_team",
            "model_result",
            "predicted_team",
            "actual_winner",
            "matchup_confidence",
            "outcome_confidence",
            "profile_strength",
            "matchup_label",
            "away_two_way_context",
            "home_two_way_context",
            "boosted_language_row_count",
            "team_comparison_unclassified_count",
            "warning_count",
            "critical_warning_count",
            "high_warning_count",
            "ranking_available",
            "ranking_max_data_lag_days",
            "elapsed_seconds",
        ],
    )

    write_csv(
        output_dir / "claim_language_rows.csv",
        all_claim_rows,
        preferred_columns=[
            "game_id",
            "section",
            "claim_type",
            "claim_layer",
            "parent_name",
            "metric",
            "label",
            "category",
            "core_area",
            "leader",
            "leader_team",
            "summary_label",
            "percentile_gap",
            "language_boost_allowed",
            "support_level",
            "rule_status",
            "language_modifier",
            "reason",
            "claim_strength_bucket",
            "claim_strength_context",
            "claim_strength_language_signal",
            "two_way_context",
            "two_way_edge_score",
            "model_result",
            "predicted_team",
            "actual_winner",
        ],
    )

    write_csv(
        output_dir / "alignment_warnings.csv",
        all_warnings,
        preferred_columns=[
            "game_id",
            "season",
            "game_week",
            "game_date",
            "away_team",
            "home_team",
            "warning_code",
            "severity",
            "message",
            "metric",
            "section",
            "core_area",
            "confidence",
            "outcome_confidence",
            "count",
        ],
    )

    write_json(
        output_dir / "fetch_errors.json",
        [
            {
                "game_id": r.game_id,
                "ok": r.ok,
                "status_code": r.status_code,
                "elapsed_seconds": r.elapsed_seconds,
                "error": r.error,
            }
            for r in fetch_results
            if not r.ok
        ],
    )

    write_analysis_prompt(
        path=output_dir / "analysis_prompt.md",
        run_name=run_name,
        run_summary=run_summary,
    )

    print("\nDone.")
    print(f"Output directory: {output_dir}")
    print(f"Successful payloads: {run_summary['successful_payloads']}")
    print(f"Failed payloads:     {run_summary['failed_payloads']}")
    print(f"Warnings:            {len(all_warnings)}")
    print("\nNext files to inspect:")
    print(f"  {output_dir / 'run_summary.json'}")
    print(f"  {output_dir / 'payload_summary.csv'}")
    print(f"  {output_dir / 'alignment_warnings.csv'}")
    print(f"  {output_dir / 'analysis_prompt.md'}")

    return 0 if run_summary["failed_payloads"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
