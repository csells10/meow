"""Batch-shaped, pregame-only evidence reads for Packet 2 shadow capture."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Dict, Iterable, List, Mapping, Sequence, Tuple

from google.cloud import bigquery

from queries.game_queries import (
    build_team_metrics_from_rows,
    build_team_rankings_from_rows,
    select_window_type,
)
from runtime_config import RuntimeConfig
from services.game_service import GameDetailsEvidence


RANKING_COLUMNS = """
    season, as_of_date, source_data_date, data_lag_days, window_type,
    team_id, team_abv, metric, value, label, definition, category,
    core_area, comparison_direction, higher_is_better, raw_or_derived,
    aggregation_method, numerator, denominator, format, decimals, notes,
    ranking_usage, signal_strength, edge_language_allowed,
    include_in_core_area_advantage, confidence_eligible,
    data_quality_status, lens_tags, league_rank, league_percentile, tier,
    tier_label, teams_ranked, ranking_kind, rank_direction,
    rank_interpretation, rank_tie_method
"""


def _as_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _as_utc(value) -> datetime:
    if not isinstance(value, datetime):
        value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _header_from_row(row: Mapping) -> dict:
    return {
        "game_id": row.get("gameID"),
        "game_date": str(row.get("gameDate")) if row.get("gameDate") else None,
        "game_time": row.get("gameTime"),
        "game_status": row.get("gameStatus"),
        "season": row.get("season"),
        "game_week": row.get("gameWeek"),
        "season_type": row.get("seasonType"),
        "away_team": {
            "id": row.get("teamIDAway"),
            "name": row.get("away"),
            "abbreviation": row.get("away"),
            "logo": row.get("away_logo"),
        },
        "home_team": {
            "id": row.get("teamIDHome"),
            "name": row.get("home"),
            "abbreviation": row.get("home"),
            "logo": row.get("home_logo"),
        },
        "espn_link": row.get("espnLink"),
    }


def _candidate_from_row(raw_row: Mapping) -> dict:
    row = dict(raw_row)
    kickoff = row.get("scheduled_kickoff")
    return {
        "game_id": str(row.get("gameID") or ""),
        "header": _header_from_row(row),
        "scheduled_kickoff": _as_utc(kickoff) if kickoff else None,
    }


class BigQuerySlateEvidenceLoader:
    """Load overlapping schedule, metric, and ranking evidence in slate batches."""

    def __init__(
        self,
        *,
        client: bigquery.Client,
        runtime_config: RuntimeConfig,
    ):
        self.client = client
        self.runtime_config = runtime_config

    def fetch_candidates(self, *, start_date: date, end_date: date) -> List[dict]:
        schedule = self.runtime_config.league_table("schedule")
        teams = f"{self.runtime_config.project_id}.Teams.team_logos"
        query = f"""
            SELECT
                s.gameID, s.gameDate, s.gameTime, s.gameTime_epoch,
                SAFE_CAST(s.gameTime_epoch AS TIMESTAMP) AS scheduled_kickoff,
                s.gameStatus, s.season, s.gameWeek, s.seasonType,
                s.teamIDAway, s.away, away_logo.logoURL AS away_logo,
                s.teamIDHome, s.home, home_logo.logoURL AS home_logo,
                s.espnLink
            FROM `{schedule}` s
            LEFT JOIN `{teams}` away_logo ON s.teamIDAway = away_logo.teamID
            LEFT JOIN `{teams}` home_logo ON s.teamIDHome = home_logo.teamID
            WHERE s.gameDate BETWEEN @start_date AND @end_date
            ORDER BY s.gameDate, scheduled_kickoff, s.gameID
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            ]
        )
        rows = self.client.query(query, job_config=config).result()
        return [_candidate_from_row(row) for row in rows]

    def recheck_candidate(self, game_id: str) -> dict:
        schedule = self.runtime_config.league_table("schedule")
        teams = f"{self.runtime_config.project_id}.Teams.team_logos"
        query = f"""
            SELECT
                s.gameID, s.gameDate, s.gameTime, s.gameTime_epoch,
                SAFE_CAST(s.gameTime_epoch AS TIMESTAMP) AS scheduled_kickoff,
                s.gameStatus, s.season, s.gameWeek, s.seasonType,
                s.teamIDAway, s.away, away_logo.logoURL AS away_logo,
                s.teamIDHome, s.home, home_logo.logoURL AS home_logo,
                s.espnLink
            FROM `{schedule}` s
            LEFT JOIN `{teams}` away_logo ON s.teamIDAway = away_logo.teamID
            LEFT JOIN `{teams}` home_logo ON s.teamIDHome = home_logo.teamID
            WHERE s.gameID = @game_id
            LIMIT 1
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("game_id", "STRING", game_id)
            ]
        )
        rows = list(self.client.query(query, job_config=config).result())
        return _candidate_from_row(rows[0]) if rows else {}

    def load_slate_evidence(
        self,
        candidates: Sequence[Mapping],
    ) -> Dict[str, GameDetailsEvidence]:
        grouped: Dict[Tuple[str, str], List[Mapping]] = defaultdict(list)
        for candidate in candidates:
            header = candidate["header"]
            grouped[(str(header["season"])[:4], select_window_type(header))].append(
                candidate
            )

        evidence_by_game: Dict[str, GameDetailsEvidence] = {}
        for (season, window_type), group in grouped.items():
            team_ids = sorted(
                {
                    str(candidate["header"][side]["id"])
                    for candidate in group
                    for side in ("away_team", "home_team")
                }
            )
            max_game_date = max(
                _as_date(candidate["header"]["game_date"])
                for candidate in group
            )
            metric_rows = self._fetch_metric_rows(
                season=season,
                window_type=window_type,
                team_ids=team_ids,
                max_game_date=max_game_date,
            )
            ranking_rows = self._fetch_ranking_rows(
                season=season,
                window_type=window_type,
                max_game_date=max_game_date,
            )

            for candidate in group:
                header = candidate["header"]
                game_id = candidate["game_id"]
                game_date = _as_date(header["game_date"])
                candidate_team_ids = {
                    str(header["away_team"]["id"]),
                    str(header["home_team"]["id"]),
                }
                selected_metrics = _latest_metric_rows(
                    metric_rows,
                    team_ids=candidate_team_ids,
                    game_date=game_date,
                )
                selected_rankings = _latest_ranking_rows(
                    ranking_rows,
                    team_ids=candidate_team_ids,
                    game_date=game_date,
                )
                away_metrics, home_metrics = build_team_metrics_from_rows(
                    selected_metrics,
                    header=header,
                    window_type=window_type,
                )
                away_rankings, home_rankings, ranking_context = (
                    build_team_rankings_from_rows(
                        selected_rankings,
                        header=header,
                        window_type=window_type,
                        game_id=game_id,
                    )
                )
                evidence_by_game[game_id] = GameDetailsEvidence(
                    header=header,
                    away_metrics=away_metrics,
                    home_metrics=home_metrics,
                    ranking_context=ranking_context,
                    away_rankings=away_rankings,
                    home_rankings=home_rankings,
                    final_score=None,
                )
        return evidence_by_game

    def _fetch_metric_rows(
        self,
        *,
        season: str,
        window_type: str,
        team_ids: Sequence[str],
        max_game_date: date,
    ) -> List[dict]:
        table = self.runtime_config.analytics_table(
            f"team_metrics_windowed_{season}"
        )
        query = f"""
            SELECT team_id, team_abv, metric, category, core_area, value, data_date
            FROM `{table}`
            WHERE team_id IN UNNEST(@team_ids)
              AND CAST(season AS STRING) = @season
              AND window_type = @window_type
              AND data_date < @max_game_date
            ORDER BY data_date, team_id, category, metric
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("team_ids", "STRING", list(team_ids)),
                bigquery.ScalarQueryParameter("season", "STRING", season),
                bigquery.ScalarQueryParameter("window_type", "STRING", window_type),
                bigquery.ScalarQueryParameter("max_game_date", "DATE", max_game_date),
            ]
        )
        return [dict(row) for row in self.client.query(query, job_config=config).result()]

    def _fetch_ranking_rows(
        self,
        *,
        season: str,
        window_type: str,
        max_game_date: date,
    ) -> List[dict]:
        table = self.runtime_config.analytics_table(
            f"team_metric_rankings_{season}"
        )
        query = f"""
            SELECT {RANKING_COLUMNS}
            FROM `{table}`
            WHERE window_type = @window_type
              AND as_of_date < @max_game_date
            ORDER BY as_of_date, core_area, category, metric, team_id
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("window_type", "STRING", window_type),
                bigquery.ScalarQueryParameter("max_game_date", "DATE", max_game_date),
            ]
        )
        return [dict(row) for row in self.client.query(query, job_config=config).result()]


def _latest_metric_rows(
    rows: Iterable[Mapping],
    *,
    team_ids: set,
    game_date: date,
) -> List[dict]:
    latest: Dict[Tuple[str, str, str], dict] = {}
    for raw_row in rows:
        row = dict(raw_row)
        if str(row.get("team_id")) not in team_ids:
            continue
        data_date = _as_date(row.get("data_date"))
        if data_date >= game_date:
            continue
        key = (str(row.get("team_id")), row.get("category"), row.get("metric"))
        current = latest.get(key)
        if current is None or _as_date(current["data_date"]) < data_date:
            latest[key] = row
    return list(latest.values())


def _latest_ranking_rows(
    rows: Iterable[Mapping],
    *,
    team_ids: set,
    game_date: date,
) -> List[dict]:
    # Match the live query exactly: it selects the latest league-wide as-of
    # date first, then filters the result to the matchup's teams.
    eligible = [
        dict(row)
        for row in rows
        if _as_date(row.get("as_of_date")) < game_date
    ]
    if not eligible:
        return []
    latest_date = max(_as_date(row["as_of_date"]) for row in eligible)
    return [
        row
        for row in eligible
        if _as_date(row["as_of_date"]) == latest_date
        and str(row.get("team_id")) in team_ids
    ]
