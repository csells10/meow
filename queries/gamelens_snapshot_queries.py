"""One-game, read-only evidence loading for Learning Lite LL-3."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Mapping

from google.cloud import bigquery

from runtime_config import PRODUCTION_DATASETS, RuntimeConfig
from services.game_service import GameDetailsEvidence


EVIDENCE_SOURCE_CONFIGURED = "configured"
EVIDENCE_SOURCE_PRODUCTION = "production"
ALLOWED_EVIDENCE_SOURCES = {
    EVIDENCE_SOURCE_CONFIGURED,
    EVIDENCE_SOURCE_PRODUCTION,
}

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


@dataclass(frozen=True)
class SnapshotGameEvidence:
    game_id: str
    scheduled_kickoff: datetime
    evidence: GameDetailsEvidence
    source_lineage: dict
    metric_source_date: Any = None
    ranking_as_of_date: Any = None


def _as_utc(value: Any) -> datetime:
    if not isinstance(value, datetime):
        value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _select_window_type(header: Mapping[str, Any]) -> str:
    season_type = " ".join(
        str(header.get("season_type") or "").strip().casefold().split()
    )
    game_week = " ".join(
        str(header.get("game_week") or "").strip().casefold().split()
    )
    if season_type == "preseason":
        return "preseason_to_date"
    if season_type == "postseason":
        if game_week in {"wild card", "wildcard", "wild card round"}:
            return "regular_season_to_date"
        return "regular_plus_postseason_to_date"
    if season_type == "regular season":
        return "regular_season_to_date"
    if game_week.startswith("preseason"):
        return "preseason_to_date"
    if game_week in {
        "divisional round",
        "conference championship",
        "super bowl",
    }:
        return "regular_plus_postseason_to_date"
    return "regular_season_to_date"


def _header_from_row(raw_row: Mapping[str, Any]) -> dict:
    row = dict(raw_row)
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


class BigQuerySingleGameEvidenceLoader:
    """Load one product-equivalent pregame evidence bundle without scores."""

    def __init__(
        self,
        *,
        client: bigquery.Client,
        runtime_config: RuntimeConfig,
        evidence_source: str,
    ):
        source = str(evidence_source or "").strip().casefold()
        if source not in ALLOWED_EVIDENCE_SOURCES:
            raise ValueError("evidence_source must be configured or production")
        if not runtime_config.is_dev:
            raise ValueError("LL-3 evidence loading is dev-only")
        self.client = client
        self.runtime_config = runtime_config
        self.evidence_source = source

    def load(self, game_id: str) -> SnapshotGameEvidence:
        schedule_row = self._fetch_schedule_row(game_id)
        if not schedule_row:
            raise ValueError(f"game_not_found:{game_id}")
        scheduled_kickoff = schedule_row.get("scheduled_kickoff")
        if not scheduled_kickoff:
            raise ValueError(f"scheduled_kickoff_missing:{game_id}")

        header = _header_from_row(schedule_row)
        window_type = _select_window_type(header)
        metric_rows = self._fetch_metric_rows(header, window_type)
        ranking_rows = self._fetch_ranking_rows(header, window_type)
        away_metrics, home_metrics = self._build_metrics(
            metric_rows,
            header=header,
            window_type=window_type,
        )
        away_rankings, home_rankings, ranking_context = self._build_rankings(
            ranking_rows,
            header=header,
            window_type=window_type,
            game_id=game_id,
        )

        metric_dates = [
            _as_date(row["data_date"])
            for row in metric_rows
            if row.get("data_date")
        ]
        evidence = GameDetailsEvidence(
            header=header,
            away_metrics=away_metrics,
            home_metrics=home_metrics,
            ranking_context=ranking_context,
            away_rankings=away_rankings,
            home_rankings=home_rankings,
            final_score=None,
        )
        lineage = self.source_lineage(header)
        lineage["window_type"] = window_type
        lineage["metric_row_count"] = len(metric_rows)
        lineage["ranking_row_count"] = len(ranking_rows)
        return SnapshotGameEvidence(
            game_id=game_id,
            scheduled_kickoff=_as_utc(scheduled_kickoff),
            evidence=evidence,
            source_lineage=lineage,
            metric_source_date=max(metric_dates) if metric_dates else None,
            ranking_as_of_date=ranking_context.get("as_of_date"),
        )

    def recheck_schedule(self, game_id: str) -> dict:
        row = self._fetch_schedule_row(game_id)
        if not row:
            return {}
        kickoff = row.get("scheduled_kickoff")
        return {
            "game_id": str(row.get("gameID") or ""),
            "scheduled_kickoff": _as_utc(kickoff) if kickoff else None,
            "header": _header_from_row(row),
        }

    def source_lineage(self, header: Mapping[str, Any]) -> dict:
        season = str(header.get("season") or "")[:4]
        return {
            "source_environment": self.evidence_source,
            "access_mode": "read_only",
            "schedule_table": self._league_table("schedule"),
            "metric_table": self._analytics_table(
                f"team_metrics_windowed_{season}"
            ),
            "ranking_table": self._analytics_table(
                f"team_metric_rankings_{season}"
            ),
            "team_logo_table": (
                f"{self.runtime_config.project_id}.Teams.team_logos"
            ),
        }

    def _league_table(self, table_name: str) -> str:
        dataset = (
            PRODUCTION_DATASETS["league"]
            if self.evidence_source == EVIDENCE_SOURCE_PRODUCTION
            else self.runtime_config.league_dataset
        )
        return f"{self.runtime_config.project_id}.{dataset}.{table_name}"

    def _analytics_table(self, table_name: str) -> str:
        dataset = (
            PRODUCTION_DATASETS["analytics"]
            if self.evidence_source == EVIDENCE_SOURCE_PRODUCTION
            else self.runtime_config.analytics_dataset
        )
        return f"{self.runtime_config.project_id}.{dataset}.{table_name}"

    def _fetch_schedule_row(self, game_id: str) -> dict:
        schedule = self._league_table("schedule")
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
        return dict(rows[0]) if rows else {}

    def _fetch_metric_rows(
        self,
        header: Mapping[str, Any],
        window_type: str,
    ) -> list[dict]:
        season = str(header["season"])[:4]
        table = self._analytics_table(f"team_metrics_windowed_{season}")
        query = f"""
            WITH base AS (
                SELECT
                    team_id, team_abv, metric, category, core_area,
                    value, data_date
                FROM `{table}`
                WHERE team_id IN UNNEST(@team_ids)
                  AND CAST(season AS STRING) = @season
                  AND window_type = @window_type
                  AND data_date < @game_date
            ),
            latest AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY team_id, category, metric
                    ORDER BY data_date DESC
                ) AS rn
                FROM base
            )
            SELECT
                team_id, team_abv, metric, category, core_area,
                value, data_date
            FROM latest
            WHERE rn = 1
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter(
                    "team_ids",
                    "STRING",
                    [
                        str(header["away_team"]["id"]),
                        str(header["home_team"]["id"]),
                    ],
                ),
                bigquery.ScalarQueryParameter("season", "STRING", season),
                bigquery.ScalarQueryParameter(
                    "window_type", "STRING", window_type
                ),
                bigquery.ScalarQueryParameter(
                    "game_date", "DATE", header["game_date"]
                ),
            ]
        )
        rows = self.client.query(query, job_config=config).result()
        return [dict(row) for row in rows]

    def _fetch_ranking_rows(
        self,
        header: Mapping[str, Any],
        window_type: str,
    ) -> list[dict]:
        season = str(header["season"])[:4]
        table = self._analytics_table(f"team_metric_rankings_{season}")
        query = f"""
            SELECT {RANKING_COLUMNS}
            FROM `{table}` r
            WHERE r.as_of_date = (
                SELECT MAX(as_of_date)
                FROM `{table}`
                WHERE as_of_date < @game_date
                  AND window_type = @window_type
            )
              AND r.window_type = @window_type
              AND r.team_id IN UNNEST(@team_ids)
            ORDER BY r.core_area, r.category, r.metric, r.team_id
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter(
                    "team_ids",
                    "STRING",
                    [
                        str(header["away_team"]["id"]),
                        str(header["home_team"]["id"]),
                    ],
                ),
                bigquery.ScalarQueryParameter(
                    "game_date", "DATE", header["game_date"]
                ),
                bigquery.ScalarQueryParameter(
                    "window_type", "STRING", window_type
                ),
            ]
        )
        rows = self.client.query(query, job_config=config).result()
        return [dict(row) for row in rows]

    def _build_metrics(
        self,
        rows: list[Mapping[str, Any]],
        *,
        header: Mapping[str, Any],
        window_type: str,
    ) -> tuple[dict, dict]:
        away_team_id = str(header["away_team"]["id"])
        home_team_id = str(header["home_team"]["id"])
        season = str(header["season"])[:4]
        source_table = (
            f"{self._analytics_dataset()}.team_metrics_windowed_{season}"
        )
        away_metrics = {}
        home_metrics = {}
        for raw_row in rows:
            row = dict(raw_row)
            category = row.get("category")
            metric = row.get("metric")
            team_id = str(row.get("team_id") or "")
            if not category or not metric or not team_id:
                continue
            metric_payload = {
                "value": row.get("value"),
                "metric": metric,
                "category": category,
                "core_area": row.get("core_area"),
                "data_date": (
                    str(row.get("data_date")) if row.get("data_date") else None
                ),
                "team_id": row.get("team_id"),
                "team_abv": row.get("team_abv"),
                "window_type": window_type,
                "source_table": source_table,
            }
            key = f"{category}::{metric}"
            if team_id == away_team_id:
                away_metrics[key] = metric_payload
            elif team_id == home_team_id:
                home_metrics[key] = metric_payload
        return away_metrics, home_metrics

    def _build_rankings(
        self,
        rows: list[Mapping[str, Any]],
        *,
        header: Mapping[str, Any],
        window_type: str,
        game_id: str,
    ) -> tuple[dict, dict, dict]:
        season = str(header["season"])[:4]
        if not rows:
            return {}, {}, {
                "available": False,
                "reason": "no_ranking_rows_found",
                "game_id": game_id,
                "game_date": header.get("game_date"),
                "season": season,
                "window_type": window_type,
            }

        away_team_id = str(header["away_team"]["id"])
        home_team_id = str(header["home_team"]["id"])
        away_rankings = {}
        home_rankings = {}
        as_of_dates = set()
        source_dates = set()
        lags = []
        for raw_row in rows:
            row = dict(raw_row)
            metric = row.get("metric")
            team_id = str(row.get("team_id") or "")
            if not metric or not team_id:
                continue
            if row.get("as_of_date"):
                as_of_dates.add(str(row["as_of_date"]))
            if row.get("source_data_date"):
                source_dates.add(str(row["source_data_date"]))
            if row.get("data_lag_days") is not None:
                lags.append(int(row["data_lag_days"]))
            payload = {
                key: (
                    str(row[key])
                    if key in {"as_of_date", "source_data_date"} and row.get(key)
                    else row.get(key)
                )
                for key in (
                    "season", "as_of_date", "source_data_date",
                    "data_lag_days", "window_type", "team_id", "team_abv",
                    "metric", "value", "label", "definition", "category",
                    "core_area", "comparison_direction", "higher_is_better",
                    "raw_or_derived", "aggregation_method", "numerator",
                    "denominator", "format", "decimals", "notes",
                    "ranking_usage", "signal_strength",
                    "edge_language_allowed", "include_in_core_area_advantage",
                    "confidence_eligible", "data_quality_status", "lens_tags",
                    "league_rank", "league_percentile", "tier", "tier_label",
                    "teams_ranked", "ranking_kind", "rank_direction",
                    "rank_interpretation", "rank_tie_method",
                )
            }
            payload["lens_tags"] = list(payload.get("lens_tags") or [])
            if team_id == away_team_id:
                away_rankings[metric] = payload
            elif team_id == home_team_id:
                home_rankings[metric] = payload

        context = {
            "available": bool(away_rankings or home_rankings),
            "game_id": game_id,
            "game_date": header.get("game_date"),
            "season": season,
            "window_type": window_type,
            "as_of_date": max(as_of_dates) if as_of_dates else None,
            "source_data_dates": sorted(source_dates),
            "max_data_lag_days": max(lags) if lags else None,
            "away_team_id": away_team_id,
            "home_team_id": home_team_id,
            "away_metric_count": len(away_rankings),
            "home_metric_count": len(home_rankings),
        }
        return away_rankings, home_rankings, context

    def _analytics_dataset(self) -> str:
        return (
            PRODUCTION_DATASETS["analytics"]
            if self.evidence_source == EVIDENCE_SOURCE_PRODUCTION
            else self.runtime_config.analytics_dataset
        )
