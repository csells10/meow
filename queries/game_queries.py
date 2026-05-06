from google.cloud import bigquery

client = bigquery.Client()


def use_windowed_metrics_for_game() -> bool:
    """
    GameLens 1.5 source decision.

    Windowed metrics are now the default /game source.
    This helper remains for compatibility with any old checks, but it no longer
    reads USE_WINDOWED_METRICS_FOR_GAME from the environment.
    """

    return True


def select_window_type(header: dict) -> str:
    game_week = str(header.get("game_week") or "").strip().lower()

    if game_week.startswith("preseason"):
        return "preseason_to_date"

    if game_week in {
        "divisional round",
        "conference championship",
        "super bowl",
    }:
        return "regular_plus_postseason_to_date"

    # Regular season and Wild Card should default here.
    return "regular_season_to_date"


def metric_value(metrics: dict, key: str):
    """
    Safely extract the numeric value from a metric payload.

    Supports:
    - new shape: {"value": 0.308, "metric": "...", "core_area": "..."}
    - old shape: 0.308
    """

    payload = metrics.get(key)

    if isinstance(payload, dict):
        return payload.get("value")

    return payload


def get_game_header(game_id: str) -> dict:
    """
    Fetch core game header data from the schedule table,
    including away/home team logo URLs.
    """

    query = """
        SELECT
            s.gameID,
            s.gameDate,
            s.gameTime,
            s.gameStatus,
            s.season,
            s.gameWeek,
            s.seasonType,
            s.teamIDAway,
            s.away,
            away_logo.logoURL AS away_logo,
            s.teamIDHome,
            s.home,
            home_logo.logoURL AS home_logo,
            s.espnLink
        FROM `nfl-stream-406420.League.schedule` s
        LEFT JOIN `nfl-stream-406420.Teams.team_logos` away_logo
            ON s.teamIDAway = away_logo.teamID
        LEFT JOIN `nfl-stream-406420.Teams.team_logos` home_logo
            ON s.teamIDHome = home_logo.teamID
        WHERE s.gameID = @game_id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("game_id", "STRING", game_id)
        ]
    )

    rows = list(client.query(query, job_config=job_config).result())

    if not rows:
        return {}

    row = dict(rows[0])

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


def get_team_metrics(game_id: str):
    """
    Fetch pregame-safe windowed team metrics for the away/home teams.

    GameLens 1.5 source decision:
    - Always use Analytics.team_metrics_windowed_{season}
    - Always apply the phase-aware window_type from select_window_type(header)
    - Always use data_date < game_date for pregame safety

    The legacy Analytics.team_metrics_season_{season} path is no longer used
    by /game.
    """

    header = get_game_header(game_id)

    if not header:
        return {}, {}

    away_team_id = header["away_team"]["id"]
    home_team_id = header["home_team"]["id"]
    game_date = header["game_date"]
    season = str(header["season"])[:4]
    window_type = select_window_type(header)

    table = f"nfl-stream-406420.Analytics.team_metrics_windowed_{season}"

    query = f"""
        WITH base AS (
            SELECT
                team_id,
                team_abv,
                metric,
                category,
                core_area,
                value,
                data_date
            FROM `{table}`
            WHERE team_id IN UNNEST(@team_ids)
              AND CAST(season AS STRING) = @season
              AND window_type = @window_type
              AND data_date < @game_date
        ),

        latest AS (
            SELECT
                team_id,
                team_abv,
                metric,
                category,
                core_area,
                value,
                data_date,
                ROW_NUMBER() OVER (
                    PARTITION BY team_id, category, metric
                    ORDER BY data_date DESC
                ) AS rn
            FROM base
        )

        SELECT
            team_id,
            team_abv,
            metric,
            category,
            core_area,
            value,
            data_date
        FROM latest
        WHERE rn = 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "team_ids",
                "STRING",
                [away_team_id, home_team_id],
            ),
            bigquery.ScalarQueryParameter("season", "STRING", season),
            bigquery.ScalarQueryParameter("window_type", "STRING", window_type),
            bigquery.ScalarQueryParameter("game_date", "DATE", game_date),
        ]
    )

    rows = [dict(row) for row in client.query(query, job_config=job_config).result()]

    away_metrics = {}
    home_metrics = {}

    for row in rows:
        category = row.get("category")
        metric = row.get("metric")
        team_id = row.get("team_id")

        if not category or not metric or not team_id:
            continue

        metric_key = f"{category}::{metric}"

        metric_payload = {
            "value": row.get("value"),
            "metric": metric,
            "category": category,
            "core_area": row.get("core_area"),
            "data_date": str(row.get("data_date")) if row.get("data_date") else None,
            "team_id": team_id,
            "team_abv": row.get("team_abv"),
            "window_type": window_type,
            "source_table": f"Analytics.team_metrics_windowed_{season}",
        }

        if str(team_id) == str(away_team_id):
            away_metrics[metric_key] = metric_payload
        elif str(team_id) == str(home_team_id):
            home_metrics[metric_key] = metric_payload

    return away_metrics, home_metrics


def get_game_profile(game_id: str):
    """
    Build game_profile using team metrics.

    Note:
    The main API currently builds game_profile inside services/game_service.py.
    This function is kept query-side compatible but should not be the primary
    product logic location.
    """

    header = get_game_header(game_id)
    away_metrics, home_metrics = get_team_metrics(game_id)

    if not header or not away_metrics or not home_metrics:
        return []

    home_abbr = header["home_team"]["abbreviation"]
    away_abbr = header["away_team"]["abbreviation"]

    def level_from_diff(diff: float):
        abs_diff = abs(diff)

        if abs_diff < 0.05:
            return "Low", 0
        if abs_diff < 0.10:
            return "Moderate", 1
        if abs_diff < 0.20:
            return "Elevated", 2

        return "High", 3

    def pick_tilt(home_val, away_val):
        if home_val is None or away_val is None:
            return "", None

        diff = home_val - away_val

        if abs(diff) < 0.01:
            return "Even matchup", "neutral"

        if diff > 0:
            return f"{home_abbr} edge", "home"

        return f"{away_abbr} edge", "away"

    def icon_for(category):
        return {
            "Pressure": "alert-triangle",
            "Explosiveness": "zap",
            "Turnover Risk": "target",
            "Defensive Strength": "shield",
        }.get(category, "activity")

    profile = []

    # Pressure
    home_val = metric_value(home_metrics, "Defense::sacks")
    away_val = metric_value(away_metrics, "Defense::sacks")

    if home_val is not None and away_val is not None:
        diff = home_val - away_val
        level, level_index = level_from_diff(diff)
        tilt_text, tilt_team = pick_tilt(home_val, away_val)

        profile.append({
            "category": "Pressure",
            "level": level,
            "tilt": tilt_text,
            "level_index": level_index,
            "icon": icon_for("Pressure"),
            "tilt_team": tilt_team,
            "tilt_text": tilt_text,
        })

    # Explosiveness
    home_val = metric_value(home_metrics, "Offense::yards_per_play")
    away_val = metric_value(away_metrics, "Offense::yards_per_play")

    if home_val is not None and away_val is not None:
        diff = home_val - away_val
        level, level_index = level_from_diff(diff)
        tilt_text, tilt_team = pick_tilt(home_val, away_val)

        profile.append({
            "category": "Explosiveness",
            "level": level,
            "tilt": tilt_text,
            "level_index": level_index,
            "icon": icon_for("Explosiveness"),
            "tilt_team": tilt_team,
            "tilt_text": tilt_text,
        })

    # Turnover Risk
    home_val = metric_value(home_metrics, "Defense::turnover_margin_per_game")
    away_val = metric_value(away_metrics, "Defense::turnover_margin_per_game")

    if home_val is not None and away_val is not None:
        diff = home_val - away_val
        level, level_index = level_from_diff(diff)
        tilt_text, tilt_team = pick_tilt(home_val, away_val)

        profile.append({
            "category": "Turnover Risk",
            "level": level,
            "tilt": tilt_text,
            "level_index": level_index,
            "icon": icon_for("Turnover Risk"),
            "tilt_team": tilt_team,
            "tilt_text": tilt_text,
        })

    return profile


def get_final_score(game_id: str):
    """
    Fetch quarter-by-quarter score from Scores.scores.
    """

    query = """
        SELECT
            team_type,
            Q1,
            Q2,
            Q3,
            Q4,
            OT,
            homePts,
            awayPts
        FROM `nfl-stream-406420.Scores.scores`
        WHERE gameID = @game_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("game_id", "STRING", game_id)
        ]
    )

    rows = [dict(row) for row in client.query(query, job_config=job_config).result()]

    if not rows or len(rows) < 2:
        return None

    away_row = next((row for row in rows if row.get("team_type") == "away"), None)
    home_row = next((row for row in rows if row.get("team_type") == "home"), None)

    if not away_row or not home_row:
        return None

    return {
        "away": {
            "q1": away_row.get("Q1", 0),
            "q2": away_row.get("Q2", 0),
            "q3": away_row.get("Q3", 0),
            "q4": away_row.get("Q4", 0),
            "ot": away_row.get("OT", 0),
            "total": away_row.get("awayPts", 0),
        },
        "home": {
            "q1": home_row.get("Q1", 0),
            "q2": home_row.get("Q2", 0),
            "q3": home_row.get("Q3", 0),
            "q4": home_row.get("Q4", 0),
            "ot": home_row.get("OT", 0),
            "total": home_row.get("homePts", 0),
        },
    }
def get_team_rankings_for_game(
    game_id: str,
    window_type=None,
    metrics=None,
):
    """
    Fetch pregame-safe as-of ranking rows for the away/home teams in a game.

    Ranking source:
        Analytics.team_metric_rankings_{season}

    Pregame safety:
        Uses the latest ranking as_of_date strictly before the target game_date.

    Window behavior:
        If window_type is not provided, select_window_type(header) is used so
        rankings follow the same phase-aware logic as windowed metrics.

    Returns:
        away_rankings, home_rankings, ranking_meta
    """

    header = get_game_header(game_id)

    if not header:
        return {}, {}, {
            "available": False,
            "reason": "missing_game_header",
            "game_id": game_id,
        }

    if window_type is None:
        window_type = select_window_type(header)

    away_team_id = str(header["away_team"]["id"])
    home_team_id = str(header["home_team"]["id"])
    game_date = header["game_date"]
    season = str(header["season"])[:4]

    table = f"nfl-stream-406420.Analytics.team_metric_rankings_{season}"

    metric_filter_sql = ""
    query_params = [
        bigquery.ArrayQueryParameter(
            "team_ids",
            "STRING",
            [away_team_id, home_team_id],
        ),
        bigquery.ScalarQueryParameter("game_date", "DATE", game_date),
        bigquery.ScalarQueryParameter("window_type", "STRING", window_type),
    ]

    if metrics:
        metric_filter_sql = "AND r.metric IN UNNEST(@metrics)"
        query_params.append(
            bigquery.ArrayQueryParameter("metrics", "STRING", metrics)
        )

    query = f"""
        SELECT
            r.season,
            r.as_of_date,
            r.source_data_date,
            r.data_lag_days,
            r.window_type,

            r.team_id,
            r.team_abv,

            r.metric,
            r.value,
            r.label,
            r.definition,
            r.category,
            r.core_area,
            r.comparison_direction,
            r.higher_is_better,
            r.raw_or_derived,
            r.aggregation_method,
            r.numerator,
            r.denominator,
            r.format,
            r.decimals,
            r.notes,

            r.ranking_usage,
            r.signal_strength,
            r.edge_language_allowed,
            r.include_in_core_area_advantage,
            r.confidence_eligible,
            r.data_quality_status,
            r.lens_tags,

            r.league_rank,
            r.league_percentile,
            r.tier,
            r.tier_label,
            r.teams_ranked,
            r.ranking_kind,
            r.rank_direction,
            r.rank_interpretation,
            r.rank_tie_method
        FROM `{table}` r
        WHERE r.as_of_date = (
            SELECT MAX(as_of_date)
            FROM `{table}`
            WHERE as_of_date < @game_date
              AND window_type = @window_type
        )
          AND r.window_type = @window_type
          AND r.team_id IN UNNEST(@team_ids)
          {metric_filter_sql}
        ORDER BY
            r.core_area,
            r.category,
            r.metric,
            r.team_id
    """

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)

    rows = [dict(row) for row in client.query(query, job_config=job_config).result()]

    if not rows:
        return {}, {}, {
            "available": False,
            "reason": "no_ranking_rows_found",
            "game_id": game_id,
            "game_date": game_date,
            "season": season,
            "window_type": window_type,
        }

    away_rankings = {}
    home_rankings = {}

    as_of_dates = set()
    source_data_dates = set()
    data_lag_days = []

    for row in rows:
        metric = row.get("metric")
        team_id = str(row.get("team_id"))

        if not metric or not team_id:
            continue

        if row.get("as_of_date"):
            as_of_dates.add(str(row.get("as_of_date")))

        if row.get("source_data_date"):
            source_data_dates.add(str(row.get("source_data_date")))

        if row.get("data_lag_days") is not None:
            try:
                data_lag_days.append(int(row.get("data_lag_days")))
            except (TypeError, ValueError):
                pass

        payload = {
            "season": row.get("season"),
            "as_of_date": str(row.get("as_of_date")) if row.get("as_of_date") else None,
            "source_data_date": (
                str(row.get("source_data_date"))
                if row.get("source_data_date")
                else None
            ),
            "data_lag_days": row.get("data_lag_days"),
            "window_type": row.get("window_type"),

            "team_id": row.get("team_id"),
            "team_abv": row.get("team_abv"),

            "metric": metric,
            "value": row.get("value"),
            "label": row.get("label"),
            "definition": row.get("definition"),
            "category": row.get("category"),
            "core_area": row.get("core_area"),
            "comparison_direction": row.get("comparison_direction"),
            "higher_is_better": row.get("higher_is_better"),
            "raw_or_derived": row.get("raw_or_derived"),
            "aggregation_method": row.get("aggregation_method"),
            "numerator": row.get("numerator"),
            "denominator": row.get("denominator"),
            "format": row.get("format"),
            "decimals": row.get("decimals"),
            "notes": row.get("notes"),

            "ranking_usage": row.get("ranking_usage"),
            "signal_strength": row.get("signal_strength"),
            "edge_language_allowed": row.get("edge_language_allowed"),
            "include_in_core_area_advantage": row.get("include_in_core_area_advantage"),
            "confidence_eligible": row.get("confidence_eligible"),
            "data_quality_status": row.get("data_quality_status"),
            "lens_tags": row.get("lens_tags") or [],

            "league_rank": row.get("league_rank"),
            "league_percentile": row.get("league_percentile"),
            "tier": row.get("tier"),
            "tier_label": row.get("tier_label"),
            "teams_ranked": row.get("teams_ranked"),
            "ranking_kind": row.get("ranking_kind"),
            "rank_direction": row.get("rank_direction"),
            "rank_interpretation": row.get("rank_interpretation"),
            "rank_tie_method": row.get("rank_tie_method"),
        }

        if team_id == away_team_id:
            away_rankings[metric] = payload
        elif team_id == home_team_id:
            home_rankings[metric] = payload

    ranking_meta = {
        "available": bool(away_rankings or home_rankings),
        "game_id": game_id,
        "game_date": game_date,
        "season": season,
        "window_type": window_type,
        "as_of_date": sorted(as_of_dates)[-1] if as_of_dates else None,
        "source_data_dates": sorted(source_data_dates),
        "max_data_lag_days": max(data_lag_days) if data_lag_days else None,
        "away_team_id": away_team_id,
        "home_team_id": home_team_id,
        "away_metric_count": len(away_rankings),
        "home_metric_count": len(home_rankings),
    }

    return away_rankings, home_rankings, ranking_meta