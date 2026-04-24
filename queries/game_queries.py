from google.cloud import bigquery

client = bigquery.Client()


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
            "logo": row.get("away_logo")
        },
        "home_team": {
            "id": row.get("teamIDHome"),
            "name": row.get("home"),
            "abbreviation": row.get("home"),
            "logo": row.get("home_logo")
        },
        "espn_link": row.get("espnLink")
    }


def get_team_metrics(game_id: str):
    """
    Fetch latest available team aggregate metrics for the away/home teams
    before the game date.

    Also creates a normalized turnover_margin_per_game value from the
    cumulative Defense::turnover_margin metric.
    """
    header = get_game_header(game_id)

    if not header:
        return {}, {}

    away_team_id = header["away_team"]["id"]
    home_team_id = header["home_team"]["id"]
    game_date = header["game_date"]
    season = str(header["season"])[:4]

    table = f"nfl-stream-406420.Analytics.team_metrics_season_{season}"

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
        ),

        turnover_pg AS (
            SELECT
                team_id,
                ANY_VALUE(team_abv) AS team_abv,
                'turnover_margin_per_game' AS metric,
                'Defense' AS category,
                'defensive_control' AS core_area,
                SAFE_DIVIDE(MAX(value), COUNT(DISTINCT data_date)) AS value,
                MAX(data_date) AS data_date
            FROM base
            WHERE metric = 'turnover_margin'
              AND category = 'Defense'
            GROUP BY team_id
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

        UNION ALL

        SELECT
            team_id,
            team_abv,
            metric,
            category,
            core_area,
            value,
            data_date
        FROM turnover_pg
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "team_ids",
                "STRING",
                [away_team_id, home_team_id]
            ),
            bigquery.ScalarQueryParameter("game_date", "DATE", game_date),
        ]
    )

    rows = [dict(row) for row in client.query(query, job_config=job_config).result()]

    away_metrics = {}
    home_metrics = {}

    for row in rows:
        metric_key = f"{row.get('category')}::{row.get('metric')}"
        team_id = row.get("team_id")
        value = row.get("value")

        if team_id == away_team_id:
            away_metrics[metric_key] = value
        elif team_id == home_team_id:
            home_metrics[metric_key] = value

    return away_metrics, home_metrics


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
            "total": away_row.get("awayPts", 0)
        },
        "home": {
            "q1": home_row.get("Q1", 0),
            "q2": home_row.get("Q2", 0),
            "q3": home_row.get("Q3", 0),
            "q4": home_row.get("Q4", 0),
            "ot": home_row.get("OT", 0),
            "total": home_row.get("homePts", 0)
        }
    }