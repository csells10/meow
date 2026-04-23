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
    Fetch team aggregate metrics for the away/home teams from the
    season-specific aggregate table.
    """
    header = get_game_header(game_id)
    if not header:
        return {}, {}

    away_team_id = header["away_team"]["id"]
    home_team_id = header["home_team"]["id"]
    season = str(header["season"])[:4]

    table = f"nfl-stream-406420.Analytics.team_metrics_season_{season}"

    query = f"""
        SELECT
            team_id,
            metric,
            value
        FROM `{table}`
        WHERE team_id IN UNNEST(@team_ids)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "team_ids",
                "STRING",
                [away_team_id, home_team_id]
            )
        ]
    )

    rows = [dict(row) for row in client.query(query, job_config=job_config).result()]

    if not rows:
        return {}, {}

    away_metrics = {}
    home_metrics = {}

    for row in rows:
        team_id = row.get("team_id")
        metric = row.get("metric")
        value = row.get("value")

        if team_id == away_team_id:
            away_metrics[metric] = value
        elif team_id == home_team_id:
            home_metrics[metric] = value

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