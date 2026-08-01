from google.cloud import bigquery
from runtime_config import load_runtime_config

RUNTIME_CONFIG = load_runtime_config()
PROJECT_ID = RUNTIME_CONFIG.project_id

TABLES = {
    "schedule": RUNTIME_CONFIG.league_table("schedule"),
}

def get_bigquery_client():
    return bigquery.Client(project=PROJECT_ID)

def fetch_games_by_date(parsed_date):
    client = get_bigquery_client()

    query = f"""
        SELECT
            gameID,
            away,
            home,
            gameTime,
            gameStatus
        FROM `{TABLES["schedule"]}`
        WHERE gameDate = @date
        ORDER BY gameTime
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("date", "DATE", parsed_date)
        ]
    )

    results = client.query(query, job_config=job_config).result()

    games = []

    for row in results:
        games.append({
            "gameID": row["gameID"],
            "away": row["away"],
            "home": row["home"],
            "gameTime": row["gameTime"],
            "gameStatus": row["gameStatus"]
        })

    return games
