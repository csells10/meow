from google.cloud import bigquery

PROJECT_ID = "nfl-stream-406420"

TABLES = {
    "schedule": f"{PROJECT_ID}.League.schedule",
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