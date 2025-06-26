from google.cloud import bigquery
import pandas as pd
import requests
from datetime import datetime

def fetch_nfl_scores(load_date=None):
    # Step 1: Determine game date using pandas
    if load_date is None:
        game_date = pd.Timestamp.now(tz="America/New_York").strftime("%Y-%m-%d")
    else:
        game_date = load_date  # Must be in "YYYY-MM-DD" format

    print(f"Fetching data for: {game_date}")

    # Step 2: Fetch games for the date
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLScoresOnly"
    params = {"dates": game_date.replace("-", "")}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return

    game_urls = response.json().get("items", [])
    print(f"Found {len(game_urls)} game URLs")

    # Step 3: Expand and transform all games
    rows = []
    for game_url in game_urls:
        try:
            game = requests.get(game_url).json()
            competition = game.get("competitions", [{}])[0]

            # Epoch time for game start
            epoch = competition.get("date")
            if epoch:
                dt = pd.to_datetime(epoch).tz_convert("America/New_York")
                game_datetime_est = dt.strftime("%Y-%m-%d %H:%M")
                game_date_est = dt.strftime("%Y-%m-%d")
            else:
                game_datetime_est = None
                game_date_est = game_date

            # Line score breakdown
            linescore = competition.get("status", {}).get("type", {}).get("description", "")
            competitors = competition.get("competitors", [])
            score_dict = {c["team"]["id"]: c for c in competitors}

            # Try to fetch linescore from nested boxscore endpoint
            try:
                box_url = competition.get("boxscore", {}).get("$ref")
                box = requests.get(box_url).json()
                line = box.get("linescores", [{}])[0]
                home = line.get("home", {})
                away = line.get("away", {})
            except:
                home = away = {}

            # Process each team (home and away)
            for side, data in zip(["home", "away"], [home, away]):
                teamID = data.get("teamID")
                row = {
                    "gameID": game.get("id"),
                    "gameTime_epoch": None,  # Keeping for schema completeness
                    "teamID": teamID,
                    "teamAbv": data.get("teamAbv"),
                    "team_type": side,
                    "Q1": pd.to_numeric(data.get("Q1", 0), errors="coerce"),
                    "Q2": pd.to_numeric(data.get("Q2", 0), errors="coerce"),
                    "Q3": pd.to_numeric(data.get("Q3", 0), errors="coerce"),
                    "Q4": pd.to_numeric(data.get("Q4", 0), errors="coerce"),
                    "OT": pd.to_numeric(data.get("OT", 0), errors="coerce"),
                    "homePts": pd.to_numeric(competition.get("competitors", [])[0].get("score", 0), errors="coerce"),
                    "awayPts": pd.to_numeric(competition.get("competitors", [])[1].get("score", 0), errors="coerce"),
                    "game_date_est": game_date_est,
                    "game_datetime_est": game_datetime_est,
                }
                rows.append(row)

        except Exception as e:
            print(f"❌ Failed to process game: {e}")
            continue

    df = pd.DataFrame(rows)

    # Step 4: Validate row count
    expected = len(game_urls) * 2
    actual = len(df)
    if actual != expected:
        print(f"⚠️ Expected {expected} rows (2 per game), but got {actual}. Skipping insert.")
        return

    # Step 5: Insert into BigQuery
    client = bigquery.Client()
    table_id = "your-project-id.your_dataset.your_table_name"  # Update accordingly
    errors = client.insert_rows_json(table_id, df.to_dict(orient="records"))
    if errors:
        print(f"❌ BigQuery insert errors: {errors}")
    else:
        print(f"✅ Inserted {len(df)} rows into {table_id}")
