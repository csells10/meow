from datetime import datetime, timedelta
from utils.logging_setup import log_event
from utils.helper import (
    insert_into_bigquery,
    get_secret,
    fetch_and_validate_api_data,
    check_existing_records,
    filter_new_records,
    delete_yesterdays_games_from_bigquery
)
from utils.response_helpers import save_raw_response


def fetch_nfl_games(load_date=None):
    log_event("info", "nfl_games_job_started")

    # True = load_date provided manually AND NOT today
    is_historical_run = (
        load_date is not None
        and datetime.strptime(load_date, "%Y-%m-%d").date() != datetime.now().date()
    )

    api_key = get_secret("Tank_Rapidapi")
    url = (
        "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLGamesForDate"
    )
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com",
    }

    table_id = "nfl-stream-406420.League.schedule"
    if load_date:
        start_date = datetime.strptime(load_date, "%Y-%m-%d").date()
    else:
        start_date = datetime.now().date()

    # ────────────────────────────────────────────────────────────────
    # Step 1 – Prefetch yesterday (skip for historical backfill)
    # ────────────────────────────────────────────────────────────────
    if not is_historical_run:
        yesterday_date = (start_date + timedelta(days=-1)).strftime("%Y%m%d")
        yesterday_data = None
        try:
            log_event("info", "prefetch_yesterday_games", game_date=yesterday_date)
            querystring = {"gameDate": yesterday_date}
            response = fetch_and_validate_api_data(url, headers, querystring)
            if response and "body" in response:
                yesterday_data = response["body"]
                log_event("info", "yesterday_games_fetched", count=len(yesterday_data))
                save_raw_response(response, yesterday_date, prefix="nfl_games")
            else:
                log_event("warning", "yesterday_games_fetch_empty", game_date=yesterday_date)
        except Exception as e:
            log_event(
                "error",
                "yesterday_games_fetch_failed",
                game_date=yesterday_date,
                error=str(e),
            )

        # Step 2 – Delete yesterday (only if data fetched)
        if yesterday_data:
            log_event("info", "delete_yesterday_games")
            delete_yesterdays_games_from_bigquery(table_id)
        else:
            log_event(
                "warning",
                "yesterday_data_not_deleted_due_to_fetch_failure",
                game_date=yesterday_date,
            )

        # Step 3 – Insert yesterday
        if yesterday_data:
            game_ids = [game.get("gameID") for game in yesterday_data]
            existing_ids = check_existing_records(table_id, "gameID", game_ids)
            rows_to_insert = filter_new_records(existing_ids, yesterday_data, "gameID")
            
            # 👇 Add default flags before inserting
            for row in rows_to_insert:
                row["boxscore_loaded"] = False
                row["score_loaded"] = False

            if rows_to_insert:
                insert_into_bigquery(table_id, rows_to_insert)
                log_event(
                    "info",
                    "games_inserted",
                    game_date=yesterday_date,
                    inserted=len(rows_to_insert),
                )
            else:
                log_event("info", "no_new_games", game_date=yesterday_date)

    # ────────────────────────────────────────────────────────────────
    # Step 4 – Today + next 2 days (skip for historical backfill)
    # ────────────────────────────────────────────────────────────────
    if not is_historical_run:
        days_range = 4                  # yesterday handled above, so 0-2 == today+tomorrow+next
        any_valid_data = False
        failed_dates = []

        for day_offset in range(0, days_range - 1):
            game_date = (start_date + timedelta(days=day_offset)).strftime("%Y%m%d")
            querystring = {"gameDate": game_date}

            try:
                games = fetch_and_validate_api_data(url, headers, querystring)
                if not games or "body" not in games:
                    failed_dates.append(game_date)
                    continue

                game_body = games.get("body", [])
                log_event(
                    "info",
                    "games_fetched",
                    game_date=game_date,
                    count=len(game_body),
                )
                save_raw_response(games, game_date, prefix="nfl_games")

            except (ValueError, TypeError) as e:
                log_event("error", "games_fetch_failed", game_date=game_date, error=str(e))
                failed_dates.append(game_date)
                continue

            any_valid_data = True

            game_ids = [game.get("gameID") for game in game_body]
            existing_ids = check_existing_records(table_id, "gameID", game_ids)
            rows_to_insert = filter_new_records(existing_ids, game_body, "gameID")
            
            # 👇 Add default flags before inserting
            for row in rows_to_insert:
                row["boxscore_loaded"] = False
                row["score_loaded"] = False

            if rows_to_insert:
                insert_into_bigquery(table_id, rows_to_insert)
                log_event(
                    "info",
                    "games_inserted",
                    game_date=game_date,
                    inserted=len(rows_to_insert),
                )
            else:
                log_event("info", "no_new_games", game_date=game_date)

        # Summary log for live runs
        if not any_valid_data:
            log_event("warning", "no_valid_games_found", failed_dates=failed_dates)
        else:
            log_event("info", "Data inserted successfully!")

    # ────────────────────────────────────────────────────────────────
    log_event("info", "nfl_games_job_completed")
    # ─────────────────────────────────────────────────────────────
    # Step 5A: Historical Mode – Fetch and Log API Data
    # ─────────────────────────────────────────────────────────────
    if is_historical_run:
        game_date = start_date.strftime("%Y%m%d")
        querystring = {"gameDate": game_date}
        try:
            games = fetch_and_validate_api_data(url, headers, querystring)
            if not games or "body" not in games:
                log_event("warning", "no_games_found", game_date=game_date)
                print(f"⚠️  No games found for {game_date}")
                return

            game_body = games["body"]
            log_event("info", "games_fetched", game_date=game_date, count=len(game_body))
            save_raw_response(games, game_date, prefix="nfl_games")

            print(f"🧠 [Debug] {game_date} – API returned {len(game_body)} games")

            # ✅ Format gamedate and sanitize gametime_epoch
            for game in game_body:
                # Format gamedate (string) → DATE format 'YYYY-MM-DD'
                if "gamedate" in game:
                    game["gamedate"] = datetime.strptime(game["gamedate"], "%Y%m%d").date()

                # Ensure gametime_epoch is either a valid timestamp or None
                epoch = game.get("gametime_epoch")
                if not epoch or not str(epoch).strip():
                    game["gametime_epoch"] = None

            # ─────────────────────────────────────────────────────────────
            # Step 5B: Deduplication and Insert to BigQuery
            # ─────────────────────────────────────────────────────────────
            game_ids = [game.get("gameID") for game in game_body]
            existing_ids = check_existing_records(table_id, "gameID", game_ids)
            rows_to_insert = filter_new_records(existing_ids, game_body, "gameID")

            print(f"🧮 [Debug] {game_date} – Found {len(existing_ids)} existing gameIDs")
            print(f"📤 [Debug] {game_date} – {len(rows_to_insert)} rows remaining after deduping")
            if rows_to_insert:
                print(f"📦 [Debug] Sample row:\n{rows_to_insert[0]}")

            if rows_to_insert:
                try:
                    insert_into_bigquery(table_id, rows_to_insert)
                    log_event("info", "games_inserted", game_date=game_date, inserted=len(rows_to_insert))
                    print(f"✅ Inserted {len(rows_to_insert)} games for {game_date}")
                except Exception as e:
                    log_event("error", "bigquery_insert_failed", game_date=game_date, error=str(e))
                    print(f"❌ BigQuery insert failed for {game_date}: {e}")
            else:
                log_event("info", "no_new_games", game_date=game_date)
                print(f"🟡 No new games inserted for {game_date}")

        except Exception as e:
            log_event("error", "games_fetch_failed", game_date=game_date, error=str(e))
            print(f"❌ API fetch failed for {game_date}: {e}")


