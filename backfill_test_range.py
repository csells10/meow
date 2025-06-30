import time
from datetime import date, timedelta
from api_calls.api_call_nfl_games import fetch_nfl_games

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
START_DATE = date(2024, 9, 4)                          # Adjust as needed
NUM_DAYS = 7                                           # How many days to backfill
TABLE_ID = "nfl-stream-406420.League.schedule_dev"     # Target table (dev or prod)
SLEEP_SECONDS = 2                                      # Delay between requests

# ─────────────────────────────────────────────────────────────
def run_backfill(start_date: date, num_days: int, table_id: str):
    for i in range(num_days):
        current_date = start_date + timedelta(days=i)
        percent_complete = round(((i + 1) / num_days) * 100, 1)
        days_remaining = num_days - (i + 1)

        print(f"\n📅 Processing {current_date.isoformat()} "
              f"({percent_complete}% complete, {days_remaining} days left)")
        
        try:
            fetch_nfl_games(load_date=current_date.isoformat())
        except Exception as e:
            print(f"❌ Error processing {current_date}: {e}")
        
        time.sleep(SLEEP_SECONDS)

# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_backfill(START_DATE, NUM_DAYS, TABLE_ID)
