import requests
import json
from datetime import date

from utils.helper import get_secret
from utils.response_helpers import save_raw_response
from utils.logging_setup import log_event


def is_injury_relevant(p: dict) -> bool:
    """
    Returns True only when the injury block contains meaningful info.
    This intentionally ignores injDate-only records (too noisy).
    """
    inj = p.get("injury") or {}
    designation = (inj.get("designation") or "").strip()
    description = (inj.get("description") or "").strip()
    ret = (inj.get("injReturnDate") or "").strip()
    return bool(designation or description or ret)


def fetch_nfl_injuries(load_date=None):
    """
    Fetch raw NFL player list payload (contains injury objects).
    Saves:
      1) full raw payload to GCS
      2) injured-only (non-free-agent) list to GCS
    Returns parsed JSON payload (dict).
    """
    if load_date is None:
        load_date = date.today().isoformat()

    api_url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLPlayerList"
    headers = {
        "x-rapidapi-key": get_secret("Tank_Rapidapi"),
        "x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com",
    }

    log_event("injuries_fetch_start", {"load_date": load_date})

    resp = requests.get(api_url, headers=headers, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    # 1) Save full raw payload for debugging/backfills
    save_raw_response(payload, load_date, prefix="raw_injuries_players")

    # 2) Extract injured-only players
    players = payload.get("body", [])
    injured = [p for p in players if is_injury_relevant(p)]
    injured_non_free_agents = [
        p for p in injured if str(p.get("isFreeAgent")).lower() != "true"
    ]

    # Save injured-only subset (much smaller + easier to browse)
    save_raw_response(injured_non_free_agents, load_date, prefix="injured_players_only")

    # Print counts + a small sample
    print("Top-level keys:", list(payload.keys())[:25])
    print("total players:", len(players))
    print("injured (relevant):", len(injured))
    print("injured (non-free agents):", len(injured_non_free_agents))

    for p in injured_non_free_agents[:10]:
        inj = p.get("injury") or {}
        print(
            p.get("team"),
            p.get("pos"),
            p.get("longName"),
            "|",
            inj.get("designation"),
            "|",
            (inj.get("injReturnDate") or ""),
        )

    log_event("injuries_fetch_success", {"status_code": resp.status_code})
    return payload


if __name__ == "__main__":
    fetch_nfl_injuries()
