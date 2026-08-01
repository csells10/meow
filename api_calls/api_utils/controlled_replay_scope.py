"""Pure selection guards for date-scoped controlled replays."""

from datetime import date, datetime
from typing import Iterable, List, Mapping, Optional


def normalize_load_date(value) -> str:
    """Return a supported request or schedule date as YYYY-MM-DD."""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    text = str(value or "").strip()
    for date_format in ("%Y-%m-%d", "%Y%m%d"):
        try:
            parsed = datetime.strptime(text, date_format)
        except ValueError:
            continue
        if parsed.strftime(date_format) == text:
            return parsed.date().isoformat()
    raise ValueError(f"load_date must be YYYY-MM-DD; received {value!r}")


def get_game_load_date(game: Mapping) -> str:
    """Resolve a schedule date without relying only on gameID shape."""
    for value in (
        game.get("gameDate"),
        game.get("game_date_est"),
        str(game.get("gameID") or "")[:8],
    ):
        if value:
            try:
                return normalize_load_date(value)
            except ValueError:
                continue
    raise ValueError(
        f"Could not determine schedule date for game {game.get('gameID')!r}"
    )


def select_games_for_load_date(
    games: Iterable[Mapping],
    *,
    load_date: Optional[str],
    controlled_replay: bool,
    configured_replay_date: Optional[str],
) -> List[dict]:
    """Select one request date and enforce the controlled-replay boundary."""
    if not load_date:
        if controlled_replay:
            raise ValueError("load_date is required for controlled replay")
        return [dict(game) for game in games]

    requested_date = normalize_load_date(load_date)
    if controlled_replay:
        if not configured_replay_date:
            raise ValueError(
                "A configured replay date is required for controlled replay"
            )
        configured_date = normalize_load_date(configured_replay_date)
        if requested_date != configured_date:
            raise ValueError(
                "Requested load_date does not match the configured replay date"
            )

    selected = [
        dict(game)
        for game in games
        if get_game_load_date(game) == requested_date
    ]
    if controlled_replay and len(selected) > 1:
        raise RuntimeError(
            "Controlled replay expected zero or exactly one game; "
            f"selected {len(selected)}"
        )
    return selected
