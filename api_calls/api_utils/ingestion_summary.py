"""Build stable, JSON-ready summaries for game ingestion stages."""

from __future__ import annotations

from typing import Iterable, Mapping, Optional


def _game_ids(values: Iterable[object]) -> list[str]:
    return [str(value) for value in values if value is not None and str(value)]


def build_ingestion_summary(
    *,
    selected_game_ids: Iterable[object],
    successful_game_ids: Iterable[object] = (),
    failures: Iterable[Mapping[str, object]] = (),
    no_op_reason: Optional[str] = None,
) -> dict:
    """Return a consistent stage result without performing any I/O."""
    selected = _game_ids(selected_game_ids)
    successful = _game_ids(successful_game_ids)
    failure_list = [dict(failure) for failure in failures]

    if failure_list and successful:
        status = "partial_failure"
    elif failure_list:
        status = "failed"
    elif not selected:
        status = "no_op"
    else:
        status = "success"

    summary = {
        "status": status,
        "selected_game_count": len(selected),
        "selected_game_ids": selected,
        "successful_game_count": len(successful),
        "failed_game_count": len(failure_list),
        "failures": failure_list,
    }
    if status == "no_op":
        summary["no_op_reason"] = no_op_reason or "no_eligible_games"
    return summary
