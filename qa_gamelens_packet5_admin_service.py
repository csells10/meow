"""Local visual checkpoint for the hierarchical Packet 5 Admin service shape."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from services.gamelens_admin_run_visibility_service import (
    MAX_GAME_ROWS,
    build_admin_run_visibility_response,
)


def _token(value: Any) -> str:
    return str(value or "").replace("_", " ").upper()


def _render_rows(
    headers: Sequence[str], rows: Iterable[Sequence[Any]], *, max_width: int = 34
) -> str:
    values = [[str(value if value is not None else "") for value in row] for row in rows]
    widths = [len(header) for header in headers]
    for row in values:
        for index, value in enumerate(row):
            widths[index] = min(max_width, max(widths[index], len(value)))

    def line(row: Sequence[str]) -> str:
        return " | ".join(
            value[: widths[index]].ljust(widths[index])
            for index, value in enumerate(row)
        )

    divider = "-+-".join("-" * width for width in widths)
    return "\n".join([line(headers), divider, *(line(row) for row in values)])


def render_admin_service_preview(response: Mapping[str, Any]) -> str:
    """Render overview first and selected-game evidence only when requested."""
    overview = response["overview"]
    games = overview["games"]
    source = overview["source_health"]
    lines = [
        "PACKET 5 / WEEK NAVIGATION / HIERARCHICAL READ SERVICE",
        (
            f"Tables: {source.get('available_count', 0)}/"
            f"{source.get('table_count', 0)} available | "
            f"duplicate keys: {source.get('duplicate_key_count', 0)} | "
            f"invalid keys: {source.get('missing_key_row_count', 0)}"
        ),
        (
            f"Games: {games['scheduled']} scheduled | {games['captured']} captured | "
            f"{games['need_attention']} need attention | "
            f"{games['known_gaps']} known gaps"
        ),
        "Drill path: Overview > Week > Game > Clock > Stage evidence",
        "",
        "WEEK SUMMARY",
    ]
    week_rows = [
        (
            week["game_week"],
            "YES" if week.get("selected") else "",
            _token(week["state"]),
            week["scheduled"],
            week["captured"],
            week["need_attention"],
            week["known_gaps"],
            f"{week.get('first_game_date') or ''} to {week.get('last_game_date') or ''}",
        )
        for week in overview.get("weeks", [])
    ]
    lines.append(
        _render_rows(
            (
                "Week",
                "Selected",
                "State",
                "Games",
                "Captured",
                "Attention",
                "Known gaps",
                "Dates",
            ),
            week_rows,
            max_width=32,
        )
    )
    lines.extend([
        "",
        "GAME JOURNEY",
    ])

    game_rows = []
    for game in response.get("games", []):
        clocks = game["clocks"]
        issue = game.get("first_issue") or {}
        game_rows.append(
            (
                game["game_id"],
                game.get("game_week"),
                game["matchup"],
                _token(game["state"]),
                _token(clocks["data_load"]["state"]),
                _token(clocks["pregame"]["state"]),
                _token(clocks["postgame"]["state"]),
                f"{issue.get('stage', '')}: {issue.get('reason', '')}".strip(": "),
            )
        )
    lines.append(
        _render_rows(
            (
                "Game",
                "Week",
                "Matchup",
                "State",
                "Data",
                "Pregame",
                "Postgame",
                "First issue",
            ),
            game_rows,
            max_width=38,
        )
    )

    for title, key in (
        ("NEEDS ATTENTION", "needs_attention"),
        ("KNOWN GAPS", "known_gaps"),
    ):
        rows = response["attention"][key]
        lines.extend(["", title])
        if not rows:
            lines.append("None.")
            continue
        lines.append(
            _render_rows(
                ("Game", "Clock", "Stage", "Status", "Reason"),
                [
                    (
                        row["game_id"],
                        row["clock"],
                        row["stage"],
                        _token(row["status"]),
                        row["reason"],
                    )
                    for row in rows
                ],
                max_width=48,
            )
        )

    lines.extend(["", "RECENT RUNS"])
    recent_runs = response.get("recent_runs", [])
    if recent_runs:
        lines.append(
            _render_rows(
                ("Run", "Status", "Scope", "Completed", "Reason"),
                [
                    (
                        run["display_label"],
                        _token(run["status"]),
                        run["scope_label"],
                        run["output_count"],
                        run["reason"],
                    )
                    for run in recent_runs
                ],
                max_width=54,
            )
        )
    else:
        lines.append("None.")

    selected = response.get("selected_game")
    if selected:
        lines.extend(
            [
                "",
                f"SELECTED GAME / {selected['game_id']} / {selected['matchup']}",
                (
                    f"State: {_token(selected['state'])} | "
                    f"Capture: {selected['lineage'].get('capture_id') or 'None'} | "
                    f"Learning run: {selected['lineage'].get('learning_run_id') or 'None'}"
                ),
                "",
                "STAGE EVIDENCE",
            ]
        )
        detail_rows = []
        for clock in selected["clocks"].values():
            for stage in clock["stages"]:
                detail_rows.append(
                    (
                        clock["label"],
                        stage["stage"],
                        _token(stage["status"]),
                        stage.get("count"),
                        _token(stage.get("attention")),
                        stage.get("reason"),
                        stage.get("source"),
                    )
                )
        lines.append(
            _render_rows(
                ("Clock", "Stage", "Status", "Count", "Attention", "Reason", "Source"),
                detail_rows,
                max_width=44,
            )
        )

    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Preview the hierarchical Packet 5 Admin read-service response."
    )
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--game-week")
    parser.add_argument("--game-id")
    parser.add_argument("--game-limit", type=int, default=MAX_GAME_ROWS)
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the hierarchical JSON contract instead of the visual preview.",
    )
    args = parser.parse_args(argv)

    report = json.loads(args.report.read_text(encoding="utf-8"))
    response = build_admin_run_visibility_response(
        report,
        game_week=args.game_week,
        game_id=args.game_id,
        game_limit=args.game_limit,
    )
    if args.json:
        print(json.dumps(response, indent=2, sort_keys=True, default=str))
    else:
        print(render_admin_service_preview(response))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
