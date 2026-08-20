"""Read-only visual rehearsal of the protected Packet 5 Flask route."""

from __future__ import annotations

import argparse
import json
from types import SimpleNamespace
from typing import Sequence
from unittest.mock import patch

from flask import Flask

import auth.firebase_auth as firebase_auth
import routes.admin_run_visibility_routes as route_module
from qa_gamelens_packet5_admin_service import render_admin_service_preview
from qa_gamelens_packet5_admin_inventory import PROJECT_ID


def _admin_token() -> dict:
    return {
        "email": "packet5-local-route-review@gamelens.local",
        "gamelens_user": {
            "email": "packet5-local-route-review@gamelens.local",
            "role": "admin",
            "active": True,
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Visually rehearse the protected Packet 5 Admin route."
    )
    parser.add_argument("--season", default="2026")
    parser.add_argument("--season-type", required=True)
    parser.add_argument("--learning-run-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--game-week")
    parser.add_argument("--game-id")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument(
        "--dev-read-only",
        action="store_true",
        help="Confirm that the route rehearsal may only read approved sources.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the HTTP JSON instead of the visual hierarchy.",
    )
    args = parser.parse_args(argv)
    if not args.dev_read_only:
        raise ValueError("Pass --dev-read-only to confirm this run is read-only")

    runtime_config = SimpleNamespace(
        is_dev=True,
        active_season=str(args.season),
        project_id=PROJECT_ID,
    )
    query = {
        "season": args.season,
        "season_type": args.season_type,
        "learning_run_id": args.learning_run_id,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "limit": str(args.limit),
    }
    if args.game_week:
        query["game_week"] = args.game_week
    if args.game_id:
        query["game_id"] = args.game_id

    app = Flask("packet5_admin_route_review")
    app.register_blueprint(route_module.admin_run_visibility_routes)
    with (
        patch.object(
            firebase_auth,
            "verify_firebase_request",
            return_value=(_admin_token(), None),
        ),
        patch.object(
            route_module,
            "load_runtime_config",
            return_value=runtime_config,
        ),
        app.test_client() as client,
    ):
        response = client.get(
            "/admin/gamelens/run-visibility",
            query_string=query,
        )

    payload = response.get_json()
    print(f"HTTP {response.status_code} / GET /admin/gamelens/run-visibility")
    if response.status_code != 200:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
        return 1
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(render_admin_service_preview(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
