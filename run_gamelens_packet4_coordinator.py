"""Run the bounded Packet 4 coordinator in development."""

from __future__ import annotations

import argparse
import json

from services.gamelens_packet4_coordinator import run_packet4_coordinator
from services.gamelens_packet4_receipts import BigQueryPacket4ReceiptStorage


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Run grade, Level 2, and Level 3 for bounded dev games."
    )
    parser.add_argument(
        "--game-id",
        action="append",
        required=True,
        help="One game ID; repeat the flag for a bounded multi-game run.",
    )
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--log-reference")
    parser.add_argument(
        "--confirm-dev-write",
        action="store_true",
        help="Confirm bounded development learning and receipt writes.",
    )
    args = parser.parse_args(argv)
    if not args.confirm_dev_write:
        raise ValueError(
            "Pass --confirm-dev-write to allow the Packet 4 development coordinator"
        )

    from google.cloud import bigquery
    from runtime_config import load_runtime_config

    runtime_config = load_runtime_config()
    if not runtime_config.is_dev:
        raise ValueError("Set GAMELENS_ENVIRONMENT=dev before Packet 4 coordination")
    client = bigquery.Client(project=runtime_config.project_id)
    storage = BigQueryPacket4ReceiptStorage(
        client=client,
        runtime_config=runtime_config,
        bigquery_module=bigquery,
    )
    result = run_packet4_coordinator(
        client=client,
        bigquery=bigquery,
        runtime_config=runtime_config,
        game_ids=args.game_id,
        attempt_id=args.attempt_id,
        receipt_storage=storage,
        log_reference=args.log_reference,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0 if result["status"] in {"success", "no_op"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
