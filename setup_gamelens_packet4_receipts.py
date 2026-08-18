"""Create or verify the Packet 4 development receipt table."""

from __future__ import annotations

import argparse
import json

from services.gamelens_packet4_receipts import ensure_packet4_receipt_table


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Create or verify the Packet 4 development receipt table."
    )
    parser.add_argument(
        "--confirm-dev-setup",
        action="store_true",
        help="Confirm creation or verification of one development receipt table.",
    )
    args = parser.parse_args(argv)
    if not args.confirm_dev_setup:
        raise ValueError("Pass --confirm-dev-setup to allow Packet 4 receipt setup")

    from google.cloud import bigquery
    from runtime_config import load_runtime_config

    runtime_config = load_runtime_config()
    if not runtime_config.is_dev:
        raise ValueError("Set GAMELENS_ENVIRONMENT=dev before Packet 4 receipt setup")
    result = ensure_packet4_receipt_table(
        client=bigquery.Client(project=runtime_config.project_id),
        runtime_config=runtime_config,
        bigquery_module=bigquery,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
