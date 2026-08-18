"""Add or verify Packet 4 Level 3 fields on the development claim table."""

from __future__ import annotations

import argparse
import json

from services.gamelens_level3_storage import ensure_level3_target_columns


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Add missing Level 3 columns to the existing dev claim table."
    )
    parser.add_argument(
        "--confirm-dev-schema-update",
        action="store_true",
        help="Confirm an additive schema update in GameLens_dev.",
    )
    args = parser.parse_args(argv)
    if not args.confirm_dev_schema_update:
        raise ValueError(
            "Pass --confirm-dev-schema-update to allow the development schema update"
        )

    from google.cloud import bigquery
    from runtime_config import load_runtime_config

    runtime_config = load_runtime_config()
    if not runtime_config.is_dev:
        raise ValueError("Set GAMELENS_ENVIRONMENT=dev before Level 3 schema setup")
    result = ensure_level3_target_columns(
        client=bigquery.Client(project=runtime_config.project_id),
        runtime_config=runtime_config,
        bigquery_module=bigquery,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
