"""Create or verify the Packet 4 development game-grade ledger."""

from __future__ import annotations

import json

from google.cloud import bigquery

from runtime_config import load_runtime_config
from services.gamelens_postgame_storage import (
    ensure_gamelens_postgame_outcome_table,
)


def main() -> int:
    runtime_config = load_runtime_config()
    if not runtime_config.is_dev:
        raise ValueError("Set GAMELENS_ENVIRONMENT=dev before running setup")
    result = ensure_gamelens_postgame_outcome_table(
        client=bigquery.Client(project=runtime_config.project_id),
        runtime_config=runtime_config,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
