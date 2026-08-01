"""Fail-closed runtime targets for production and controlled replays.

Production keeps the destinations used before Packet 4. Development must
declare every writable destination explicitly; it never inherits production
dataset or raw-response bucket defaults.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
import re
from typing import Mapping, Optional


DEFAULT_PROJECT_ID = "nfl-stream-406420"
PRODUCTION_DATASETS = {
    "league": "League",
    "scores": "Scores",
    "analytics": "Analytics",
}
APPROVED_DEV_DATASETS = {
    "league": "League_dev",
    "scores": "Scores_dev",
    "analytics": "Analytics_dev",
}
PRODUCTION_RAW_BUCKET = "xtra_point"


@dataclass(frozen=True)
class RuntimeConfig:
    project_id: str
    environment: str
    run_mode: str
    active_season: str
    league_dataset: str
    scores_dataset: str
    analytics_dataset: str
    raw_response_bucket: str
    replay_date: Optional[str] = None

    @property
    def is_dev(self) -> bool:
        return self.environment == "dev"

    @property
    def is_controlled_replay(self) -> bool:
        return self.run_mode == "controlled_replay"

    def league_table(self, table_name: str) -> str:
        return self._table(self.league_dataset, table_name)

    def scores_table(self, table_name: str) -> str:
        return self._table(self.scores_dataset, table_name)

    def analytics_table(self, table_name: str) -> str:
        return self._table(self.analytics_dataset, table_name)

    def league_object(self, table_name: str) -> str:
        return f"{self.league_dataset}.{table_name}"

    def scores_object(self, table_name: str) -> str:
        return f"{self.scores_dataset}.{table_name}"

    def analytics_object(self, table_name: str) -> str:
        return f"{self.analytics_dataset}.{table_name}"

    def _table(self, dataset: str, table_name: str) -> str:
        name = str(table_name or "").strip()
        if not name:
            raise ValueError("BigQuery table name cannot be blank")
        return f"{self.project_id}.{dataset}.{name}"


def _required(source: Mapping[str, str], name: str) -> str:
    value = str(source.get(name) or "").strip()
    if not value:
        raise ValueError(f"{name} is required in dev mode")
    return value


def _validate_season(value: str) -> str:
    season = str(value or "").strip()
    if not re.fullmatch(r"20\d{2}", season):
        raise ValueError("GAMELENS_ACTIVE_SEASON must be a four-digit NFL season")
    return season


def _validate_dev_targets(config: RuntimeConfig) -> None:
    actual_datasets = {
        "league": config.league_dataset,
        "scores": config.scores_dataset,
        "analytics": config.analytics_dataset,
    }
    for target_type, production_name in PRODUCTION_DATASETS.items():
        actual = actual_datasets[target_type]
        if actual == production_name:
            raise ValueError(
                f"Dev mode cannot use production dataset {production_name}"
            )
        approved = APPROVED_DEV_DATASETS[target_type]
        if actual != approved:
            raise ValueError(
                f"Dev {target_type} dataset must be the approved target {approved}"
            )

    bucket = config.raw_response_bucket
    if bucket == PRODUCTION_RAW_BUCKET:
        raise ValueError("Dev mode cannot use the production raw-response bucket")
    if not re.search(r"(^|[-.])dev($|[-.])", bucket.lower()):
        raise ValueError("Dev raw-response bucket name must contain a dev segment")


def load_runtime_config(
    environ: Optional[Mapping[str, str]] = None,
) -> RuntimeConfig:
    """Resolve and validate runtime targets without making any cloud call."""
    source = os.environ if environ is None else environ
    environment = str(
        source.get("GAMELENS_ENVIRONMENT", "production")
    ).strip().lower()
    if environment == "development":
        environment = "dev"
    if environment not in {"production", "dev"}:
        raise ValueError("GAMELENS_ENVIRONMENT must be production or dev")

    if environment == "dev":
        run_mode = _required(source, "GAMELENS_RUN_MODE").lower()
        active_season = _validate_season(
            _required(source, "GAMELENS_ACTIVE_SEASON")
        )
        league_dataset = _required(source, "GAMELENS_LEAGUE_DATASET")
        scores_dataset = _required(source, "GAMELENS_SCORES_DATASET")
        analytics_dataset = _required(source, "GAMELENS_ANALYTICS_DATASET")
        raw_response_bucket = _required(source, "GCS_BUCKET_NAME")
    else:
        run_mode = str(source.get("GAMELENS_RUN_MODE", "daily")).strip().lower()
        active_season = _validate_season(
            str(source.get("GAMELENS_ACTIVE_SEASON", "2026"))
        )
        league_dataset = str(
            source.get("GAMELENS_LEAGUE_DATASET", PRODUCTION_DATASETS["league"])
        ).strip()
        scores_dataset = str(
            source.get("GAMELENS_SCORES_DATASET", PRODUCTION_DATASETS["scores"])
        ).strip()
        analytics_dataset = str(
            source.get(
                "GAMELENS_ANALYTICS_DATASET",
                PRODUCTION_DATASETS["analytics"],
            )
        ).strip()
        raw_response_bucket = str(
            source.get("GCS_BUCKET_NAME", PRODUCTION_RAW_BUCKET)
        ).strip()

    if run_mode not in {"daily", "controlled_replay"}:
        raise ValueError("GAMELENS_RUN_MODE must be daily or controlled_replay")
    if environment == "production" and run_mode != "daily":
        raise ValueError("controlled_replay mode is allowed only in dev")

    project_id = str(
        source.get("GAMELENS_PROJECT_ID", DEFAULT_PROJECT_ID)
    ).strip()
    if not project_id:
        raise ValueError("GAMELENS_PROJECT_ID cannot be blank")

    config = RuntimeConfig(
        project_id=project_id,
        environment=environment,
        run_mode=run_mode,
        active_season=active_season,
        league_dataset=league_dataset,
        scores_dataset=scores_dataset,
        analytics_dataset=analytics_dataset,
        raw_response_bucket=raw_response_bucket,
        replay_date=str(source.get("GAMELENS_REPLAY_DATE") or "").strip() or None,
    )

    if config.is_dev:
        _validate_dev_targets(config)

    return config
