"""Bounded one-game capture orchestration for Learning Lite LL-3."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional

from runtime_config import RuntimeConfig
from services.game_service import GameDetailsEvidence, get_pregame_game_contract
from services.gamelens_pregame_contract import require_eligible_pregame_payload


LL3_IMPLEMENTATION_VERSION = "learning_lite_ll3_v1"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def collect_lens_tags(evidence: GameDetailsEvidence) -> list[str]:
    tags = set()
    for rankings in (evidence.away_rankings, evidence.home_rankings):
        for metric, row in rankings.items():
            metric_tags = row.get("lens_tags") or []
            if not isinstance(metric_tags, list) or any(
                not isinstance(tag, str) or not tag.strip()
                for tag in metric_tags
            ):
                raise ValueError(f"malformed_lens_tags:{metric}")
            tags.update(tag.strip() for tag in metric_tags)
    return sorted(tags)


def build_evidence_context(
    evidence: GameDetailsEvidence,
    *,
    source_lineage: Mapping[str, Any],
    metric_pipeline_run_id: Optional[str],
) -> dict:
    return {
        "implementation_version": LL3_IMPLEMENTATION_VERSION,
        "metric_pipeline_run_id": metric_pipeline_run_id,
        "source_lineage": dict(source_lineage),
        "ranking_context": evidence.ranking_context,
        "away_metrics": evidence.away_metrics,
        "home_metrics": evidence.home_metrics,
        "away_rankings": evidence.away_rankings,
        "home_rankings": evidence.home_rankings,
    }


def build_snapshot_row(
    *,
    runtime_config: RuntimeConfig,
    contract: Mapping[str, Any],
    evidence: GameDetailsEvidence,
    scheduled_kickoff: datetime,
    captured_at: datetime,
    source_lineage: Mapping[str, Any],
    metric_source_date: Any,
    ranking_as_of_date: Any,
    metric_pipeline_run_id: Optional[str],
    model_version: str,
    ruleset_version: str,
) -> dict:
    payload = contract["payload"]
    header = payload.get("header") or {}
    ranking_context = payload.get("ranking_context") or {}
    require_eligible_pregame_payload(
        payload=payload,
        game_id=header.get("game_id"),
        captured_at=captured_at,
        scheduled_kickoff=scheduled_kickoff,
    )
    return {
        "capture_id": contract["capture_id"],
        "learning_run_id": contract["learning_run_id"],
        "game_id": str(header.get("game_id") or ""),
        "environment": runtime_config.environment,
        "season": str(header.get("season") or "") or None,
        "season_type": header.get("season_type"),
        "game_week": header.get("game_week"),
        "game_status": header.get("game_status"),
        "scheduled_kickoff": scheduled_kickoff,
        "captured_at": captured_at,
        "capture_status": "captured",
        "payload_sha256": contract["payload_sha256"],
        "response_payload": payload,
        "evidence_context": build_evidence_context(
            evidence,
            source_lineage=source_lineage,
            metric_pipeline_run_id=metric_pipeline_run_id,
        ),
        "lens_tags": collect_lens_tags(evidence),
        "ranking_context_available": bool(ranking_context.get("available")),
        "ranking_context_reason": ranking_context.get("reason"),
        "metric_source_date": metric_source_date,
        "ranking_as_of_date": ranking_as_of_date,
        "metric_pipeline_run_id": metric_pipeline_run_id,
        "model_version": model_version,
        "ruleset_version": ruleset_version,
    }


class SnapshotCaptureService:
    """Build, optionally persist, and verify exactly one pregame snapshot."""

    def __init__(
        self,
        *,
        runtime_config: RuntimeConfig,
        evidence_loader,
        storage,
        contract_builder: Callable[..., dict] = get_pregame_game_contract,
        now: Callable[[], datetime] = _utc_now,
    ):
        if not runtime_config.is_dev:
            raise ValueError("LL-3 capture is dev-only")
        self.runtime_config = runtime_config
        self.evidence_loader = evidence_loader
        self.storage = storage
        self.contract_builder = contract_builder
        self.now = now

    def capture_one(
        self,
        *,
        game_id: str,
        learning_run_id: str,
        model_version: str,
        ruleset_version: str,
        metric_pipeline_run_id: Optional[str] = None,
        write: bool = False,
    ) -> dict:
        loaded = self.evidence_loader.load(game_id)
        captured_at = self.now()
        contract = self.contract_builder(
            game_id,
            learning_run_id=learning_run_id,
            captured_at=captured_at,
            scheduled_kickoff=loaded.scheduled_kickoff,
            evidence=loaded.evidence,
        )
        contract = {**contract, "learning_run_id": learning_run_id}
        row = build_snapshot_row(
            runtime_config=self.runtime_config,
            contract=contract,
            evidence=loaded.evidence,
            scheduled_kickoff=loaded.scheduled_kickoff,
            captured_at=captured_at,
            source_lineage=loaded.source_lineage,
            metric_source_date=loaded.metric_source_date,
            ranking_as_of_date=loaded.ranking_as_of_date,
            metric_pipeline_run_id=metric_pipeline_run_id,
            model_version=model_version,
            ruleset_version=ruleset_version,
        )

        if not write:
            self.storage.verify_table_contract()
            return self._summary(
                row,
                status="dry_run",
                material_change=False,
            )

        refreshed = self.evidence_loader.recheck_schedule(game_id)
        self._require_unchanged_schedule(
            game_id=game_id,
            scheduled_kickoff=loaded.scheduled_kickoff,
            refreshed=refreshed,
        )
        require_eligible_pregame_payload(
            payload=row["response_payload"],
            game_id=game_id,
            captured_at=self.now(),
            scheduled_kickoff=loaded.scheduled_kickoff,
        )
        result = self.storage.reconcile_snapshot(row)
        saved = self.storage.read_snapshot(row["capture_id"])
        if saved is None:
            raise RuntimeError("saved_snapshot_readback_missing")
        return {
            **self._summary(
                row,
                status=result["status"],
                material_change=result["material_change"],
            ),
            "readback": "verified",
            "stored_captured_at": str(saved["captured_at"]),
        }

    def _summary(
        self,
        row: Mapping[str, Any],
        *,
        status: str,
        material_change: bool,
    ) -> dict:
        return {
            "status": status,
            "material_change": material_change,
            "table": self.storage.table_id,
            "game_id": row["game_id"],
            "capture_id": row["capture_id"],
            "learning_run_id": row["learning_run_id"],
            "payload_sha256": row["payload_sha256"],
            "scheduled_kickoff": row["scheduled_kickoff"].isoformat(),
            "captured_at": row["captured_at"].isoformat(),
            "ranking_context_available": row[
                "ranking_context_available"
            ],
            "ranking_context_reason": row["ranking_context_reason"],
            "lens_tag_count": len(row["lens_tags"]),
            "model_version": row["model_version"],
            "ruleset_version": row["ruleset_version"],
        }

    @staticmethod
    def _require_unchanged_schedule(
        *,
        game_id: str,
        scheduled_kickoff: datetime,
        refreshed: Mapping[str, Any],
    ) -> None:
        refreshed_header = refreshed.get("header") or {}
        if str(refreshed.get("game_id") or "") != str(game_id):
            raise ValueError("schedule_identity_changed_before_save")
        if refreshed.get("scheduled_kickoff") != scheduled_kickoff:
            raise ValueError("schedule_identity_changed_before_save")
        if str(refreshed_header.get("game_status") or "").casefold() != "scheduled":
            raise ValueError("game_not_scheduled")
