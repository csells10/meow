"""Thin Packet 2 coordinator for sequential shadow pregame capture."""

from __future__ import annotations

from datetime import date, datetime, timezone
from time import perf_counter
from typing import Any, Callable, Dict, Mapping, Optional, Sequence
from uuid import uuid4

try:
    import resource
except ImportError:  # pragma: no cover - resource is unavailable on Windows.
    resource = None

from runtime_config import RuntimeConfig
from services.game_service import GameDetailsEvidence, get_pregame_game_details
from services.gamelens_learning_contract import (
    build_capture_id,
    build_capture_manifest,
    build_learning_run_id,
    evaluate_capture_candidate,
    payload_sha256,
    validate_pregame_payload,
)
from services.gamelens_snapshot_storage import (
    BigQueryBufferPending,
    SnapshotStorageError,
)


REQUIRED_METRIC_STAGES = ("facts", "windowed_metrics", "rankings")


def readiness_from_metric_pipeline_summary(
    summary: Mapping[str, Any],
    *,
    upstream_run_id: str,
) -> dict:
    """Accept an observed successful pipeline summary without rerunning it."""
    status = str(summary.get("status") or "").strip().casefold()
    if status != "success":
        return {
            "ready": False,
            "status": "blocked",
            "reason": f"metric_pipeline_{status or 'status_missing'}",
            "upstream_run_id": upstream_run_id,
        }
    stages = summary.get("stages") or {}
    incomplete = [
        stage
        for stage in REQUIRED_METRIC_STAGES
        if str((stages.get(stage) or {}).get("status") or "").casefold()
        != "completed"
    ]
    if incomplete:
        return {
            "ready": False,
            "status": "blocked",
            "reason": f"metric_pipeline_incomplete:{','.join(incomplete)}",
            "upstream_run_id": upstream_run_id,
        }
    return {
        "ready": True,
        "status": "ready",
        "reason": None,
        "upstream_run_id": upstream_run_id,
    }


def semantic_json_diff(expected: Any, actual: Any, path: str = "$") -> list:
    """Return exact product-field differences while ignoring object key order."""
    differences = []
    if type(expected) is not type(actual):
        return [{
            "path": path,
            "expected": expected,
            "actual": actual,
            "reason": "type_mismatch",
        }]
    if isinstance(expected, Mapping):
        expected_keys = set(expected)
        actual_keys = set(actual)
        for key in sorted(expected_keys - actual_keys):
            differences.append({
                "path": f"{path}.{key}",
                "expected": expected[key],
                "actual": None,
                "reason": "missing_actual_field",
            })
        for key in sorted(actual_keys - expected_keys):
            differences.append({
                "path": f"{path}.{key}",
                "expected": None,
                "actual": actual[key],
                "reason": "unexpected_actual_field",
            })
        for key in sorted(expected_keys & actual_keys):
            differences.extend(
                semantic_json_diff(expected[key], actual[key], f"{path}.{key}")
            )
        return differences
    if isinstance(expected, list):
        if len(expected) != len(actual):
            differences.append({
                "path": path,
                "expected": len(expected),
                "actual": len(actual),
                "reason": "list_length_mismatch",
            })
        for index, (expected_item, actual_item) in enumerate(
            zip(expected, actual)
        ):
            differences.extend(
                semantic_json_diff(
                    expected_item,
                    actual_item,
                    f"{path}[{index}]",
                )
            )
        return differences
    if expected != actual:
        differences.append({
            "path": path,
            "expected": expected,
            "actual": actual,
            "reason": "value_mismatch",
        })
    return differences


def verify_saved_snapshot(
    saved: Mapping[str, Any],
    *,
    expected_capture_id: str,
    expected_learning_run_id: Optional[str] = None,
    expected_payload: Optional[Mapping[str, Any]] = None,
) -> list:
    """Verify identity/hash and optionally exact builder-output parity."""
    differences = []
    if str(saved.get("capture_id") or "") != str(expected_capture_id):
        differences.append({
            "path": "$.capture_id",
            "expected": expected_capture_id,
            "actual": saved.get("capture_id"),
            "reason": "capture_identity_mismatch",
        })
    if (
        expected_learning_run_id is not None
        and str(saved.get("learning_run_id") or "")
        != str(expected_learning_run_id)
    ):
        differences.append({
            "path": "$.learning_run_id",
            "expected": expected_learning_run_id,
            "actual": saved.get("learning_run_id"),
            "reason": "learning_run_identity_mismatch",
        })
    saved_payload = saved.get("response_payload")
    if not isinstance(saved_payload, Mapping):
        differences.append({
            "path": "$.response_payload",
            "expected": "JSON object",
            "actual": saved_payload,
            "reason": "payload_type_mismatch",
        })
        return differences
    actual_hash = payload_sha256(saved_payload)
    if saved.get("payload_sha256") != actual_hash:
        differences.append({
            "path": "$.payload_sha256",
            "expected": actual_hash,
            "actual": saved.get("payload_sha256"),
            "reason": "payload_hash_mismatch",
        })
    if expected_payload is not None:
        differences.extend(semantic_json_diff(expected_payload, saved_payload))
    return differences


def collect_lens_tags(evidence: GameDetailsEvidence) -> list:
    """Collect validated, de-duplicated tags from already-loaded rankings."""
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
    upstream_run_id: Optional[str],
    source_lineage: Optional[Mapping[str, Any]] = None,
) -> dict:
    """Keep rich lineage beside, never inside, the frontend response."""
    return {
        "upstream_metric_pipeline_run_id": upstream_run_id,
        "source_lineage": dict(source_lineage or {}),
        "ranking_context": evidence.ranking_context,
        "away_metrics": evidence.away_metrics,
        "home_metrics": evidence.home_metrics,
        "away_rankings": evidence.away_rankings,
        "home_rankings": evidence.home_rankings,
    }


def _peak_memory_mb() -> Optional[float]:
    if resource is None:
        return None
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux reports KiB; macOS reports bytes. Cloud Run and this repo are Linux.
    return round(float(peak) / 1024.0, 3)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _same_schedule_identity(original: Mapping, refreshed: Mapping) -> bool:
    if not refreshed:
        return False
    return (
        str(original.get("game_id")) == str(refreshed.get("game_id"))
        and original.get("scheduled_kickoff")
        == refreshed.get("scheduled_kickoff")
    )


def _shared_season_type(candidates: Sequence[Mapping]) -> Optional[str]:
    """Return the common season type only when every candidate provides it."""
    if not candidates:
        return None
    season_types = []
    for candidate in candidates:
        header = candidate.get("header") or {}
        season_type = str(
            candidate.get("season_type")
            or header.get("season_type")
            or ""
        ).strip()
        if not season_type:
            return None
        season_types.append(season_type)
    first = season_types[0]
    return (
        first
        if all(value.casefold() == first.casefold() for value in season_types)
        else None
    )


def build_stage_game_result_rows(
    *,
    attempt_id: str,
    game_results: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
    default_season: str,
    upstream_run_id: Optional[str],
    recorded_at: str,
) -> list:
    """Build the durable per-game audit complement to one stage receipt."""
    candidates_by_game = {
        str(candidate.get("game_id")): candidate
        for candidate in candidates
        if candidate.get("game_id")
    }
    rows = []
    for game_result in game_results:
        game_id = str(game_result.get("game_id") or "").strip()
        if not game_id:
            raise ValueError("stage_game_result_game_id_missing")
        candidate = candidates_by_game.get(game_id) or {}
        header = candidate.get("header") or {}
        status = str(game_result.get("status") or "").strip()
        eligible = game_result.get("eligible")
        if eligible is None:
            eligible = status != "skipped"
        rows.append({
            "attempt_id": attempt_id,
            "stage_name": "snapshot_capture",
            "game_id": game_id,
            "capture_id": game_result.get("capture_id"),
            "learning_run_id": game_result.get("learning_run_id"),
            "season": (
                game_result.get("season")
                or candidate.get("season")
                or header.get("season")
                or default_season
            ),
            "season_type": (
                game_result.get("season_type")
                or candidate.get("season_type")
                or header.get("season_type")
            ),
            "status": status,
            "reason": game_result.get("reason"),
            "eligible": bool(eligible),
            "rebuilt": bool(game_result.get("rebuilt", False)),
            "input_count": 1,
            "output_count": 1 if status == "success" else 0,
            "upstream_run_id": upstream_run_id,
            "recorded_at": recorded_at,
            "is_backfill": False,
            "backfill_source": None,
        })
    return rows


class SnapshotCaptureCoordinator:
    """Coordinate one bounded slate without HTTP calls or outcome work."""

    def __init__(
        self,
        *,
        runtime_config: RuntimeConfig,
        evidence_loader,
        storage,
        upstream_readiness: Callable[[Sequence[Mapping]], Mapping[str, Any]],
        response_builder: Callable[..., dict] = get_pregame_game_details,
        now: Callable[[], datetime] = _utc_now,
    ):
        if not runtime_config.is_dev:
            raise ValueError("Packet 2 Snapshot Capture is dev-only")
        self.runtime_config = runtime_config
        self.evidence_loader = evidence_loader
        self.storage = storage
        self.upstream_readiness = upstream_readiness
        self.response_builder = response_builder
        self.now = now

    def run(
        self,
        *,
        start_date: date,
        end_date: date,
        ruleset_version: str,
        model_version: str,
        game_id: Optional[str] = None,
    ) -> dict:
        started_at = self.now()
        started_clock = perf_counter()
        attempt_id = (
            f"snapshot_{started_at.strftime('%Y%m%dT%H%M%SZ')}_"
            f"{uuid4().hex[:8]}"
        )
        requested_game_id = str(game_id or "").strip() or None
        discovery_error = None
        try:
            discovered_candidates = self.evidence_loader.fetch_candidates(
                start_date=start_date,
                end_date=end_date,
            )
            candidates = (
                [
                    candidate
                    for candidate in discovered_candidates
                    if str(candidate.get("game_id")) == requested_game_id
                ]
                if requested_game_id
                else discovered_candidates
            )
        except Exception as exc:
            discovered_candidates = []
            candidates = []
            discovery_error = str(exc)
        result = {
            "attempt_id": attempt_id,
            "status": "failure" if discovery_error else "no_op",
            "reason": (
                f"candidate_discovery_failed:{discovery_error}"
                if discovery_error
                else "requested_game_not_found"
                if requested_game_id and not candidates
                else "no_games_scheduled" if not candidates else None
            ),
            "requested_game_id": requested_game_id,
            "games_discovered": len(discovered_candidates),
            "games_checked": len(candidates),
            "games_eligible": 0,
            "games_captured": 0,
            "games_skipped": 0,
            "games_failed": 0,
            "games_waiting": 0,
            "internal_http_game_calls": 0,
            "game_results": [],
        }

        uncaptured = []
        existing_upstream_run_ids = set()
        for candidate in candidates:
            preparation = self._prepare_candidate(
                candidate,
                captured_at=started_at,
                ruleset_version=ruleset_version,
            )
            if not preparation["eligible"]:
                result["games_skipped"] += 1
                result["game_results"].append(preparation)
                continue
            result["games_eligible"] += 1
            try:
                existing = self.storage.find_capture(preparation["capture_id"])
            except BigQueryBufferPending as exc:
                result["games_waiting"] += 1
                result["game_results"].append({
                    **preparation,
                    "status": "waiting",
                    "reason": f"bigquery_buffer:{exc}",
                    "rebuilt": False,
                })
                continue
            except Exception as exc:
                result["games_failed"] += 1
                result["game_results"].append({
                    **preparation,
                    "status": "failure",
                    "reason": f"canonical_lookup_failed:{exc}",
                    "rebuilt": False,
                })
                continue
            if existing:
                existing_differences = verify_saved_snapshot(
                    existing,
                    expected_capture_id=preparation["capture_id"],
                    expected_learning_run_id=preparation["learning_run_id"],
                )
                if existing_differences:
                    result["games_failed"] += 1
                    result["game_results"].append({
                        **preparation,
                        "status": "failure",
                        "reason": (
                            "existing_capture_verification_failed:"
                            f"{existing_differences}"
                        ),
                        "rebuilt": False,
                    })
                    continue
                existing_upstream_run_id = str(
                    existing.get("metric_pipeline_run_id") or ""
                ).strip()
                if existing_upstream_run_id:
                    existing_upstream_run_ids.add(existing_upstream_run_id)
                result["games_skipped"] += 1
                result["game_results"].append({
                    **preparation,
                    "status": "no_op",
                    "reason": "canonical_capture_exists",
                    "rebuilt": False,
                })
                continue
            uncaptured.append({**candidate, **preparation})

        if len(existing_upstream_run_ids) == 1:
            result["upstream_run_id"] = next(iter(existing_upstream_run_ids))

        if uncaptured:
            try:
                readiness = dict(self.upstream_readiness(uncaptured))
            except BigQueryBufferPending as exc:
                readiness = {
                    "ready": False,
                    "status": "waiting",
                    "reason": f"bigquery_buffer:{exc}",
                }
            except Exception as exc:
                readiness = {
                    "ready": False,
                    "status": "blocked",
                    "reason": f"upstream_readiness_failed:{exc}",
                }
            result["upstream_run_id"] = (
                readiness.get("upstream_run_id")
                or result.get("upstream_run_id")
            )
            if not readiness.get("ready"):
                waiting = readiness.get("status") == "waiting"
                game_status = "waiting" if waiting else "failure"
                game_reason = (
                    readiness.get("reason") or "upstream_not_ready"
                )
                result["status"] = "waiting" if waiting else "failure"
                result["reason"] = game_reason
                result["games_waiting" if waiting else "games_failed"] += len(
                    uncaptured
                )
                result["game_results"].extend({
                    "game_id": candidate["game_id"],
                    "eligible": True,
                    "capture_id": candidate["capture_id"],
                    "learning_run_id": candidate["learning_run_id"],
                    "status": game_status,
                    "reason": game_reason,
                    "rebuilt": False,
                } for candidate in uncaptured)
            else:
                self._capture_uncaptured(
                    uncaptured,
                    readiness=readiness,
                    ruleset_version=ruleset_version,
                    model_version=model_version,
                    result=result,
                )

        self._finalize_status(result)
        finished_at = self.now()
        result["started_at"] = started_at.isoformat()
        result["finished_at"] = finished_at.isoformat()
        result["duration_ms"] = round(
            (perf_counter() - started_clock) * 1000
        )
        result["peak_memory_mb"] = _peak_memory_mb()
        result["stage_game_results"] = "not_applicable"
        try:
            detail_rows = build_stage_game_result_rows(
                attempt_id=attempt_id,
                game_results=result["game_results"],
                candidates=candidates,
                default_season=self.runtime_config.active_season,
                upstream_run_id=result.get("upstream_run_id"),
                recorded_at=result["finished_at"],
            )
            if detail_rows:
                detail_write = self.storage.write_stage_game_results(
                    detail_rows
                )
                result["stage_game_results"] = "saved"
                result["stage_game_result_counts"] = detail_write
        except BigQueryBufferPending as exc:
            result["stage_game_results"] = "waiting/retryable"
            result["stage_game_results_reason"] = str(exc)
            self._mark_observability_failure(
                result,
                reason="stage_game_results_waiting",
            )
        except Exception as exc:
            result["stage_game_results"] = "failed"
            result["stage_game_results_reason"] = str(exc)
            self._mark_observability_failure(
                result,
                reason="stage_game_results_write_failed",
            )
        receipt = {
            "attempt_id": attempt_id,
            "stage_name": "snapshot_capture",
            "status": result["status"],
            "game_id": requested_game_id,
            "season": self.runtime_config.active_season,
            "season_type": _shared_season_type(candidates),
            "input_count": result["games_checked"],
            "output_count": result["games_captured"],
            "started_at": result["started_at"],
            "finished_at": result["finished_at"],
            "duration_ms": result["duration_ms"],
            "upstream_run_id": result.get("upstream_run_id"),
            "reason": result.get("reason"),
        }
        try:
            self.storage.write_stage_receipt(receipt)
            result["stage_receipt"] = "saved"
        except BigQueryBufferPending as exc:
            result["stage_receipt"] = "waiting/retryable"
            result["stage_receipt_reason"] = str(exc)
        except SnapshotStorageError as exc:
            result["stage_receipt"] = "failed"
            result["stage_receipt_reason"] = str(exc)
        return result

    @staticmethod
    def _mark_observability_failure(result: dict, *, reason: str) -> None:
        if result["status"] in {"success", "no_op"}:
            result["status"] = "partial_failure"
            result["reason"] = reason

    def _prepare_candidate(
        self,
        candidate: Mapping,
        *,
        captured_at: datetime,
        ruleset_version: str,
    ) -> dict:
        game_id = candidate.get("game_id")
        kickoff = candidate.get("scheduled_kickoff")
        if not kickoff:
            return {
                "game_id": game_id,
                "eligible": False,
                "status": "skipped",
                "reason": "scheduled_kickoff_missing",
            }
        eligibility = evaluate_capture_candidate(
            candidate["header"],
            captured_at=captured_at,
            scheduled_kickoff=kickoff,
            production=False,
        )
        if not eligibility["eligible"]:
            return {
                "game_id": game_id,
                "eligible": False,
                "status": "skipped",
                "reason": eligibility["reason"],
            }
        learning_run_id = build_learning_run_id(
            season=candidate["header"].get("season"),
            season_type=candidate["header"].get("season_type"),
            ruleset_version=ruleset_version,
        )
        return {
            "game_id": game_id,
            "eligible": True,
            "status": "ready",
            "reason": None,
            "learning_run_id": learning_run_id,
            "capture_id": build_capture_id(
                learning_run_id=learning_run_id,
                game_id=game_id,
                scheduled_kickoff=kickoff,
            ),
        }

    def _capture_uncaptured(
        self,
        uncaptured: Sequence[Mapping],
        *,
        readiness: Mapping[str, Any],
        ruleset_version: str,
        model_version: str,
        result: dict,
    ) -> None:
        result["upstream_run_id"] = readiness.get("upstream_run_id")
        try:
            evidence_by_game = self.evidence_loader.load_slate_evidence(uncaptured)
        except BigQueryBufferPending as exc:
            result["games_waiting"] += len(uncaptured)
            result["reason"] = str(exc)
            result["game_results"].extend({
                "game_id": candidate["game_id"],
                "eligible": True,
                "capture_id": candidate["capture_id"],
                "learning_run_id": candidate["learning_run_id"],
                "status": "waiting",
                "reason": f"bigquery_buffer:{exc}",
                "rebuilt": False,
            } for candidate in uncaptured)
            return
        except Exception as exc:
            result["games_failed"] += len(uncaptured)
            result["reason"] = f"slate_evidence_load_failed:{exc}"
            result["game_results"].extend({
                "game_id": candidate["game_id"],
                "eligible": True,
                "capture_id": candidate["capture_id"],
                "learning_run_id": candidate["learning_run_id"],
                "status": "failure",
                "reason": result["reason"],
                "rebuilt": False,
            } for candidate in uncaptured)
            return

        for candidate in uncaptured:
            game_id = candidate["game_id"]
            try:
                evidence = evidence_by_game.get(game_id)
                if evidence is None:
                    raise ValueError("slate_evidence_missing")
                response_payload = self.response_builder(
                    game_id,
                    evidence=evidence,
                )
                validation = validate_pregame_payload(response_payload)
                if not validation["valid"]:
                    raise ValueError(
                        f"{validation['reason']}:{validation['violations']}"
                    )
                refreshed = self.evidence_loader.recheck_candidate(game_id)
                if not _same_schedule_identity(candidate, refreshed):
                    raise ValueError("schedule_identity_changed_before_save")
                final_check = evaluate_capture_candidate(
                    refreshed["header"],
                    captured_at=self.now(),
                    scheduled_kickoff=refreshed["scheduled_kickoff"],
                    production=False,
                )
                if not final_check["eligible"]:
                    raise ValueError(final_check["reason"])

                tags = collect_lens_tags(evidence)
                context = build_evidence_context(
                    evidence,
                    upstream_run_id=readiness.get("upstream_run_id"),
                    source_lineage=(
                        self.evidence_loader.evidence_lineage()
                        if hasattr(self.evidence_loader, "evidence_lineage")
                        else None
                    ),
                )
                captured_at = self.now()
                manifest = build_capture_manifest(
                    payload=response_payload,
                    game=refreshed["header"],
                    pipeline_run_id=readiness.get("upstream_run_id") or "unknown",
                    learning_run_id=candidate["learning_run_id"],
                    captured_at=captured_at,
                    scheduled_kickoff=refreshed["scheduled_kickoff"],
                    model_version=model_version,
                    ruleset_version=ruleset_version,
                    source_dates={
                        "rankings": evidence.ranking_context.get("as_of_date")
                    },
                    production=False,
                )
                row = {
                    "capture_id": manifest["capture_id"],
                    "learning_run_id": manifest["learning_run_id"],
                    "game_id": game_id,
                    "environment": self.runtime_config.environment,
                    "season": manifest["season"],
                    "season_type": manifest["season_type"],
                    "game_week": manifest["game_week"],
                    "game_status": manifest["game_status"],
                    "scheduled_kickoff": manifest["scheduled_kickoff"],
                    "captured_at": manifest["captured_at"],
                    "capture_status": manifest["capture_status"],
                    "payload_sha256": manifest["payload_sha256"],
                    "response_payload": response_payload,
                    "evidence_context": context,
                    "lens_tags": tags,
                    "ranking_context_available": manifest[
                        "ranking_context_available"
                    ],
                    "ranking_context_reason": manifest[
                        "ranking_context_reason"
                    ],
                    "metric_source_date": _latest_metric_source_date(evidence),
                    "ranking_as_of_date": evidence.ranking_context.get("as_of_date"),
                    "metric_pipeline_run_id": readiness.get("upstream_run_id"),
                    "model_version": model_version,
                    "ruleset_version": ruleset_version,
                }
                self.storage.save_snapshot(row)
                saved = self.storage.read_snapshot(manifest["capture_id"])
                if saved is None:
                    raise SnapshotStorageError("saved_snapshot_readback_missing")
                differences = verify_saved_snapshot(
                    saved,
                    expected_capture_id=manifest["capture_id"],
                    expected_learning_run_id=manifest["learning_run_id"],
                    expected_payload=response_payload,
                )
                if differences:
                    raise SnapshotStorageError(
                        f"saved_response_mismatch:{differences}"
                    )
                result["games_captured"] += 1
                result["game_results"].append({
                    "game_id": game_id,
                    "capture_id": manifest["capture_id"],
                    "learning_run_id": manifest["learning_run_id"],
                    "eligible": True,
                    "status": "success",
                    "reason": None,
                    "rebuilt": True,
                    "lens_tags": tags,
                    "saved_response_parity": "matched",
                })
                del row, context, response_payload, evidence
            except BigQueryBufferPending as exc:
                result["games_waiting"] += 1
                result["game_results"].append({
                    "game_id": game_id,
                    "capture_id": candidate["capture_id"],
                    "learning_run_id": candidate["learning_run_id"],
                    "eligible": True,
                    "status": "waiting",
                    "reason": f"bigquery_buffer:{exc}",
                    "rebuilt": False,
                })
            except Exception as exc:
                result["games_failed"] += 1
                result["game_results"].append({
                    "game_id": game_id,
                    "capture_id": candidate["capture_id"],
                    "learning_run_id": candidate["learning_run_id"],
                    "eligible": True,
                    "status": "failure",
                    "reason": str(exc),
                    "rebuilt": False,
                })

    @staticmethod
    def _finalize_status(result: dict) -> None:
        if result["games_failed"]:
            result["status"] = (
                "partial_failure"
                if result["games_captured"] or result["games_skipped"]
                else "failure"
            )
            result["reason"] = result.get("reason") or "one_or_more_games_failed"
        elif result["games_waiting"]:
            result["status"] = (
                "partial_failure" if result["games_captured"] else "waiting"
            )
            result["reason"] = result.get("reason") or "retryable_wait"
        elif result["games_captured"]:
            result["status"] = "success"
            result["reason"] = None
        elif result["games_checked"]:
            result["status"] = "no_op"
            game_reasons = {
                str(game_result.get("reason") or "").strip()
                for game_result in result.get("game_results", [])
                if str(game_result.get("reason") or "").strip()
            }
            if not result.get("reason"):
                result["reason"] = (
                    next(iter(game_reasons))
                    if len(game_reasons) == 1
                    else "no_eligible_uncaptured_games"
                )


def _latest_metric_source_date(evidence: GameDetailsEvidence) -> Optional[str]:
    dates = []
    for metrics in (evidence.away_metrics, evidence.home_metrics):
        for metric in metrics.values():
            if metric.get("data_date"):
                dates.append(str(metric["data_date"]))
    return max(dates) if dates else None
