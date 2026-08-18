"""Thin Packet 4 coordinator over the proven grade, Level 2, and Level 3 boundaries."""

from __future__ import annotations

from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Callable, Mapping, Sequence


STAGE_ORDER = ("game_grade", "level2_validation", "level3_features")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.isoformat()


def _grade_executor(**kwargs):
    from run_gamelens_packet4_grade_write import execute_packet4_grade_write

    return execute_packet4_grade_write(**kwargs)


def _level2_executor(**kwargs):
    from run_gamelens_packet4_level2_write import execute_packet4_level2_write

    return execute_packet4_level2_write(**kwargs)


def _level3_executor(**kwargs):
    from run_gamelens_packet4_level3_write import execute_packet4_level3_write

    return execute_packet4_level3_write(**kwargs)


def _empty_counts() -> dict[str, int]:
    return {
        "input_count": 0,
        "output_count": 0,
        "inserted_count": 0,
        "updated_count": 0,
        "unchanged_count": 0,
        "conflict_count": 0,
        "unavailable_count": 0,
        "rejected_count": 0,
    }


def _stage_counts(stage_name: str, result: Mapping[str, Any]) -> dict[str, int]:
    reconciliation = result.get("reconciliation") or {}
    counts = _empty_counts()
    if stage_name == "game_grade":
        source = result.get("source_counts") or {}
        counts.update(
            input_count=int(source.get("canonical_snapshot_rows") or 0),
            output_count=int(reconciliation.get("grades_out") or 0),
            inserted_count=int(reconciliation.get("inserted") or 0),
            unchanged_count=int(reconciliation.get("unchanged") or 0),
            conflict_count=int(reconciliation.get("conflicts") or 0),
        )
    elif stage_name == "level2_validation":
        source = result.get("source_counts") or {}
        counts.update(
            input_count=int(source.get("claims") or 0),
            output_count=int(reconciliation.get("validations_in") or 0),
            updated_count=int(reconciliation.get("updated") or 0),
            unchanged_count=int(reconciliation.get("unchanged") or 0),
            conflict_count=int(reconciliation.get("conflicts") or 0),
            unavailable_count=int(
                (result.get("by_validation_result") or {}).get("unavailable") or 0
            ),
            rejected_count=int(reconciliation.get("rejected") or 0),
        )
    elif stage_name == "level3_features":
        source = result.get("source_counts") or {}
        counts.update(
            input_count=int(source.get("claims") or 0),
            output_count=int(reconciliation.get("features_in") or 0),
            updated_count=int(reconciliation.get("updated") or 0),
            unchanged_count=int(reconciliation.get("unchanged") or 0),
            conflict_count=int(reconciliation.get("conflicts") or 0),
            unavailable_count=int(source.get("level2_unavailable") or 0),
            rejected_count=int(reconciliation.get("rejected") or 0),
        )
    return counts


def _result_status(stage_name: str, result: Mapping[str, Any]) -> tuple[str, Any]:
    if stage_name == "game_grade":
        return "success", None
    if stage_name == "level2_validation":
        worker = result.get("validation_status")
        reason = result.get("validation_reason")
    else:
        worker = result.get("feature_status")
        reason = result.get("feature_reason")
    if worker in {"completed", "success"}:
        return "success", reason
    if worker == "no_op":
        return "no_op", reason
    if worker in {"deferred", "waiting"}:
        return "skipped", reason
    return "failed", reason or f"{stage_name}_unexpected_status"


def _identity(result: Mapping[str, Any], stage_name: str) -> dict[str, Any]:
    if stage_name == "game_grade":
        capture = result.get("capture") or {}
        return {
            "learning_run_id": capture.get("learning_run_id"),
            "capture_id": capture.get("capture_id"),
            "pipeline_run_id": capture.get("pipeline_run_id"),
            "source_payload_sha256": capture.get("source_payload_sha256"),
        }
    return {
        "learning_run_id": result.get("learning_run_id"),
        "capture_id": result.get("capture_id"),
        "pipeline_run_id": None,
        "source_payload_sha256": None,
    }


def _is_retryable_exception(exc: Exception) -> bool:
    text = str(exc).casefold()
    non_retryable = ("conflict", "disagrees", "only in dev", "required")
    return not any(token in text for token in non_retryable)


def _run_stage(
    *,
    stage_name: str,
    stage_order: int,
    game_id: str,
    executor: Callable[..., Mapping[str, Any]],
    kwargs: dict[str, Any],
    log_reference: str | None,
    now: Callable[[], datetime],
) -> dict[str, Any]:
    started = now()
    clock = perf_counter()
    try:
        result = dict(executor(**kwargs))
        status, reason = _result_status(stage_name, result)
        identity = _identity(result, stage_name)
        return {
            "game_id": game_id,
            "stage_name": stage_name,
            "stage_order": stage_order,
            "status": status,
            "reason": reason,
            **identity,
            "started_at": _iso(started),
            "finished_at": _iso(now()),
            "duration_ms": max(0, int((perf_counter() - clock) * 1000)),
            **_stage_counts(stage_name, result),
            "write_performed": bool(result.get("write_performed")),
            "failed_boundary": stage_name if status == "failed" else None,
            "retryable": status == "failed",
            "exception_class": None,
            "message": None,
            "log_reference": log_reference if status == "failed" else None,
            "details": result,
        }
    except Exception as exc:
        return {
            "game_id": game_id,
            "stage_name": stage_name,
            "stage_order": stage_order,
            "status": "failed",
            "reason": f"{stage_name}_exception",
            "learning_run_id": None,
            "capture_id": None,
            "pipeline_run_id": None,
            "source_payload_sha256": None,
            "started_at": _iso(started),
            "finished_at": _iso(now()),
            "duration_ms": max(0, int((perf_counter() - clock) * 1000)),
            **_empty_counts(),
            "write_performed": False,
            "failed_boundary": stage_name,
            "retryable": _is_retryable_exception(exc),
            "exception_class": type(exc).__name__,
            "message": str(exc),
            "log_reference": log_reference,
            "details": {},
        }


def _skipped_stage(
    *, game_id: str, stage_name: str, stage_order: int, reason: str,
    identity: Mapping[str, Any], failed_boundary: str | None,
    retryable: bool, now: Callable[[], datetime]
) -> dict[str, Any]:
    timestamp = _iso(now())
    return {
        "game_id": game_id,
        "stage_name": stage_name,
        "stage_order": stage_order,
        "status": "skipped",
        "reason": reason,
        **identity,
        "started_at": timestamp,
        "finished_at": timestamp,
        "duration_ms": 0,
        **_empty_counts(),
        "write_performed": False,
        "failed_boundary": failed_boundary,
        "retryable": retryable,
        "exception_class": None,
        "message": None,
        "log_reference": None,
        "details": {},
    }


def _assert_identity(expected: Mapping[str, Any], actual: Mapping[str, Any]) -> None:
    for field in ("learning_run_id", "capture_id"):
        if expected.get(field) != actual.get(field):
            raise ValueError(f"Packet 4 coordinator {field} disagrees")


def build_packet4_receipt_rows(summary: Mapping[str, Any]) -> list[dict[str, Any]]:
    base = {
        "attempt_id": summary["attempt_id"],
        "started_at": summary["started_at"],
        "finished_at": summary["finished_at"],
        "duration_ms": summary["duration_ms"],
        "log_reference": summary.get("log_reference"),
    }
    stages = [
        stage
        for game in summary["game_results"]
        for stage in game["stages"]
    ]
    first_failure = next(
        (stage for stage in stages if stage["status"] == "failed"), None
    )
    rows = [
        {
            **base,
            "receipt_scope": "attempt",
            "game_id": None,
            "stage_name": None,
            "stage_order": None,
            "status": summary["status"],
            "reason": summary.get("reason"),
            "learning_run_id": None,
            "capture_id": None,
            "pipeline_run_id": None,
            "source_payload_sha256": None,
            "input_count": summary["counts"]["games_requested"],
            "output_count": summary["counts"]["games_completed"],
            "inserted_count": sum(stage["inserted_count"] for stage in stages),
            "updated_count": sum(stage["updated_count"] for stage in stages),
            "unchanged_count": sum(stage["unchanged_count"] for stage in stages),
            "conflict_count": sum(stage["conflict_count"] for stage in stages),
            "unavailable_count": sum(
                stage["unavailable_count"] for stage in stages
            ),
            "rejected_count": sum(stage["rejected_count"] for stage in stages),
            "write_performed": summary["learning_write_performed"],
            "failed_boundary": summary.get("failed_boundary"),
            "retryable": any(
                stage["status"] == "failed" and stage["retryable"]
                for stage in stages
            ),
            "exception_class": (
                first_failure.get("exception_class") if first_failure else None
            ),
            "message": first_failure.get("message") if first_failure else None,
            "details": {"counts": summary["counts"], "funnel": summary["funnel"]},
        }
    ]
    for game in summary["game_results"]:
        for stage in game["stages"]:
            rows.append(
                {
                    **base,
                    **stage,
                    "receipt_scope": "game_stage",
                    "details": stage.get("details") or {},
                }
            )
    return rows


def run_packet4_coordinator(
    *,
    client: Any,
    bigquery: Any,
    runtime_config: Any,
    game_ids: Sequence[str],
    attempt_id: str,
    receipt_storage: Any,
    grade_executor: Callable[..., Mapping[str, Any]] = _grade_executor,
    level2_executor: Callable[..., Mapping[str, Any]] = _level2_executor,
    level3_executor: Callable[..., Mapping[str, Any]] = _level3_executor,
    log_reference: str | None = None,
    now: Callable[[], datetime] = _utc_now,
) -> dict[str, Any]:
    if not runtime_config.is_dev:
        raise ValueError("Packet 4 coordinator writes are allowed only in dev")
    attempt = str(attempt_id or "").strip()
    if not attempt:
        raise ValueError("attempt_id is required")
    games = [str(game_id or "").strip() for game_id in game_ids]
    if not games or any(not game_id for game_id in games):
        raise ValueError("at least one game_id is required")
    if len(set(games)) != len(games):
        raise ValueError("duplicate Packet 4 coordinator game_id")

    started = now()
    clock = perf_counter()
    game_results = []
    for game_id in games:
        stages = []
        grade = _run_stage(
            stage_name="game_grade",
            stage_order=1,
            game_id=game_id,
            executor=grade_executor,
            kwargs={
                "client": client,
                "bigquery": bigquery,
                "runtime_config": runtime_config,
                "game_id": game_id,
                "attempt_id": f"{attempt}:{game_id}:grade",
            },
            log_reference=log_reference,
            now=now,
        )
        stages.append(grade)
        identity = {
            field: grade.get(field)
            for field in (
                "learning_run_id",
                "capture_id",
                "pipeline_run_id",
                "source_payload_sha256",
            )
        }
        if grade["status"] == "failed":
            stages.extend(
                [
                    _skipped_stage(
                        game_id=game_id,
                        stage_name=name,
                        stage_order=index,
                        reason="game_grade_failed",
                        identity=identity,
                        failed_boundary="game_grade",
                        retryable=grade["retryable"],
                        now=now,
                    )
                    for index, name in enumerate(STAGE_ORDER[1:], start=2)
                ]
            )
        else:
            level2 = _run_stage(
                stage_name="level2_validation",
                stage_order=2,
                game_id=game_id,
                executor=level2_executor,
                kwargs={
                    "client": client,
                    "bigquery": bigquery,
                    "runtime_config": runtime_config,
                    "game_id": game_id,
                    "attempt_id": f"{attempt}:{game_id}:level2",
                },
                log_reference=log_reference,
                now=now,
            )
            try:
                if level2["status"] != "failed":
                    _assert_identity(identity, level2)
            except Exception as exc:
                level2.update(
                    status="failed",
                    reason="level2_identity_mismatch",
                    failed_boundary="level2_validation",
                    retryable=False,
                    exception_class=type(exc).__name__,
                    message=str(exc),
                    log_reference=log_reference,
                )
            stages.append(level2)
            if level2["status"] in {"failed", "skipped"}:
                stages.append(
                    _skipped_stage(
                        game_id=game_id,
                        stage_name="level3_features",
                        stage_order=3,
                        reason=(
                            "level2_validation_failed"
                            if level2["status"] == "failed"
                            else "level2_validation_deferred"
                        ),
                        identity=identity,
                        failed_boundary=(
                            "level2_validation"
                            if level2["status"] == "failed"
                            else None
                        ),
                        retryable=level2["retryable"],
                        now=now,
                    )
                )
            else:
                level3 = _run_stage(
                    stage_name="level3_features",
                    stage_order=3,
                    game_id=game_id,
                    executor=level3_executor,
                    kwargs={
                        "client": client,
                        "bigquery": bigquery,
                        "runtime_config": runtime_config,
                        "game_id": game_id,
                        "attempt_id": f"{attempt}:{game_id}:level3",
                    },
                    log_reference=log_reference,
                    now=now,
                )
                try:
                    if level3["status"] != "failed":
                        _assert_identity(identity, level3)
                except Exception as exc:
                    level3.update(
                        status="failed",
                        reason="level3_identity_mismatch",
                        failed_boundary="level3_features",
                        retryable=False,
                        exception_class=type(exc).__name__,
                        message=str(exc),
                        log_reference=log_reference,
                    )
                stages.append(level3)

        failed = [stage for stage in stages if stage["status"] == "failed"]
        no_op_reason = next(
            (
                stage["reason"]
                for stage in reversed(stages)
                if stage["status"] == "no_op" and stage.get("reason")
            ),
            None,
        )
        game_results.append(
            {
                "game_id": game_id,
                **identity,
                "status": "failed" if failed else "success",
                "reason": failed[0]["reason"] if failed else no_op_reason,
                "failed_boundary": failed[0]["stage_name"] if failed else None,
                "learning_write_performed": any(
                    stage["write_performed"] for stage in stages
                ),
                "stages": stages,
            }
        )

    failures = [game for game in game_results if game["status"] == "failed"]
    completed = len(game_results) - len(failures)
    status = (
        "failure"
        if failures and not completed
        else "partial_failure"
        if failures
        else "success"
    )
    finished = now()
    summary = {
        "environment": "dev",
        "access_mode": "development_write",
        "attempt_id": attempt,
        "status": status,
        "reason": "one_or_more_games_failed" if failures else None,
        "failed_boundary": failures[0]["failed_boundary"] if failures else None,
        "started_at": _iso(started),
        "finished_at": _iso(finished),
        "duration_ms": max(0, int((perf_counter() - clock) * 1000)),
        "log_reference": log_reference,
        "counts": {
            "games_requested": len(games),
            "games_completed": completed,
            "games_failed": len(failures),
        },
        "learning_write_performed": any(
            game["learning_write_performed"] for game in game_results
        ),
        "funnel": [
            {
                "game_id": game["game_id"],
                "capture": bool(game.get("capture_id")),
                "final": game["stages"][0]["status"] != "failed",
                "facts": game["stages"][1]["status"] not in {"failed", "skipped"},
                "claims": game["stages"][1]["input_count"],
                "grade": game["stages"][0]["status"],
                "level2": game["stages"][1]["status"],
                "level3": game["stages"][2]["status"],
                "final_status": game["status"],
                "reason": game["reason"],
            }
            for game in game_results
        ],
        "game_results": game_results,
    }
    receipt_rows = build_packet4_receipt_rows(summary)
    try:
        receipt_reconciliation = receipt_storage.store_receipts(
            rows=receipt_rows, attempt_id=attempt
        )
    except Exception as exc:
        summary["receipt_reconciliation"] = {
            "status": "failed",
            "reason": "receipt_storage_failed",
            "exception_class": type(exc).__name__,
            "message": str(exc),
            "write_performed": False,
        }
        if summary["status"] == "success":
            summary["status"] = "partial_failure"
            summary["reason"] = "receipt_storage_failed"
        return summary
    summary["receipt_reconciliation"] = receipt_reconciliation
    return summary
