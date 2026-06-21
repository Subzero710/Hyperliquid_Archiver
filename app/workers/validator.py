from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from app.config import Settings
from app.storage.metadata_store import MetadataStore
from app.storage.object_store import ObjectStore

logger = logging.getLogger("xyz_archiver.validator")


def validate_once(settings: Settings, *, started_at_s: float | None = None) -> dict[str, Any]:
    now_s = time.time()
    started_at_s = _validator_started_at_s(settings=settings, override=started_at_s)
    runtime_s = max(0, int(now_s - started_at_s))

    spool_dir = settings.spool_dir
    open_dir = spool_dir / "open"
    sealed_dir = spool_dir / "sealed"
    done_dir = spool_dir / "done"
    failed_dir = spool_dir / "failed"

    open_segments = _list_files(open_dir)
    sealed_segments = _list_files(sealed_dir)
    done_segments = _list_files(done_dir)
    failed_segments = _list_files(failed_dir)

    checks: list[dict[str, Any]] = []

    object_store_ok = True
    if settings.validator_verify_object_store:
        try:
            ObjectStore(settings).ensure_bucket()
        except Exception as exc:
            object_store_ok = False
            checks.append(
                {
                    "name": "object_store_reachable",
                    "status": "error",
                    "detail": repr(exc),
                }
            )

    metadata_ok = True
    object_count = 0
    latest_objects: list[dict[str, Any]] = []
    latest_health: list[dict[str, Any]] = []

    try:
        metadata_store = MetadataStore(settings.metadata_db_path)
        object_count = metadata_store.object_count()
        latest_objects = metadata_store.latest_objects(limit=10)
        latest_health = metadata_store.latest_health(limit=10)
    except Exception as exc:
        metadata_ok = False
        checks.append(
            {
                "name": "metadata_store_readable",
                "status": "error",
                "detail": repr(exc),
            }
        )

    failed_segments_count = len(failed_segments)
    if failed_segments_count > settings.validator_max_failed_segments:
        checks.append(
            {
                "name": "failed_segments",
                "status": "error",
                "count": failed_segments_count,
                "max": settings.validator_max_failed_segments,
                "files": [str(path) for path in failed_segments[:10]],
            }
        )

    if len(sealed_segments) > settings.validator_max_sealed_segments:
        checks.append(
            {
                "name": "sealed_backlog",
                "status": "error",
                "count": len(sealed_segments),
                "max": settings.validator_max_sealed_segments,
            }
        )

    oldest_sealed_age_s = _oldest_age_s(sealed_segments, now_s)
    if oldest_sealed_age_s is not None and oldest_sealed_age_s > settings.validator_writer_stale_seconds:
        checks.append(
            {
                "name": "writer_stale",
                "status": "error",
                "oldest_sealed_age_s": oldest_sealed_age_s,
                "max_age_s": settings.validator_writer_stale_seconds,
            }
        )

    newest_open_age_s = _newest_age_s(open_segments, now_s)
    if newest_open_age_s is None:
        if runtime_s > settings.validator_startup_grace_seconds:
            checks.append(
                {
                    "name": "recorder_open_segment",
                    "status": "error",
                    "detail": "no open segment after startup grace",
                }
            )
    elif newest_open_age_s > settings.validator_recorder_stale_seconds:
        checks.append(
            {
                "name": "recorder_stale",
                "status": "error",
                "newest_open_age_s": newest_open_age_s,
                "max_age_s": settings.validator_recorder_stale_seconds,
            }
        )

    if (
        settings.validator_require_objects_after_grace
        and runtime_s > settings.validator_startup_grace_seconds
        and object_count <= 0
    ):
        checks.append(
            {
                "name": "no_uploaded_objects",
                "status": "error",
                "runtime_s": runtime_s,
                "startup_grace_s": settings.validator_startup_grace_seconds,
            }
        )

    errors = [check for check in checks if check.get("status") == "error"]
    status = "ok" if not errors and metadata_ok and object_store_ok else "error"

    report = {
        "status": status,
        "runtime_s": runtime_s,
        "object_store_ok": object_store_ok,
        "metadata_ok": metadata_ok,
        "object_count": object_count,
        "spool": {
            "open": len(open_segments),
            "sealed": len(sealed_segments),
            "done": len(done_segments),
            "failed": len(failed_segments),
            "oldest_sealed_age_s": oldest_sealed_age_s,
            "newest_open_age_s": newest_open_age_s,
        },
        "checks": checks,
        "latest_objects": latest_objects,
        "latest_health": latest_health,
    }

    if metadata_ok:
        try:
            metadata_store.record_health(
                event_type="validator_report",
                severity="info" if status == "ok" else "error",
                message=f"validator status={status}",
                details_json=json.dumps(report, sort_keys=True, default=str),
            )
        except Exception:
            logger.exception("validator_record_health_error")

    if status == "ok":
        logger.info(
            "validator_ok object_count=%s open=%s sealed=%s done=%s failed=%s",
            object_count,
            len(open_segments),
            len(sealed_segments),
            len(done_segments),
            len(failed_segments),
        )
    else:
        logger.error("validator_error report=%s", json.dumps(report, sort_keys=True, default=str))

    return report


def validate_loop(settings: Settings) -> None:
    started_at_s = _validator_started_at_s(settings=settings, override=None)

    logger.info(
        "validator_start db=%s interval_s=%s grace_s=%s",
        settings.metadata_db_path,
        settings.validator_loop_sleep_seconds,
        settings.validator_startup_grace_seconds,
    )

    while True:
        validate_once(settings, started_at_s=started_at_s)
        time.sleep(settings.validator_loop_sleep_seconds)


def _validator_started_at_s(*, settings: Settings, override: float | None) -> float:
    if override is not None:
        return override

    marker = settings.archiver_state_dir / ".validator_started_at"
    settings.archiver_state_dir.mkdir(parents=True, exist_ok=True)

    if marker.exists():
        try:
            return float(marker.read_text(encoding="utf-8").strip())
        except ValueError:
            pass

    started_at_s = time.time()
    marker.write_text(str(started_at_s), encoding="utf-8")
    return started_at_s


def _list_files(path: Path) -> list[Path]:
    if not path.exists():
        return []

    return sorted(item for item in path.iterdir() if item.is_file())


def _oldest_age_s(paths: list[Path], now_s: float) -> int | None:
    if not paths:
        return None

    oldest_mtime = min(path.stat().st_mtime for path in paths)
    return max(0, int(now_s - oldest_mtime))


def _newest_age_s(paths: list[Path], now_s: float) -> int | None:
    if not paths:
        return None

    newest_mtime = max(path.stat().st_mtime for path in paths)
    return max(0, int(now_s - newest_mtime))