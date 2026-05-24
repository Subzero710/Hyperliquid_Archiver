from __future__ import annotations

import logging
import time

from app.config import Settings
from app.storage.metadata_store import MetadataStore
from app.utils.json import dumps

logger = logging.getLogger("xyz_archiver.validator")


def validate_once(settings: Settings) -> dict[str, object]:
    metadata_store = MetadataStore(settings.metadata_db_path)

    objects = metadata_store.latest_objects(limit=20)
    health = metadata_store.latest_health(limit=20)

    report: dict[str, object] = {
        "object_count": metadata_store.object_count(),
        "latest_objects": objects,
        "latest_health": health,
    }

    metadata_store.record_health(
        event_type="validator_report",
        severity="info",
        message="validator completed",
        details_json=dumps(report),
    )

    logger.info(
        "validate_once object_count=%s latest_objects=%s latest_health=%s",
        report["object_count"],
        len(objects),
        len(health),
    )

    return report


def validate_loop(settings: Settings) -> None:
    logger.info(
        "validator_start db=%s interval_s=%s",
        settings.metadata_db_path,
        settings.validator_loop_sleep_seconds,
    )

    while True:
        try:
            report = validate_once(settings)
            logger.info(
                "validator_heartbeat object_count=%s",
                report["object_count"],
            )
        except Exception:
            logger.exception("validator_error")

        time.sleep(settings.validator_loop_sleep_seconds)