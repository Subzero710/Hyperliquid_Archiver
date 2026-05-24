from __future__ import annotations

import time

from app.config import Settings
from app.storage.metadata_store import MetadataStore
from app.utils.json import dumps


def validate_once(settings: Settings) -> dict[str, object]:
    metadata_store = MetadataStore(settings.metadata_db_path)
    objects = metadata_store.latest_objects(limit=20)
    health = metadata_store.latest_health(limit=20)
    report = {
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
    return report


def validate_loop(settings: Settings) -> None:
    while True:
        validate_once(settings)
        time.sleep(settings.validator_loop_sleep_seconds)
