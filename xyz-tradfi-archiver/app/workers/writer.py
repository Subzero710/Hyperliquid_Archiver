from __future__ import annotations

import time

from app.config import Settings
from app.spool.durable_spool import DurableSpool
from app.storage.metadata_store import MetadataStore
from app.storage.object_store import ObjectStore
from app.utils.json import dumps
from app.writers.brut_writer import BrutWriter
from app.writers.parquet_writer import ParquetWriter


def run_writer(settings: Settings) -> None:
    object_store = ObjectStore(settings)
    object_store.ensure_bucket()
    metadata_store = MetadataStore(settings.metadata_db_path)
    spool = DurableSpool(
        root=settings.spool_dir,
        fsync_every_events=settings.recorder_fsync_every_events,
        segment_max_bytes=settings.recorder_segment_max_bytes,
        segment_max_age_seconds=settings.recorder_segment_max_age_seconds,
    )
    brut_writer = BrutWriter(object_store=object_store, metadata_store=metadata_store)
    parquet_writer = ParquetWriter(object_store=object_store, metadata_store=metadata_store)

    while True:
        wrote_any = False
        for segment in spool.sealed_segments():
            try:
                events = brut_writer.write_segment(segment)
                parquet_writer.write_events(events=events, source_segment=segment.name)
                spool.mark_done(segment)
                wrote_any = True
            except Exception as exc:
                metadata_store.record_health(
                    event_type="writer_error",
                    severity="error",
                    message=repr(exc),
                    details_json=dumps({"segment": str(segment)}),
                )
                spool.mark_failed(segment)
        if not wrote_any:
            time.sleep(settings.writer_loop_sleep_seconds)
