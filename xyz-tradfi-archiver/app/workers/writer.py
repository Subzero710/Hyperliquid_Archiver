from __future__ import annotations

import logging
import time

from app.config import Settings
from app.spool.durable_spool import DurableSpool
from app.storage.metadata_store import MetadataStore
from app.storage.object_store import ObjectStore
from app.utils.json import dumps
from app.writers.brut_writer import BrutWriter
from app.writers.parquet_writer import ParquetWriter

logger = logging.getLogger("xyz_archiver.writer")


def run_writer(settings: Settings) -> None:
    logger.info(
        "writer_start run_id=%s spool_dir=%s bucket=%s endpoint=%s db=%s",
        settings.archiver_run_id,
        settings.spool_dir,
        settings.archive_bucket,
        settings.archive_s3_endpoint,
        settings.metadata_db_path,
    )

    object_store = ObjectStore(settings)

    logger.info(
        "object_store_check_start bucket=%s endpoint=%s verify=%s",
        settings.archive_bucket,
        settings.archive_s3_endpoint,
        settings.archive_s3_verify_bucket,
    )

    object_store.ensure_bucket()

    logger.info(
        "object_store_ready bucket=%s endpoint=%s",
        settings.archive_bucket,
        settings.archive_s3_endpoint,
    )

    metadata_store = MetadataStore(settings.metadata_db_path)

    logger.info("metadata_store_ready db=%s", settings.metadata_db_path)

    spool = DurableSpool(
        root=settings.spool_dir,
        fsync_every_events=settings.recorder_fsync_every_events,
        segment_max_bytes=settings.recorder_segment_max_bytes,
        segment_max_age_seconds=settings.recorder_segment_max_age_seconds,
    )

    logger.info(
        "spool_ready sealed_dir=%s done_dir=%s failed_dir=%s",
        spool.sealed_dir,
        spool.done_dir,
        spool.failed_dir,
    )

    brut_writer = BrutWriter(object_store=object_store, metadata_store=metadata_store)
    parquet_writer = ParquetWriter(object_store=object_store, metadata_store=metadata_store)

    last_idle_log_ms = 0

    while True:
        wrote_any = False
        segments = spool.sealed_segments()

        if not segments:
            current_ms = int(time.time() * 1000)
            if current_ms - last_idle_log_ms >= 30_000:
                logger.info("writer_idle no_sealed_segments=true sealed_dir=%s", spool.sealed_dir)
                last_idle_log_ms = current_ms

            time.sleep(settings.writer_loop_sleep_seconds)
            continue

        logger.info("sealed_segments_found count=%s", len(segments))

        for segment in segments:
            try:
                logger.info("segment_start path=%s", segment)

                events = brut_writer.write_segment(segment)

                logger.info(
                    "segment_brut_done path=%s events=%s",
                    segment,
                    len(events),
                )

                parquet_writer.write_events(events=events, source_segment=segment.name)

                logger.info(
                    "segment_parquet_done path=%s events=%s",
                    segment,
                    len(events),
                )

                done_path = spool.mark_done(segment)

                logger.info(
                    "segment_done source=%s destination=%s events=%s",
                    segment,
                    done_path,
                    len(events),
                )

                wrote_any = True

            except Exception as exc:
                logger.exception("segment_error path=%s", segment)

                metadata_store.record_health(
                    event_type="writer_error",
                    severity="error",
                    message=repr(exc),
                    details_json=dumps({"segment": str(segment)}),
                )

                failed_path = spool.mark_failed(segment)

                logger.error(
                    "segment_failed source=%s destination=%s error=%r",
                    segment,
                    failed_path,
                    exc,
                )

        if not wrote_any:
            time.sleep(settings.writer_loop_sleep_seconds)