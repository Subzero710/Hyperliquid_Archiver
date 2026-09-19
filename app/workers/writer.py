from __future__ import annotations

import fcntl
import logging
import time
from pathlib import Path
from typing import IO

from app.config import Settings
from app.spool.durable_spool import DurableSpool
from app.storage.metadata_store import MetadataStore
from app.storage.object_store import ObjectStore
from app.utils.json import dumps
from app.writers.brut_writer import BrutWriter

logger = logging.getLogger("xyz_archiver.writer")


def run_writer(settings: Settings) -> None:
    writer_lock = _acquire_writer_lock(settings.archiver_state_dir / ".writer.lock")

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

    logger.info(
        "metadata_store_ready db=%s",
        settings.metadata_db_path,
    )

    spool = DurableSpool(
        root=settings.spool_dir,
        fsync_every_events=settings.recorder_fsync_every_events,
        segment_max_bytes=settings.recorder_segment_max_bytes,
        segment_max_age_seconds=settings.recorder_segment_max_age_seconds,
    )

    logger.info(
        "spool_ready sealed_dir=%s failed_dir=%s",
        spool.sealed_dir,
        spool.failed_dir,
    )

    removed_done = spool.cleanup_done_segments()
    if removed_done:
        logger.info("done_segments_cleaned count=%s", removed_done)

    brut_writer = BrutWriter(
        object_store=object_store,
        metadata_store=metadata_store,
    )

    pruned_rollups = brut_writer.prune_rollups()
    if pruned_rollups:
        logger.info("stale_rollups_pruned count=%s", pruned_rollups)

    _reconcile_failed_segments(
        spool=spool,
        brut_writer=brut_writer,
        metadata_store=metadata_store,
    )

    pruned_transactions = metadata_store.prune_applied_segment_objects(
        active_source_segments=spool.active_source_segments()
    )
    if pruned_transactions:
        logger.info("stale_applied_transactions_pruned count=%s", pruned_transactions)

    last_idle_log_ms = 0

    while True:
        wrote_any = False
        segments = spool.sealed_segments()

        if not segments:
            current_ms = int(time.time() * 1000)

            if current_ms - last_idle_log_ms >= 30_000:
                logger.info(
                    "writer_idle no_sealed_segments=true sealed_dir=%s",
                    spool.sealed_dir,
                )
                last_idle_log_ms = current_ms

            time.sleep(settings.writer_loop_sleep_seconds)
            continue

        logger.info(
            "sealed_segments_found count=%s",
            len(segments),
        )

        for segment in segments:
            segment_committed = False
            result = None

            try:
                logger.info(
                    "segment_start path=%s",
                    segment,
                )

                result = brut_writer.write_segment(segment)

                logger.info(
                    "segment_upload_done path=%s events=%s objects=%s skipped_events=%s",
                    segment,
                    len(result.events),
                    len(result.objects),
                    result.skipped_events,
                )

                for obj in result.objects:
                    logger.info(
                        "official_object_written key=%s kind=%s rows=%s min_event_ts_ms=%s max_event_ts_ms=%s",
                        obj.key,
                        obj.kind,
                        obj.row_count,
                        obj.min_event_ts_ms,
                        obj.max_event_ts_ms,
                    )

                segment_committed = True
                wrote_any = True
            except Exception as exc:
                logger.exception(
                    "segment_error path=%s",
                    segment,
                )

                metadata_store.record_health(
                    event_type="writer_error",
                    severity="error",
                    message=repr(exc),
                    details_json=dumps(
                        {
                            "segment": str(segment),
                        }
                    ),
                )

                failed_path = spool.mark_failed(segment)

                if isinstance(exc, RuntimeError) and (
                    str(exc).startswith("invalid JSON at ")
                    or str(exc).startswith("invalid raw event schema at ")
                    or str(exc).startswith("invalid raw event at ")
                ):
                    quarantine_path = spool.quarantine_failed(failed_path)
                    logger.warning(
                        "invalid_segment_quarantined source=%s destination=%s error=%r",
                        failed_path,
                        quarantine_path,
                        exc,
                    )
                else:
                    logger.error(
                        "segment_failed source=%s destination=%s error=%r",
                        segment,
                        failed_path,
                        exc,
                    )

            if not segment_committed:
                continue

            source_segment = segment.name
            try:
                spool.delete_processed(segment)

                logger.info(
                    "segment_cleaned source=%s events=%s objects=%s",
                    segment,
                    len(result.events) if result is not None else 0,
                    len(result.objects) if result is not None else 0,
                )
            except Exception as exc:
                logger.exception(
                    "segment_cleanup_error path=%s",
                    segment,
                )

                metadata_store.record_health(
                    event_type="segment_cleanup_error",
                    severity="error",
                    message=repr(exc),
                    details_json=dumps(
                        {
                            "segment": str(segment),
                        }
                    ),
                )

                done_path = spool.mark_done(segment)

                logger.error(
                    "segment_retained_after_cleanup_error source=%s destination=%s error=%r",
                    segment,
                    done_path,
                    exc,
                )
                continue

            try:
                metadata_store.delete_segment_objects(source_segment=source_segment)
            except Exception as exc:
                logger.exception("segment_transaction_cleanup_error source_segment=%s", source_segment)
                metadata_store.record_health(
                    event_type="segment_transaction_cleanup_error",
                    severity="error",
                    message=repr(exc),
                    details_json=dumps({"source_segment": source_segment}),
                )

        if not wrote_any:
            time.sleep(settings.writer_loop_sleep_seconds)

    _ = writer_lock


def _reconcile_failed_segments(
    *,
    spool: DurableSpool,
    brut_writer: BrutWriter,
    metadata_store: MetadataStore,
) -> None:
    pending_sources = {
        str(row["source_segment"])
        for row in metadata_store.pending_segment_objects()
    }

    for failed in spool.failed_segments():
        source_segment = failed.name.replace(".failed.jsonl", ".sealed.jsonl")

        if source_segment in pending_sources:
            destination = spool.requeue_failed(failed)
            logger.warning(
                "failed_segment_requeued_pending_transaction source=%s destination=%s",
                failed,
                destination,
            )
            continue

        try:
            state = brut_writer.classify_failed_segment(failed)
        except Exception as exc:
            logger.exception("failed_segment_reconcile_error path=%s", failed)
            destination = spool.quarantine_failed(failed)
            metadata_store.record_health(
                event_type="historical_segment_quarantined",
                severity="warning",
                message=repr(exc),
                details_json=dumps(
                    {
                        "source": str(failed),
                        "quarantine": str(destination),
                        "reason": "unreadable_or_invalid_segment",
                    }
                ),
            )
            logger.warning(
                "failed_segment_quarantined source=%s destination=%s error=%r",
                failed,
                destination,
                exc,
            )
            continue

        if state == "already_archived":
            spool.delete_failed(failed)
            logger.info("failed_segment_already_archived_removed path=%s", failed)
            continue

        if state == "not_archived":
            destination = spool.requeue_failed(failed)
            logger.info(
                "failed_segment_requeued source=%s destination=%s",
                failed,
                destination,
            )
            continue

        destination = spool.quarantine_failed(failed)
        metadata_store.record_health(
            event_type="historical_segment_quarantined",
            severity="warning",
            message="failed segment is partially represented in the official archive",
            details_json=dumps(
                {
                    "source": str(failed),
                    "quarantine": str(destination),
                    "reason": "partial_or_ambiguous_archive_state",
                }
            ),
        )
        logger.warning(
            "failed_segment_partial_archive_quarantined source=%s destination=%s",
            failed,
            destination,
        )


def _acquire_writer_lock(path: Path) -> IO[str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise RuntimeError(f"another writer process already owns {path}") from exc
    return handle
