from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from app.config import Settings
from app.domain.events import make_raw_event
from app.hyperliquid.driver import HyperliquidInfoDriver
from app.hyperliquid.symbols import HyperliquidSymbol, discover_symbols
from app.hyperliquid.websocket_recorder import HyperliquidWebSocketRecorder
from app.spool.durable_spool import DurableSpool
from app.utils.time import now_ms

logger = logging.getLogger("xyz_archiver.recorder")


@dataclass
class PollSchedule:
    name: str
    every_s: int
    action: Callable[[], None]
    next_at_ms: int

    def __post_init__(self) -> None:
        self.every_s = max(1, self.every_s)
        self.next_at_ms = max(0, self.next_at_ms)

    def maybe_run(self, current_ms: int) -> bool:
        if current_ms < self.next_at_ms:
            return False

        self.next_at_ms = current_ms + self.every_s * 1000
        self.action()
        return True


def _delay_ms(*, current_ms: int, delay_s: int) -> int:
    return current_ms + max(0, delay_s) * 1000


def run_recorder(settings: Settings) -> None:
    settings.archiver_state_dir.mkdir(parents=True, exist_ok=True)

    spool = DurableSpool(
        root=settings.spool_dir,
        fsync_every_events=settings.recorder_fsync_every_events,
        segment_max_bytes=settings.recorder_segment_max_bytes,
        segment_max_age_seconds=settings.recorder_segment_max_age_seconds,
    )

    logger.info(
        "recorder_start run_id=%s dex=%s state_dir=%s spool_dir=%s",
        settings.archiver_run_id,
        settings.hyperliquid_dex,
        settings.archiver_state_dir,
        settings.spool_dir,
    )

    logger.info(
        "spool_ready open_dir=%s sealed_dir=%s done_dir=%s failed_dir=%s",
        spool.open_dir,
        spool.sealed_dir,
        spool.done_dir,
        spool.failed_dir,
    )

    driver = HyperliquidInfoDriver(
        base_url=settings.hyperliquid_base_url,
        timeout_s=settings.hyperliquid_http_timeout_s,
    )

    logger.info(
        "hyperliquid_driver_ready base_url=%s timeout_s=%s",
        settings.hyperliquid_base_url,
        settings.hyperliquid_http_timeout_s,
    )

    state = RecorderState(settings=settings, driver=driver, spool=spool)
    state.refresh_symbols()

    ws_recorder = HyperliquidWebSocketRecorder(
        websocket_url=settings.hyperliquid_websocket_url,
        run_id=settings.archiver_run_id,
        dex=settings.hyperliquid_dex,
        emit=spool.append,
    )

    ws_recorder.start()

    initial_symbols = [symbol.name for symbol in state.symbols_or_empty()]
    ws_recorder.ensure_subscriptions(initial_symbols)

    logger.info(
        "websocket_started url=%s l2_subscriptions=%s",
        settings.hyperliquid_websocket_url,
        len(initial_symbols),
    )

    start_ms = now_ms()
    schedules: list[PollSchedule] = [
        PollSchedule(
            name="meta_asset_ctxs",
            every_s=settings.poll_meta_asset_ctxs_seconds,
            action=state.poll_meta_asset_ctxs,
            next_at_ms=start_ms,
        ),
    ]

    if settings.archive_enable_l2_snapshots:
        schedules.append(
            PollSchedule(
                name="l2_snapshot",
                every_s=settings.poll_l2_snapshot_seconds,
                action=state.poll_l2_snapshots,
                next_at_ms=_delay_ms(
                    current_ms=start_ms,
                    delay_s=settings.archive_startup_l2_delay_seconds,
                ),
            )
        )
    else:
        logger.info("poll_disabled datatype=l2_snapshot")

    for schedule in schedules:
        logger.info(
            "poll_schedule name=%s every_s=%s next_at_ms=%s",
            schedule.name,
            schedule.every_s,
            schedule.next_at_ms,
        )

    last_heartbeat_ms = 0
    last_subscription_sync_ms = 0

    try:
        while True:
            current_ms = now_ms()

            for schedule in schedules:
                try:
                    if schedule.maybe_run(current_ms):
                        logger.debug("poll_ran datatype=%s", schedule.name)
                except Exception as exc:
                    logger.exception("poll_error datatype=%s", schedule.name)
                    spool.append(
                        make_raw_event(
                            run_id=settings.archiver_run_id,
                            dex=settings.hyperliquid_dex,
                            datatype="health",
                            payload={
                                "event_type": "poll_error",
                                "poll": schedule.name,
                                "error": repr(exc),
                            },
                            event_ts_ms=now_ms(),
                        )
                    )

            if current_ms - last_subscription_sync_ms >= 60_000:
                symbols = [symbol.name for symbol in state.symbols_or_empty()]
                ws_recorder.ensure_subscriptions(symbols)
                logger.debug("websocket_subscription_sync count=%s", len(symbols))
                last_subscription_sync_ms = current_ms

            if current_ms - last_heartbeat_ms >= 60_000:
                logger.info(
                    "recorder_heartbeat run_id=%s symbols=%s open_segment=%s",
                    settings.archiver_run_id,
                    len(state.symbols_or_empty()),
                    spool._current_path,
                )
                last_heartbeat_ms = current_ms

            time.sleep(settings.recorder_idle_sleep_seconds)
    finally:
        logger.info("recorder_stopping")
        ws_recorder.stop()
        spool.rotate()
        logger.info("recorder_stopped")


@dataclass
class RecorderState:
    settings: Settings
    driver: HyperliquidInfoDriver
    spool: DurableSpool
    symbols: list[HyperliquidSymbol] | None = None
    meta_payload: Any = None
    contexts_payload: Any = None

    def symbols_or_empty(self) -> list[HyperliquidSymbol]:
        return self.symbols or []

    def refresh_symbols(self) -> None:
        logger.info(
            "symbols_refresh_start dex=%s allowlist_count=%s",
            self.settings.hyperliquid_dex,
            len(self.settings.symbol_allowlist),
        )

        symbols, meta, contexts = discover_symbols(
            driver=self.driver,
            dex=self.settings.hyperliquid_dex,
            allowlist=self.settings.symbol_allowlist,
        )

        self.symbols = symbols
        self.meta_payload = meta
        self.contexts_payload = contexts

        self.spool.append(
            make_raw_event(
                run_id=self.settings.archiver_run_id,
                dex=self.settings.hyperliquid_dex,
                datatype="health",
                payload={
                    "event_type": "symbols_refreshed",
                    "count": len(symbols),
                    "symbols": [symbol.name for symbol in symbols],
                },
                event_ts_ms=now_ms(),
            )
        )

        logger.info("symbols_refreshed count=%s", len(symbols))

    def poll_meta_asset_ctxs(self) -> None:
        symbols, meta, contexts = discover_symbols(
            driver=self.driver,
            dex=self.settings.hyperliquid_dex,
            allowlist=self.settings.symbol_allowlist,
        )

        self.symbols = symbols
        self.meta_payload = meta
        self.contexts_payload = contexts

        self.spool.append(
            make_raw_event(
                run_id=self.settings.archiver_run_id,
                dex=self.settings.hyperliquid_dex,
                datatype="meta_asset_ctxs",
                payload={
                    "meta": meta,
                    "contexts": contexts,
                },
                event_ts_ms=now_ms(),
            )
        )

        logger.info("poll_ok datatype=meta_asset_ctxs symbols=%s", len(symbols))

    def poll_l2_snapshots(self) -> None:
        symbols = self._symbols()
        success = 0
        failed = 0

        for index, symbol in enumerate(symbols):
            try:
                payload = self.driver.l2_snapshot(coin=symbol.name)
                self.spool.append(
                    make_raw_event(
                        run_id=self.settings.archiver_run_id,
                        dex=self.settings.hyperliquid_dex,
                        datatype="l2_snapshot",
                        symbol=symbol.name,
                        payload=payload,
                    )
                )
                success += 1
            except Exception as exc:
                failed += 1
                logger.warning("l2_snapshot_error symbol=%s error=%r", symbol.name, exc)
                self.spool.append(
                    make_raw_event(
                        run_id=self.settings.archiver_run_id,
                        dex=self.settings.hyperliquid_dex,
                        datatype="health",
                        symbol=symbol.name,
                        payload={
                            "event_type": "l2_snapshot_error",
                            "symbol": symbol.name,
                            "error": repr(exc),
                        },
                        event_ts_ms=now_ms(),
                    )
                )
            finally:
                self._sleep_between_symbol_requests(
                    index=index,
                    total=len(symbols),
                    seconds=self.settings.archive_l2_request_sleep_seconds,
                )

        logger.info(
            "poll_ok datatype=l2_snapshot symbols=%s success=%s failed=%s",
            len(symbols),
            success,
            failed,
        )

    def _symbols(self) -> list[HyperliquidSymbol]:
        if self.symbols is None:
            self.refresh_symbols()

        return self.symbols or []

    @staticmethod
    def _sleep_between_symbol_requests(*, index: int, total: int, seconds: float) -> None:
        if seconds <= 0:
            return

        if index >= total - 1:
            return

        time.sleep(seconds)