from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from app.config import Settings
from app.domain.events import RawEvent, make_raw_event
from app.hyperliquid.driver import HyperliquidInfoDriver
from app.hyperliquid.symbols import HyperliquidSymbol, discover_symbols
from app.hyperliquid.websocket_recorder import HyperliquidWebSocketRecorder
from app.spool.durable_spool import DurableSpool
from app.utils.time import interval_to_ms, now_ms


@dataclass
class PollSchedule:
    name: str
    every_s: int
    action: Callable[[], None]
    next_at_ms: int = 0

    def maybe_run(self, current_ms: int) -> None:
        if current_ms < self.next_at_ms:
            return
        self.action()
        self.next_at_ms = current_ms + self.every_s * 1000


def run_recorder(settings: Settings) -> None:
    settings.archiver_state_dir.mkdir(parents=True, exist_ok=True)
    spool = DurableSpool(
        root=settings.spool_dir,
        fsync_every_events=settings.recorder_fsync_every_events,
        segment_max_bytes=settings.recorder_segment_max_bytes,
        segment_max_age_seconds=settings.recorder_segment_max_age_seconds,
    )
    driver = HyperliquidInfoDriver(
        base_url=settings.hyperliquid_base_url,
        timeout_s=settings.hyperliquid_http_timeout_s,
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
    ws_recorder.ensure_subscriptions([symbol.name for symbol in state.symbols])

    schedules = [
        PollSchedule("perp_dexs", settings.poll_perp_dexs_seconds, state.poll_perp_dexs),
        PollSchedule("meta_asset_ctxs", settings.poll_meta_asset_ctxs_seconds, state.poll_meta_asset_ctxs),
        PollSchedule("all_mids", settings.poll_all_mids_seconds, state.poll_all_mids),
        PollSchedule("l2_snapshot", settings.poll_l2_snapshot_seconds, state.poll_l2_snapshots),
        PollSchedule("funding_history", settings.poll_funding_history_seconds, state.poll_funding_history),
        PollSchedule("candles", settings.poll_candles_seconds, state.poll_candles),
    ]

    try:
        while True:
            current_ms = now_ms()
            for schedule in schedules:
                try:
                    schedule.maybe_run(current_ms)
                except Exception as exc:
                    spool.append(
                        make_raw_event(
                            run_id=settings.archiver_run_id,
                            dex=settings.hyperliquid_dex,
                            datatype="health",
                            payload={"event_type": "poll_error", "poll": schedule.name, "error": repr(exc)},
                            event_ts_ms=now_ms(),
                        )
                    )
            ws_recorder.ensure_subscriptions([symbol.name for symbol in state.symbols])
            time.sleep(settings.recorder_idle_sleep_seconds)
    finally:
        ws_recorder.stop()
        spool.rotate()


@dataclass
class RecorderState:
    settings: Settings
    driver: HyperliquidInfoDriver
    spool: DurableSpool
    symbols: list[HyperliquidSymbol] | None = None
    meta_payload: Any = None
    contexts_payload: Any = None

    def refresh_symbols(self) -> None:
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
                payload={"event_type": "symbols_refreshed", "count": len(symbols), "symbols": [s.name for s in symbols]},
                event_ts_ms=now_ms(),
            )
        )

    def poll_perp_dexs(self) -> None:
        payload = self.driver.perp_dexs()
        self.spool.append(
            make_raw_event(
                run_id=self.settings.archiver_run_id,
                dex=self.settings.hyperliquid_dex,
                datatype="perp_dexs",
                payload=payload,
                event_ts_ms=now_ms(),
            )
        )

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
                payload={"meta": meta, "contexts": contexts},
                event_ts_ms=now_ms(),
            )
        )

    def poll_all_mids(self) -> None:
        payload = self.driver.all_mids()
        self.spool.append(
            make_raw_event(
                run_id=self.settings.archiver_run_id,
                dex=self.settings.hyperliquid_dex,
                datatype="all_mids",
                payload=payload,
                event_ts_ms=now_ms(),
            )
        )

    def poll_l2_snapshots(self) -> None:
        for symbol in self._symbols():
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

    def poll_funding_history(self) -> None:
        end_ms = now_ms()
        start_ms = end_ms - self.settings.archive_funding_lookback_days * 86_400_000
        for symbol in self._symbols():
            rows = self.driver.funding_history(coin=symbol.name, start_time_ms=start_ms, end_time_ms=end_ms)
            self.spool.append(
                make_raw_event(
                    run_id=self.settings.archiver_run_id,
                    dex=self.settings.hyperliquid_dex,
                    datatype="funding_history",
                    symbol=symbol.name,
                    payload={"start_time_ms": start_ms, "end_time_ms": end_ms, "rows": rows},
                    event_ts_ms=end_ms,
                )
            )

    def poll_candles(self) -> None:
        end_ms = now_ms()
        for symbol in self._symbols():
            for interval in self.settings.timeframes:
                start_ms = end_ms - interval_to_ms(interval) * self.settings.archive_candle_lookback_bars
                rows = self.driver.candles_snapshot(
                    coin=symbol.name,
                    interval=interval,
                    start_time_ms=start_ms,
                    end_time_ms=end_ms,
                )
                self.spool.append(
                    make_raw_event(
                        run_id=self.settings.archiver_run_id,
                        dex=self.settings.hyperliquid_dex,
                        datatype="candles",
                        symbol=symbol.name,
                        interval=interval,
                        payload={"interval": interval, "start_time_ms": start_ms, "end_time_ms": end_ms, "rows": rows},
                        event_ts_ms=end_ms,
                    )
                )

    def _symbols(self) -> list[HyperliquidSymbol]:
        if self.symbols is None:
            self.refresh_symbols()
        return self.symbols or []
