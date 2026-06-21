from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import websocket

from app.domain.events import RawEvent, make_raw_event
from app.utils.time import now_ms


@dataclass
class HyperliquidWebSocketRecorder:
    websocket_url: str
    run_id: str
    dex: str
    emit: Callable[[RawEvent], None]
    reconnect_sleep_s: float = 2.0

    _lock: threading.RLock = field(default_factory=threading.RLock)
    _ws: websocket.WebSocketApp | None = None
    _thread: threading.Thread | None = None
    _stop: threading.Event = field(default_factory=threading.Event)
    _connected: bool = False
    _pending_symbols: set[str] = field(default_factory=set)
    _subscribed_l2: set[str] = field(default_factory=set)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return

        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run_forever,
            name="hyperliquid-ws-recorder",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        ws = self._ws

        if ws is not None:
            ws.close()

    def ensure_subscriptions(self, symbols: list[str]) -> None:
        with self._lock:
            for symbol in symbols:
                if symbol not in self._subscribed_l2:
                    self._pending_symbols.add(symbol)

        self._flush_subscriptions()

    def _run_forever(self) -> None:
        while not self._stop.is_set():
            self._connected = False
            self._emit_health("ws_connecting", {})
            self._ws = websocket.WebSocketApp(
                self.websocket_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._ws.run_forever(ping_interval=20, ping_timeout=10)

            if not self._stop.is_set():
                time.sleep(self.reconnect_sleep_s)

    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        self._connected = True
        self._emit_health("ws_open", {})
        self._flush_subscriptions()

    def _on_close(self, ws: websocket.WebSocketApp, status_code: int, message: str) -> None:
        self._connected = False
        self._emit_health(
            "ws_close",
            {
                "status_code": status_code,
                "message": message,
            },
        )

        with self._lock:
            self._pending_symbols.update(self._subscribed_l2)
            self._subscribed_l2.clear()

    def _on_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        self._connected = False
        self._emit_health("ws_error", {"error": repr(error)})

    def _on_message(self, ws: websocket.WebSocketApp, raw: str) -> None:
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            self._emit_health("ws_bad_json", {"raw": raw[:512]})
            return

        if not isinstance(message, dict):
            return

        channel = message.get("channel")
        data = message.get("data")

        if channel == "subscriptionResponse":
            self._handle_subscription_response(data)
            return

        if channel == "l2Book" and isinstance(data, dict):
            coin = data.get("coin")
            if isinstance(coin, str):
                self.emit(
                    make_raw_event(
                        run_id=self.run_id,
                        dex=self.dex,
                        datatype="ws_l2_book",
                        symbol=coin,
                        payload=data,
                    )
                )
            return

    def _handle_subscription_response(self, data: Any) -> None:
        if not isinstance(data, dict):
            return

        subscription = data.get("subscription")
        if not isinstance(subscription, dict):
            return

        coin = subscription.get("coin")
        sub_type = subscription.get("type")

        if not isinstance(coin, str):
            return

        with self._lock:
            if sub_type == "l2Book":
                self._subscribed_l2.add(coin)
                self._pending_symbols.discard(coin)

        self._emit_health("ws_subscription_response", data)

    def _flush_subscriptions(self) -> None:
        ws = self._ws

        if ws is None or not self._connected:
            return

        with self._lock:
            symbols = sorted(self._pending_symbols)

        for symbol in symbols:
            sent = self._subscribe(ws=ws, coin=symbol)

            with self._lock:
                if sent:
                    self._subscribed_l2.add(symbol)
                    self._pending_symbols.discard(symbol)

    def _subscribe(self, *, ws: websocket.WebSocketApp, coin: str) -> bool:
        payload = {
            "method": "subscribe",
            "subscription": {
                "type": "l2Book",
                "coin": coin,
            },
        }

        try:
            ws.send(json.dumps(payload))
        except Exception as exc:
            self._connected = False
            self._emit_health(
                "ws_subscribe_error",
                {
                    "coin": coin,
                    "type": "l2Book",
                    "error": repr(exc),
                },
            )
            return False

        return True

    def _emit_health(self, event_type: str, payload: dict[str, Any]) -> None:
        self.emit(
            make_raw_event(
                run_id=self.run_id,
                dex=self.dex,
                datatype="health",
                payload={
                    "event_type": event_type,
                    "payload": payload,
                },
                event_ts_ms=now_ms(),
            )
        )