from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
from typing import Any, Literal
from uuid import uuid4

from app.utils.json import dumps
from app.utils.time import date_hour_from_ms, now_ms

DataType = Literal[
    "perp_dexs",
    "meta_asset_ctxs",
    "all_mids",
    "l2_snapshot",
    "ws_l2_book",
    "ws_trades",
    "funding_history",
    "candles",
    "health",
]


@dataclass(frozen=True)
class RawEvent:
    run_id: str
    dex: str
    datatype: DataType
    event_ts_ms: int
    payload: dict[str, Any] | list[Any] | str | int | float | bool | None

    schema_version: Literal["xyz_tradfi_archiver.raw_event.v1"] = "xyz_tradfi_archiver.raw_event.v1"
    event_id: str = field(default_factory=lambda: uuid4().hex)
    source: Literal["hyperliquid"] = "hyperliquid"
    symbol: str | None = None
    canonical_symbol: str | None = None
    interval: str | None = None
    ingested_at_ms: int = field(default_factory=now_ms)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "run_id": self.run_id,
            "source": self.source,
            "dex": self.dex,
            "datatype": self.datatype,
            "symbol": self.symbol,
            "canonical_symbol": self.canonical_symbol,
            "interval": self.interval,
            "event_ts_ms": self.event_ts_ms,
            "ingested_at_ms": self.ingested_at_ms,
            "payload": self.payload,
        }

    def line(self) -> str:
        return dumps(self.to_dict()) + "\n"

    def fingerprint(self) -> str:
        return sha256(self.line().encode("utf-8")).hexdigest()

    def object_partition(self, *, root: str) -> str:
        date, hour = date_hour_from_ms(self.event_ts_ms)
        clean_root = root.rstrip("/")

        parts = [
            clean_root,
            f"source={self.source}",
            f"dex={self.dex}",
            f"datatype={self.datatype}" if clean_root == "brut" else f"dataset={dataset_for_datatype(self.datatype)}",
        ]

        if self.interval:
            parts.append(f"interval={self.interval}")

        parts.append(f"date={date}")
        parts.append(f"hour={hour}")

        if self.canonical_symbol:
            parts.append(f"symbol={self.canonical_symbol}")

        return "/".join(parts)


def canonical_symbol(name: str | None) -> str | None:
    if not name:
        return None
    if ":" in name:
        return name.split(":", 1)[1]
    return name


def event_timestamp_from_payload(payload: Any) -> int:
    if isinstance(payload, dict):
        for key in ("time", "timestamp", "t", "T"):
            raw = payload.get(key)
            if raw is None:
                continue
            try:
                return int(raw)
            except (TypeError, ValueError):
                pass

    return now_ms()


def dataset_for_datatype(datatype: str) -> str:
    mapping = {
        "perp_dexs": "perp_dexs",
        "meta_asset_ctxs": "asset_ctxs",
        "all_mids": "mids",
        "l2_snapshot": "l2_snapshot",
        "ws_l2_book": "l2_book",
        "ws_trades": "trades",
        "funding_history": "funding",
        "candles": "candles",
        "health": "health",
    }
    return mapping.get(datatype, datatype)


def make_raw_event(
    *,
    run_id: str,
    dex: str,
    datatype: DataType,
    payload: Any,
    symbol: str | None = None,
    interval: str | None = None,
    event_ts_ms: int | None = None,
) -> RawEvent:
    return RawEvent(
        run_id=run_id,
        dex=dex,
        datatype=datatype,
        symbol=symbol,
        canonical_symbol=canonical_symbol(symbol),
        interval=interval,
        event_ts_ms=event_ts_ms or event_timestamp_from_payload(payload),
        payload=payload,
    )