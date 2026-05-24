from __future__ import annotations

from collections import defaultdict
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from app.domain.events import RawEvent
from app.storage.metadata_store import MetadataStore
from app.storage.object_store import ObjectStore
from app.utils.json import dumps
from app.writers.manifest_writer import write_object_manifest


class ParquetWriter:
    def __init__(self, *, object_store: ObjectStore, metadata_store: MetadataStore) -> None:
        self.object_store = object_store
        self.metadata_store = metadata_store

    def write_events(self, *, events: list[RawEvent], source_segment: str) -> None:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        event_times: dict[str, list[int]] = defaultdict(list)

        for event in events:
            for row in rows_for_event(event):
                partition = event.object_partition(root="parquet")
                grouped[partition].append(row)
                event_times[partition].append(event.event_ts_ms)

        segment_id = source_segment.replace(".sealed.jsonl", "")
        with TemporaryDirectory() as tmp_dir_raw:
            tmp_dir = Path(tmp_dir_raw)
            for partition, rows in grouped.items():
                if not rows:
                    continue
                key = f"{partition}/part-{segment_id}.parquet"
                path = tmp_dir / (sha256(key.encode("utf-8")).hexdigest() + ".parquet")
                table = pa.Table.from_pylist(rows)
                pq.write_table(table, path, compression="zstd", use_dictionary=True)
                checksum = sha256(path.read_bytes()).hexdigest()
                self.object_store.put_file(key=key, path=path, content_type="application/vnd.apache.parquet")
                times = event_times[partition]
                min_event_ts_ms = min(times) if times else None
                max_event_ts_ms = max(times) if times else None
                self.metadata_store.record_object(
                    key=key,
                    kind="parquet",
                    source_segment=source_segment,
                    row_count=len(rows),
                    checksum_sha256=checksum,
                    min_event_ts_ms=min_event_ts_ms,
                    max_event_ts_ms=max_event_ts_ms,
                )
                write_object_manifest(
                    object_store=self.object_store,
                    object_key=key,
                    kind="parquet",
                    source_segment=source_segment,
                    row_count=len(rows),
                    checksum_sha256=checksum,
                    min_event_ts_ms=min_event_ts_ms,
                    max_event_ts_ms=max_event_ts_ms,
                )


def rows_for_event(event: RawEvent) -> list[dict[str, Any]]:
    if event.datatype == "ws_trades":
        return [_trade_row(event)] if isinstance(event.payload, dict) else []
    if event.datatype in {"ws_l2_book", "l2_snapshot"}:
        return [_book_row(event)] if isinstance(event.payload, dict) else []
    if event.datatype == "meta_asset_ctxs":
        return _asset_ctx_rows(event)
    if event.datatype == "all_mids":
        return _mid_rows(event)
    if event.datatype == "funding_history":
        return _funding_rows(event)
    if event.datatype == "candles":
        return _candle_rows(event)
    if event.datatype == "perp_dexs":
        return _perp_dex_rows(event)
    if event.datatype == "health":
        return [_health_row(event)]
    return []


def _base(event: RawEvent) -> dict[str, Any]:
    return {
        "schema_version": event.schema_version,
        "event_id": event.event_id,
        "run_id": event.run_id,
        "source": event.source,
        "dex": event.dex,
        "datatype": event.datatype,
        "symbol": event.symbol,
        "canonical_symbol": event.canonical_symbol,
        "interval": event.interval,
        "event_ts_ms": event.event_ts_ms,
        "ingested_at_ms": event.ingested_at_ms,
    }


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if result != result or result in (float("inf"), float("-inf")):
        return None
    return result


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _trade_row(event: RawEvent) -> dict[str, Any]:
    payload = event.payload if isinstance(event.payload, dict) else {}
    raw_side = payload.get("side")
    price = _to_float(payload.get("px"))
    size = _to_float(payload.get("sz"))
    return {
        **_base(event),
        "coin": payload.get("coin") if isinstance(payload.get("coin"), str) else event.symbol,
        "raw_side": raw_side if raw_side in {"A", "B"} else None,
        "aggressor_side": "buy" if raw_side == "B" else "sell" if raw_side == "A" else None,
        "price": price,
        "size": size,
        "notional": price * size if price is not None and size is not None else None,
        "trade_ts_ms": _to_int(payload.get("time")),
        "trade_id": _to_int(payload.get("tid")),
        "trade_hash": payload.get("hash") if isinstance(payload.get("hash"), str) else None,
        "raw_json": dumps(payload),
    }


def _book_row(event: RawEvent) -> dict[str, Any]:
    payload = event.payload if isinstance(event.payload, dict) else {}
    levels = payload.get("levels")
    best_bid, best_ask = _best_bid_ask(levels)
    mid = (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None and best_ask >= best_bid else None
    spread_bps = ((best_ask - best_bid) / mid) * 10_000 if mid and best_bid is not None and best_ask is not None else None
    return {
        **_base(event),
        "coin": payload.get("coin") if isinstance(payload.get("coin"), str) else event.symbol,
        "book_ts_ms": _to_int(payload.get("time")) or _to_int(payload.get("timestamp")),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": mid,
        "spread_bps": spread_bps,
        "levels_json": dumps(levels),
        "raw_json": dumps(payload),
    }


def _best_bid_ask(levels: Any) -> tuple[float | None, float | None]:
    if not isinstance(levels, list) or len(levels) < 2:
        return None, None
    bids = [_to_float(row.get("px")) for row in levels[0] if isinstance(row, dict)] if isinstance(levels[0], list) else []
    asks = [_to_float(row.get("px")) for row in levels[1] if isinstance(row, dict)] if isinstance(levels[1], list) else []
    clean_bids = [price for price in bids if price is not None and price > 0]
    clean_asks = [price for price in asks if price is not None and price > 0]
    return (max(clean_bids) if clean_bids else None, min(clean_asks) if clean_asks else None)


def _asset_ctx_rows(event: RawEvent) -> list[dict[str, Any]]:
    payload = event.payload
    if not isinstance(payload, dict):
        return []
    meta = payload.get("meta")
    contexts = payload.get("contexts")
    if not isinstance(meta, dict) or not isinstance(contexts, list):
        return []
    universe = meta.get("universe")
    if not isinstance(universe, list):
        return []
    rows: list[dict[str, Any]] = []
    for index, item in enumerate(universe):
        if not isinstance(item, dict):
            continue
        context = contexts[index] if index < len(contexts) and isinstance(contexts[index], dict) else {}
        name = item.get("name") if isinstance(item.get("name"), str) else None
        rows.append(
            {
                **_base(event),
                "asset_index": index,
                "coin": name,
                "asset_name": name,
                "mark_price": _first_float(context, "markPx", "mark_price", "markPrice"),
                "mid_price": _first_float(context, "midPx", "mid_price", "midPrice"),
                "oracle_price": _first_float(context, "oraclePx", "oracle_price", "oraclePrice"),
                "day_notional_volume": _first_float(context, "dayNtlVlm", "day_notional_volume", "dayNotionalVolume"),
                "open_interest": _first_float(context, "openInterest", "open_interest"),
                "funding": _first_float(context, "funding", "currentFunding", "current_funding"),
                "meta_json": dumps(item),
                "context_json": dumps(context),
            }
        )
    return rows


def _first_float(mapping: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _to_float(mapping.get(key))
        if value is not None:
            return value
    return None


def _mid_rows(event: RawEvent) -> list[dict[str, Any]]:
    if not isinstance(event.payload, dict):
        return []
    rows = []
    for coin, value in event.payload.items():
        if not isinstance(coin, str):
            continue
        rows.append({**_base(event), "coin": coin, "mid_price": _to_float(value)})
    return rows


def _funding_rows(event: RawEvent) -> list[dict[str, Any]]:
    payload = event.payload if isinstance(event.payload, dict) else {}
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return []
    result = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        result.append(
            {
                **_base(event),
                "coin": row.get("coin") if isinstance(row.get("coin"), str) else event.symbol,
                "funding_time_ms": _to_int(row.get("time")),
                "funding_rate": _to_float(row.get("fundingRate")),
                "premium": _to_float(row.get("premium")),
                "raw_json": dumps(row),
            }
        )
    return result


def _candle_rows(event: RawEvent) -> list[dict[str, Any]]:
    payload = event.payload if isinstance(event.payload, dict) else {}
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return []
    result = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        result.append(
            {
                **_base(event),
                "coin": row.get("s") if isinstance(row.get("s"), str) else event.symbol,
                "start_time_ms": _to_int(row.get("t")),
                "end_time_ms": _to_int(row.get("T")),
                "open": _to_float(row.get("o")),
                "high": _to_float(row.get("h")),
                "low": _to_float(row.get("l")),
                "close": _to_float(row.get("c")),
                "volume": _to_float(row.get("v")),
                "trades": _to_int(row.get("n")),
                "raw_json": dumps(row),
            }
        )
    return result


def _perp_dex_rows(event: RawEvent) -> list[dict[str, Any]]:
    payload = event.payload
    items = payload if isinstance(payload, list) else []
    rows = []
    for index, item in enumerate(items):
        if isinstance(item, str):
            name = item
            raw = item
        elif isinstance(item, dict):
            name = item.get("name") or item.get("dex") or item.get("id")
            raw = item
        else:
            continue
        rows.append({**_base(event), "dex_index": index, "dex_name": name if isinstance(name, str) else None, "raw_json": dumps(raw)})
    return rows


def _health_row(event: RawEvent) -> dict[str, Any]:
    payload = event.payload if isinstance(event.payload, dict) else {}
    return {
        **_base(event),
        "event_type": payload.get("event_type") if isinstance(payload.get("event_type"), str) else None,
        "payload_json": dumps(payload.get("payload")),
        "raw_json": dumps(payload),
    }
