from __future__ import annotations

import csv
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal

import lz4.frame

from app.domain.events import RawEvent
from app.storage.metadata_store import MetadataStore
from app.storage.object_store import ObjectStore
from app.utils.json import loads
from app.utils.time import is_timestamp_ms, official_date_hour_from_ms

OfficialKind = Literal["market_data_l2_book", "asset_ctxs"]

_ASSET_CTXS_COLUMNS = (
    "time",
    "coin",
    "asset_index",
    "markPx",
    "midPx",
    "oraclePx",
    "openInterest",
    "dayNtlVlm",
    "funding",
    "premium",
    "prevDayPx",
    "impactBidPx",
    "impactAskPx",
    "maxLeverage",
    "onlyIsolated",
    "isDelisted",
)


@dataclass(frozen=True)
class OfficialObjectWrite:
    key: str
    kind: OfficialKind
    row_count: int
    checksum_sha256: str
    min_event_ts_ms: int
    max_event_ts_ms: int


@dataclass(frozen=True)
class SegmentWriteResult:
    events: list[RawEvent]
    objects: list[OfficialObjectWrite]
    skipped_events: int


@dataclass(frozen=True)
class _OfficialRows:
    key: str
    kind: OfficialKind
    rows: list[bytes]


class BrutWriter:
    def __init__(
        self,
        *,
        object_store: ObjectStore,
        metadata_store: MetadataStore,
    ) -> None:
        self.object_store = object_store
        self.metadata_store = metadata_store
        self.rollup_root = metadata_store.path.parent / "official-rollups"

    def write_segment(self, segment: Path) -> SegmentWriteResult:
        events = self._read_events(segment)

        if not events:
            raise RuntimeError(f"sealed segment contains no events: {segment}")

        grouped, skipped_events = self._group_events(events)
        written: list[OfficialObjectWrite] = []

        for rows in grouped.values():
            result = self._write_object_group(rows=rows, source_segment=segment.name)
            written.append(result)

        return SegmentWriteResult(
            events=events,
            objects=written,
            skipped_events=skipped_events,
        )

    def _read_events(self, segment: Path) -> list[RawEvent]:
        events: list[RawEvent] = []

        with segment.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()

                if not line:
                    continue

                payload = loads(line)

                if not isinstance(payload, dict):
                    raise RuntimeError(f"invalid raw event at {segment}:{line_number}")

                events.append(RawEvent(**payload))

        return events

    def _group_events(self, events: list[RawEvent]) -> tuple[dict[str, _OfficialRows], int]:
        grouped: dict[str, _OfficialRows] = {}
        skipped = 0

        for event in events:
            rows = self._official_rows_for_event(event)

            if rows is None:
                skipped += 1
                continue

            current = grouped.get(rows.key)
            if current is None:
                grouped[rows.key] = rows
                continue

            if current.kind != rows.kind:
                raise RuntimeError(f"official object kind collision for key={rows.key}")

            grouped[rows.key] = _OfficialRows(
                key=current.key,
                kind=current.kind,
                rows=[*current.rows, *rows.rows],
            )

        return grouped, skipped

    def _official_rows_for_event(self, event: RawEvent) -> _OfficialRows | None:
        if event.datatype in {"ws_l2_book", "l2_snapshot"}:
            return self._l2_book_rows(event)

        if event.datatype == "meta_asset_ctxs":
            return self._asset_ctx_rows(event)

        return None

    def _l2_book_rows(self, event: RawEvent) -> _OfficialRows:
        payload = event.payload

        if not isinstance(payload, dict):
            raise RuntimeError(f"{event.datatype} payload must be an object")

        self._validate_l2_book_payload(payload, event=event)

        timestamp_ms = int(payload["time"])
        date, hour = official_date_hour_from_ms(timestamp_ms)
        coin = str(payload["coin"])
        key = f"market_data/{date}/{hour}/l2Book/{self._safe_coin_path_component(coin)}.lz4"
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"

        return _OfficialRows(
            key=key,
            kind="market_data_l2_book",
            rows=[line],
        )

    def _asset_ctx_rows(self, event: RawEvent) -> _OfficialRows:
        payload = event.payload

        if not isinstance(payload, dict):
            raise RuntimeError("meta_asset_ctxs payload must be an object")

        meta = payload.get("meta")
        contexts = payload.get("contexts")

        if not isinstance(meta, dict):
            raise RuntimeError("meta_asset_ctxs.meta must be an object")
        if not isinstance(contexts, list):
            raise RuntimeError("meta_asset_ctxs.contexts must be an array")

        universe = meta.get("universe")
        if not isinstance(universe, list):
            raise RuntimeError("meta_asset_ctxs.meta.universe must be an array")
        if len(universe) != len(contexts):
            raise RuntimeError("meta_asset_ctxs universe/context length mismatch")

        date, _ = official_date_hour_from_ms(event.event_ts_ms)
        key = f"asset_ctxs/{date}.csv.lz4"
        rows: list[bytes] = []

        for index, item in enumerate(universe):
            if not isinstance(item, dict):
                raise RuntimeError("meta_asset_ctxs universe entries must be objects")

            context = contexts[index]
            if not isinstance(context, dict):
                raise RuntimeError("meta_asset_ctxs context entries must be objects")

            coin = item.get("name")
            if not isinstance(coin, str) or not coin:
                raise RuntimeError("meta_asset_ctxs universe entry missing name")

            rows.append(self._asset_ctx_csv_row(event_ts_ms=event.event_ts_ms, index=index, item=item, context=context))

        return _OfficialRows(
            key=key,
            kind="asset_ctxs",
            rows=rows,
        )

    def _asset_ctx_csv_row(
        self,
        *,
        event_ts_ms: int,
        index: int,
        item: dict[str, Any],
        context: dict[str, Any],
    ) -> bytes:
        impact_pxs = context.get("impactPxs")
        impact_bid_px = ""
        impact_ask_px = ""

        if isinstance(impact_pxs, list) and len(impact_pxs) >= 2:
            impact_bid_px = self._csv_value(impact_pxs[0])
            impact_ask_px = self._csv_value(impact_pxs[1])

        values = {
            "time": str(event_ts_ms),
            "coin": self._csv_value(item.get("name")),
            "asset_index": str(index),
            "markPx": self._csv_value(context.get("markPx")),
            "midPx": self._csv_value(context.get("midPx")),
            "oraclePx": self._csv_value(context.get("oraclePx")),
            "openInterest": self._csv_value(context.get("openInterest")),
            "dayNtlVlm": self._csv_value(context.get("dayNtlVlm")),
            "funding": self._csv_value(context.get("funding")),
            "premium": self._csv_value(context.get("premium")),
            "prevDayPx": self._csv_value(context.get("prevDayPx")),
            "impactBidPx": impact_bid_px,
            "impactAskPx": impact_ask_px,
            "maxLeverage": self._csv_value(item.get("maxLeverage")),
            "onlyIsolated": self._csv_value(item.get("onlyIsolated")),
            "isDelisted": self._csv_value(item.get("isDelisted")),
        }

        return self._csv_line([values[column] for column in _ASSET_CTXS_COLUMNS]).encode("utf-8")

    def _write_object_group(self, *, rows: _OfficialRows, source_segment: str) -> OfficialObjectWrite:
        key_dir = self._key_dir(rows.key)
        segments_dir = key_dir / "segments"

        key_dir.mkdir(parents=True, exist_ok=True)
        segments_dir.mkdir(parents=True, exist_ok=True)

        self._write_text_if_absent(key_dir / "key.txt", rows.key + "\n")
        self._initialize_rollup_dir(key=rows.key, kind=rows.kind, key_dir=key_dir)

        segment_path = segments_dir / f"{hashlib.sha256(source_segment.encode('utf-8')).hexdigest()}.rows"
        segment_payload = b"".join(rows.rows)
        self._write_segment_rows(segment_path=segment_path, payload=segment_payload)

        with TemporaryDirectory() as tmp_dir_raw:
            tmp_dir = Path(tmp_dir_raw)
            object_path = tmp_dir / "object.lz4"
            stats = self._write_lz4_object(kind=rows.kind, key_dir=key_dir, output_path=object_path)
            checksum = self._sha256_file(object_path)

            self.object_store.put_file(
                key=rows.key,
                path=object_path,
                content_type="application/x-lz4",
            )

        self.metadata_store.record_object(
            key=rows.key,
            kind=rows.kind,
            source_segment=source_segment,
            row_count=stats["row_count"],
            checksum_sha256=checksum,
            min_event_ts_ms=stats["min_event_ts_ms"],
            max_event_ts_ms=stats["max_event_ts_ms"],
        )

        return OfficialObjectWrite(
            key=rows.key,
            kind=rows.kind,
            row_count=stats["row_count"],
            checksum_sha256=checksum,
            min_event_ts_ms=stats["min_event_ts_ms"],
            max_event_ts_ms=stats["max_event_ts_ms"],
        )

    def _initialize_rollup_dir(self, *, key: str, kind: OfficialKind, key_dir: Path) -> None:
        marker = key_dir / "initialized"
        if marker.exists():
            return

        segments_dir = key_dir / "segments"
        if any(segments_dir.glob("*.rows")):
            self._write_text_if_absent(marker, "local\n")
            return

        if self.object_store.exists(key=key):
            with TemporaryDirectory() as tmp_dir_raw:
                tmp_dir = Path(tmp_dir_raw)
                compressed = tmp_dir / "remote.lz4"
                self.object_store.client.download_file(self.object_store.bucket, key, str(compressed))

                base_path = key_dir / ("base.csv" if kind == "asset_ctxs" else "base.jsonl")
                self._decompress_lz4_file(input_path=compressed, output_path=base_path)

        self._write_text_if_absent(marker, "initialized\n")

    def _write_segment_rows(self, *, segment_path: Path, payload: bytes) -> None:
        if segment_path.exists():
            current = segment_path.read_bytes()
            if hashlib.sha256(current).digest() != hashlib.sha256(payload).digest():
                raise RuntimeError(f"segment row payload changed for {segment_path}")
            return

        tmp_path = segment_path.with_suffix(".tmp")
        with tmp_path.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(tmp_path, segment_path)
        self._fsync_dir(segment_path.parent)

    def _write_lz4_object(self, *, kind: OfficialKind, key_dir: Path, output_path: Path) -> dict[str, int]:
        row_count = 0
        min_event_ts_ms: int | None = None
        max_event_ts_ms: int | None = None
        has_base = (key_dir / "base.jsonl").exists() or (key_dir / "base.csv").exists()

        with output_path.open("wb") as raw_output:
            with lz4.frame.open(raw_output, mode="wb", compression_level=0) as compressed_output:
                if kind == "asset_ctxs" and not has_base:
                    compressed_output.write(self._csv_line(list(_ASSET_CTXS_COLUMNS)).encode("utf-8"))

                for line in self._iter_rollup_lines(key_dir=key_dir):
                    if not line.strip():
                        continue

                    compressed_output.write(line)

                    timestamp_ms = self._timestamp_from_official_line(kind=kind, line=line)
                    if timestamp_ms is None:
                        continue

                    row_count += 1
                    min_event_ts_ms = timestamp_ms if min_event_ts_ms is None else min(min_event_ts_ms, timestamp_ms)
                    max_event_ts_ms = timestamp_ms if max_event_ts_ms is None else max(max_event_ts_ms, timestamp_ms)

        if row_count <= 0 or min_event_ts_ms is None or max_event_ts_ms is None:
            raise RuntimeError(f"official object has no data rows: {key_dir}")

        return {
            "row_count": row_count,
            "min_event_ts_ms": min_event_ts_ms,
            "max_event_ts_ms": max_event_ts_ms,
        }

    def _iter_rollup_lines(self, *, key_dir: Path) -> list[bytes]:
        lines: list[bytes] = []

        for base_name in ("base.jsonl", "base.csv"):
            base_path = key_dir / base_name
            if base_path.exists():
                lines.extend(base_path.read_bytes().splitlines(keepends=True))

        segments_dir = key_dir / "segments"
        for segment_path in sorted(segments_dir.glob("*.rows")):
            lines.extend(segment_path.read_bytes().splitlines(keepends=True))

        return lines

    def _timestamp_from_official_line(self, *, kind: OfficialKind, line: bytes) -> int | None:
        if kind == "market_data_l2_book":
            payload = json.loads(line.decode("utf-8"))
            if not isinstance(payload, dict):
                raise RuntimeError("l2Book line is not an object")
            timestamp_ms = payload.get("time")
            if not is_timestamp_ms(timestamp_ms):
                raise RuntimeError(f"invalid l2Book time: {timestamp_ms}")
            return int(timestamp_ms)

        text = line.decode("utf-8").strip()
        row = next(csv.reader([text]))
        if not row or row[0] == "time":
            return None

        timestamp_ms = int(row[0])
        if not is_timestamp_ms(timestamp_ms):
            raise RuntimeError(f"invalid asset_ctxs time: {timestamp_ms}")

        return timestamp_ms

    def _validate_l2_book_payload(self, payload: dict[str, Any], *, event: RawEvent) -> None:
        forbidden_wrapper_keys = {
            "schema_version",
            "event_id",
            "run_id",
            "source",
            "dex",
            "datatype",
            "payload",
            "ingested_at_ms",
        }

        if forbidden_wrapper_keys.intersection(payload):
            raise RuntimeError("l2Book official payload contains Mosaic wrapper keys")

        coin = payload.get("coin")
        timestamp_ms = payload.get("time")
        levels = payload.get("levels")

        if not isinstance(coin, str) or not coin:
            raise RuntimeError(f"{event.datatype}.coin must be a non-empty string")
        if event.symbol is not None and coin != event.symbol:
            raise RuntimeError(f"{event.datatype}.coin does not match event.symbol")
        if not is_timestamp_ms(timestamp_ms):
            raise RuntimeError(f"{event.datatype}.time must be a millisecond timestamp")
        if not isinstance(levels, list) or len(levels) != 2:
            raise RuntimeError(f"{event.datatype}.levels must contain bids and asks")
        if not all(isinstance(side, list) for side in levels):
            raise RuntimeError(f"{event.datatype}.levels sides must be arrays")

        for side in levels:
            for level in side:
                self._validate_l2_level(level)

    @staticmethod
    def _validate_l2_level(level: Any) -> None:
        if not isinstance(level, dict):
            raise RuntimeError("l2Book level must be an object")

        px = level.get("px")
        sz = level.get("sz")
        n = level.get("n")

        if not isinstance(px, str) or not px:
            raise RuntimeError("l2Book level.px must be a non-empty string")
        if not isinstance(sz, str) or not sz:
            raise RuntimeError("l2Book level.sz must be a non-empty string")
        if not isinstance(n, int) or isinstance(n, bool) or n < 0:
            raise RuntimeError("l2Book level.n must be a non-negative integer")

    @staticmethod
    def _safe_coin_path_component(coin: str) -> str:
        if not coin or "/" in coin or "\x00" in coin:
            raise RuntimeError(f"invalid coin for official object key: {coin!r}")
        return coin

    @staticmethod
    def _csv_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    @staticmethod
    def _csv_line(values: list[str]) -> str:
        from io import StringIO

        buffer = StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(values)
        return buffer.getvalue()

    def _key_dir(self, key: str) -> Path:
        return self.rollup_root / hashlib.sha256(key.encode("utf-8")).hexdigest()

    @staticmethod
    def _write_text_if_absent(path: Path, text: str) -> None:
        if path.exists():
            return

        tmp_path = path.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(tmp_path, path)
        BrutWriter._fsync_dir(path.parent)

    @staticmethod
    def _decompress_lz4_file(*, input_path: Path, output_path: Path) -> None:
        tmp_path = output_path.with_suffix(".tmp")

        with input_path.open("rb") as raw_input:
            with lz4.frame.open(raw_input, mode="rb") as compressed_input:
                with tmp_path.open("wb") as output:
                    while chunk := compressed_input.read(1024 * 1024):
                        output.write(chunk)
                    output.flush()
                    os.fsync(output.fileno())

        os.replace(tmp_path, output_path)
        BrutWriter._fsync_dir(output_path.parent)

    @staticmethod
    def _sha256_file(path: Path) -> str:
        hasher = hashlib.sha256()

        with path.open("rb") as handle:
            while chunk := handle.read(8 * 1024 * 1024):
                hasher.update(chunk)

        return hasher.hexdigest()

    @staticmethod
    def _fsync_dir(directory: Path) -> None:
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)