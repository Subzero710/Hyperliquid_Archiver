from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal

import lz4.frame

from app.config import Settings
from app.domain.events import RawEvent
from app.storage.metadata_store import MetadataStore
from app.storage.object_store import ObjectStore
from app.utils.json import loads
from app.utils.time import is_timestamp_ms

OfficialObjectKind = Literal["market_data_l2_book", "asset_ctxs"]

_ALLOWED_RAW_DATATYPES = {
    "meta_asset_ctxs",
    "l2_snapshot",
    "ws_l2_book",
    "health",
}

_L2_OBJECT_KEY_RE = re.compile(
    r"^market_data/(?P<date>\d{8})/(?P<hour>\d{1,2})/l2Book/(?P<coin>[^/\x00]+)\.lz4$"
)

_ASSET_CTXS_OBJECT_KEY_RE = re.compile(
    r"^asset_ctxs/(?P<date>\d{8})\.csv\.lz4$"
)

_FORBIDDEN_OFFICIAL_L2_KEYS = {
    "schema_version",
    "event_id",
    "run_id",
    "source",
    "dex",
    "datatype",
    "payload",
    "ingested_at_ms",
}


@dataclass(frozen=True)
class ValidationResult:
    source: str
    kind: str
    row_count: int
    min_event_ts_ms: int
    max_event_ts_ms: int
    details: dict[str, Any]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="validate-data-shape")
    subparsers = parser.add_subparsers(dest="command", required=True)

    segment_parser = subparsers.add_parser("segment")
    segment_parser.add_argument("path")

    latest_parser = subparsers.add_parser("latest-object")
    latest_parser.add_argument("--key", default=None)

    args = parser.parse_args(argv)

    if args.command == "segment":
        result = validate_segment(Path(args.path))
        print(json.dumps(result.__dict__, indent=2, sort_keys=True), flush=True)
        return 0

    if args.command == "latest-object":
        result = validate_latest_object(key=args.key)
        print(json.dumps(result.__dict__, indent=2, sort_keys=True), flush=True)
        return 0

    return 2


def validate_segment(path: Path) -> ValidationResult:
    if not path.is_file():
        raise RuntimeError(f"segment does not exist: {path}")

    events: list[RawEvent] = []

    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue

            payload = _load_json_object(line, source=f"{path}:{line_number}")
            event = _raw_event(payload, source=f"{path}:{line_number}")
            _validate_raw_event(event, source=f"{path}:{line_number}")
            events.append(event)

    if not events:
        raise RuntimeError(f"no raw events found in {path}")

    datatypes: dict[str, int] = {}
    for event in events:
        datatypes[event.datatype] = datatypes.get(event.datatype, 0) + 1

    return ValidationResult(
        source=str(path),
        kind="raw_segment",
        row_count=len(events),
        min_event_ts_ms=min(event.event_ts_ms for event in events),
        max_event_ts_ms=max(event.event_ts_ms for event in events),
        details={"datatypes": dict(sorted(datatypes.items()))},
    )


def validate_latest_object(*, key: str | None) -> ValidationResult:
    settings = Settings.from_env()
    metadata_store = MetadataStore(settings.metadata_db_path)

    try:
        if key is None:
            latest_objects = metadata_store.latest_objects(limit=1)
            if not latest_objects:
                raise RuntimeError("metadata store contains no archived object")
            object_record = latest_objects[0]
        else:
            object_record = _object_record_by_key(metadata_store, key=key)

        object_key = str(object_record["key"])
        object_kind = _official_object_kind_from_key(object_key)

        expected_kind = str(object_record["kind"])
        if expected_kind != object_kind:
            raise RuntimeError(
                f"metadata kind mismatch key={object_key} metadata={expected_kind} actual={object_kind}"
            )

        object_store = ObjectStore(settings)

        with TemporaryDirectory() as tmp_raw:
            local_path = Path(tmp_raw) / "object.lz4"
            object_store.client.download_file(
                settings.archive_bucket,
                object_key,
                str(local_path),
            )

            checksum = _sha256_file(local_path)
            expected_checksum = str(object_record["checksum_sha256"])
            if checksum != expected_checksum:
                raise RuntimeError(
                    f"object checksum mismatch key={object_key} "
                    f"expected={expected_checksum} actual={checksum}"
                )

            raw_payload = _read_lz4_file(local_path)

        if object_kind == "market_data_l2_book":
            result = _validate_l2_book_object(object_key=object_key, payload=raw_payload)
        elif object_kind == "asset_ctxs":
            result = _validate_asset_ctxs_object(object_key=object_key, payload=raw_payload)
        else:
            raise RuntimeError(f"unhandled official object kind: {object_kind}")

        row_count = int(object_record["row_count"])
        min_event_ts_ms = int(object_record["min_event_ts_ms"])
        max_event_ts_ms = int(object_record["max_event_ts_ms"])

        if result.row_count != row_count:
            raise RuntimeError(
                f"row_count mismatch key={object_key} metadata={row_count} actual={result.row_count}"
            )

        if result.min_event_ts_ms != min_event_ts_ms:
            raise RuntimeError(
                f"min_event_ts_ms mismatch key={object_key} "
                f"metadata={min_event_ts_ms} actual={result.min_event_ts_ms}"
            )

        if result.max_event_ts_ms != max_event_ts_ms:
            raise RuntimeError(
                f"max_event_ts_ms mismatch key={object_key} "
                f"metadata={max_event_ts_ms} actual={result.max_event_ts_ms}"
            )

        return result
    finally:
        metadata_store.close()


def _object_record_by_key(metadata_store: MetadataStore, *, key: str) -> dict[str, Any]:
    rows = metadata_store.connection.execute(
        "select * from archive_object where key = ?",
        (key,),
    ).fetchall()

    if not rows:
        raise RuntimeError(f"metadata store does not contain object key: {key}")

    return dict(rows[0])


def _official_object_kind_from_key(key: str) -> OfficialObjectKind:
    if _L2_OBJECT_KEY_RE.fullmatch(key):
        return "market_data_l2_book"

    if _ASSET_CTXS_OBJECT_KEY_RE.fullmatch(key):
        return "asset_ctxs"

    raise RuntimeError(f"unexpected official archive object key shape: {key}")


def _validate_l2_book_object(*, object_key: str, payload: bytes) -> ValidationResult:
    match = _L2_OBJECT_KEY_RE.fullmatch(object_key)
    if match is None:
        raise RuntimeError(f"invalid l2Book object key: {object_key}")

    expected_date = match.group("date")
    expected_hour = int(match.group("hour"))
    expected_coin = match.group("coin")

    if expected_hour < 0 or expected_hour > 23:
        raise RuntimeError(f"invalid l2Book object hour in key: {object_key}")

    _parse_yyyymmdd(expected_date, source=object_key)

    row_count = 0
    min_event_ts_ms: int | None = None
    max_event_ts_ms: int | None = None

    for line_number, line in _iter_non_empty_lines(payload):
        item = _load_json_object(line, source=f"{object_key}:{line_number}")
        _validate_l2_book_payload(item, expected_coin=expected_coin, source=f"{object_key}:{line_number}")

        timestamp_ms = int(item["time"])
        actual_date, actual_hour = _official_date_hour_from_ms(timestamp_ms)

        if actual_date != expected_date:
            raise RuntimeError(
                f"l2Book time date mismatch at {object_key}:{line_number} "
                f"key={expected_date} payload={actual_date}"
            )

        if actual_hour != expected_hour:
            raise RuntimeError(
                f"l2Book time hour mismatch at {object_key}:{line_number} "
                f"key={expected_hour} payload={actual_hour}"
            )

        row_count += 1
        min_event_ts_ms = timestamp_ms if min_event_ts_ms is None else min(min_event_ts_ms, timestamp_ms)
        max_event_ts_ms = timestamp_ms if max_event_ts_ms is None else max(max_event_ts_ms, timestamp_ms)

    if row_count <= 0 or min_event_ts_ms is None or max_event_ts_ms is None:
        raise RuntimeError(f"l2Book object contains no rows: {object_key}")

    return ValidationResult(
        source=object_key,
        kind="market_data_l2_book",
        row_count=row_count,
        min_event_ts_ms=min_event_ts_ms,
        max_event_ts_ms=max_event_ts_ms,
        details={
            "coin": expected_coin,
            "date": expected_date,
            "hour": expected_hour,
        },
    )


def _validate_asset_ctxs_object(*, object_key: str, payload: bytes) -> ValidationResult:
    match = _ASSET_CTXS_OBJECT_KEY_RE.fullmatch(object_key)
    if match is None:
        raise RuntimeError(f"invalid asset_ctxs object key: {object_key}")

    expected_date = match.group("date")
    _parse_yyyymmdd(expected_date, source=object_key)

    text = payload.decode("utf-8")
    rows = list(csv.reader(text.splitlines()))

    if not rows:
        raise RuntimeError(f"asset_ctxs object contains no rows: {object_key}")

    header = rows[0] if rows and rows[0] and rows[0][0] == "time" else None
    data_rows = rows[1:] if header is not None else rows

    if not data_rows:
        raise RuntimeError(f"asset_ctxs object contains no data rows: {object_key}")

    if header is not None:
        if "time" not in header:
            raise RuntimeError(f"asset_ctxs header missing time: {object_key}")
        if "coin" not in header:
            raise RuntimeError(f"asset_ctxs header missing coin: {object_key}")
        time_index = header.index("time")
        coin_index = header.index("coin")
    else:
        time_index = 0
        coin_index = 1

    row_count = 0
    min_event_ts_ms: int | None = None
    max_event_ts_ms: int | None = None
    coins: set[str] = set()

    for row_index, row in enumerate(data_rows, start=2 if header is not None else 1):
        if not row or all(not cell for cell in row):
            continue

        if len(row) <= max(time_index, coin_index):
            raise RuntimeError(f"asset_ctxs row too short at {object_key}:{row_index}")

        try:
            timestamp_ms = int(row[time_index])
        except ValueError as exc:
            raise RuntimeError(f"asset_ctxs invalid time at {object_key}:{row_index}") from exc

        if not is_timestamp_ms(timestamp_ms):
            raise RuntimeError(f"asset_ctxs invalid millisecond timestamp at {object_key}:{row_index}")

        actual_date, _ = _official_date_hour_from_ms(timestamp_ms)
        if actual_date != expected_date:
            raise RuntimeError(
                f"asset_ctxs time date mismatch at {object_key}:{row_index} "
                f"key={expected_date} row={actual_date}"
            )

        coin = row[coin_index]
        if not coin:
            raise RuntimeError(f"asset_ctxs empty coin at {object_key}:{row_index}")

        row_count += 1
        coins.add(coin)
        min_event_ts_ms = timestamp_ms if min_event_ts_ms is None else min(min_event_ts_ms, timestamp_ms)
        max_event_ts_ms = timestamp_ms if max_event_ts_ms is None else max(max_event_ts_ms, timestamp_ms)

    if row_count <= 0 or min_event_ts_ms is None or max_event_ts_ms is None:
        raise RuntimeError(f"asset_ctxs object contains no valid data rows: {object_key}")

    return ValidationResult(
        source=object_key,
        kind="asset_ctxs",
        row_count=row_count,
        min_event_ts_ms=min_event_ts_ms,
        max_event_ts_ms=max_event_ts_ms,
        details={
            "date": expected_date,
            "coins": len(coins),
            "has_header": header is not None,
        },
    )


def _validate_raw_event(event: RawEvent, *, source: str) -> None:
    if event.schema_version != "xyz_tradfi_archiver.raw_event.v1":
        raise RuntimeError(f"invalid schema_version at {source}: {event.schema_version}")

    if event.source != "hyperliquid":
        raise RuntimeError(f"invalid source at {source}: {event.source}")

    if event.dex != "xyz":
        raise RuntimeError(f"invalid dex at {source}: {event.dex}")

    if event.datatype not in _ALLOWED_RAW_DATATYPES:
        raise RuntimeError(f"invalid datatype at {source}: {event.datatype}")

    if not is_timestamp_ms(event.event_ts_ms):
        raise RuntimeError(f"invalid event_ts_ms at {source}: {event.event_ts_ms}")

    if not is_timestamp_ms(event.ingested_at_ms):
        raise RuntimeError(f"invalid ingested_at_ms at {source}: {event.ingested_at_ms}")

    if event.symbol is not None and not isinstance(event.symbol, str):
        raise RuntimeError(f"invalid symbol at {source}: {event.symbol}")

    if event.canonical_symbol is not None and not isinstance(event.canonical_symbol, str):
        raise RuntimeError(f"invalid canonical_symbol at {source}: {event.canonical_symbol}")

    if event.interval is not None:
        raise RuntimeError(f"interval must be null at {source}: {event.interval}")

    _validate_raw_payload_shape(event, source=source)


def _validate_raw_payload_shape(event: RawEvent, *, source: str) -> None:
    payload = event.payload

    if event.datatype == "meta_asset_ctxs":
        if not isinstance(payload, dict):
            raise RuntimeError(f"meta_asset_ctxs payload must be an object at {source}")

        meta = payload.get("meta")
        contexts = payload.get("contexts")

        if not isinstance(meta, dict):
            raise RuntimeError(f"meta_asset_ctxs.meta must be an object at {source}")
        if not isinstance(contexts, list):
            raise RuntimeError(f"meta_asset_ctxs.contexts must be an array at {source}")

        universe = meta.get("universe")
        if not isinstance(universe, list):
            raise RuntimeError(f"meta_asset_ctxs.meta.universe must be an array at {source}")
        if len(universe) != len(contexts):
            raise RuntimeError(f"meta_asset_ctxs universe/context length mismatch at {source}")

        return

    if event.datatype in {"l2_snapshot", "ws_l2_book"}:
        if not isinstance(payload, dict):
            raise RuntimeError(f"{event.datatype} payload must be an object at {source}")

        expected_coin = event.symbol
        if expected_coin is None:
            raise RuntimeError(f"{event.datatype} raw event missing symbol at {source}")

        _validate_l2_book_payload(payload, expected_coin=expected_coin, source=source)
        return

    if event.datatype == "health":
        if not isinstance(payload, dict):
            raise RuntimeError(f"health payload must be an object at {source}")
        return

    raise RuntimeError(f"unhandled datatype at {source}: {event.datatype}")


def _validate_l2_book_payload(payload: dict[str, Any], *, expected_coin: str, source: str) -> None:
    if _FORBIDDEN_OFFICIAL_L2_KEYS.intersection(payload):
        raise RuntimeError(f"l2Book payload contains Mosaic wrapper keys at {source}")

    coin = payload.get("coin")
    timestamp_ms = payload.get("time")
    levels = payload.get("levels")

    if not isinstance(coin, str) or not coin:
        raise RuntimeError(f"l2Book.coin must be a non-empty string at {source}")

    if coin != expected_coin:
        raise RuntimeError(f"l2Book.coin mismatch at {source}: expected={expected_coin} actual={coin}")

    if not is_timestamp_ms(timestamp_ms):
        raise RuntimeError(f"l2Book.time must be a millisecond timestamp at {source}")

    if not isinstance(levels, list) or len(levels) != 2:
        raise RuntimeError(f"l2Book.levels must contain bids and asks at {source}")

    if not all(isinstance(side, list) for side in levels):
        raise RuntimeError(f"l2Book.levels sides must be arrays at {source}")

    for side_index, side in enumerate(levels):
        for level_index, level in enumerate(side):
            _validate_l2_level(level, source=f"{source}:levels[{side_index}][{level_index}]")


def _validate_l2_level(level: Any, *, source: str) -> None:
    if not isinstance(level, dict):
        raise RuntimeError(f"l2Book level must be an object at {source}")

    px = level.get("px")
    sz = level.get("sz")
    n = level.get("n")

    if not isinstance(px, str) or not px:
        raise RuntimeError(f"l2Book level.px must be a non-empty string at {source}")

    if not isinstance(sz, str) or not sz:
        raise RuntimeError(f"l2Book level.sz must be a non-empty string at {source}")

    if not isinstance(n, int) or isinstance(n, bool) or n < 0:
        raise RuntimeError(f"l2Book level.n must be a non-negative integer at {source}")


def _read_lz4_file(path: Path) -> bytes:
    with path.open("rb") as raw_file:
        with lz4.frame.open(raw_file, mode="rb") as compressed_file:
            return compressed_file.read()


def _iter_non_empty_lines(payload: bytes) -> list[tuple[int, str]]:
    result: list[tuple[int, str]] = []

    for line_number, raw_line in enumerate(payload.splitlines(), start=1):
        if not raw_line.strip():
            continue

        try:
            line = raw_line.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RuntimeError(f"invalid UTF-8 line at {line_number}") from exc

        result.append((line_number, line))

    return result


def _load_json_object(line: str, *, source: str) -> dict[str, Any]:
    try:
        payload = loads(line)
    except Exception as exc:
        raise RuntimeError(f"invalid strict JSON at {source}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError(f"JSON line is not an object at {source}")

    return payload


def _raw_event(payload: dict[str, Any], *, source: str) -> RawEvent:
    try:
        return RawEvent(**payload)
    except Exception as exc:
        raise RuntimeError(f"invalid RawEvent at {source}") from exc


def _official_date_hour_from_ms(timestamp_ms: int) -> tuple[str, int]:
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
    return dt.strftime("%Y%m%d"), dt.hour


def _parse_yyyymmdd(value: str, *, source: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y%m%d").replace(tzinfo=UTC)
    except ValueError as exc:
        raise RuntimeError(f"invalid YYYYMMDD date at {source}: {value}") from exc


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()

    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            hasher.update(chunk)

    return hasher.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))