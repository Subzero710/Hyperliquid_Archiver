from __future__ import annotations

import time
from datetime import UTC, datetime


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).isoformat()


def date_hour_from_ms(timestamp_ms: int) -> tuple[str, str]:
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H")


def official_date_hour_from_ms(timestamp_ms: int) -> tuple[str, str]:
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
    return dt.strftime("%Y%m%d"), str(dt.hour)


def is_timestamp_ms(value: object) -> bool:
    if not isinstance(value, int) or isinstance(value, bool):
        return False
    return 946684800000 <= value <= 4102444800000


def interval_to_ms(interval: str) -> int:
    units = {
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
    }
    if len(interval) < 2:
        raise ValueError(f"invalid interval: {interval}")
    unit = interval[-1]
    if unit not in units:
        raise ValueError(f"invalid interval unit: {interval}")
    return int(interval[:-1]) * units[unit]