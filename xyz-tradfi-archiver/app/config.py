from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        if default is None:
            raise RuntimeError(f"missing required environment variable: {name}")
        return default
    return value


def _env_int(name: str, default: int) -> int:
    value = _env(name, str(default))
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"invalid integer environment variable: {name}={value}") from exc


def _env_float(name: str, default: float) -> float:
    value = _env(name, str(default))
    try:
        return float(value)
    except ValueError as exc:
        raise RuntimeError(f"invalid float environment variable: {name}={value}") from exc


def _env_bool(name: str, default: bool) -> bool:
    value = _env(name, "true" if default else "false").strip().lower()

    if value in {"1", "true", "yes", "y", "on"}:
        return True

    if value in {"0", "false", "no", "n", "off"}:
        return False

    raise RuntimeError(f"invalid boolean environment variable: {name}={value}")


@dataclass(frozen=True)
class Settings:
    archive_bucket: str
    archive_s3_endpoint: str
    archive_s3_access_key: str
    archive_s3_secret_key: str
    archive_s3_region: str
    archive_s3_force_path_style: bool
    archive_s3_verify_bucket: bool

    archiver_state_dir: Path
    archiver_run_id: str

    hyperliquid_base_url: str
    hyperliquid_websocket_url: str
    hyperliquid_dex: str
    hyperliquid_symbol_allowlist: str
    hyperliquid_http_timeout_s: float

    archive_timeframes: str
    archive_candle_lookback_bars: int
    archive_funding_lookback_days: int

    recorder_fsync_every_events: int
    recorder_segment_max_bytes: int
    recorder_segment_max_age_seconds: int
    recorder_idle_sleep_seconds: float

    poll_perp_dexs_seconds: int
    poll_meta_asset_ctxs_seconds: int
    poll_all_mids_seconds: int
    poll_l2_snapshot_seconds: int
    poll_funding_history_seconds: int
    poll_candles_seconds: int

    writer_loop_sleep_seconds: float
    validator_loop_sleep_seconds: float

    validator_startup_grace_seconds: int
    validator_recorder_stale_seconds: int
    validator_writer_stale_seconds: int
    validator_max_sealed_segments: int
    validator_max_failed_segments: int
    validator_require_objects_after_grace: bool
    validator_verify_object_store: bool

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            archive_bucket=_env("ARCHIVE_BUCKET"),
            archive_s3_endpoint=_env("ARCHIVE_S3_ENDPOINT"),
            archive_s3_access_key=_env("ARCHIVE_S3_ACCESS_KEY"),
            archive_s3_secret_key=_env("ARCHIVE_S3_SECRET_KEY"),
            archive_s3_region=_env("ARCHIVE_S3_REGION", "us-east-1"),
            archive_s3_force_path_style=_env_bool("ARCHIVE_S3_FORCE_PATH_STYLE", True),
            archive_s3_verify_bucket=_env_bool("ARCHIVE_S3_VERIFY_BUCKET", True),
            archiver_state_dir=Path(_env("ARCHIVER_STATE_DIR", "/var/lib/xyz-tradfi-archiver")),
            archiver_run_id=_env("ARCHIVER_RUN_ID", uuid4().hex),
            hyperliquid_base_url=_env("HYPERLIQUID_BASE_URL", "https://api.hyperliquid.xyz"),
            hyperliquid_websocket_url=_env("HYPERLIQUID_WEBSOCKET_URL", "wss://api.hyperliquid.xyz/ws"),
            hyperliquid_dex=_env("HYPERLIQUID_DEX", "xyz"),
            hyperliquid_symbol_allowlist=_env("HYPERLIQUID_SYMBOL_ALLOWLIST", ""),
            hyperliquid_http_timeout_s=_env_float("HYPERLIQUID_HTTP_TIMEOUT_S", 10.0),
            archive_timeframes=_env("ARCHIVE_TIMEFRAMES", "15m,4h,1d"),
            archive_candle_lookback_bars=_env_int("ARCHIVE_CANDLE_LOOKBACK_BARS", 8),
            archive_funding_lookback_days=_env_int("ARCHIVE_FUNDING_LOOKBACK_DAYS", 2),
            recorder_fsync_every_events=_env_int("RECORDER_FSYNC_EVERY_EVENTS", 1),
            recorder_segment_max_bytes=_env_int("RECORDER_SEGMENT_MAX_BYTES", 134217728),
            recorder_segment_max_age_seconds=_env_int("RECORDER_SEGMENT_MAX_AGE_SECONDS", 300),
            recorder_idle_sleep_seconds=_env_float("RECORDER_IDLE_SLEEP_SECONDS", 1.0),
            poll_perp_dexs_seconds=_env_int("POLL_PERP_DEXS_SECONDS", 21600),
            poll_meta_asset_ctxs_seconds=_env_int("POLL_META_ASSET_CTXS_SECONDS", 300),
            poll_all_mids_seconds=_env_int("POLL_ALL_MIDS_SECONDS", 60),
            poll_l2_snapshot_seconds=_env_int("POLL_L2_SNAPSHOT_SECONDS", 300),
            poll_funding_history_seconds=_env_int("POLL_FUNDING_HISTORY_SECONDS", 3600),
            poll_candles_seconds=_env_int("POLL_CANDLES_SECONDS", 900),
            writer_loop_sleep_seconds=_env_float("WRITER_LOOP_SLEEP_SECONDS", 5.0),
            validator_loop_sleep_seconds=_env_float("VALIDATOR_LOOP_SLEEP_SECONDS", 60.0),
            validator_startup_grace_seconds=_env_int("VALIDATOR_STARTUP_GRACE_SECONDS", 900),
            validator_recorder_stale_seconds=_env_int("VALIDATOR_RECORDER_STALE_SECONDS", 180),
            validator_writer_stale_seconds=_env_int("VALIDATOR_WRITER_STALE_SECONDS", 900),
            validator_max_sealed_segments=_env_int("VALIDATOR_MAX_SEALED_SEGMENTS", 20),
            validator_max_failed_segments=_env_int("VALIDATOR_MAX_FAILED_SEGMENTS", 0),
            validator_require_objects_after_grace=_env_bool("VALIDATOR_REQUIRE_OBJECTS_AFTER_GRACE", True),
            validator_verify_object_store=_env_bool("VALIDATOR_VERIFY_OBJECT_STORE", True),
        )

    @property
    def spool_dir(self) -> Path:
        return self.archiver_state_dir / "spool"

    @property
    def metadata_db_path(self) -> Path:
        return self.archiver_state_dir / "metadata.sqlite3"

    @property
    def timeframes(self) -> tuple[str, ...]:
        return tuple(item.strip() for item in self.archive_timeframes.split(",") if item.strip())

    @property
    def symbol_allowlist(self) -> set[str]:
        return {item.strip() for item in self.hyperliquid_symbol_allowlist.split(",") if item.strip()}