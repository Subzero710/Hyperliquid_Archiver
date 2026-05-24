from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    archive_bucket: str = "xyz-tradfi-archive"
    archive_s3_endpoint: str | None = "http://minio:9000"
    archive_s3_access_key: str = "minioadmin"
    archive_s3_secret_key: str = "minioadmin"
    archive_s3_region: str = "us-east-1"
    archive_s3_force_path_style: bool = True

    archiver_state_dir: Path = Path("/var/lib/xyz-tradfi-archiver")
    archiver_run_id: str = Field(default_factory=lambda: uuid4().hex)

    hyperliquid_base_url: str = "https://api.hyperliquid.xyz"
    hyperliquid_websocket_url: str = "wss://api.hyperliquid.xyz/ws"
    hyperliquid_dex: str = "xyz"
    hyperliquid_symbol_allowlist: str = ""
    hyperliquid_http_timeout_s: float = 10.0

    archive_timeframes: str = "15m,4h,1d"
    archive_candle_lookback_bars: int = 240
    archive_funding_lookback_days: int = 7

    recorder_fsync_every_events: int = 1
    recorder_segment_max_bytes: int = 128 * 1024 * 1024
    recorder_segment_max_age_seconds: int = 300
    recorder_idle_sleep_seconds: float = 1.0

    poll_perp_dexs_seconds: int = 3600
    poll_meta_asset_ctxs_seconds: int = 30
    poll_all_mids_seconds: int = 5
    poll_l2_snapshot_seconds: int = 10
    poll_funding_history_seconds: int = 900
    poll_candles_seconds: int = 60

    writer_loop_sleep_seconds: float = 5.0
    validator_loop_sleep_seconds: float = 60.0

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
