from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env(name: str) -> str:
    value = os.environ.get(name)

    if value is None or value == "":
        raise RuntimeError(f"missing required environment variable: {name}")

    return value


def _env_int(name: str) -> int:
    value = _env(name)
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"invalid integer environment variable: {name}={value}") from exc


def _env_float(name: str) -> float:
    value = _env(name)
    try:
        return float(value)
    except ValueError as exc:
        raise RuntimeError(f"invalid float environment variable: {name}={value}") from exc


def _env_bool(name: str) -> bool:
    value = _env(name).strip().lower()

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

    archive_enable_l2_snapshots: bool
    archive_l2_request_sleep_seconds: float
    archive_startup_l2_delay_seconds: int

    recorder_fsync_every_events: int
    recorder_segment_max_bytes: int
    recorder_segment_max_age_seconds: int
    recorder_idle_sleep_seconds: float

    poll_meta_asset_ctxs_seconds: int
    poll_l2_snapshot_seconds: int

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
            archive_s3_region=_env("ARCHIVE_S3_REGION"),
            archive_s3_force_path_style=_env_bool("ARCHIVE_S3_FORCE_PATH_STYLE"),
            archive_s3_verify_bucket=_env_bool("ARCHIVE_S3_VERIFY_BUCKET"),
            archiver_state_dir=Path(_env("ARCHIVER_STATE_DIR")),
            archiver_run_id=_env("ARCHIVER_RUN_ID"),
            hyperliquid_base_url=_env("HYPERLIQUID_BASE_URL"),
            hyperliquid_websocket_url=_env("HYPERLIQUID_WEBSOCKET_URL"),
            hyperliquid_dex=_env("HYPERLIQUID_DEX"),
            hyperliquid_symbol_allowlist=os.environ.get("HYPERLIQUID_SYMBOL_ALLOWLIST", "").strip(),
            hyperliquid_http_timeout_s=_env_float("HYPERLIQUID_HTTP_TIMEOUT_S"),
            archive_enable_l2_snapshots=_env_bool("ARCHIVE_ENABLE_L2_SNAPSHOTS"),
            archive_l2_request_sleep_seconds=_env_float("ARCHIVE_L2_REQUEST_SLEEP_SECONDS"),
            archive_startup_l2_delay_seconds=_env_int("ARCHIVE_STARTUP_L2_DELAY_SECONDS"),
            recorder_fsync_every_events=_env_int("RECORDER_FSYNC_EVERY_EVENTS"),
            recorder_segment_max_bytes=_env_int("RECORDER_SEGMENT_MAX_BYTES"),
            recorder_segment_max_age_seconds=_env_int("RECORDER_SEGMENT_MAX_AGE_SECONDS"),
            recorder_idle_sleep_seconds=_env_float("RECORDER_IDLE_SLEEP_SECONDS"),
            poll_meta_asset_ctxs_seconds=_env_int("POLL_META_ASSET_CTXS_SECONDS"),
            poll_l2_snapshot_seconds=_env_int("POLL_L2_SNAPSHOT_SECONDS"),
            writer_loop_sleep_seconds=_env_float("WRITER_LOOP_SLEEP_SECONDS"),
            validator_loop_sleep_seconds=_env_float("VALIDATOR_LOOP_SLEEP_SECONDS"),
            validator_startup_grace_seconds=_env_int("VALIDATOR_STARTUP_GRACE_SECONDS"),
            validator_recorder_stale_seconds=_env_int("VALIDATOR_RECORDER_STALE_SECONDS"),
            validator_writer_stale_seconds=_env_int("VALIDATOR_WRITER_STALE_SECONDS"),
            validator_max_sealed_segments=_env_int("VALIDATOR_MAX_SEALED_SEGMENTS"),
            validator_max_failed_segments=_env_int("VALIDATOR_MAX_FAILED_SEGMENTS"),
            validator_require_objects_after_grace=_env_bool("VALIDATOR_REQUIRE_OBJECTS_AFTER_GRACE"),
            validator_verify_object_store=_env_bool("VALIDATOR_VERIFY_OBJECT_STORE"),
        )

    @property
    def spool_dir(self) -> Path:
        return self.archiver_state_dir / "spool"

    @property
    def metadata_db_path(self) -> Path:
        return self.archiver_state_dir / "metadata.sqlite3"

    @property
    def symbol_allowlist(self) -> set[str]:
        return {item.strip() for item in self.hyperliquid_symbol_allowlist.split(",") if item.strip()}