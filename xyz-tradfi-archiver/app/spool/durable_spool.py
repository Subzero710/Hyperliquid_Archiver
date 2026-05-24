from __future__ import annotations

import os
import time
from pathlib import Path
from uuid import uuid4

from app.domain.events import RawEvent
from app.utils.time import now_ms


class DurableSpool:
    def __init__(
        self,
        *,
        root: Path,
        fsync_every_events: int,
        segment_max_bytes: int,
        segment_max_age_seconds: int,
    ) -> None:
        self.root = root
        self.open_dir = root / "open"
        self.sealed_dir = root / "sealed"
        self.done_dir = root / "done"
        self.failed_dir = root / "failed"
        self.fsync_every_events = max(1, fsync_every_events)
        self.segment_max_bytes = segment_max_bytes
        self.segment_max_age_seconds = segment_max_age_seconds
        self._current_path: Path | None = None
        self._current_file = None
        self._current_started_ms: int | None = None
        self._events_since_fsync = 0
        for directory in (self.open_dir, self.sealed_dir, self.done_dir, self.failed_dir):
            directory.mkdir(parents=True, exist_ok=True)

    def append(self, event: RawEvent) -> None:
        self._ensure_current_file()
        assert self._current_file is not None
        self._current_file.write(event.line())
        self._events_since_fsync += 1
        if self._events_since_fsync >= self.fsync_every_events:
            self._flush_current()
        if self._should_rotate():
            self.rotate()

    def rotate(self) -> None:
        if self._current_file is None or self._current_path is None:
            return
        self._flush_current()
        self._current_file.close()
        sealed_path = self.sealed_dir / self._current_path.name.replace(".open.jsonl", ".sealed.jsonl")
        os.replace(self._current_path, sealed_path)
        self._current_path = None
        self._current_file = None
        self._current_started_ms = None
        self._events_since_fsync = 0

    def sealed_segments(self) -> list[Path]:
        return sorted(self.sealed_dir.glob("*.sealed.jsonl"))

    def mark_done(self, segment: Path) -> Path:
        self.done_dir.mkdir(parents=True, exist_ok=True)
        destination = self.done_dir / segment.name.replace(".sealed.jsonl", ".done.jsonl")
        os.replace(segment, destination)
        return destination

    def mark_failed(self, segment: Path) -> Path:
        self.failed_dir.mkdir(parents=True, exist_ok=True)
        destination = self.failed_dir / segment.name.replace(".sealed.jsonl", ".failed.jsonl")
        os.replace(segment, destination)
        return destination

    def _ensure_current_file(self) -> None:
        if self._current_file is not None:
            return
        created_ms = now_ms()
        name = f"segment-{created_ms}-{uuid4().hex}.open.jsonl"
        self._current_path = self.open_dir / name
        self._current_started_ms = created_ms
        self._current_file = self._current_path.open("a", encoding="utf-8", buffering=1)

    def _flush_current(self) -> None:
        if self._current_file is None:
            return
        self._current_file.flush()
        os.fsync(self._current_file.fileno())
        self._events_since_fsync = 0

    def _should_rotate(self) -> bool:
        if self._current_path is None or self._current_started_ms is None:
            return False
        try:
            size = self._current_path.stat().st_size
        except FileNotFoundError:
            return False
        if size >= self.segment_max_bytes:
            return True
        age_s = (now_ms() - self._current_started_ms) / 1000
        return age_s >= self.segment_max_age_seconds
