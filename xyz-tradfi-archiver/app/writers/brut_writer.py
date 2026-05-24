from __future__ import annotations

from collections import defaultdict
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

import zstandard as zstd

from app.domain.events import RawEvent
from app.storage.metadata_store import MetadataStore
from app.storage.object_store import ObjectStore
from app.utils.json import loads
from app.writers.manifest_writer import write_object_manifest


class BrutWriter:
    def __init__(self, *, object_store: ObjectStore, metadata_store: MetadataStore) -> None:
        self.object_store = object_store
        self.metadata_store = metadata_store

    def write_segment(self, segment: Path) -> list[RawEvent]:
        events = self._read_events(segment)
        grouped: dict[str, list[RawEvent]] = defaultdict(list)
        for event in events:
            grouped[event.object_partition(root="brut")].append(event)

        segment_id = segment.name.replace(".sealed.jsonl", "")
        with TemporaryDirectory() as tmp_dir_raw:
            tmp_dir = Path(tmp_dir_raw)
            for partition, partition_events in grouped.items():
                key = f"{partition}/part-{segment_id}.jsonl.zst"
                path = tmp_dir / (sha256(key.encode("utf-8")).hexdigest() + ".jsonl.zst")
                checksum = self._write_zst(path=path, events=partition_events)
                self.object_store.put_file(key=key, path=path, content_type="application/zstd")
                min_event_ts_ms = min(event.event_ts_ms for event in partition_events)
                max_event_ts_ms = max(event.event_ts_ms for event in partition_events)
                self.metadata_store.record_object(
                    key=key,
                    kind="brut",
                    source_segment=segment.name,
                    row_count=len(partition_events),
                    checksum_sha256=checksum,
                    min_event_ts_ms=min_event_ts_ms,
                    max_event_ts_ms=max_event_ts_ms,
                )
                write_object_manifest(
                    object_store=self.object_store,
                    object_key=key,
                    kind="brut",
                    source_segment=segment.name,
                    row_count=len(partition_events),
                    checksum_sha256=checksum,
                    min_event_ts_ms=min_event_ts_ms,
                    max_event_ts_ms=max_event_ts_ms,
                )
        return events

    def _read_events(self, segment: Path) -> list[RawEvent]:
        events: list[RawEvent] = []
        with segment.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                events.append(RawEvent.model_validate(loads(line)))
        return events

    def _write_zst(self, *, path: Path, events: list[RawEvent]) -> str:
        digest = sha256()
        compressor = zstd.ZstdCompressor(level=6)
        with path.open("wb") as raw_file:
            with compressor.stream_writer(raw_file) as writer:
                for event in events:
                    encoded = event.line().encode("utf-8")
                    digest.update(encoded)
                    writer.write(encoded)
        return digest.hexdigest()
