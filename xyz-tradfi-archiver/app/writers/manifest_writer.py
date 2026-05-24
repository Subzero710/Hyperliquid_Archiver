from __future__ import annotations

from hashlib import sha256
from typing import Any

from app.storage.object_store import ObjectStore
from app.utils.json import dumps
from app.utils.time import now_ms


def write_object_manifest(
    *,
    object_store: ObjectStore,
    object_key: str,
    kind: str,
    source_segment: str,
    row_count: int,
    checksum_sha256: str,
    min_event_ts_ms: int | None,
    max_event_ts_ms: int | None,
) -> None:
    object_id = sha256(object_key.encode("utf-8")).hexdigest()
    manifest: dict[str, Any] = {
        "schema_version": "xyz_tradfi_archiver.object_manifest.v1",
        "object_key": object_key,
        "kind": kind,
        "source_segment": source_segment,
        "row_count": row_count,
        "checksum_sha256": checksum_sha256,
        "min_event_ts_ms": min_event_ts_ms,
        "max_event_ts_ms": max_event_ts_ms,
        "created_at_ms": now_ms(),
    }
    object_store.put_bytes(
        key=f"manifests/kind={kind}/{object_id}.json",
        data=(dumps(manifest) + "\n").encode("utf-8"),
        content_type="application/json",
    )
    object_store.put_bytes(
        key=f"checksums/kind={kind}/{object_id}.sha256",
        data=f"{checksum_sha256}  {object_key}\n".encode("utf-8"),
        content_type="text/plain",
    )
