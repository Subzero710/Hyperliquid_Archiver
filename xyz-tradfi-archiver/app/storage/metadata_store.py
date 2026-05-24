from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from app.utils.time import now_ms


class MetadataStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(
            str(path),
            isolation_level=None,
            timeout=30.0,
        )
        self.connection.row_factory = sqlite3.Row
        self._configure_connection()
        self._init_schema()

    def record_object(
        self,
        *,
        key: str,
        kind: str,
        source_segment: str,
        row_count: int,
        checksum_sha256: str,
        min_event_ts_ms: int | None,
        max_event_ts_ms: int | None,
    ) -> None:
        self.connection.execute(
            """
            insert into archive_object(
              key, kind, source_segment, row_count, checksum_sha256,
              min_event_ts_ms, max_event_ts_ms, created_at_ms
            ) values (?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(key) do update set
              kind = excluded.kind,
              source_segment = excluded.source_segment,
              row_count = excluded.row_count,
              checksum_sha256 = excluded.checksum_sha256,
              min_event_ts_ms = excluded.min_event_ts_ms,
              max_event_ts_ms = excluded.max_event_ts_ms,
              created_at_ms = excluded.created_at_ms
            """,
            (
                key,
                kind,
                source_segment,
                row_count,
                checksum_sha256,
                min_event_ts_ms,
                max_event_ts_ms,
                now_ms(),
            ),
        )

    def record_health(self, *, event_type: str, severity: str, message: str, details_json: str) -> None:
        self.connection.execute(
            """
            insert into archive_health(event_type, severity, message, details_json, created_at_ms)
            values (?, ?, ?, ?, ?)
            """,
            (event_type, severity, message, details_json, now_ms()),
        )

    def object_count(self) -> int:
        row = self.connection.execute("select count(*) as n from archive_object").fetchone()
        return int(row["n"])

    def latest_objects(self, *, limit: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "select * from archive_object order by created_at_ms desc limit ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def latest_health(self, *, limit: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "select * from archive_health order by created_at_ms desc limit ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def close(self) -> None:
        self.connection.close()

    def _configure_connection(self) -> None:
        self.connection.execute("pragma journal_mode = wal")
        self.connection.execute("pragma synchronous = full")
        self.connection.execute("pragma busy_timeout = 30000")
        self.connection.execute("pragma foreign_keys = on")

    def _init_schema(self) -> None:
        self.connection.executescript(
            """
            create table if not exists archive_object (
              key text primary key,
              kind text not null,
              source_segment text not null,
              row_count integer not null,
              checksum_sha256 text not null,
              min_event_ts_ms integer,
              max_event_ts_ms integer,
              created_at_ms integer not null
            );

            create index if not exists idx_archive_object_created_at_ms
            on archive_object(created_at_ms);

            create index if not exists idx_archive_object_source_segment
            on archive_object(source_segment);

            create table if not exists archive_health (
              id integer primary key autoincrement,
              event_type text not null,
              severity text not null,
              message text not null,
              details_json text not null,
              created_at_ms integer not null
            );

            create index if not exists idx_archive_health_created_at_ms
            on archive_health(created_at_ms);

            create index if not exists idx_archive_health_severity
            on archive_health(severity);
            """
        )