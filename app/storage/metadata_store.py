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
        self._upsert_archive_object(
            key=key,
            kind=kind,
            source_segment=source_segment,
            row_count=row_count,
            checksum_sha256=checksum_sha256,
            min_event_ts_ms=min_event_ts_ms,
            max_event_ts_ms=max_event_ts_ms,
        )

    def prune_archive_objects(self, *, current_date: str, current_hour: str) -> int:
        cursor = self.connection.execute(
            """
            delete from archive_object
            where key not like ?
              and key != ?
            """,
            (
                f"market_data/{current_date}/{current_hour}/%",
                f"asset_ctxs/{current_date}.csv.lz4",
            ),
        )
        return max(0, int(cursor.rowcount))

    def get_segment_object(self, *, source_segment: str, object_key: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            """
            select *
            from archive_segment_object
            where source_segment = ? and object_key = ?
            """,
            (source_segment, object_key),
        ).fetchone()
        return dict(row) if row is not None else None

    def begin_segment_object(
        self,
        *,
        source_segment: str,
        object_key: str,
        kind: str,
        row_count: int,
        checksum_sha256: str,
        min_event_ts_ms: int,
        max_event_ts_ms: int,
    ) -> None:
        existing = self.get_segment_object(source_segment=source_segment, object_key=object_key)
        if existing is not None:
            expected = {
                "kind": kind,
                "row_count": row_count,
                "checksum_sha256": checksum_sha256,
                "min_event_ts_ms": min_event_ts_ms,
                "max_event_ts_ms": max_event_ts_ms,
            }
            actual = {name: existing[name] for name in expected}
            if actual != expected:
                raise RuntimeError(
                    "segment/object transaction changed "
                    f"source_segment={source_segment} object_key={object_key} "
                    f"expected={expected} actual={actual}"
                )
            return

        self.connection.execute(
            """
            insert into archive_segment_object(
              source_segment, object_key, status, kind, row_count,
              checksum_sha256, min_event_ts_ms, max_event_ts_ms, updated_at_ms
            ) values (?, ?, 'pending', ?, ?, ?, ?, ?, ?)
            """,
            (
                source_segment,
                object_key,
                kind,
                row_count,
                checksum_sha256,
                min_event_ts_ms,
                max_event_ts_ms,
                now_ms(),
            ),
        )

    def complete_segment_object(self, *, source_segment: str, object_key: str) -> dict[str, Any]:
        row = self.connection.execute(
            """
            select *
            from archive_segment_object
            where source_segment = ? and object_key = ?
            """,
            (source_segment, object_key),
        ).fetchone()
        if row is None:
            raise RuntimeError(
                f"missing segment/object transaction source_segment={source_segment} object_key={object_key}"
            )

        payload = dict(row)
        try:
            self.connection.execute("begin immediate")
            self._upsert_archive_object(
                key=object_key,
                kind=str(payload["kind"]),
                source_segment=source_segment,
                row_count=int(payload["row_count"]),
                checksum_sha256=str(payload["checksum_sha256"]),
                min_event_ts_ms=int(payload["min_event_ts_ms"]),
                max_event_ts_ms=int(payload["max_event_ts_ms"]),
            )
            self.connection.execute(
                """
                update archive_segment_object
                set status = 'applied', updated_at_ms = ?
                where source_segment = ? and object_key = ?
                """,
                (now_ms(), source_segment, object_key),
            )
            self.connection.execute("commit")
        except Exception:
            self.connection.execute("rollback")
            raise

        payload["status"] = "applied"
        return payload

    def delete_segment_objects(self, *, source_segment: str) -> None:
        self.connection.execute(
            "delete from archive_segment_object where source_segment = ? and status = 'applied'",
            (source_segment,),
        )

    def prune_applied_segment_objects(self, *, active_source_segments: set[str]) -> int:
        rows = self.connection.execute(
            "select distinct source_segment from archive_segment_object where status = 'applied'"
        ).fetchall()
        stale = [str(row["source_segment"]) for row in rows if str(row["source_segment"]) not in active_source_segments]
        if not stale:
            return 0

        self.connection.executemany(
            "delete from archive_segment_object where source_segment = ? and status = 'applied'",
            [(name,) for name in stale],
        )
        return len(stale)

    def pending_segment_objects(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "select * from archive_segment_object where status = 'pending' order by updated_at_ms"
        ).fetchall()
        return [dict(row) for row in rows]

    def record_health(self, *, event_type: str, severity: str, message: str, details_json: str) -> None:
        self.connection.execute(
            """
            insert into archive_health_state(event_type, severity, message, details_json, created_at_ms)
            values (?, ?, ?, ?, ?)
            on conflict(event_type) do update set
              severity = excluded.severity,
              message = excluded.message,
              details_json = excluded.details_json,
              created_at_ms = excluded.created_at_ms
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
            "select * from archive_health_state order by created_at_ms desc limit ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def vacuum(self) -> None:
        self.connection.execute("pragma wal_checkpoint(truncate)")
        self.connection.execute("vacuum")
        self.connection.execute("pragma wal_checkpoint(truncate)")

    def close(self) -> None:
        self.connection.close()

    def _upsert_archive_object(
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

            create table if not exists archive_segment_object (
              source_segment text not null,
              object_key text not null,
              status text not null check(status in ('pending', 'applied')),
              kind text not null,
              row_count integer not null,
              checksum_sha256 text not null,
              min_event_ts_ms integer not null,
              max_event_ts_ms integer not null,
              updated_at_ms integer not null,
              primary key(source_segment, object_key)
            );

            create index if not exists idx_archive_segment_object_status
            on archive_segment_object(status, updated_at_ms);

            create table if not exists archive_health_state (
              event_type text primary key,
              severity text not null,
              message text not null,
              details_json text not null,
              created_at_ms integer not null
            );
            """
        )

        legacy_health = self.connection.execute(
            "select 1 from sqlite_master where type = 'table' and name = 'archive_health'"
        ).fetchone()
        if legacy_health is not None:
            self.connection.execute("drop table archive_health")
