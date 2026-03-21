"""ActiveMemTable — the live mutable memtable wrapping a SkipList.

Adds a ``table_id`` for the audit chain and ``freeze()`` to produce
an immutable snapshot.  Tracks metadata (entry count, seq range,
creation time) for observability.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass

from app.common.errors import SnapshotEmptyError
from app.memtable.skiplist import SkipList
from app.observability import get_logger
from app.types import Key, SeqNum, TableID, Value

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ActiveMemTableMeta:
    """Point-in-time metadata snapshot for an active memtable."""

    table_id: TableID
    size_bytes: int
    entry_count: int
    created_at: int           # nanoseconds since epoch (time.time_ns())
    seq_first: SeqNum
    seq_last: SeqNum


# ---------------------------------------------------------------------------
# ActiveMemTable
# ---------------------------------------------------------------------------


class ActiveMemTable:
    """Public interface to the live mutable memtable.

    Wraps a :class:`SkipList` and adds:
      - ``table_id`` for the audit chain
      - ``freeze()`` to produce an immutable snapshot
      - Metadata tracking for observability
    """

    def __init__(self) -> None:
        self.table_id: TableID = uuid.uuid4().hex
        self._skiplist = SkipList()
        self._created_at = time.time_ns()
        self._entry_count = 0
        self._seq_first: SeqNum = 0
        self._seq_last: SeqNum = 0

        logger.info("ActiveMemTable created", table_id=self.table_id)

    # ── write path ────────────────────────────────────────────────────────

    def put(self, key: Key, seq: SeqNum, timestamp_ms: int, value: Value) -> None:
        """Insert or update *key* in the underlying SkipList."""
        try:
            self._skiplist.put(key, seq, timestamp_ms, value)
        except Exception as exc:
            logger.error(
                "ActiveMemTable put failed",
                table_id=self.table_id,
                key=key,
                seq=seq,
                error=str(exc),
            )
            raise
        self._entry_count += 1
        if self._seq_first == 0:
            self._seq_first = seq
        self._seq_last = seq

    # ── read path ─────────────────────────────────────────────────────────

    def get(self, key: Key) -> tuple[SeqNum, Value] | None:
        """Look up *key*. Returns ``(seq, value)`` or ``None``."""
        return self._skiplist.get(key)

    def items(self) -> Iterator[tuple[Key, SeqNum, int, Value]]:
        """Yield ``(key, seq, timestamp_ms, value)`` in sorted key order."""
        return iter(self._skiplist)

    # ── delete path ───────────────────────────────────────────────────────

    def delete(self, key: Key, seq: SeqNum, timestamp_ms: int) -> bool:
        """Logically delete *key*. Returns whether the key existed."""
        try:
            result = self._skiplist.delete(key, seq, timestamp_ms)
        except Exception as exc:
            logger.error(
                "ActiveMemTable delete failed",
                table_id=self.table_id,
                key=key,
                seq=seq,
                error=str(exc),
            )
            raise
        self._entry_count += 1  # tombstone is an entry
        if self._seq_first == 0:
            self._seq_first = seq
        self._seq_last = seq
        return result

    # ── freeze ────────────────────────────────────────────────────────────

    def freeze(self) -> list[tuple[Key, SeqNum, int, Value]]:
        """Return a sorted snapshot of all visible entries.

        The snapshot_id is assigned by the caller (MemTableManager),
        not by this method.

        Raises :class:`SnapshotEmptyError` if the table has no entries.
        """
        logger.info(
            "ActiveMemTable freeze start",
            table_id=self.table_id,
            size_bytes=self.size_bytes,
        )
        data = self._skiplist.snapshot()
        if not data:
            raise SnapshotEmptyError(
                f"Cannot freeze empty ActiveMemTable {self.table_id}"
            )
        logger.info(
            "ActiveMemTable freeze done",
            table_id=self.table_id,
            entry_count=len(data),
        )
        return data

    # ── properties ────────────────────────────────────────────────────────

    @property
    def size_bytes(self) -> int:
        """Estimated data size in bytes."""
        return self._skiplist.size_bytes

    @property
    def metadata(self) -> ActiveMemTableMeta:
        """Return a frozen metadata snapshot — safe to pass across threads."""
        return ActiveMemTableMeta(
            table_id=self.table_id,
            size_bytes=self.size_bytes,
            entry_count=self._entry_count,
            created_at=self._created_at,
            seq_first=self._seq_first,
            seq_last=self._seq_last,
        )
