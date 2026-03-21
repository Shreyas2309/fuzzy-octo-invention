"""ImmutableMemTable — frozen, read-only snapshot of an ActiveMemTable.

Created by ``MemTableManager.maybe_freeze()`` and lives in the
immutable queue until flushed to an SSTable.  All reads are lock-free.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass

from app.common.errors import ImmutableTableAccessError
from app.observability import get_logger
from app.types import TOMBSTONE, Key, SeqNum, SnapshotID, TableID, Value

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ImmutableMemTableMeta:
    """Metadata for a frozen memtable snapshot."""

    snapshot_id: SnapshotID
    source_table_id: TableID
    size_bytes: int
    entry_count: int
    tombstone_count: int
    frozen_at: int             # nanoseconds since epoch (time.time_ns())
    seq_min: SeqNum
    seq_max: SeqNum


# ---------------------------------------------------------------------------
# ImmutableMemTable
# ---------------------------------------------------------------------------


class ImmutableMemTable:
    """Read-only, sorted snapshot of an :class:`ActiveMemTable`.

    Supports O(1) point lookups via an internal ``_index`` dict
    and sorted iteration via ``items()`` for the flush path.
    """

    _sealed: bool = False  # class-level default; instance override after init

    def __init__(
        self,
        snapshot_id: SnapshotID,
        data: list[tuple[Key, SeqNum, int, Value]],
    ) -> None:
        self.snapshot_id = snapshot_id
        self._data = data  # must be sorted by key ascending
        self._index: dict[Key, int] = {entry[0]: i for i, entry in enumerate(data)}
        self._frozen_at = time.time_ns()
        self._tombstone_count = sum(1 for _, _, _, v in data if v == TOMBSTONE)
        self._sealed = True  # must be last

        logger.info(
            "ImmutableMemTable created",
            snapshot_id=snapshot_id,
            entry_count=len(data),
            tombstones=self._tombstone_count,
        )

    # ── read path ─────────────────────────────────────────────────────────

    def get(self, key: Key) -> tuple[SeqNum, Value] | None:
        """O(1) point lookup. Returns ``(seq, value)`` or ``None``.

        Does **not** return ``timestamp_ms`` — only ``seq`` is needed
        for MVCC ordering.
        """
        idx = self._index.get(key)
        if idx is None:
            return None
        _, seq, _, value = self._data[idx]
        return (seq, value)

    def items(self) -> Iterator[tuple[Key, SeqNum, int, Value]]:
        """Yield ``(key, seq, timestamp_ms, value)`` in sorted key order.

        Used by the flush path to iterate entries for SSTable writing.
        """
        return iter(self._data)

    # ── properties ────────────────────────────────────────────────────────

    @property
    def size_bytes(self) -> int:
        """Total key + value bytes in this snapshot."""
        return sum(len(k) + len(v) for k, _, _, v in self._data)

    @property
    def seq_min(self) -> SeqNum:
        """Smallest sequence number in this snapshot."""
        return min(seq for _, seq, _, _ in self._data) if self._data else 0

    @property
    def seq_max(self) -> SeqNum:
        """Largest sequence number in this snapshot."""
        return max(seq for _, seq, _, _ in self._data) if self._data else 0

    @property
    def metadata(self) -> ImmutableMemTableMeta:
        """Return a frozen metadata snapshot."""
        return ImmutableMemTableMeta(
            snapshot_id=self.snapshot_id,
            source_table_id=self.snapshot_id,  # snapshot_id == source table_id
            size_bytes=self.size_bytes,
            entry_count=len(self._data),
            tombstone_count=self._tombstone_count,
            frozen_at=self._frozen_at,
            seq_min=self.seq_min,
            seq_max=self.seq_max,
        )

    def __len__(self) -> int:
        return len(self._data)

    # ── immutability guard ────────────────────────────────────────────────

    def __setattr__(self, name: str, value: object) -> None:
        if self._sealed and name != "_sealed":
            raise ImmutableTableAccessError(
                f"Cannot set '{name}' on ImmutableMemTable {self.snapshot_id}"
            )
        super().__setattr__(name, value)
