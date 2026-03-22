"""MemTableManager — coordinates active memtable, immutable queue, and flush signal.

Owns all in-memory write-side state.  Has no knowledge of SSTables,
WAL, or disk.  The engine holds ``_write_lock`` during the atomic
write path (WAL append + memtable put + maybe_freeze).

All thresholds are read from :class:`LSMConfig` at the point of use
so runtime config changes take effect immediately.
"""

from __future__ import annotations

import threading
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING

from app.common.errors import FreezeBackpressureTimeout, MemTableRestoreError
from app.memtable.active import ActiveMemTable, ActiveMemTableMeta
from app.memtable.immutable import ImmutableMemTable, ImmutableMemTableMeta
from app.observability import get_logger
from app.types import Key, SeqNum, Value
from app.wal.writer import WALEntry

if TYPE_CHECKING:
    from app.engine.config import LSMConfig

logger = get_logger(__name__)


class MemTableManager:
    """Single point of coordination for all in-memory write-side state.

    All thresholds are read from *config* at the moment they're needed,
    so runtime config changes take effect on the next operation.
    """

    def __init__(self, config: LSMConfig) -> None:
        self._config = config
        self._active = ActiveMemTable()
        self._immutable_q: deque[ImmutableMemTable] = deque()
        self._write_lock = threading.RLock()
        self._queue_not_full = threading.Condition(self._write_lock)
        self._flush_event = threading.Event()
        # BUG-14: optional callback to bridge flush signal to asyncio
        self._flush_notify: Callable[[], None] | None = None

    # ── write path ────────────────────────────────────────────────────────

    def put(self, key: Key, seq: SeqNum, timestamp_ms: int, value: Value) -> None:
        """Write *key* into the active memtable.

        Called by the engine while holding ``_write_lock``.
        """
        logger.debug("MemTableManager put", key=key, seq=seq)
        try:
            self._active.put(key, seq, timestamp_ms, value)
        except Exception as exc:
            logger.error(
                "MemTableManager put failed",
                key=key, seq=seq, error=str(exc),
            )
            raise

    # ── read path ─────────────────────────────────────────────────────────

    def get(self, key: Key) -> tuple[SeqNum, Value] | None:
        """Look up *key* in active memtable then immutable queue.

        Scans newest → oldest.  Returns raw value including TOMBSTONE —
        the engine resolves it.  Takes a snapshot copy of the immutable
        queue to avoid concurrent-modification issues (BUG-02).
        """
        result = self._active.get(key)
        if result is not None:
            logger.debug("MemTableManager get", key=key, source="active")
            return result

        # BUG-02: snapshot the queue to avoid mutation during iteration.
        # deque copy is O(n) but n ≤ immutable_queue_max_len (default 4).
        snapshot = list(self._immutable_q)

        for i, table in enumerate(snapshot):
            result = table.get(key)
            if result is not None:
                logger.debug(
                    "MemTableManager get",
                    key=key,
                    source=f"immutable_{i}",
                )
                return result

        logger.debug("MemTableManager get", key=key, source="miss")
        return None

    # ── freeze ────────────────────────────────────────────────────────────

    def _should_freeze(self) -> bool:
        """Check if the active memtable exceeds the configured threshold.

        In **prod** mode, the trigger is data size (``max_memtable_bytes``).
        In **dev** mode, the trigger is entry count (``max_memtable_entries``).
        """
        if self._config.is_prod:
            return self._active.size_bytes >= self._config.max_memtable_bytes
        # dev mode: entry-count based
        entry_limit = int(self._config.max_memtable_entries)
        return self._active.metadata.entry_count >= entry_limit

    def maybe_freeze(self) -> ImmutableMemTable | None:
        """Freeze the active memtable if it exceeds size or entry threshold.

        Called under ``_write_lock`` by the engine.  Applies backpressure
        via ``_queue_not_full.wait()`` if the immutable queue is full.
        """
        if not self._should_freeze():
            return None

        meta = self._active.metadata
        logger.info(
            "Freeze triggered",
            size_bytes=meta.size_bytes,
            size_limit_bytes=self._config.max_memtable_bytes,
            entry_count=meta.entry_count,
            entry_limit=int(self._config.max_memtable_entries),
        )

        # Backpressure: wait if queue is full (with timeout)
        queue_max = int(self._config.immutable_queue_max_len)
        bp_timeout = float(self._config.backpressure_timeout)

        while len(self._immutable_q) >= queue_max:
            logger.warning(
                "Backpressure active",
                queue_len=len(self._immutable_q),
                max=queue_max,
            )
            signalled = self._queue_not_full.wait(timeout=bp_timeout)
            if not signalled and len(self._immutable_q) >= queue_max:
                logger.error(
                    "Backpressure timeout",
                    queue_len=len(self._immutable_q),
                    timeout=bp_timeout,
                )
                raise FreezeBackpressureTimeout(
                    f"Waited {bp_timeout}s for queue space "
                    f"(queue_len={len(self._immutable_q)})"
                )
            logger.info(
                "Backpressure released",
                queue_len=len(self._immutable_q),
            )

        # Freeze the active table
        snapshot_id = self._active.table_id
        data = self._active.freeze()
        snapshot = ImmutableMemTable(snapshot_id, data)

        self._immutable_q.appendleft(snapshot)
        self._active = ActiveMemTable()
        self._flush_event.set()
        if self._flush_notify is not None:
            self._flush_notify()

        logger.info(
            "Freeze complete",
            snapshot_id=snapshot_id,
            queue_len=len(self._immutable_q),
        )
        return snapshot

    def force_freeze(self) -> ImmutableMemTable | None:
        """Freeze the active memtable unconditionally.

        Bypasses threshold checks.  Returns ``None`` only if the active
        memtable is empty (nothing to freeze).  Called under
        ``_write_lock`` by the engine's ``flush()`` command.
        """
        if self._active.metadata.entry_count == 0:
            logger.info("Force freeze skipped (empty memtable)")
            return None

        meta = self._active.metadata
        logger.info(
            "Force freeze triggered",
            size_bytes=meta.size_bytes,
            entry_count=meta.entry_count,
        )

        # Backpressure: wait if queue is full (with timeout)
        queue_max = int(self._config.immutable_queue_max_len)
        bp_timeout = float(self._config.backpressure_timeout)

        while len(self._immutable_q) >= queue_max:
            logger.warning(
                "Backpressure active (force freeze)",
                queue_len=len(self._immutable_q),
                max=queue_max,
            )
            signalled = self._queue_not_full.wait(timeout=bp_timeout)
            if not signalled and len(self._immutable_q) >= queue_max:
                raise FreezeBackpressureTimeout(
                    f"Waited {bp_timeout}s for queue space "
                    f"(queue_len={len(self._immutable_q)})"
                )

        # Freeze the active table
        snapshot_id = self._active.table_id
        data = self._active.freeze()
        snapshot = ImmutableMemTable(snapshot_id, data)

        self._immutable_q.appendleft(snapshot)
        self._active = ActiveMemTable()
        self._flush_event.set()
        if self._flush_notify is not None:
            self._flush_notify()

        logger.info(
            "Force freeze complete",
            snapshot_id=snapshot_id,
            queue_len=len(self._immutable_q),
        )
        return snapshot

    # ── queue operations ──────────────────────────────────────────────────

    def peek_oldest(self) -> ImmutableMemTable | None:
        """Return the oldest snapshot without removing it."""
        if not self._immutable_q:
            return None
        return self._immutable_q[-1]

    def peek_at_depth(self, depth: int) -> ImmutableMemTable | None:
        """Return the snapshot at *depth* (0 = oldest, 1 = second oldest, etc.).

        Protected by ``_write_lock`` to prevent TOCTOU race (BUG-03).
        """
        with self._write_lock:
            if depth >= len(self._immutable_q):
                return None
            return self._immutable_q[-(depth + 1)]

    def snapshot_queue(self) -> list[ImmutableMemTable]:
        """Return a snapshot of the immutable queue (oldest first).

        Used by the flush pipeline to safely iterate all pending
        snapshots without holding a lock during the flush.
        """
        return list(reversed(self._immutable_q))

    def pop_oldest(self) -> None:
        """Remove the oldest snapshot and unblock any stalled freeze."""
        if self._immutable_q:
            removed = self._immutable_q.pop()
            logger.info(
                "Snapshot popped",
                snapshot_id=removed.snapshot_id,
                queue_remaining=len(self._immutable_q),
            )
        with self._queue_not_full:
            self._queue_not_full.notify_all()

    def queue_len(self) -> int:
        """Return the number of snapshots in the immutable queue."""
        return len(self._immutable_q)

    # ── recovery ──────────────────────────────────────────────────────────

    def restore(self, entries: list[WALEntry]) -> None:
        """Replay WAL entries into the active memtable.

        Called by ``engine._recover()`` only — single-threaded, no lock
        needed.
        """
        logger.info("Restore start", entry_count=len(entries))
        try:
            for entry in entries:
                self._active.put(
                    entry.key, entry.seq, entry.timestamp_ms, entry.value,
                )
        except Exception as exc:
            last_seq = entries[-1].seq if entries else "?"
            raise MemTableRestoreError(
                f"Restore failed at seq={last_seq}: {exc}"
            ) from exc

        logger.info(
            "Restore done",
            live_keys=self._active.metadata.entry_count,
        )

    # ── properties ────────────────────────────────────────────────────────

    @property
    def flush_event(self) -> threading.Event:
        """The event signalled when a new snapshot is added to the queue."""
        return self._flush_event

    def set_flush_notify(self, callback: Callable[[], None] | None) -> None:
        """Register (or clear) a callback invoked after each freeze.

        Used by FlushPipeline to bridge the sync flush_event to an
        asyncio.Event via ``loop.call_soon_threadsafe`` (BUG-14).
        """
        self._flush_notify = callback

    @property
    def write_lock(self) -> threading.RLock:
        """The write lock for the atomic write path."""
        return self._write_lock

    @property
    def size_bytes(self) -> int:
        """Size of the active memtable in bytes."""
        return self._active.size_bytes

    @property
    def active_metadata(self) -> ActiveMemTableMeta:
        """Metadata for the current active memtable."""
        return self._active.metadata

    @property
    def immutable_metadata(self) -> list[ImmutableMemTableMeta]:
        """Metadata for all snapshots (newest first)."""
        return [t.metadata for t in self._immutable_q]

    # ── show_mem ───────────────────────────────────────────────────────────

    def show_mem(
        self, table_id: str | None = None,
    ) -> dict[str, object]:
        """Inspect memtable contents.

        Parameters
        ----------
        table_id:
            **Optional.** The table ID (active) or snapshot ID (immutable)
            to inspect.  When provided, returns the full entry list for
            that table.  When ``None`` (the default), returns a summary
            listing all active and immutable memtables without their
            entries.

        Returns
        -------
        A dict with the structure::

            # When table_id is None (listing mode):
            {
                "active": { "table_id": ..., "entry_count": ..., ... },
                "immutable": [ { "snapshot_id": ..., ... }, ... ],
            }

            # When table_id matches a table (detail mode):
            {
                "type": "active" | "immutable",
                "table_id": ...,
                "entry_count": ...,
                "entries": [ {"key": ..., "seq": ..., "ts": ..., "value": ...}, ... ],
            }
        """
        # ── Detail mode: return entries for a specific table ───────────
        if table_id is not None:
            # Check active memtable
            if self._active.table_id == table_id:
                entries = [
                    {
                        "key": k,
                        "seq": seq,
                        "timestamp_ms": ts,
                        "value": v,
                    }
                    for k, seq, ts, v in self._active.items()
                ]
                meta = self._active.metadata
                return {
                    "type": "active",
                    "table_id": meta.table_id,
                    "entry_count": meta.entry_count,
                    "size_bytes": meta.size_bytes,
                    "entries": entries,
                }

            # Check immutable queue
            for table in self._immutable_q:
                if table.snapshot_id == table_id:
                    entries = [
                        {
                            "key": k,
                            "seq": seq,
                            "timestamp_ms": ts,
                            "value": v,
                        }
                        for k, seq, ts, v in table.items()
                    ]
                    imm_meta = table.metadata
                    return {
                        "type": "immutable",
                        "table_id": imm_meta.snapshot_id,
                        "entry_count": imm_meta.entry_count,
                        "size_bytes": imm_meta.size_bytes,
                        "seq_min": imm_meta.seq_min,
                        "seq_max": imm_meta.seq_max,
                        "entries": entries,
                    }

            return {"error": f"No table found with id {table_id!r}"}

        # ── Listing mode: summarise all tables ─────────────────────────
        active_meta = self._active.metadata
        immutable_metas = [t.metadata for t in self._immutable_q]

        return {
            "active": {
                "table_id": active_meta.table_id,
                "entry_count": active_meta.entry_count,
                "size_bytes": active_meta.size_bytes,
                "seq_first": active_meta.seq_first,
                "seq_last": active_meta.seq_last,
            },
            "immutable": [
                {
                    "snapshot_id": m.snapshot_id,
                    "entry_count": m.entry_count,
                    "size_bytes": m.size_bytes,
                    "seq_min": m.seq_min,
                    "seq_max": m.seq_max,
                    "tombstone_count": m.tombstone_count,
                }
                for m in immutable_metas
            ],
        }
