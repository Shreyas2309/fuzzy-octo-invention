"""Async-aware, thread-safe wrapper around WALWriter.

Serialises concurrent access via a :class:`threading.Lock`.  Public
``append`` and ``truncate_before`` methods are *async*, delegating
blocking I/O to :func:`asyncio.to_thread` so the event loop stays
unblocked.  ``replay()`` is synchronous — it runs once at startup
before any async workers.
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path

from app.observability import get_logger
from app.types import SeqNum
from app.wal.writer import WALEntry, WALWriter

logger = get_logger(__name__)


class WALManager:
    """Thread-safe, async-aware wrapper around :class:`WALWriter`."""

    def __init__(self, wal: WALWriter) -> None:
        self._wal = wal
        self._wal_lock = threading.Lock()

    # ── factory ───────────────────────────────────────────────────────────

    @classmethod
    def open(cls, path: Path) -> WALManager:
        """Create a :class:`WALWriter` at *path* and wrap it."""
        logger.info("WALManager opening", path=str(path))
        try:
            mgr = cls(WALWriter(path))
        except OSError as exc:
            logger.error(
                "WALManager open failed",
                path=str(path),
                error=str(exc),
            )
            raise
        logger.info("WALManager opened", path=str(path))
        return mgr

    # ── sync write path (for use under external lock) ────────────────────
    # BUG-18 LOCK ORDER: caller holds _mem.write_lock → _wal_lock acquired here.
    # Never acquire _mem.write_lock from within _wal_lock.

    def sync_append(self, entry: WALEntry) -> None:
        """Synchronous append for use under external lock."""
        self._sync_append(entry)

    # ── async write path ──────────────────────────────────────────────────

    async def append(self, entry: WALEntry) -> None:
        """Write + fsync in a thread so the loop stays free."""
        await asyncio.to_thread(self._sync_append, entry)

    def _sync_append(self, entry: WALEntry) -> None:
        with self._wal_lock:
            self._wal.append(entry)

    # ── sync replay (startup only) ────────────────────────────────────────

    def replay(self) -> list[WALEntry]:
        """Replay the WAL — synchronous, called before workers."""
        logger.info("WALManager replay start")
        entries = self._wal.replay()
        logger.info("WALManager replay done", entry_count=len(entries))
        return entries

    # ── async truncation ──────────────────────────────────────────────────

    async def truncate_before(self, seq: SeqNum) -> None:
        """Remove entries with ``seq <= seq`` in a background thread."""
        await asyncio.to_thread(self._sync_truncate, seq)

    def _sync_truncate(self, seq: SeqNum) -> None:
        with self._wal_lock:
            self._wal.truncate_before(seq)

    # ── async close ───────────────────────────────────────────────────────

    async def close(self) -> None:
        """Fsync and close the WAL file handle."""
        await asyncio.to_thread(self._sync_close)

    def _sync_close(self) -> None:
        with self._wal_lock:
            logger.info(
                "WALManager closing", path=str(self._wal.path),
            )
            self._wal.close()
            logger.info("WALManager closed")
