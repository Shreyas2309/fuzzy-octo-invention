"""LSMEngine — central entry point for the lsm-kv key-value store.

Coordinates all managers (WAL, MemTable, SSTable) and exposes the
public ``put`` / ``get`` / ``delete`` interface.  The write path is
atomic: WAL append + memtable put + maybe_freeze all happen under
``_mem.write_lock``.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path

from app.cache.block import BlockCache
from app.common.errors import EngineClosed
from app.engine.compaction_manager import CompactionManager
from app.engine.config import LSMConfig
from app.engine.flush_pipeline import FlushPipeline
from app.engine.memtable_manager import MemTableManager
from app.engine.seq_generator import SeqGenerator
from app.engine.sstable_manager import SSTableManager
from app.engine.wal_manager import WALManager
from app.observability import configure_logging, get_logger
from app.observability.log_server import LogBroadcastServer
from app.types import TOMBSTONE, Key, OpType, SeqNum, Value
from app.wal.writer import WALEntry

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Engine stats — returned by stats()
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EngineStats:
    """Snapshot of engine statistics."""

    key_count: int
    seq: SeqNum
    wal_entry_count: int
    data_root: str
    active_table_id: str
    active_size_bytes: int
    immutable_queue_len: int
    immutable_snapshots: list[str]
    l0_sstable_count: int


# ---------------------------------------------------------------------------
# LSMEngine — the public API
# ---------------------------------------------------------------------------


class LSMEngine:
    """Central entry point for the lsm-kv key-value store.

    Use the :meth:`open` classmethod to create an instance — never
    call ``__init__`` directly.
    """

    def __init__(self) -> None:
        # Private — use open() instead
        self._wal: WALManager
        self._mem: MemTableManager
        self._sst: SSTableManager
        self._seq: SeqGenerator
        self._config: LSMConfig
        self._pipeline: FlushPipeline
        self._compaction: CompactionManager | None = None
        self._pipeline_task: asyncio.Task[None] | None = None
        self._data_root: Path
        self._closed: bool = False
        self._log_server: LogBroadcastServer | None = None

    @property
    def data_root(self) -> Path:
        """Return the root data directory path."""
        return self._data_root

    @property
    def log_port(self) -> int:
        """Return the TCP log broadcast port, or 0 if disabled."""
        return self._log_server.port if self._log_server else 0

    # ── factory ───────────────────────────────────────────────────────────

    @classmethod
    async def open(
        cls,
        data_root: str | Path = "./data",
        config_path: Path | None = None,
    ) -> LSMEngine:
        """Open or create an LSM engine rooted at *data_root*.

        Parameters
        ----------
        data_root:
            Root data directory for WAL, logs, and SSTables.
        config_path:
            Optional override for config file location.
            Defaults to ``<data_root>/config.json``.
        """
        engine = cls()
        engine._data_root = Path(data_root)

        # 1. Logging — configured BEFORE any log call
        engine._log_server = configure_logging(
            data_root=engine._data_root,
        )
        log_port = engine._log_server.port if engine._log_server else 0
        logger.info(
            "Startup started",
            data_root=str(engine._data_root),
            log_file=str(engine._data_root / "logs" / "lsm-kv.log"),
            log_port=log_port,
        )

        # 2. Config — loaded early so all components can use it
        engine._config = LSMConfig.load(
            engine._data_root, config_path,
        )
        logger.info("Config loaded", config=engine._config.to_dict())

        # 3. WAL
        wal_path = engine._data_root / "wal" / "wal.log"
        engine._wal = WALManager.open(wal_path)
        logger.info("WAL opened", path=str(wal_path))

        # 4. SeqGenerator
        engine._seq = SeqGenerator()

        # 5. MemTableManager — uses config for thresholds
        engine._mem = MemTableManager(engine._config)
        logger.info(
            "MemTableManager created",
            table_id=engine._mem.active_metadata.table_id,
        )

        # 6. SSTableManager — load existing SSTables from disk
        cache = BlockCache(
            data_maxsize=int(engine._config.cache_data_entry_limit),
            index_maxsize=int(engine._config.cache_index_entry_limit),
            bloom_maxsize=int(engine._config.cache_bloom_entry_limit),
        )
        engine._sst = await SSTableManager.load(
            engine._data_root, cache, engine._config,
        )

        # 7. Recovery — use max seq from SSTables + WAL
        engine._recover()

        # 8. CompactionManager + FlushPipeline — start as daemon task
        engine._compaction = CompactionManager(
            sst=engine._sst,
            config=engine._config,
            data_root=engine._data_root,
        )
        max_workers = int(engine._config.flush_max_workers)
        engine._pipeline = FlushPipeline(
            mem=engine._mem,
            sst=engine._sst,
            wal=engine._wal,
            max_workers=max_workers,
            compaction=engine._compaction,
        )
        engine._pipeline_task = asyncio.create_task(engine._pipeline.run())
        engine._pipeline_task.add_done_callback(engine._on_pipeline_done)

        # 9. Startup compaction check (L0 may already be at threshold)
        if engine._sst.l0_count >= int(
            engine._config.l0_compaction_threshold,
        ):
            asyncio.create_task(engine._compaction.check_and_compact())

        # 10. Ready
        logger.info(
            "Startup complete",
            data_root=str(engine._data_root),
            live_keys=engine._mem.active_metadata.entry_count,
            seq=engine._seq.current,
            l0_sstables=engine._sst.l0_count,
            log_port=log_port,
        )
        return engine

    # ── write path ────────────────────────────────────────────────────────
    # BUG-18 LOCK ORDER: _mem.write_lock → WALManager._wal_lock
    # Never acquire _mem.write_lock while holding _wal_lock.

    async def put(self, key: Key, value: Value) -> None:
        """Write a key-value pair.

        Atomic under ``write_lock``: seq generation + WAL append +
        memtable put + maybe_freeze.
        """
        self._check_closed()
        logger.debug("Engine PUT start", key=key)

        try:
            with self._mem.write_lock:
                seq = self._seq.next()
                timestamp_ms = time.time_ns() // 1_000_000

                entry = WALEntry(
                    seq=seq,
                    timestamp_ms=timestamp_ms,
                    op=OpType.PUT,
                    key=key,
                    value=value,
                )

                # WAL first — durability before visibility
                self._wal.sync_append(entry)

                # Memtable write
                self._mem.put(key, seq, timestamp_ms, value)

                # Check if freeze is needed
                self._mem.maybe_freeze()
        except Exception as exc:
            logger.error("Engine PUT failed", key=key, error=str(exc))
            raise

        logger.debug("Engine PUT done", key=key, seq=seq)

    async def delete(self, key: Key) -> None:
        """Delete a key by writing a tombstone."""
        self._check_closed()
        logger.debug("Engine DELETE start", key=key)

        try:
            with self._mem.write_lock:
                seq = self._seq.next()
                timestamp_ms = time.time_ns() // 1_000_000

                entry = WALEntry(
                    seq=seq,
                    timestamp_ms=timestamp_ms,
                    op=OpType.DELETE,
                    key=key,
                    value=TOMBSTONE,
                )

                self._wal.sync_append(entry)
                self._mem.put(key, seq, timestamp_ms, TOMBSTONE)
                self._mem.maybe_freeze()
        except Exception as exc:
            logger.error("Engine DELETE failed", key=key, error=str(exc))
            raise

        logger.debug("Engine DELETE done", key=key, seq=seq)

    # ── flush (manual) ──────────────────────────────────────────────────────

    async def flush(self) -> bool:
        """Force-flush the active memtable to an SSTable.

        Freezes the current memtable regardless of size/entry thresholds
        and queues it for the flush pipeline.  Returns ``True`` if a
        snapshot was created, ``False`` if the memtable was empty.
        """
        self._check_closed()
        logger.info("Engine FLUSH (manual) requested")

        with self._mem.write_lock:
            snapshot = self._mem.force_freeze()

        if snapshot is None:
            logger.info("Engine FLUSH skipped (empty memtable)")
            return False

        logger.info(
            "Engine FLUSH queued",
            snapshot_id=snapshot.snapshot_id,
            entry_count=len(snapshot),
            size_bytes=snapshot.size_bytes,
        )
        return True

    # ── read path ─────────────────────────────────────────────────────────

    async def get(self, key: Key) -> Value | None:
        """Read a value by key. Returns None if not found or deleted.

        Checks memtable first, then SSTables.
        """
        self._check_closed()

        # 1. MemTable (active + immutable queue)
        result = self._mem.get(key)
        if result is not None:
            _, value = result
            hit = value != TOMBSTONE
            logger.debug("Engine GET", key=key, hit=hit, source="mem")
            return value if hit else None

        # 2. SSTables
        sst_result = await self._sst.get(key)
        if sst_result is not None:
            _, _, value = sst_result
            hit = value != TOMBSTONE
            logger.debug("Engine GET", key=key, hit=hit, source="sst")
            return value if hit else None

        logger.debug("Engine GET", key=key, hit=False, source="miss")
        return None

    # ── config ─────────────────────────────────────────────────────────────

    @property
    def config(self) -> LSMConfig:
        """Return the live config instance."""
        return self._config

    def update_config(
        self, key: str, value: int | float | str,
    ) -> tuple[int | float | str, int | float | str]:
        """Update a config field, persist to disk, return (old, new)."""
        self._check_closed()
        return self._config.set(key, value)

    # ── stats ─────────────────────────────────────────────────────────────

    def stats(self) -> EngineStats:
        """Return a snapshot of engine statistics."""
        active_meta = self._mem.active_metadata
        immutable_metas = self._mem.immutable_metadata
        wal_entries = len(self._wal.replay())
        return EngineStats(
            key_count=active_meta.entry_count,
            seq=self._seq.current,
            wal_entry_count=wal_entries,
            data_root=str(self._data_root),
            active_table_id=active_meta.table_id,
            active_size_bytes=active_meta.size_bytes,
            immutable_queue_len=len(immutable_metas),
            immutable_snapshots=[
                m.snapshot_id for m in immutable_metas
            ],
            l0_sstable_count=self._sst.l0_count,
        )

    # ── show_mem / show_disk ─────────────────────────────────────────────

    def show_mem(self, table_id: str | None = None) -> dict[str, object]:
        """Show memtable contents.

        Parameters
        ----------
        table_id:
            **Optional.**  Pass a table ID (for the active memtable) or
            a snapshot ID (for an immutable memtable) to see its full
            entry list.  When omitted or ``None``, returns a summary
            listing all active and immutable memtables without entries.

        Returns
        -------
        dict
            In listing mode (no *table_id*)::

                {
                    "active": {"table_id": ..., "entry_count": ..., ...},
                    "immutable": [{"snapshot_id": ..., ...}, ...],
                }

            In detail mode (with *table_id*)::

                {
                    "type": "active" | "immutable",
                    "table_id": ...,
                    "entries": [{"key": ..., "seq": ..., ...}, ...],
                }
        """
        self._check_closed()
        return self._mem.show_mem(table_id)

    def show_disk(self, file_id: str | None = None) -> dict[str, object]:
        """Show SSTable contents on disk.

        Parameters
        ----------
        file_id:
            **Optional.**  Pass a file ID to see the full record list
            for that SSTable.  When omitted or ``None``, returns a
            summary listing all SSTables organised by level.

        Returns
        -------
        dict
            In listing mode (no *file_id*)::

                {"L0": [{"file_id": ..., "record_count": ..., ...}, ...]}

            In detail mode (with *file_id*)::

                {"file_id": ..., "entries": [...], ...}
        """
        self._check_closed()
        return self._sst.show_disk(file_id)

    # ── lifecycle ─────────────────────────────────────────────────────────

    async def close(self) -> None:
        """Flush and close the engine. Idempotent."""
        if self._closed:
            return
        self._closed = True
        logger.info("Shutdown started", data_root=str(self._data_root))

        # 1. Stop the flush pipeline and wait for it to drain
        self._pipeline.stop()
        if self._pipeline_task is not None:
            try:
                await asyncio.wait_for(self._pipeline_task, timeout=30.0)
            except TimeoutError:
                logger.warning("FlushPipeline drain timed out")
                self._pipeline_task.cancel()
            except asyncio.CancelledError:
                pass

        # 2. Log memtable state before closing
        active_meta = self._mem.active_metadata
        logger.info(
            "MemTable state at shutdown",
            active_entries=active_meta.entry_count,
            active_size_bytes=active_meta.size_bytes,
            immutable_queue_len=self._mem.queue_len(),
            seq=self._seq.current,
        )

        # 3. WAL — fsync + close
        await self._wal.close()
        logger.info("WAL closed")

        # 4. SSTable readers — close all
        self._sst.close_all()
        logger.info("SSTables closed")

        # 5. Log server — stop last so shutdown logs are broadcast
        if self._log_server:
            logger.info(
                "Log server stopping", port=self._log_server.port,
            )
            self._log_server.stop()

        logger.info("Shutdown complete")

    # ── recovery (startup) ────────────────────────────────────────────────

    def _recover(self) -> None:
        """Replay WAL to rebuild in-memory state and seq counter."""
        logger.info("Recovery start")

        # Restore seq from SSTable max first
        sst_max = self._sst.max_seq_seen()
        if sst_max > 0:
            self._seq.restore(sst_max)
            logger.info("Seq restored from SSTables", max_seq=sst_max)

        try:
            entries = self._wal.replay()
        except Exception as exc:
            logger.error("Recovery failed: WAL replay error", error=str(exc))
            raise

        if not entries:
            logger.info("Recovery skipped (no WAL entries)")
            return

        # Filter out entries already flushed to SSTables
        unflushed = [e for e in entries if e.seq > sst_max]

        if not unflushed:
            logger.info(
                "All WAL entries already flushed",
                total=len(entries),
                sst_max_seq=sst_max,
            )
            return

        try:
            self._mem.restore(unflushed)
        except Exception as exc:
            logger.error(
                "Recovery failed: memtable restore error",
                entry_count=len(unflushed),
                error=str(exc),
            )
            raise

        max_seq = max(e.seq for e in unflushed)
        self._seq.restore(max_seq)

        logger.info(
            "Recovery complete",
            replayed=len(unflushed),
            skipped=len(entries) - len(unflushed),
            live_keys=self._mem.active_metadata.entry_count,
            max_seq=self._seq.current,
        )

    # ── internal helpers ──────────────────────────────────────────────────

    def _on_pipeline_done(self, task: asyncio.Task[None]) -> None:
        """BUG-13: detect pipeline crash so failures aren't silent."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.critical(
                "FlushPipeline crashed — flushes stopped",
                error=str(exc),
            )

    def _check_closed(self) -> None:
        """Raise if the engine has been closed."""
        if self._closed:
            raise EngineClosed("Engine is closed")
