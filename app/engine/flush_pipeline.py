"""FlushPipeline — parallel flush with ordered commits.

Writes are parallel (bounded by semaphore), commits are serialized
via an asyncio.Event chain so SSTables appear in oldest-first order.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from app.memtable.immutable import ImmutableMemTable
from app.observability import get_logger
from app.sstable.meta import SSTableMeta
from app.sstable.reader import SSTableReader
from app.types import FileID

if TYPE_CHECKING:
    from app.engine.memtable_manager import MemTableManager
    from app.engine.sstable_manager import SSTableManager
    from app.engine.wal_manager import WALManager

logger = get_logger(__name__)


@dataclass
class FlushSlot:
    """Tracks one in-flight flush operation."""

    snapshot: ImmutableMemTable
    file_id: FileID
    prev_committed: asyncio.Event
    my_committed: asyncio.Event = field(default_factory=asyncio.Event)
    batch_abort: asyncio.Event = field(default_factory=asyncio.Event)
    position: int = 0


class FlushPipeline:
    """Daemon that drains the immutable queue to SSTables.

    Writes run in parallel (up to ``max_workers``), but commits
    are serialized: each slot waits for the previous slot's commit
    before committing its own result.
    """

    def __init__(
        self,
        mem: MemTableManager,
        sst: SSTableManager,
        wal: WALManager,
        max_workers: int = 2,
    ) -> None:
        self._mem = mem
        self._sst = sst
        self._wal = wal
        self._max_workers = max_workers
        self._semaphore = asyncio.Semaphore(max_workers)
        self._stop_event = asyncio.Event()
        self._running = False
        # BUG-14: async event bridged from threading.Event via
        # loop.call_soon_threadsafe in the notify callback
        self._flush_async = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None

    # ── main loop ───────────────────────────────────────────────────────

    async def run(self) -> None:
        """Main daemon loop — runs until :meth:`stop` is called."""
        self._running = True
        self._loop = asyncio.get_running_loop()

        # BUG-14: register callback so threading.Event bridges to async
        self._mem.set_flush_notify(self._on_flush_notify)

        logger.info("FlushPipeline started", max_workers=self._max_workers)

        while not self._stop_event.is_set():
            dispatched = await self._dispatch_all()
            if not dispatched:
                # No work — wait for async flush signal or stop
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._flush_async.wait(),
                        timeout=0.5,
                    )
                self._flush_async.clear()

        # Final drain on shutdown
        await self._dispatch_all()
        self._mem.set_flush_notify(None)
        self._running = False
        logger.info("FlushPipeline stopped")

    def _on_flush_notify(self) -> None:
        """Callback from MemTableManager (sync thread) to wake the loop."""
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._flush_async.set)

    # ── dispatch ────────────────────────────────────────────────────────

    async def _dispatch_all(self) -> bool:
        """Assign slots for all pending snapshots and flush them."""
        # BUG-03: use snapshot_queue() to avoid TOCTOU race
        pending = self._mem.snapshot_queue()
        if not pending:
            return False

        # Build slots for all pending snapshots (oldest first).
        # All slots share one batch_abort event — if any slot fails,
        # all downstream slots skip their commit (no wrong pops).
        slots: list[FlushSlot] = []
        prev_event = asyncio.Event()
        prev_event.set()  # first slot has no predecessor
        abort_event = asyncio.Event()

        for depth, snapshot in enumerate(pending):
            file_id = self._sst.new_file_id()
            slot = FlushSlot(
                snapshot=snapshot,
                file_id=file_id,
                prev_committed=prev_event,
                batch_abort=abort_event,
                position=depth,
            )
            slots.append(slot)
            prev_event = slot.my_committed

        logger.info(
            "Dispatching flush",
            slot_count=len(slots),
            queue_len=len(pending),
        )

        # Launch all slots concurrently
        tasks = [asyncio.create_task(self._flush_slot(s)) for s in slots]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # BUG-13: count and log failures
        failures = sum(
            1 for r in results if isinstance(r, BaseException)
        )
        for i, result in enumerate(results):
            if isinstance(result, BaseException):
                logger.error(
                    "Flush slot failed",
                    position=slots[i].position,
                    file_id=slots[i].file_id,
                    error=str(result),
                )

        if failures:
            logger.warning(
                "Flush dispatch had failures",
                total=len(slots),
                failed=failures,
            )

        return True

    # ── per-slot flush ──────────────────────────────────────────────────

    async def _flush_slot(self, slot: FlushSlot) -> None:
        """Phase 1: write SSTable (parallel). Phase 2: commit (serialized).

        BUG-01 fix: on failure, sets ``batch_abort`` so downstream slots
        skip their commits (no wrong pops), then signals ``my_committed``
        to unblock the chain.
        """
        try:
            # Abort early if a predecessor already failed
            if slot.batch_abort.is_set():
                logger.warning(
                    "Flush slot skipped (batch aborted)",
                    file_id=slot.file_id,
                    position=slot.position,
                )
                return

            async with self._semaphore:
                meta, reader = await self._write_sstable(slot)

            # Phase 2: wait for previous slot to commit, then commit ours
            await self._commit_slot(slot, meta, reader)
        except Exception:
            # Signal all downstream slots to abort
            slot.batch_abort.set()
            logger.exception(
                "Flush slot error — batch aborted",
                file_id=slot.file_id,
                position=slot.position,
            )
            raise
        finally:
            # ALWAYS signal downstream — prevents event chain deadlock
            slot.my_committed.set()

    async def _write_sstable(
        self, slot: FlushSlot,
    ) -> tuple[SSTableMeta, SSTableReader]:
        """Write the snapshot to an SSTable (parallel phase)."""
        logger.info(
            "Flush write start",
            file_id=slot.file_id,
            snapshot_id=slot.snapshot.snapshot_id,
            position=slot.position,
        )

        meta, reader = await self._sst.flush(slot.snapshot, slot.file_id)

        logger.info(
            "Flush write done",
            file_id=slot.file_id,
            records=meta.record_count,
        )
        return meta, reader

    async def _commit_slot(
        self,
        slot: FlushSlot,
        meta: SSTableMeta,
        reader: SSTableReader,
    ) -> None:
        """Wait for ordering, then commit the SSTable and pop the snapshot."""
        # Wait for previous slot to finish committing
        await slot.prev_committed.wait()

        # Double-check: predecessor may have failed after signalling
        if slot.batch_abort.is_set():
            reader.close()
            logger.warning(
                "Flush commit skipped (batch aborted)",
                file_id=slot.file_id,
            )
            return

        sst_dir = self._sst.sst_dir_for(slot.file_id)
        self._sst.commit(slot.file_id, reader, sst_dir)

        # Pop the oldest snapshot (which is what we just flushed)
        self._mem.pop_oldest()

        # Truncate WAL up to this snapshot's max seq
        try:
            await self._wal.truncate_before(slot.snapshot.seq_max)
        except Exception:
            logger.exception(
                "WAL truncate failed after flush",
                file_id=slot.file_id,
            )

        logger.info(
            "Flush commit done",
            file_id=slot.file_id,
            position=slot.position,
        )

    # ── control ─────────────────────────────────────────────────────────

    def stop(self) -> None:
        """Signal the pipeline to stop after draining."""
        self._stop_event.set()
        logger.info("FlushPipeline stop requested")

    @property
    def running(self) -> bool:
        """Whether the pipeline loop is active."""
        return self._running
