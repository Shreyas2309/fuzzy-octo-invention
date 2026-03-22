"""CompactionManager — flush-triggered compaction with level-reservation scheduling.

No daemon loop. Triggered by FlushPipeline after each SSTable commit.
Concurrent non-conflicting jobs run in parallel; conflicting jobs are
skipped and re-tried after the in-progress job finishes.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import time as _time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING

from app.common.uuid7 import uuid7_hex
from app.compaction.task import CompactionTask
from app.compaction.worker import run_compaction
from app.observability import get_logger
from app.sstable.meta import SSTableMeta
from app.sstable.reader import SSTableReader
from app.types import Level

if TYPE_CHECKING:
    from app.engine.config import LSMConfig
    from app.engine.sstable_manager import SSTableManager

logger = get_logger(__name__)


class CompactionManager:
    """Flush-triggered compaction with level-reservation scheduling."""

    def __init__(
        self,
        sst: SSTableManager,
        config: LSMConfig,
        data_root: Path,
    ) -> None:
        self._sst = sst
        self._config = config
        self._data_root = data_root
        self._reservation_lock = asyncio.Lock()
        self._active_levels: set[Level] = set()
        self._active_jobs: dict[
            tuple[Level, Level], asyncio.Task[None]
        ] = {}

    # ── public entry point ─────────────────────────────────────────────

    async def check_and_compact(self) -> None:
        """Entry point. Called after every flush commit and after each
        completed compaction job.
        """
        logger.debug(
            "Compaction check triggered",
            l0_count=self._sst.l0_count,
            threshold=int(self._config.l0_compaction_threshold),
            active_jobs=len(self._active_jobs),
        )

        jobs = self._find_eligible_jobs()
        if not jobs:
            logger.debug(
                "No eligible compaction jobs",
                l0_count=self._sst.l0_count,
            )
            return

        for src, dst in jobs:
            reserved = await self._try_reserve(src, dst)
            if reserved:
                logger.info(
                    "Launching compaction job",
                    src_level=src,
                    dst_level=dst,
                )
                task = asyncio.create_task(self._run_job(src, dst))
                self._active_jobs[(src, dst)] = task

    # ── job lifecycle ──────────────────────────────────────────────────

    async def _run_job(self, src: Level, dst: Level) -> None:
        """Run one compaction job from src to dst level."""
        try:
            await self._run_one_compaction(src, dst)
        except Exception:
            logger.exception(
                "Compaction job failed",
                src_level=src,
                dst_level=dst,
            )
        finally:
            await self._release(src, dst)
            self._active_jobs.pop((src, dst), None)

        # Re-trigger: handles L0 refill during compaction
        await self.check_and_compact()

    async def _run_one_compaction(
        self, src: Level, dst: Level,
    ) -> None:
        logger.debug("Building compaction task", src=src, dst=dst)
        task = self._build_task(src, dst)
        if task is None:
            logger.debug("Task build skipped (no input files)")
            return

        logger.info(
            "Compaction started",
            task_id=task.task_id,
            src_level=src,
            dst_level=dst,
            input_count=len(task.input_file_ids),
            seq_cutoff=task.seq_cutoff,
        )
        self._append_compaction_log("started", task)

        # Phase 1: run merge in subprocess — event loop stays free
        t0 = _time.monotonic()
        new_meta: SSTableMeta = await asyncio.to_thread(
            self._run_in_subprocess, task,
        )
        merge_ms = (_time.monotonic() - t0) * 1000

        logger.info(
            "Compaction merge done",
            task_id=task.task_id,
            output_records=new_meta.record_count,
            output_bytes=new_meta.size_bytes,
            merge_ms=round(merge_ms, 1),
        )

        # Phase 2: open reader for output SSTable
        new_reader = await SSTableReader.open(
            Path(task.output_dir),
            task.output_file_id,
            self._sst.cache,
            level=dst,
        )

        # Phase 3: atomic commit (acquires write locks on src + dst)
        await self._sst.commit_compaction_async(
            task, new_meta, new_reader,
        )

        self._append_compaction_log(
            "committed",
            task,
            output_records=new_meta.record_count,
            output_bytes=new_meta.size_bytes,
        )
        logger.info(
            "Compaction committed",
            task_id=task.task_id,
            l0_remaining=self._sst.l0_count,
        )

    # ── level reservation ──────────────────────────────────────────────

    def _find_eligible_jobs(self) -> list[tuple[Level, Level]]:
        """Return eligible (src, dst) pairs ordered by priority.

        L0→L1: triggered by L0 file count (``l0_compaction_threshold``).
        L1→L2, L2→L3: triggered by cascading thresholds.
          - Dev mode: entry-count based.
            L(N) threshold = 10^(N+1) × max_memtable_entries
          - Prod mode: size-bytes based.
            L(N) threshold = 10^(N+1) × max_memtable_bytes
        """
        jobs: list[tuple[Level, Level]] = []

        # L0→L1: file count trigger (same for dev/prod)
        threshold = int(self._config.l0_compaction_threshold)
        if self._sst.l0_count >= threshold:
            jobs.append((0, 1))

        # L1→L2, L2→L3: cascading threshold
        is_dev = self._config.is_dev
        max_level = self._sst.max_level
        for src in range(1, max_level):
            dst = src + 1
            if is_dev:
                # Dev: entry-count based
                base = int(self._config.max_memtable_entries)
                level_threshold = (10 ** (src + 1)) * base
                current = self._sst.level_record_count(src)
                if current >= level_threshold:
                    logger.debug(
                        "Level compaction eligible (dev/entries)",
                        src=src,
                        dst=dst,
                        records=current,
                        threshold=level_threshold,
                    )
                    jobs.append((src, dst))
            else:
                # Prod: size-bytes based
                base = self._config.max_memtable_bytes
                level_threshold = (10 ** (src + 1)) * base
                current = self._sst.level_size_bytes(src)
                if current >= level_threshold:
                    logger.debug(
                        "Level compaction eligible (prod/bytes)",
                        src=src,
                        dst=dst,
                        size_bytes=current,
                        threshold=level_threshold,
                    )
                    jobs.append((src, dst))

        return jobs

    async def _try_reserve(
        self, src: Level, dst: Level,
    ) -> bool:
        """Atomically check and reserve levels."""
        async with self._reservation_lock:
            if (
                src in self._active_levels
                or dst in self._active_levels
            ):
                logger.debug(
                    "Compaction reservation skipped",
                    src=src,
                    dst=dst,
                    active=sorted(self._active_levels),
                )
                return False
            self._active_levels.add(src)
            self._active_levels.add(dst)
            logger.debug(
                "Compaction levels reserved",
                src=src,
                dst=dst,
            )
            return True

    async def _release(self, src: Level, dst: Level) -> None:
        """Release reserved levels."""
        async with self._reservation_lock:
            self._active_levels.discard(src)
            self._active_levels.discard(dst)
            logger.debug(
                "Compaction levels released",
                src=src,
                dst=dst,
            )

    # ── task construction ──────────────────────────────────────────────

    def _build_task(
        self, src: Level, dst: Level,
    ) -> CompactionTask | None:
        """Snapshot current input files via SSTableManager."""
        snap = self._sst.compaction_snapshot()
        l0_order: list[str] = snap["l0_order"]  # type: ignore[assignment]
        l0_dirs: dict[str, str] = snap["l0_dirs"]  # type: ignore[assignment]
        level_files: dict[str, tuple[str, str]] = snap["level_files"]  # type: ignore[assignment]

        input_file_ids: list[str] = []
        input_dirs: dict[str, str] = {}

        if src == 0:
            if not l0_order:
                return None
            input_file_ids = list(l0_order)
            input_dirs = {fid: l0_dirs[fid] for fid in input_file_ids}
        else:
            # L1+: single file at src level
            src_entry = level_files.get(str(src))
            if src_entry is None:
                return None
            src_fid, src_dir = src_entry
            input_file_ids = [src_fid]
            input_dirs = {src_fid: src_dir}

        # Include existing dst level file for full replacement merge
        dst_entry = level_files.get(str(dst))
        if dst_entry is not None:
            dst_fid, dst_dir = dst_entry
            input_file_ids.append(dst_fid)
            input_dirs[dst_fid] = dst_dir

        seq_cutoff = self._sst.level_seq_min(dst)
        logger.debug(
            "Compaction task built",
            src=src,
            dst=dst,
            input_count=len(input_file_ids),
            includes_dst=dst_entry is not None,
            seq_cutoff=seq_cutoff,
        )

        output_file_id = uuid7_hex()
        output_dir = (
            self._data_root / "sstable" / f"L{dst}" / output_file_id
        )

        return CompactionTask(
            task_id=uuid7_hex(),
            input_file_ids=input_file_ids,
            input_dirs=input_dirs,
            output_file_id=output_file_id,
            output_dir=str(output_dir),
            output_level=dst,
            seq_cutoff=seq_cutoff,
        )

    @staticmethod
    def _run_in_subprocess(task: CompactionTask) -> SSTableMeta:
        with ProcessPoolExecutor(max_workers=1) as pool:
            return pool.submit(run_compaction, task).result()

    # ── observability ──────────────────────────────────────────────────

    def _append_compaction_log(
        self, event: str, task: CompactionTask, **kwargs: object,
    ) -> None:
        """Append one line to ``data_root/compaction.log``."""
        entry: dict[str, object] = {
            "ts": datetime.datetime.now(datetime.UTC).isoformat(),
            "event": event,
            "task_id": task.task_id,
            "src": task.output_level - 1,
            "dst": task.output_level,
            "inputs": task.input_file_ids,
            "output": task.output_file_id,
        }
        entry.update(kwargs)
        try:
            log_path = self._data_root / "compaction.log"
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except OSError as exc:
            logger.warning(
                "Failed to write compaction.log", error=str(exc),
            )

    @property
    def active_jobs(
        self,
    ) -> dict[tuple[Level, Level], asyncio.Task[None]]:
        """Currently running compaction jobs."""
        return dict(self._active_jobs)
