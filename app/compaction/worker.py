"""Compaction subprocess worker.

``run_compaction()`` is a pure function — receives file paths, writes one
output SSTable, returns ``SSTableMeta``.  No asyncio, no shared state,
no locks.  Runs in a ``ProcessPoolExecutor`` subprocess with its own
GIL instance.

``_open_reader_sync()`` replicates ``SSTableReader.open()`` without
asyncio, for use where no event loop exists.
"""

from __future__ import annotations

import mmap
import os
import time
from pathlib import Path

from app.bloom.filter import BloomFilter
from app.common.merge_iterator import KWayMergeIterator
from app.compaction.task import CompactionTask
from app.index.sparse import SparseIndex
from app.observability import get_logger
from app.sstable.meta import SSTableMeta
from app.sstable.reader import SSTableReader
from app.sstable.writer import SSTableWriter

logger = get_logger(__name__)


def run_compaction(task: CompactionTask) -> SSTableMeta:
    """Merge all input SSTables into one output SSTable.

    Called in a subprocess — no event loop, no registry, no cache.
    Opens its own file handles via ``_open_reader_sync()``.
    Uses ``finish_sync()`` because blocking is fine in a subprocess.
    """
    t0 = time.monotonic()
    total_input_records = 0

    logger.info(
        "Subprocess merge starting",
        task_id=task.task_id,
        input_count=len(task.input_file_ids),
        output_level=task.output_level,
        seq_cutoff=task.seq_cutoff,
    )

    readers: list[SSTableReader] = []
    try:
        for file_id in task.input_file_ids:
            sst_dir = Path(task.input_dirs[file_id])
            reader = _open_reader_sync(sst_dir, file_id)
            readers.append(reader)
            total_input_records += reader.meta.record_count
            logger.debug(
                "Input reader opened",
                file_id=file_id,
                records=reader.meta.record_count,
                size_bytes=reader.meta.size_bytes,
            )

        logger.info(
            "All inputs opened",
            task_id=task.task_id,
            total_input_records=total_input_records,
            reader_count=len(readers),
        )

        merge_iter = KWayMergeIterator(
            iterators=[r.iter_sorted() for r in readers],
            seq_cutoff=task.seq_cutoff,
            skip_tombstones=False,
            deduplicate=True,
        )

        output_dir = Path(task.output_dir)
        writer = SSTableWriter(
            directory=output_dir,
            file_id=task.output_file_id,
            snapshot_id=task.task_id,
            level=task.output_level,
        )

        records_written = 0
        for key, seq, ts, value in merge_iter:
            writer.put(key, seq, ts, value)
            records_written += 1

        meta = writer.finish_sync()

        elapsed_ms = (time.monotonic() - t0) * 1000
        dedup_dropped = total_input_records - records_written
        logger.info(
            "Subprocess merge complete",
            task_id=task.task_id,
            input_records=total_input_records,
            output_records=records_written,
            dedup_dropped=dedup_dropped,
            output_bytes=meta.size_bytes,
            output_blocks=meta.block_count,
            elapsed_ms=round(elapsed_ms, 1),
        )
        return meta

    finally:
        for reader in readers:
            reader.close()


def _open_reader_sync(directory: Path, file_id: str) -> SSTableReader:
    """Open an SSTableReader synchronously — no event loop required."""
    meta = SSTableMeta.from_json(
        (directory / "meta.json").read_text(encoding="utf-8"),
    )
    index = SparseIndex.from_bytes(
        (directory / meta.index_file).read_bytes(),
    )
    bloom = BloomFilter.from_bytes(
        (directory / meta.filter_file).read_bytes(),
    )

    data_path = directory / meta.data_file
    fd = os.open(str(data_path), os.O_RDONLY)
    try:
        size = os.fstat(fd).st_size
        mm: mmap.mmap | None = (
            mmap.mmap(fd, 0, access=mmap.ACCESS_READ) if size > 0 else None
        )
    except Exception:
        os.close(fd)
        raise

    return SSTableReader(
        directory=directory,
        file_id=file_id,
        meta=meta,
        index=index,
        bloom=bloom,
        cache=None,
        mm=mm,
        fd=fd,
    )
