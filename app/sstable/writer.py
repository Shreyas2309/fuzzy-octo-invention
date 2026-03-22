"""SSTableWriter — write-once state machine: OPEN → put() × N → finish() → DONE.

Writes four files into a directory:
    - ``data.bin``   — sorted KV records grouped into blocks
    - ``index.bin``  — sparse index (first key per block → offset)
    - ``filter.bin`` — Bloom filter for quick negative lookups
    - ``meta.json``  — metadata (written LAST as completeness signal)
"""

from __future__ import annotations

import asyncio
import datetime
import enum
import os
from pathlib import Path

from app.bloom.filter import BloomFilter
from app.common.encoding import encode_record
from app.common.errors import SSTableWriteError
from app.index.sparse import SparseIndex
from app.observability import get_logger
from app.sstable.meta import SSTableMeta
from app.types import BLOCK_SIZE_DEFAULT, FileID, Key, Level, SeqNum, SnapshotID, Value

logger = get_logger(__name__)


class _State(enum.Enum):
    OPEN = "OPEN"
    FINISHING = "FINISHING"
    DONE = "DONE"


class SSTableWriter:
    """Write-once SSTable builder.

    Records must be added in ascending key order via :meth:`put`.
    Call :meth:`finish` (async) or :meth:`finish_sync` when done.
    """

    def __init__(
        self,
        directory: Path,
        file_id: FileID,
        snapshot_id: SnapshotID,
        level: Level,
        block_size: int = BLOCK_SIZE_DEFAULT,
        block_entries: int = 0,
        bloom_n: int = 1_000_000,
        bloom_fpr: float = 0.01,
    ) -> None:
        self._dir = directory
        self._file_id = file_id
        self._snapshot_id = snapshot_id
        self._level = level
        self._block_size = block_size
        self._block_entries = block_entries
        self._state = _State.OPEN

        # File paths
        self._data_path = directory / "data.bin"
        self._index_path = directory / "index.bin"
        self._filter_path = directory / "filter.bin"
        self._meta_path = directory / "meta.json"

        # Bloom + index builders
        self._bloom = BloomFilter(n=bloom_n, fpr=bloom_fpr)
        self._bloom_fpr = bloom_fpr
        self._index = SparseIndex()

        # Block buffering
        self._block_buf: list[bytes] = []
        self._block_buf_size: int = 0
        self._block_buf_count: int = 0
        self._block_first_key: Key | None = None

        # Output file handle
        directory.mkdir(parents=True, exist_ok=True)
        self._data_fd = open(self._data_path, "wb")  # noqa: SIM115
        self._data_offset: int = 0

        # Stats
        self._record_count: int = 0
        self._block_count: int = 0
        self._min_key: Key | None = None
        self._max_key: Key | None = None
        self._seq_min: SeqNum | None = None
        self._seq_max: SeqNum | None = None
        self._last_key: Key | None = None

    # ── put ─────────────────────────────────────────────────────────────

    def put(
        self,
        key: Key,
        seq: SeqNum,
        timestamp_ms: int,
        value: Value,
    ) -> None:
        """Add a record. Keys must be in ascending order."""
        if self._state is not _State.OPEN:
            raise SSTableWriteError("Writer is not in OPEN state")

        # Validate ascending key order
        if self._last_key is not None and key <= self._last_key:
            raise SSTableWriteError(
                f"Keys must be ascending: {key!r} <= {self._last_key!r}"
            )

        encoded = encode_record(key, seq, timestamp_ms, value)

        # Track first key of current block
        if self._block_first_key is None:
            self._block_first_key = key
            self._index.add(key, self._data_offset)

        self._block_buf.append(encoded)
        self._block_buf_size += len(encoded)
        self._block_buf_count += 1

        # Add to bloom
        self._bloom.add(key)

        # Stats
        self._record_count += 1
        if self._min_key is None:
            self._min_key = key
        self._max_key = key
        self._last_key = key
        if self._seq_min is None or seq < self._seq_min:
            self._seq_min = seq
        if self._seq_max is None or seq > self._seq_max:
            self._seq_max = seq

        # Flush block if full (entry-count mode or byte-size mode)
        if self._block_entries > 0:
            if self._block_buf_count >= self._block_entries:
                self._flush_block()
        elif self._block_buf_size >= self._block_size:
            self._flush_block()

    # ── finish ──────────────────────────────────────────────────────────

    async def finish(self) -> SSTableMeta:
        """Finalize the SSTable (async). Bloom + index written concurrently."""
        if self._state is not _State.OPEN:
            raise SSTableWriteError("Writer is not in OPEN state")
        self._state = _State.FINISHING
        self._flush_block_and_close()

        bloom_bytes = self._bloom.to_bytes()
        index_bytes = self._index.to_bytes()

        # Write bloom and index concurrently
        await asyncio.gather(
            asyncio.to_thread(self._write_file, self._filter_path, bloom_bytes),
            asyncio.to_thread(self._write_file, self._index_path, index_bytes),
        )

        return self._finalize()

    def finish_sync(self) -> SSTableMeta:
        """Finalize the SSTable (sync). For L1+ subprocess use."""
        if self._state is not _State.OPEN:
            raise SSTableWriteError("Writer is not in OPEN state")
        self._state = _State.FINISHING
        self._flush_block_and_close()

        self._write_file(self._filter_path, self._bloom.to_bytes())
        self._write_file(self._index_path, self._index.to_bytes())

        return self._finalize()

    # ── internal ────────────────────────────────────────────────────────

    def _flush_block(self) -> None:
        """Write buffered records as one block."""
        if not self._block_buf:
            return
        data = b"".join(self._block_buf)
        self._data_fd.write(data)
        self._data_offset += len(data)
        self._block_count += 1
        self._block_buf.clear()
        self._block_buf_size = 0
        self._block_buf_count = 0
        self._block_first_key = None

    def _flush_block_and_close(self) -> None:
        """Flush remaining buffered records and close data file.

        Rejects empty SSTables (no records written via put).
        """
        if self._record_count == 0:
            self._data_fd.close()
            raise SSTableWriteError("Cannot finish an empty SSTable")

        self._flush_block()
        self._data_fd.flush()
        os.fsync(self._data_fd.fileno())
        self._data_fd.close()

    def _finalize(self) -> SSTableMeta:
        """Write meta.json and return metadata."""
        meta = SSTableMeta(
            file_id=self._file_id,
            snapshot_id=self._snapshot_id,
            level=self._level,
            size_bytes=self._data_offset,
            record_count=self._record_count,
            block_count=self._block_count,
            min_key=self._min_key or b"",
            max_key=self._max_key or b"",
            seq_min=self._seq_min or 0,
            seq_max=self._seq_max or 0,
            bloom_fpr=self._bloom_fpr,
            created_at=datetime.datetime.now(datetime.UTC).isoformat(),
            data_file="data.bin",
            index_file="index.bin",
            filter_file="filter.bin",
        )

        # meta.json written LAST — completeness signal
        self._write_file(self._meta_path, meta.to_json().encode("utf-8"))
        self._state = _State.DONE

        logger.info(
            "SSTable written",
            file_id=self._file_id,
            records=self._record_count,
            blocks=self._block_count,
            size_bytes=self._data_offset,
        )
        return meta

    @staticmethod
    def _write_file(path: Path, data: bytes) -> None:
        """Write *data* to *path* with fsync."""
        with open(path, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
