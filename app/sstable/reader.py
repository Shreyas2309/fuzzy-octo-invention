"""SSTableReader — read-only access to one SSTable via mmap.

Provides point lookups: bloom → sparse index bisect → mmap scan.
Bloom filter and sparse index are loaded lazily on first ``get()``
and cached in the shared ``BlockCache`` for cross-restart reuse.
"""

from __future__ import annotations

import contextlib
import mmap
import os
from collections.abc import Iterator
from pathlib import Path

from app.bloom.filter import BloomFilter
from app.cache.block import BlockCache
from app.common.encoding import iter_block
from app.index.sparse import SparseIndex
from app.observability import get_logger
from app.sstable.meta import SSTableMeta
from app.types import FileID, Key, Level, SeqNum, Value

logger = get_logger(__name__)

# Sentinel offsets used as cache keys for bloom/index (never collide
# with real data block offsets which are always >= 0).
_BLOOM_CACHE_OFFSET: int = -1
_INDEX_CACHE_OFFSET: int = -2


class SSTableReader:
    """Read-only access to one SSTable.

    Bloom filter and sparse index are loaded **lazily** on the first
    call to :meth:`get`.  Once loaded, they are cached in the shared
    :class:`BlockCache` so a future reader for the same file (e.g.
    after engine restart) can skip the disk read.
    """

    def __init__(
        self,
        directory: Path,
        file_id: FileID,
        meta: SSTableMeta,
        index: SparseIndex | None,
        bloom: BloomFilter | None,
        cache: BlockCache | None,
        mm: mmap.mmap | None,
        fd: int,
    ) -> None:
        self._dir = directory
        self._file_id = file_id
        self._meta = meta
        self._index = index
        self._bloom = bloom
        self._cache = cache
        self._mm = mm
        self._fd = fd
        self._loaded = index is not None and bloom is not None

    @property
    def meta(self) -> SSTableMeta:
        """Return the SSTable metadata."""
        return self._meta

    @property
    def file_id(self) -> FileID:
        """Return the file ID."""
        return self._file_id

    # ── lazy loading ────────────────────────────────────────────────────

    def _ensure_loaded(self) -> None:
        """Load bloom + index on first access (lazy).

        Checks the block cache first — if the raw bytes are cached from
        a previous reader instance, deserialization is the only cost.
        Falls back to disk read on cache miss, then populates the cache.
        """
        if self._loaded:
            return

        # Load bloom filter
        if self._bloom is None:
            bloom_bytes = self._cache_get(_BLOOM_CACHE_OFFSET)
            if bloom_bytes is None:
                bloom_path = self._dir / self._meta.filter_file
                bloom_bytes = bloom_path.read_bytes()
                self._cache_put(_BLOOM_CACHE_OFFSET, bloom_bytes)
            self._bloom = BloomFilter.from_bytes(bloom_bytes)

        # Load sparse index
        if self._index is None:
            index_bytes = self._cache_get(_INDEX_CACHE_OFFSET)
            if index_bytes is None:
                index_path = self._dir / self._meta.index_file
                index_bytes = index_path.read_bytes()
                self._cache_put(_INDEX_CACHE_OFFSET, index_bytes)
            self._index = SparseIndex.from_bytes(index_bytes)

        self._loaded = True
        logger.debug(
            "Bloom + index loaded lazily",
            file_id=self._file_id,
        )

    def _cache_get(self, offset: int) -> bytes | None:
        if self._cache is not None:
            return self._cache.get(self._file_id, offset)
        return None

    def _cache_put(self, offset: int, data: bytes) -> None:
        if self._cache is not None:
            self._cache.put(self._file_id, offset, data)

    # ── factory ─────────────────────────────────────────────────────────

    @classmethod
    async def open(
        cls,
        directory: Path,
        file_id: FileID,
        cache: BlockCache | None = None,
        level: Level = 0,
    ) -> SSTableReader:
        """Open an SSTable for reading.

        Bloom and index are NOT loaded here — they are deferred to the
        first ``get()`` call (lazy loading).  Only ``meta.json`` is
        parsed and ``data.bin`` is memory-mapped.
        """
        meta_path = directory / "meta.json"
        meta = SSTableMeta.from_json(
            meta_path.read_text(encoding="utf-8"),
        )

        # mmap the data file
        data_path = directory / meta.data_file
        fd = os.open(str(data_path), os.O_RDONLY)
        try:
            size = os.fstat(fd).st_size
            if size == 0:
                mm: mmap.mmap | None = None
            else:
                mm = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)
        except Exception:
            os.close(fd)
            raise

        reader = cls(
            directory=directory,
            file_id=file_id,
            meta=meta,
            index=None,   # lazy
            bloom=None,   # lazy
            cache=cache,
            mm=mm,
            fd=fd,
        )

        logger.info(
            "SSTable opened (lazy)",
            file_id=file_id,
            records=meta.record_count,
            level=level,
        )
        return reader

    # ── point lookup ────────────────────────────────────────────────────

    def get(self, key: Key) -> tuple[SeqNum, int, Value] | None:
        """Look up *key*. Returns ``(seq, timestamp_ms, value)`` or None.

        Flow: bloom check → sparse index bisect → block scan.
        Bloom + index loaded lazily on first call.
        """
        # Empty SSTable
        if self._mm is None:
            return None

        # Lazy load bloom + index
        self._ensure_loaded()
        assert self._bloom is not None  # noqa: S101
        assert self._index is not None  # noqa: S101

        # 1. Bloom filter — fast negative
        if not self._bloom.may_contain(key):
            return None

        # 2. Sparse index — find candidate block
        block_offset = self._index.floor_offset(key)
        if block_offset is None:
            return None

        # 3. Determine block end (next block offset or EOF)
        block_end = self._find_block_end(block_offset)

        # 4. Check block cache before scanning
        block_data: bytes | None = None
        if self._cache is not None:
            block_data = self._cache.get(self._file_id, block_offset)

        if block_data is not None:
            mv = memoryview(block_data)
            scan_start = 0
            scan_end = len(block_data)
        else:
            mv = memoryview(self._mm)
            scan_start = block_offset
            scan_end = block_end
            # Populate cache on miss
            if self._cache is not None:
                self._cache.put(
                    self._file_id,
                    block_offset,
                    bytes(self._mm[block_offset:block_end]),
                )

        # 5. Scan the block
        best: tuple[SeqNum, int, Value] | None = None
        for rec in iter_block(mv, scan_start, scan_end):
            if rec.key == key:
                if best is None or rec.seq > best[0]:
                    best = (rec.seq, rec.timestamp_ms, rec.value)
            elif rec.key > key:
                break  # keys are sorted, no point continuing

        return best

    def scan_all(self) -> list[tuple[Key, SeqNum, int, Value]]:
        """Return all records in this SSTable as a sorted list.

        Used by the ``disk`` command to display SSTable contents.
        """
        if self._mm is None or self._meta.size_bytes == 0:
            return []
        mv = memoryview(self._mm)
        return [
            (rec.key, rec.seq, rec.timestamp_ms, rec.value)
            for rec in iter_block(mv, 0, self._meta.size_bytes)
        ]

    def iter_sorted(self) -> Iterator[tuple[Key, SeqNum, int, Value]]:
        """Yield all records in ascending key order without materialising.

        Used as input to KWayMergeIterator during compaction.
        More memory-efficient than scan_all() for large SSTables.
        """
        if self._mm is None or self._meta.size_bytes == 0:
            return
        mv = memoryview(self._mm)
        for rec in iter_block(mv, 0, self._meta.size_bytes):
            yield (rec.key, rec.seq, rec.timestamp_ms, rec.value)

    def _find_block_end(self, block_offset: int) -> int:
        """Return the end offset of the block starting at *block_offset*."""
        assert self._index is not None  # noqa: S101
        next_offset = self._index.next_offset_after(block_offset)
        if next_offset is not None:
            return next_offset
        return self._meta.size_bytes

    # ── cleanup ─────────────────────────────────────────────────────────

    def close(self) -> None:
        """Release mmap and file descriptor. Never raises."""
        if self._mm is not None:
            with contextlib.suppress(Exception):
                self._mm.close()
        with contextlib.suppress(Exception):
            os.close(self._fd)

        logger.debug("SSTable closed", file_id=self._file_id)


