"""BlockCache — tiered LRU cache for SSTable blocks, bloom filters, and indexes.

Three separate LRU pools with different eviction priorities:
  - **Bloom filters** (offset ``-1``): highest retention — evicted last
  - **Sparse indexes** (offset ``-2``): medium retention
  - **Data blocks** (offset ``>= 0``): lowest retention — evicted first

Each tier has its own capacity.  A ``get``/``put`` call is routed to
the correct tier automatically based on the offset value.
"""

from __future__ import annotations

import threading

from cachetools import LRUCache

from app.types import FileID, Offset

# Sentinel offsets (must match app.sstable.reader constants)
_BLOOM_OFFSET: int = -1
_INDEX_OFFSET: int = -2


class BlockCache:
    """Tiered thread-safe LRU cache.

    Eviction priority (first evicted → last evicted):
        data blocks → indexes → bloom filters
    """

    def __init__(
        self,
        data_maxsize: int = 256,
        index_maxsize: int = 64,
        bloom_maxsize: int = 64,
    ) -> None:
        self._data: LRUCache[tuple[FileID, Offset], bytes] = LRUCache(
            maxsize=data_maxsize,
        )
        self._index: LRUCache[tuple[FileID, Offset], bytes] = LRUCache(
            maxsize=index_maxsize,
        )
        self._bloom: LRUCache[tuple[FileID, Offset], bytes] = LRUCache(
            maxsize=bloom_maxsize,
        )
        self._lock = threading.Lock()

    def _tier(
        self, offset: Offset,
    ) -> LRUCache[tuple[FileID, Offset], bytes]:
        """Route to the correct LRU tier based on offset."""
        if offset == _BLOOM_OFFSET:
            return self._bloom
        if offset == _INDEX_OFFSET:
            return self._index
        return self._data

    def get(self, file_id: FileID, offset: Offset) -> bytes | None:
        """Return cached entry or None."""
        with self._lock:
            result: bytes | None = self._tier(offset).get(
                (file_id, offset),
            )
            return result

    def put(
        self, file_id: FileID, offset: Offset, block: bytes,
    ) -> None:
        """Insert an entry into the appropriate tier."""
        with self._lock:
            self._tier(offset)[(file_id, offset)] = block

    def invalidate(self, file_id: FileID) -> None:
        """Evict all entries (all tiers) belonging to *file_id*."""
        with self._lock:
            for cache in (self._data, self._index, self._bloom):
                keys = [k for k in cache if k[0] == file_id]
                for k in keys:
                    del cache[k]

    def invalidate_all(self, file_ids: list[FileID]) -> None:
        """Evict all entries (all tiers) for any of the given file IDs."""
        ids = set(file_ids)
        with self._lock:
            for cache in (self._data, self._index, self._bloom):
                keys = [k for k in cache if k[0] in ids]
                for k in keys:
                    del cache[k]
