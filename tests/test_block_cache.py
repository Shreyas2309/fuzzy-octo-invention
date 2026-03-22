"""Tests for app.cache.block — tiered BlockCache."""

from __future__ import annotations

import threading

from app.cache.block import BlockCache


class TestBlockCache:
    def test_put_and_get(self) -> None:
        cache = BlockCache(data_maxsize=10)
        cache.put("file1", 0, b"block_data")
        assert cache.get("file1", 0) == b"block_data"

    def test_get_missing(self) -> None:
        cache = BlockCache(data_maxsize=10)
        assert cache.get("nope", 0) is None

    def test_invalidate(self) -> None:
        cache = BlockCache(data_maxsize=10)
        cache.put("file1", 0, b"a")
        cache.put("file1", 4096, b"b")
        cache.put("file2", 0, b"c")

        cache.invalidate("file1")

        assert cache.get("file1", 0) is None
        assert cache.get("file1", 4096) is None
        assert cache.get("file2", 0) == b"c"

    def test_lru_eviction(self) -> None:
        cache = BlockCache(data_maxsize=2)
        cache.put("f", 0, b"first")
        cache.put("f", 1, b"second")
        cache.put("f", 2, b"third")  # should evict "first"

        assert cache.get("f", 0) is None
        assert cache.get("f", 1) == b"second"
        assert cache.get("f", 2) == b"third"

    def test_thread_safety(self) -> None:
        cache = BlockCache(data_maxsize=1000)

        def writer(tid: int) -> None:
            for i in range(100):
                cache.put(f"t{tid}", i, f"data_{tid}_{i}".encode())

        def reader(tid: int) -> None:
            for i in range(100):
                cache.get(f"t{tid}", i)

        threads = []
        for t in range(4):
            threads.append(threading.Thread(target=writer, args=(t,)))
            threads.append(threading.Thread(target=reader, args=(t,)))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def test_bloom_tier_separate(self) -> None:
        """Bloom entries don't evict data entries."""
        cache = BlockCache(data_maxsize=2, bloom_maxsize=2)
        cache.put("f", 0, b"data_block")
        cache.put("f", -1, b"bloom_bytes")

        # Both should be present (separate tiers)
        assert cache.get("f", 0) == b"data_block"
        assert cache.get("f", -1) == b"bloom_bytes"

    def test_index_tier_separate(self) -> None:
        """Index entries don't evict data entries."""
        cache = BlockCache(data_maxsize=2, index_maxsize=2)
        cache.put("f", 0, b"data_block")
        cache.put("f", -2, b"index_bytes")

        assert cache.get("f", 0) == b"data_block"
        assert cache.get("f", -2) == b"index_bytes"

    def test_data_evicts_before_bloom(self) -> None:
        """Data tier fills up and evicts without touching bloom tier."""
        cache = BlockCache(data_maxsize=1, bloom_maxsize=2)
        cache.put("f", -1, b"bloom")  # bloom tier
        cache.put("f", 0, b"block_0")  # data tier (full)
        cache.put("f", 4096, b"block_1")  # evicts block_0

        assert cache.get("f", 0) is None  # evicted from data tier
        assert cache.get("f", 4096) == b"block_1"
        assert cache.get("f", -1) == b"bloom"  # bloom untouched

    def test_invalidate_all_clears_all_tiers(self) -> None:
        cache = BlockCache()
        cache.put("f1", 0, b"data")
        cache.put("f1", -1, b"bloom")
        cache.put("f1", -2, b"index")
        cache.put("f2", 0, b"other")

        cache.invalidate_all(["f1"])

        assert cache.get("f1", 0) is None
        assert cache.get("f1", -1) is None
        assert cache.get("f1", -2) is None
        assert cache.get("f2", 0) == b"other"
