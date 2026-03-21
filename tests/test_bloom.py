"""Tests for app.bloom.filter — BloomFilter."""

from __future__ import annotations

from app.bloom.filter import BloomFilter


class TestBloomFilter:
    def test_add_and_may_contain(self) -> None:
        bf = BloomFilter(n=100, fpr=0.01)
        bf.add(b"hello")
        bf.add(b"world")

        assert bf.may_contain(b"hello") is True
        assert bf.may_contain(b"world") is True

    def test_missing_key(self) -> None:
        bf = BloomFilter(n=100, fpr=0.01)
        bf.add(b"present")
        # A key never added should likely return False
        # (could be FP but extremely unlikely with small n)
        assert bf.may_contain(b"definitely_not_here_xyz_123") is False

    def test_serialization_roundtrip(self) -> None:
        bf = BloomFilter(n=1000, fpr=0.01)
        keys = [f"key_{i}".encode() for i in range(50)]
        for k in keys:
            bf.add(k)

        data = bf.to_bytes()
        bf2 = BloomFilter.from_bytes(data)

        for k in keys:
            assert bf2.may_contain(k) is True

    def test_fpr_within_bounds(self) -> None:
        """Insert n keys, test FPR on n non-inserted keys."""
        n = 10_000
        target_fpr = 0.05
        bf = BloomFilter(n=n, fpr=target_fpr)

        for i in range(n):
            bf.add(f"inserted_{i}".encode())

        false_positives = 0
        test_count = n
        for i in range(test_count):
            if bf.may_contain(f"not_inserted_{i}".encode()):
                false_positives += 1

        observed_fpr = false_positives / test_count
        # Allow 2x the target FPR as margin
        assert observed_fpr < target_fpr * 2, (
            f"FPR too high: {observed_fpr:.4f} > {target_fpr * 2:.4f}"
        )
