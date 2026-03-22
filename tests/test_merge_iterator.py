"""Tests for app.common.merge_iterator — KWayMergeIterator."""

from __future__ import annotations

from app.common.merge_iterator import KWayMergeIterator
from app.types import TOMBSTONE


def _it(
    records: list[tuple[bytes, int, int, bytes]],
) -> list[tuple[bytes, int, int, bytes]]:
    """Helper: return records as-is (already a list, iter() works)."""
    return records


class TestKWayMergeIterator:
    def test_two_sources_no_overlap(self) -> None:
        s1 = iter(_it([(b"a", 1, 100, b"v1"), (b"c", 3, 100, b"v3")]))
        s2 = iter(_it([(b"b", 2, 100, b"v2"), (b"d", 4, 100, b"v4")]))
        result = list(KWayMergeIterator([s1, s2]))
        keys = [r[0] for r in result]
        assert keys == [b"a", b"b", b"c", b"d"]

    def test_same_key_highest_seq_wins(self) -> None:
        s1 = iter(_it([(b"k", 10, 100, b"old")]))
        s2 = iter(_it([(b"k", 50, 200, b"new")]))
        result = list(KWayMergeIterator([s1, s2]))
        assert len(result) == 1
        assert result[0] == (b"k", 50, 200, b"new")

    def test_same_key_three_sources(self) -> None:
        s1 = iter(_it([(b"x", 1, 100, b"v1")]))
        s2 = iter(_it([(b"x", 5, 100, b"v5")]))
        s3 = iter(_it([(b"x", 3, 100, b"v3")]))
        result = list(KWayMergeIterator([s1, s2, s3]))
        assert len(result) == 1
        assert result[0][1] == 5  # highest seq

    def test_skip_tombstones(self) -> None:
        s1 = iter(_it([(b"a", 1, 100, b"v"), (b"b", 2, 100, TOMBSTONE)]))
        result = list(KWayMergeIterator([s1], skip_tombstones=True))
        keys = [r[0] for r in result]
        assert keys == [b"a"]

    def test_tombstone_gc_below_cutoff(self) -> None:
        s1 = iter(
            _it([(b"a", 5, 100, TOMBSTONE), (b"b", 15, 100, TOMBSTONE)])
        )
        result = list(KWayMergeIterator([s1], seq_cutoff=10))
        # seq=5 < cutoff=10 → GC'd; seq=15 >= cutoff → preserved
        assert len(result) == 1
        assert result[0][0] == b"b"

    def test_tombstone_preserved_above_cutoff(self) -> None:
        s1 = iter(_it([(b"k", 20, 100, TOMBSTONE)]))
        result = list(KWayMergeIterator([s1], seq_cutoff=10))
        assert len(result) == 1
        assert result[0][3] == TOMBSTONE

    def test_empty_source(self) -> None:
        s1 = iter(_it([(b"a", 1, 100, b"v")]))
        s2 = iter(_it([]))
        result = list(KWayMergeIterator([s1, s2]))
        assert len(result) == 1

    def test_all_sources_empty(self) -> None:
        result = list(KWayMergeIterator([iter([]), iter([])]))
        assert result == []

    def test_deduplicate_false(self) -> None:
        s1 = iter(_it([(b"k", 10, 100, b"old")]))
        s2 = iter(_it([(b"k", 50, 200, b"new")]))
        result = list(
            KWayMergeIterator([s1, s2], deduplicate=False)
        )
        assert len(result) == 2
        seqs = [r[1] for r in result]
        assert seqs == [50, 10]  # highest seq first (heap order)

    def test_zero_cutoff_preserves_all_tombstones(self) -> None:
        s1 = iter(_it([(b"a", 1, 100, TOMBSTONE)]))
        result = list(KWayMergeIterator([s1], seq_cutoff=0))
        assert len(result) == 1
        assert result[0][3] == TOMBSTONE
