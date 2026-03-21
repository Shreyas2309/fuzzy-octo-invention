"""Unit tests for ActiveMemTable."""

from __future__ import annotations

import pytest

from app.common.errors import SnapshotEmptyError
from app.memtable.active import ActiveMemTable


class TestActiveMemTable:
    """Core ActiveMemTable operations."""

    def test_table_id_unique(self) -> None:
        t1 = ActiveMemTable()
        t2 = ActiveMemTable()
        assert t1.table_id != t2.table_id

    def test_put_then_get(self) -> None:
        t = ActiveMemTable()
        t.put(b"k", seq=1, timestamp_ms=100, value=b"v")
        assert t.get(b"k") == (1, b"v")

    def test_get_missing(self) -> None:
        t = ActiveMemTable()
        assert t.get(b"nope") is None

    def test_delete(self) -> None:
        t = ActiveMemTable()
        t.put(b"k", seq=1, timestamp_ms=100, value=b"v")
        existed = t.delete(b"k", seq=2, timestamp_ms=200)
        assert existed is True
        assert t.get(b"k") is None

    def test_size_bytes(self) -> None:
        t = ActiveMemTable()
        t.put(b"key", seq=1, timestamp_ms=100, value=b"val")
        assert t.size_bytes == len(b"key") + len(b"val")

    def test_freeze_returns_sorted(self) -> None:
        t = ActiveMemTable()
        t.put(b"c", seq=1, timestamp_ms=100, value=b"3")
        t.put(b"a", seq=2, timestamp_ms=100, value=b"1")
        t.put(b"b", seq=3, timestamp_ms=100, value=b"2")

        data = t.freeze()
        keys = [k for k, _, _, _ in data]
        assert keys == [b"a", b"b", b"c"]

    def test_freeze_empty_raises(self) -> None:
        t = ActiveMemTable()
        with pytest.raises(SnapshotEmptyError):
            t.freeze()

    def test_freeze_is_a_copy(self) -> None:
        t = ActiveMemTable()
        t.put(b"a", seq=1, timestamp_ms=100, value=b"1")
        snap = t.freeze()

        # Write after freeze — snapshot should not change
        t.put(b"b", seq=2, timestamp_ms=100, value=b"2")
        assert len(snap) == 1


class TestActiveMemTableMetadata:
    """Metadata tracking."""

    def test_metadata_after_puts(self) -> None:
        t = ActiveMemTable()
        t.put(b"a", seq=5, timestamp_ms=100, value=b"1")
        t.put(b"b", seq=10, timestamp_ms=200, value=b"2")

        m = t.metadata
        assert m.table_id == t.table_id
        assert m.entry_count == 2
        assert m.seq_first == 5
        assert m.seq_last == 10
        assert m.size_bytes > 0
        assert m.created_at > 0

    def test_metadata_counts_tombstones(self) -> None:
        t = ActiveMemTable()
        t.put(b"a", seq=1, timestamp_ms=100, value=b"1")
        t.delete(b"a", seq=2, timestamp_ms=200)

        m = t.metadata
        assert m.entry_count == 2  # put + tombstone

    def test_metadata_is_frozen(self) -> None:
        t = ActiveMemTable()
        t.put(b"a", seq=1, timestamp_ms=100, value=b"1")
        m = t.metadata
        t.put(b"b", seq=2, timestamp_ms=200, value=b"2")
        # m is a snapshot — should not change
        assert m.entry_count == 1
