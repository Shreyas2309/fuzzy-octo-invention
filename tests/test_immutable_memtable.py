"""Unit tests for ImmutableMemTable."""

from __future__ import annotations

import pytest

from app.common.errors import ImmutableTableAccessError
from app.memtable.immutable import ImmutableMemTable
from app.types import TOMBSTONE


def _sample_data() -> list[tuple[bytes, int, int, bytes]]:
    """Sorted sample data: (key, seq, timestamp_ms, value)."""
    return [
        (b"a", 1, 100, b"alpha"),
        (b"b", 2, 200, b"beta"),
        (b"c", 3, 300, TOMBSTONE),
    ]


class TestImmutableMemTableGet:
    """Point lookup tests."""

    def test_get_existing(self) -> None:
        t = ImmutableMemTable("snap1", _sample_data())
        assert t.get(b"a") == (1, b"alpha")
        assert t.get(b"b") == (2, b"beta")

    def test_get_returns_tombstone_raw(self) -> None:
        t = ImmutableMemTable("snap1", _sample_data())
        result = t.get(b"c")
        assert result is not None
        assert result[1] == TOMBSTONE  # not resolved here

    def test_get_missing(self) -> None:
        t = ImmutableMemTable("snap1", _sample_data())
        assert t.get(b"z") is None

    def test_get_returns_no_timestamp(self) -> None:
        t = ImmutableMemTable("snap1", _sample_data())
        result = t.get(b"a")
        assert result is not None
        assert len(result) == 2  # (seq, value) only


class TestImmutableMemTableIteration:
    """Iteration and items() tests."""

    def test_items_sorted(self) -> None:
        t = ImmutableMemTable("snap1", _sample_data())
        keys = [k for k, _, _, _ in t.items()]
        assert keys == [b"a", b"b", b"c"]

    def test_items_includes_timestamp(self) -> None:
        t = ImmutableMemTable("snap1", _sample_data())
        items = list(t.items())
        assert items[0] == (b"a", 1, 100, b"alpha")


class TestImmutableMemTableProperties:
    """Properties and metadata."""

    def test_seq_min_max(self) -> None:
        t = ImmutableMemTable("snap1", _sample_data())
        assert t.seq_min == 1
        assert t.seq_max == 3

    def test_seq_empty(self) -> None:
        t = ImmutableMemTable("snap1", [])
        assert t.seq_min == 0
        assert t.seq_max == 0

    def test_size_bytes(self) -> None:
        t = ImmutableMemTable("snap1", _sample_data())
        expected = sum(len(k) + len(v) for k, _, _, v in _sample_data())
        assert t.size_bytes == expected

    def test_len(self) -> None:
        t = ImmutableMemTable("snap1", _sample_data())
        assert len(t) == 3

    def test_snapshot_id(self) -> None:
        t = ImmutableMemTable("my-snap-id", _sample_data())
        assert t.snapshot_id == "my-snap-id"


class TestImmutableMemTableMeta:
    """Metadata dataclass."""

    def test_metadata(self) -> None:
        t = ImmutableMemTable("snap1", _sample_data())
        m = t.metadata
        assert m.snapshot_id == "snap1"
        assert m.source_table_id == "snap1"
        assert m.entry_count == 3
        assert m.tombstone_count == 1
        assert m.seq_min == 1
        assert m.seq_max == 3
        assert m.frozen_at > 0

    def test_metadata_empty(self) -> None:
        t = ImmutableMemTable("snap1", [])
        m = t.metadata
        assert m.entry_count == 0
        assert m.tombstone_count == 0


class TestImmutableMemTableImmutability:
    """Mutation guard."""

    def test_setattr_raises(self) -> None:
        t = ImmutableMemTable("snap1", _sample_data())
        with pytest.raises(ImmutableTableAccessError):
            t.snapshot_id = "hacked"  # type: ignore[misc]
