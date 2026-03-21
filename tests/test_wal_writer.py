"""Unit tests for WALWriter — synchronous WAL operations."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.common.errors import WALCorruptError
from app.types import TOMBSTONE, OpType
from app.wal.writer import WALEntry, WALWriter

from tests.conftest import make_entry, make_tombstone


class TestWALEntry:
    """Tests for the WALEntry dataclass."""

    def test_is_tombstone_true(self) -> None:
        entry = make_tombstone(seq=1)
        assert entry.is_tombstone is True
        assert entry.op == OpType.DELETE

    def test_is_tombstone_false(self) -> None:
        entry = make_entry(seq=1, value=b"hello")
        assert entry.is_tombstone is False
        assert entry.op == OpType.PUT

    def test_frozen(self) -> None:
        entry = make_entry(seq=1)
        with pytest.raises(AttributeError):
            entry.seq = 2  # type: ignore[misc]

    def test_op_type_round_trip(self) -> None:
        put = make_entry(seq=1, op=OpType.PUT)
        delete = make_entry(seq=2, op=OpType.DELETE)
        assert put.op == OpType.PUT
        assert delete.op == OpType.DELETE


class TestWALWriterAppendReplay:
    """Tests for append + replay round-trip."""

    def test_single_entry(self, wal_path: Path) -> None:
        writer = WALWriter(wal_path)
        entry = make_entry(seq=1, key=b"key1", value=b"val1")
        writer.append(entry)
        writer.close()

        writer2 = WALWriter(wal_path)
        entries = writer2.replay()
        assert len(entries) == 1
        assert entries[0].seq == 1
        assert entries[0].key == b"key1"
        assert entries[0].value == b"val1"
        writer2.close()

    def test_multiple_entries_sorted_by_seq(self, wal_path: Path) -> None:
        writer = WALWriter(wal_path)
        # Append out of order
        writer.append(make_entry(seq=3, key=b"c"))
        writer.append(make_entry(seq=1, key=b"a"))
        writer.append(make_entry(seq=2, key=b"b"))
        writer.close()

        writer2 = WALWriter(wal_path)
        entries = writer2.replay()
        assert [e.seq for e in entries] == [1, 2, 3]
        writer2.close()

    def test_round_trip_100_entries(self, wal_path: Path) -> None:
        writer = WALWriter(wal_path)
        for i in range(100):
            writer.append(
                make_entry(seq=i, key=f"key{i}".encode(), value=f"val{i}".encode())
            )
        writer.close()

        writer2 = WALWriter(wal_path)
        entries = writer2.replay()
        assert len(entries) == 100
        assert [e.seq for e in entries] == list(range(100))
        writer2.close()

    def test_tombstone_entry_round_trip(self, wal_path: Path) -> None:
        writer = WALWriter(wal_path)
        writer.append(make_tombstone(seq=1, key=b"deleted"))
        writer.close()

        writer2 = WALWriter(wal_path)
        entries = writer2.replay()
        assert len(entries) == 1
        assert entries[0].value == TOMBSTONE
        assert entries[0].op == OpType.DELETE
        assert entries[0].is_tombstone is True
        writer2.close()

    def test_op_type_preserved_across_replay(self, wal_path: Path) -> None:
        writer = WALWriter(wal_path)
        writer.append(make_entry(seq=1, key=b"a", op=OpType.PUT))
        writer.append(make_tombstone(seq=2, key=b"a"))
        writer.append(make_entry(seq=3, key=b"b", op=OpType.PUT))
        writer.close()

        writer2 = WALWriter(wal_path)
        entries = writer2.replay()
        assert [e.op for e in entries] == [OpType.PUT, OpType.DELETE, OpType.PUT]
        writer2.close()

    def test_timestamp_ms_preserved(self, wal_path: Path) -> None:
        writer = WALWriter(wal_path)
        writer.append(make_entry(seq=1, timestamp_ms=1234567890))
        writer.close()

        writer2 = WALWriter(wal_path)
        entries = writer2.replay()
        assert entries[0].timestamp_ms == 1234567890
        writer2.close()


class TestWALWriterReplayEdgeCases:
    """Tests for replay edge cases."""

    def test_replay_nonexistent_file(self, tmp_path: Path) -> None:
        path = tmp_path / "does_not_exist" / "wal.log"
        # Create parent so WALWriter init succeeds, but file is empty
        path.parent.mkdir(parents=True, exist_ok=True)
        writer = WALWriter(path)
        entries = writer.replay()
        assert entries == []
        writer.close()

    def test_replay_empty_file(self, wal_path: Path) -> None:
        writer = WALWriter(wal_path)
        entries = writer.replay()
        assert entries == []
        writer.close()

    def test_replay_corrupt_data(self, wal_path: Path) -> None:
        wal_path.parent.mkdir(parents=True, exist_ok=True)
        # Write garbage bytes
        with open(wal_path, "wb") as f:
            f.write(b"\xff\xfe\xfd\xfc\xfb\xfa")

        writer = WALWriter(wal_path)
        with pytest.raises(WALCorruptError):
            writer.replay()
        writer.close()


class TestWALWriterTruncate:
    """Tests for truncate_before."""

    def test_truncate_removes_old_entries(self, wal_path: Path) -> None:
        writer = WALWriter(wal_path)
        for i in range(1, 6):
            writer.append(make_entry(seq=i, key=f"k{i}".encode()))

        writer.truncate_before(3)

        entries = writer.replay()
        assert [e.seq for e in entries] == [4, 5]
        writer.close()

    def test_truncate_all(self, wal_path: Path) -> None:
        writer = WALWriter(wal_path)
        for i in range(1, 4):
            writer.append(make_entry(seq=i))

        writer.truncate_before(3)

        entries = writer.replay()
        assert entries == []
        writer.close()

    def test_truncate_none(self, wal_path: Path) -> None:
        writer = WALWriter(wal_path)
        for i in range(1, 4):
            writer.append(make_entry(seq=i))

        writer.truncate_before(0)

        entries = writer.replay()
        assert [e.seq for e in entries] == [1, 2, 3]
        writer.close()

    def test_append_after_truncate(self, wal_path: Path) -> None:
        writer = WALWriter(wal_path)
        writer.append(make_entry(seq=1, key=b"old"))
        writer.append(make_entry(seq=2, key=b"old2"))

        writer.truncate_before(1)

        writer.append(make_entry(seq=3, key=b"new"))

        entries = writer.replay()
        assert [e.seq for e in entries] == [2, 3]
        writer.close()


class TestWALWriterCloseReopen:
    """Tests for close and reopen."""

    def test_close_and_reopen(self, wal_path: Path) -> None:
        writer = WALWriter(wal_path)
        writer.append(make_entry(seq=1, key=b"persist"))
        writer.close()

        writer2 = WALWriter(wal_path)
        entries = writer2.replay()
        assert len(entries) == 1
        assert entries[0].key == b"persist"
        writer2.close()

    def test_append_across_sessions(self, wal_path: Path) -> None:
        writer = WALWriter(wal_path)
        writer.append(make_entry(seq=1))
        writer.close()

        writer2 = WALWriter(wal_path)
        writer2.append(make_entry(seq=2))
        writer2.close()

        writer3 = WALWriter(wal_path)
        entries = writer3.replay()
        assert [e.seq for e in entries] == [1, 2]
        writer3.close()
