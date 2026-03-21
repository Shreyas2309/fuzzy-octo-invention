"""Tests for app.engine.sstable_manager — SSTableManager."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.cache.block import BlockCache
from app.engine.sstable_manager import SSTableManager
from app.memtable.immutable import ImmutableMemTable


@pytest.fixture()
def data_root(tmp_path: Path) -> Path:
    return tmp_path / "data"


def _make_snapshot(
    sid: str, entries: list[tuple[bytes, int, int, bytes]],
) -> ImmutableMemTable:
    return ImmutableMemTable(snapshot_id=sid, data=entries)


class TestSSTableManager:
    async def test_load_empty(self, data_root: Path) -> None:
        mgr = await SSTableManager.load(data_root)
        assert mgr.l0_count == 0
        assert mgr.max_seq_seen() == 0

    async def test_flush_and_get(self, data_root: Path) -> None:
        mgr = await SSTableManager.load(data_root)
        snapshot = _make_snapshot("s1", [
            (b"aaa", 1, 100, b"v1"),
            (b"bbb", 2, 200, b"v2"),
            (b"ccc", 3, 300, b"v3"),
        ])

        file_id = mgr.new_file_id()
        meta, reader = await mgr.flush(snapshot, file_id)
        sst_dir = data_root / "sstable" / "L0" / file_id
        mgr.commit(file_id, reader, sst_dir)

        assert mgr.l0_count == 1

        result = await mgr.get(b"bbb")
        assert result is not None
        seq, ts, val = result
        assert val == b"v2"
        assert seq == 2

    async def test_get_missing(self, data_root: Path) -> None:
        mgr = await SSTableManager.load(data_root)
        result = await mgr.get(b"nope")
        assert result is None

    async def test_load_at_startup(self, data_root: Path) -> None:
        """Flush, close, reload — data should survive."""
        mgr = await SSTableManager.load(data_root)
        snapshot = _make_snapshot("s2", [
            (b"key1", 10, 100, b"val1"),
            (b"key2", 11, 200, b"val2"),
        ])
        file_id = mgr.new_file_id()
        meta, reader = await mgr.flush(snapshot, file_id)
        sst_dir = data_root / "sstable" / "L0" / file_id
        mgr.commit(file_id, reader, sst_dir)
        mgr.close_all()

        # Reload
        mgr2 = await SSTableManager.load(data_root)
        assert mgr2.l0_count == 1
        assert mgr2.max_seq_seen() == 11

        result = await mgr2.get(b"key1")
        assert result is not None
        assert result[2] == b"val1"
        mgr2.close_all()

    async def test_multiple_flushes(self, data_root: Path) -> None:
        mgr = await SSTableManager.load(data_root)

        snap1 = _make_snapshot("s3", [
            (b"aaa", 1, 100, b"v1"),
        ])
        fid1 = mgr.new_file_id()
        m1, r1 = await mgr.flush(snap1, fid1)
        mgr.commit(fid1, r1, data_root / "sstable" / "L0" / fid1)

        snap2 = _make_snapshot("s4", [
            (b"bbb", 2, 200, b"v2"),
        ])
        fid2 = mgr.new_file_id()
        m2, r2 = await mgr.flush(snap2, fid2)
        mgr.commit(fid2, r2, data_root / "sstable" / "L0" / fid2)

        assert mgr.l0_count == 2
        assert (await mgr.get(b"aaa")) is not None
        assert (await mgr.get(b"bbb")) is not None
        mgr.close_all()
