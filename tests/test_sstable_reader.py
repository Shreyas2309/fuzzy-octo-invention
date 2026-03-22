"""Tests for app.sstable.reader — SSTableReader."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.cache.block import BlockCache
from app.sstable.reader import SSTableReader
from app.sstable.writer import SSTableWriter


@pytest.fixture()
async def populated_sst(tmp_path: Path) -> tuple[Path, str]:
    """Write a small SSTable and return (directory, file_id)."""
    sst_dir = tmp_path / "sst" / "L0" / "reader_test"
    file_id = "reader_test"
    writer = SSTableWriter(
        directory=sst_dir,
        file_id=file_id,
        snapshot_id="snap_r",
        level=0,
    )
    for i in range(10):
        key = f"key_{i:04d}".encode()
        val = f"val_{i}".encode()
        writer.put(key, i + 1, 1000 + i, val)

    await writer.finish()
    return sst_dir, file_id


class TestSSTableReader:
    async def test_open_and_get_existing(
        self, populated_sst: tuple[Path, str],
    ) -> None:
        sst_dir, file_id = populated_sst
        reader = await SSTableReader.open(sst_dir, file_id)

        result = reader.get(b"key_0005")
        assert result is not None
        seq, ts, val = result
        assert seq == 6
        assert val == b"val_5"

        reader.close()

    async def test_get_missing_key(
        self, populated_sst: tuple[Path, str],
    ) -> None:
        sst_dir, file_id = populated_sst
        reader = await SSTableReader.open(sst_dir, file_id)

        result = reader.get(b"nonexistent_key")
        assert result is None

        reader.close()

    async def test_bloom_skips_missing(
        self, populated_sst: tuple[Path, str],
    ) -> None:
        sst_dir, file_id = populated_sst
        reader = await SSTableReader.open(sst_dir, file_id)

        # A key the bloom filter should reject
        result = reader.get(b"zzz_definitely_not_in_sst_xyz_123")
        assert result is None

        reader.close()

    async def test_get_first_and_last_key(
        self, populated_sst: tuple[Path, str],
    ) -> None:
        sst_dir, file_id = populated_sst
        reader = await SSTableReader.open(sst_dir, file_id)

        first = reader.get(b"key_0000")
        assert first is not None
        assert first[2] == b"val_0"

        last = reader.get(b"key_0009")
        assert last is not None
        assert last[2] == b"val_9"

        reader.close()

    async def test_with_cache(
        self, populated_sst: tuple[Path, str],
    ) -> None:
        sst_dir, file_id = populated_sst
        cache = BlockCache(data_maxsize=32)
        reader = await SSTableReader.open(sst_dir, file_id, cache=cache)

        result = reader.get(b"key_0003")
        assert result is not None
        assert result[2] == b"val_3"

        reader.close()
