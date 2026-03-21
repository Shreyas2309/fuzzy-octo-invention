"""Tests for app.engine.flush_pipeline — FlushPipeline."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from app.cache.block import BlockCache
from app.engine.config import LSMConfig
from app.engine.flush_pipeline import FlushPipeline
from app.engine.memtable_manager import MemTableManager
from app.engine.sstable_manager import SSTableManager
from app.engine.wal_manager import WALManager


@pytest.fixture()
def data_root(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture()
def config(data_root: Path) -> LSMConfig:
    cfg = LSMConfig.load(data_root)
    # Freeze after just 2 entries for easy testing
    cfg.set("max_memtable_entries", 2)
    return cfg


@pytest.fixture()
def wal_mgr(data_root: Path) -> WALManager:
    return WALManager.open(data_root / "wal" / "wal.log")


class TestFlushPipeline:
    async def test_single_snapshot_flush(
        self, data_root: Path, config: LSMConfig, wal_mgr: WALManager,
    ) -> None:
        mem = MemTableManager(config)
        sst = await SSTableManager.load(data_root)

        pipeline = FlushPipeline(mem=mem, sst=sst, wal=wal_mgr, max_workers=1)
        task = asyncio.create_task(pipeline.run())

        # Write entries and freeze
        mem.put(b"k1", 1, 100, b"v1")
        mem.put(b"k2", 2, 200, b"v2")
        with mem.write_lock:
            mem.maybe_freeze()

        # Wait for pipeline to process
        await asyncio.sleep(1.0)

        assert mem.queue_len() == 0
        assert sst.l0_count == 1

        result = await sst.get(b"k1")
        assert result is not None
        assert result[2] == b"v1"

        pipeline.stop()
        await asyncio.wait_for(task, timeout=5.0)
        sst.close_all()

    async def test_multiple_snapshots_ordered(
        self, data_root: Path, config: LSMConfig, wal_mgr: WALManager,
    ) -> None:
        mem = MemTableManager(config)
        sst = await SSTableManager.load(data_root)

        pipeline = FlushPipeline(mem=mem, sst=sst, wal=wal_mgr, max_workers=2)
        task = asyncio.create_task(pipeline.run())

        # Create multiple freezes
        mem.put(b"a1", 1, 100, b"v1")
        mem.put(b"a2", 2, 200, b"v2")
        with mem.write_lock:
            mem.maybe_freeze()

        mem.put(b"b1", 3, 300, b"v3")
        mem.put(b"b2", 4, 400, b"v4")
        with mem.write_lock:
            mem.maybe_freeze()

        # Wait for pipeline to process both
        await asyncio.sleep(2.0)

        assert mem.queue_len() == 0
        assert sst.l0_count == 2

        assert (await sst.get(b"a1")) is not None
        assert (await sst.get(b"b1")) is not None

        pipeline.stop()
        await asyncio.wait_for(task, timeout=5.0)
        sst.close_all()

    async def test_pipeline_stops_cleanly(
        self, data_root: Path, config: LSMConfig, wal_mgr: WALManager,
    ) -> None:
        mem = MemTableManager(config)
        sst = await SSTableManager.load(data_root)

        pipeline = FlushPipeline(mem=mem, sst=sst, wal=wal_mgr)
        task = asyncio.create_task(pipeline.run())

        await asyncio.sleep(0.2)
        pipeline.stop()
        await asyncio.wait_for(task, timeout=5.0)

        assert not pipeline.running
        sst.close_all()
