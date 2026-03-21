"""Tests for LSMEngine — end-to-end put/get/delete with WAL recovery."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.common.errors import EngineClosed
from app.engine import LSMEngine


@pytest.fixture()
def data_root(tmp_path: Path) -> Path:
    """Return a temporary data root directory."""
    return tmp_path / "data"


class TestPutGetDelete:
    """Basic put/get/delete operations."""

    async def test_put_then_get(self, data_root: Path) -> None:
        engine = await LSMEngine.open(data_root)
        await engine.put(b"name", b"alice")

        assert await engine.get(b"name") == b"alice"
        await engine.close()

    async def test_get_missing_key(self, data_root: Path) -> None:
        engine = await LSMEngine.open(data_root)

        assert await engine.get(b"nope") is None
        await engine.close()

    async def test_put_overwrite(self, data_root: Path) -> None:
        engine = await LSMEngine.open(data_root)
        await engine.put(b"name", b"alice")
        await engine.put(b"name", b"bob")

        assert await engine.get(b"name") == b"bob"
        await engine.close()

    async def test_delete(self, data_root: Path) -> None:
        engine = await LSMEngine.open(data_root)
        await engine.put(b"name", b"alice")
        await engine.delete(b"name")

        assert await engine.get(b"name") is None
        await engine.close()

    async def test_delete_nonexistent_key(self, data_root: Path) -> None:
        engine = await LSMEngine.open(data_root)
        await engine.delete(b"ghost")

        assert await engine.get(b"ghost") is None
        await engine.close()

    async def test_multiple_keys(self, data_root: Path) -> None:
        engine = await LSMEngine.open(data_root)
        await engine.put(b"k1", b"v1")
        await engine.put(b"k2", b"v2")
        await engine.put(b"k3", b"v3")

        assert await engine.get(b"k1") == b"v1"
        assert await engine.get(b"k2") == b"v2"
        assert await engine.get(b"k3") == b"v3"
        await engine.close()


class TestRecovery:
    """WAL replay restores state after close + reopen."""

    async def test_recovery_after_close(self, data_root: Path) -> None:
        engine = await LSMEngine.open(data_root)
        await engine.put(b"city", b"paris")
        await engine.put(b"country", b"france")
        await engine.close()

        # Reopen — should recover from WAL
        engine2 = await LSMEngine.open(data_root)
        assert await engine2.get(b"city") == b"paris"
        assert await engine2.get(b"country") == b"france"
        await engine2.close()

    async def test_recovery_with_deletes(self, data_root: Path) -> None:
        engine = await LSMEngine.open(data_root)
        await engine.put(b"a", b"1")
        await engine.put(b"b", b"2")
        await engine.delete(b"a")
        await engine.close()

        engine2 = await LSMEngine.open(data_root)
        assert await engine2.get(b"a") is None
        assert await engine2.get(b"b") == b"2"
        await engine2.close()

    async def test_recovery_with_overwrites(self, data_root: Path) -> None:
        engine = await LSMEngine.open(data_root)
        await engine.put(b"x", b"old")
        await engine.put(b"x", b"new")
        await engine.close()

        engine2 = await LSMEngine.open(data_root)
        assert await engine2.get(b"x") == b"new"
        await engine2.close()

    async def test_recovery_seq_monotonic(self, data_root: Path) -> None:
        engine = await LSMEngine.open(data_root)
        await engine.put(b"a", b"1")
        await engine.put(b"b", b"2")
        seq_before = engine._seq.current
        await engine.close()

        engine2 = await LSMEngine.open(data_root)
        assert engine2._seq.current == seq_before

        # New writes get higher seq
        await engine2.put(b"c", b"3")
        assert engine2._seq.current > seq_before
        await engine2.close()


class TestStats:
    """Engine stats reporting."""

    async def test_stats_empty(self, data_root: Path) -> None:
        engine = await LSMEngine.open(data_root)
        s = engine.stats()
        assert s.key_count == 0
        assert s.seq == 0
        await engine.close()

    async def test_stats_after_writes(self, data_root: Path) -> None:
        engine = await LSMEngine.open(data_root)
        await engine.put(b"a", b"1")
        await engine.put(b"b", b"2")
        await engine.delete(b"a")

        s = engine.stats()
        assert s.key_count == 3  # counts all entries including tombstone
        assert s.seq == 3
        assert s.wal_entry_count == 3
        await engine.close()


class TestEngineClosed:
    """Operations on a closed engine raise EngineClosed."""

    async def test_put_after_close(self, data_root: Path) -> None:
        engine = await LSMEngine.open(data_root)
        await engine.close()

        with pytest.raises(EngineClosed):
            await engine.put(b"k", b"v")

    async def test_get_after_close(self, data_root: Path) -> None:
        engine = await LSMEngine.open(data_root)
        await engine.close()

        with pytest.raises(EngineClosed):
            await engine.get(b"k")

    async def test_delete_after_close(self, data_root: Path) -> None:
        engine = await LSMEngine.open(data_root)
        await engine.close()

        with pytest.raises(EngineClosed):
            await engine.delete(b"k")

    async def test_close_is_idempotent(self, data_root: Path) -> None:
        engine = await LSMEngine.open(data_root)
        await engine.close()
        await engine.close()  # Should not raise
