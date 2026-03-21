"""Shared test fixtures for lsm-kv."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from app.engine.config import LSMConfig
from app.observability.logging import reset_logging
from app.types import TOMBSTONE, OpType
from app.wal.writer import WALEntry


@pytest.fixture(autouse=True)
def _disable_tcp_logging_in_tests(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Disable TCP log server in tests to avoid port conflicts."""
    monkeypatch.setenv("LSM_LOG_PORT", "0")
    yield
    reset_logging()


@pytest.fixture()
def test_config(tmp_path: Path) -> LSMConfig:
    """Return an LSMConfig backed by a temp directory."""
    return LSMConfig.load(tmp_path / "data")


@pytest.fixture()
def wal_path(tmp_path: Path) -> Path:
    """Return a temporary WAL file path (not yet created)."""
    return tmp_path / "wal" / "wal.log"


def make_entry(
    seq: int,
    key: bytes = b"k",
    value: bytes = b"v",
    timestamp_ms: int = 1000,
    op: OpType = OpType.PUT,
) -> WALEntry:
    """Factory for WALEntry with sensible defaults."""
    return WALEntry(seq=seq, timestamp_ms=timestamp_ms, op=op, key=key, value=value)


def make_tombstone(seq: int, key: bytes = b"k", timestamp_ms: int = 1000) -> WALEntry:
    """Factory for a tombstone WALEntry."""
    return WALEntry(
        seq=seq, timestamp_ms=timestamp_ms, op=OpType.DELETE, key=key, value=TOMBSTONE,
    )
