from __future__ import annotations

import enum
import sys
from collections.abc import Iterator
from typing import Final, Protocol, Self, runtime_checkable

# ---------------------------------------------------------------------------
# Operation types — encoded in WAL entries for unambiguous replay
# ---------------------------------------------------------------------------


class OpType(enum.IntEnum):
    """Operation type stored in each WAL entry.

    Encoded as an integer for compact msgpack serialisation.
    Using IntEnum so the value is directly packable without conversion.
    """

    PUT = 1
    DELETE = 2

# ---------------------------------------------------------------------------
# Primitive type aliases (PEP 695, requires Python 3.12+)
# ---------------------------------------------------------------------------

type Key = bytes        # Raw byte key used in all read/write operations
type Value = bytes      # Raw byte value stored alongside a key
type SeqNum = int       # Monotonically increasing write sequence number
type Offset = int       # Byte offset within a file (SSTable, WAL, etc.)
type BlockSize = int    # Size of a data block in bytes
type FileID = str       # UUID or hex string identifying an SSTable file
type Level = int        # Compaction level (0 = freshest / most recent data)
type SnapshotID = str   # UUID4 hex — identifies one ImmutableMemTable snapshot
type TableID = str      # UUID4 hex — identifies one ActiveMemTable instance

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TOMBSTONE: Final[bytes] = b"\x00__tomb__\x00"
"""Sentinel value written in place of a value to mark a key as deleted."""

BLOCK_SIZE_DEFAULT: Final[int] = 4096
"""Default SSTable data-block size in bytes (4 KB)."""

MAX_MEMTABLE_SIZE: Final[int] = 67_108_864
"""Maximum memtable size in bytes (64 MB) before it is frozen and flushed."""

L0_COMPACTION_THRESHOLD: Final[int] = 10
"""Number of L0 SSTables that triggers a compaction run to L1."""

IMMUTABLE_QUEUE_MAX: Final[int] = 4
"""Maximum frozen memtable snapshots queued before backpressure kicks in."""

COMPACTION_CHECK_INTERVAL: Final[float] = 0.5
"""Seconds between compaction worker wake-ups to check L0 file count."""

def _detect_parallelism_mode() -> str:
    """
        Return 'free_threaded' when GIL is disabled (3.13+ no-GIL),
        else 'multiprocessing'.
    """
    try:
        # sys._is_gil_enabled() exists only on CPython free-threaded builds (3.13+)
        gil_on: bool = sys._is_gil_enabled()  # type: ignore[attr-defined]
        return "multiprocessing" if gil_on else "free_threaded"
    except AttributeError:
        # Python 3.12: GIL always present; use multiprocessing for true parallelism
        return "multiprocessing"


PARALLELISM_MODE: Final[str] = _detect_parallelism_mode()
"""
    Parallelism strategy: 'free_threaded' when GIL is disabled, 
    else 'multiprocessing'.
"""

# ---------------------------------------------------------------------------
# Protocols
# ---------------------------------------------------------------------------


@runtime_checkable
class BloomFilterProtocol(Protocol):
    """Structural interface for a probabilistic bloom-filter membership set."""

    def add(self, key: Key) -> None:
        """Insert *key* into the filter."""
        ...

    def may_contain(self, key: Key) -> bool:
        """Return True if *key* might be present (false positives are allowed)."""
        ...

    def to_bytes(self) -> bytes:
        """Serialize the filter to a compact byte string for persistence."""
        ...

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        """Deserialize and return a filter instance from *data*."""
        ...


@runtime_checkable
class KVIteratorProtocol(Protocol):
    """Structural interface for a sorted key-value cursor (e.g. SSTable, MemTable)."""

    def seek(self, key: Key) -> None:
        """Position the cursor at the first entry >= *key*."""
        ...

    def __iter__(self) -> Iterator[tuple[Key, Value]]:
        """Return the iterator object itself."""
        ...

    def __next__(self) -> tuple[Key, Value]:
        """Advance and return the next (key, value) pair, or raise StopIteration."""
        ...
