"""Exception hierarchy for lsm-kv.

All custom exceptions inherit from LSMError.
Import only from this module — never define exceptions in implementation files.
"""

from __future__ import annotations


class LSMError(Exception):
    """Base class for all lsm-kv exceptions."""


# ── WAL errors ───────────────────────────────────────────────────────────


class WALCorruptError(LSMError):
    """WAL replay encountered a malformed entry."""


class WALTruncateError(LSMError):
    """WAL truncation failed — file system error."""


# ── Engine errors ─────────────────────────────────────────────────────────


class EngineClosed(LSMError):
    """put() / get() / delete() called after LSMEngine.close()."""


# ── MemTable errors ───────────────────────────────────────────────────────


class MemTableFullError(LSMError):
    """ActiveMemTable exceeded MAX_MEMTABLE_SIZE."""


class SnapshotEmptyError(LSMError):
    """freeze() called on an empty ActiveMemTable."""


class SkipListInsertError(LSMError):
    """SkipList.put() failed after exhausting retries."""


class SkipListKeyError(LSMError):
    """Operation on a key that violates SkipList invariants (e.g., empty key)."""


class FreezeBackpressureTimeout(LSMError):
    """maybe_freeze() waited too long for queue space — flush worker may be stuck."""


class ImmutableTableAccessError(LSMError):
    """Mutation attempted on an ImmutableMemTable."""


class MemTableRestoreError(LSMError):
    """WAL replay into MemTable failed during recovery."""


# ── SSTable / encoding errors ────────────────────────────────────────────


class CorruptRecordError(LSMError):
    """Data-block record failed CRC or structural validation."""


class SSTableWriteError(LSMError):
    """SSTable write failed (disk I/O, ordering violation, etc.)."""


class SSTableReadError(LSMError):
    """SSTable read or open failed."""


class FlushError(LSMError):
    """Flush pipeline encountered an unrecoverable error."""
