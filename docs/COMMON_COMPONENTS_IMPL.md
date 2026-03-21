# Common Components — Implementation Specification
> Reusable infrastructure components abstracted from business logic.
> These have zero knowledge of LSM semantics — they are general-purpose primitives.

---

## 0. Implementation order

```
Phase 1 → app/common/errors.py           (exception hierarchy)
Phase 2 → app/common/crc.py              (CRC32 helper)
Phase 3 → app/common/encoding.py         (record encode / decode_from)
Phase 4 → app/bloom/filter.py            (BloomFilter)
Phase 5 → app/index/sparse.py            (SparseIndex)
Phase 6 → app/cache/block.py             (BlockCache)
Phase 7 → app/sstable/registry.py        (SSTableRegistry)
Phase 8 → app/common/config.py           (all tunables in one place)
```

Each phase is independently testable and has no dependency on any later phase.

---

## 1. Exception hierarchy — `app/common/errors.py`

One file, one class hierarchy. Every custom exception in the project is defined here. No business logic imports scattered exception definitions from implementation files.

```python
# app/common/errors.py

class LSMError(Exception):
    """Base class for all lsm-kv exceptions."""

# ── Wire / encoding errors ────────────────────────────────────────────────

class CorruptRecordError(LSMError):
    """CRC32 mismatch on a data record — data.bin is corrupt."""

class CorruptSSTableError(LSMError):
    """SSTable footer magic mismatch or structurally invalid file."""

class OutOfOrderKeyError(LSMError):
    """SSTableWriter received a key that is not strictly greater than the previous."""

class WriterSealedError(LSMError):
    """put() or finish() called on an SSTableWriter after finish() completed."""

class ReaderClosedError(LSMError):
    """get() or iter_range() called on a closed SSTableReader."""

# ── MemTable errors ───────────────────────────────────────────────────────

class MemTableFullError(LSMError):
    """ActiveMemTable has exceeded MAX_MEMTABLE_SIZE — should not escape MemTableManager."""

class SnapshotEmptyError(LSMError):
    """freeze() called on an empty ActiveMemTable."""

# ── Engine errors ─────────────────────────────────────────────────────────

class EngineClosed(LSMError):
    """put() / get() / delete() called after LSMEngine.close()."""

class WALCorruptError(LSMError):
    """WAL replay encountered a malformed entry."""

class WALTruncateError(LSMError):
    """WAL truncation failed — file system error."""

# ── Recovery errors ───────────────────────────────────────────────────────

class IncompleteSSTableError(LSMError):
    """SSTable directory exists but meta.json is missing — incomplete write."""
    def __init__(self, file_id: str):
        super().__init__(f"Incomplete SSTable: {file_id} (no meta.json)")
        self.file_id = file_id
```

**Rules:**
- Import only from `app/common/errors.py` — never catch or raise raw `Exception` in business logic
- All exceptions carry enough context in their message to identify the source without a stack trace
- Never use `except Exception` in business logic — always be specific

---

## 2. CRC32 helper — `app/common/crc.py`

A tiny module so CRC computation is never duplicated. The algorithm and parameters are fixed in one place.

```python
# app/common/crc.py
from __future__ import annotations

import binascii
import struct
from typing import Final

_CRC_STRUCT: Final = struct.Struct(">I")    # big-endian u32
CRC_SIZE:    Final[int] = _CRC_STRUCT.size  # 4 bytes


def compute(data: bytes | bytearray | memoryview) -> int:
    """
    Compute CRC32 over `data`.
    Returns an unsigned 32-bit integer.
    Mask with 0xFFFFFFFF for Python's signed crc32 result.
    """
    return binascii.crc32(data) & 0xFFFF_FFFF


def pack(crc: int) -> bytes:
    """Pack CRC as 4-byte big-endian bytes."""
    return _CRC_STRUCT.pack(crc)


def unpack(data: bytes | memoryview, offset: int = 0) -> int:
    """Unpack CRC from 4 bytes at `offset`."""
    (value,) = _CRC_STRUCT.unpack_from(data, offset)
    return value


def verify(data: bytes | memoryview, expected_crc: int) -> bool:
    """Return True if CRC of `data` matches `expected_crc`."""
    return compute(data) == expected_crc
```

**Usage everywhere:**
```python
from app.common.crc import compute, pack, unpack, verify, CRC_SIZE
```

---

## 3. Record encoding — `app/common/encoding.py`

All KV record encoding and decoding lives here. `SSTableWriter`, `WALWriter`, and any future component that reads `data.bin` must import from this module — never duplicate `struct.pack` calls.

### 3a. Struct definitions

```python
# app/common/encoding.py
from __future__ import annotations

import struct
from typing import Final, NamedTuple

from app.common.crc import compute, pack as crc_pack, unpack as crc_unpack, CRC_SIZE, verify
from app.common.errors import CorruptRecordError
from app.types import Key, Value, SeqNum, Offset

# Record header: key_len(4) + val_len(4) + seq(8) + timestamp_ms(8) = 24 bytes
_RECORD_HEADER: Final = struct.Struct(">IIqq")
RECORD_HEADER_SIZE: Final[int] = _RECORD_HEADER.size   # 24
RECORD_FOOTER_SIZE: Final[int] = CRC_SIZE              # 4
RECORD_OVERHEAD:    Final[int] = RECORD_HEADER_SIZE + RECORD_FOOTER_SIZE  # 28

# Index entry header: key_len(4) + block_offset(8) = 12 bytes
_IDX_HEADER: Final = struct.Struct(">IQ")
IDX_HEADER_SIZE: Final[int] = _IDX_HEADER.size         # 12
```

### 3b. `encode_record(key, seq, timestamp_ms, value) -> bytes`

```python
def encode_record(
    key:          Key,
    seq:          SeqNum,
    timestamp_ms: int,
    value:        Value,
) -> bytes:
    header = _RECORD_HEADER.pack(len(key), len(value), seq, timestamp_ms)
    body   = key + value
    crc    = crc_pack(compute(header + body))
    return header + body + crc
```

### 3c. `DecodedRecord` — NamedTuple returned by `decode_from`

```python
class DecodedRecord(NamedTuple):
    key:          Key
    seq:          SeqNum
    timestamp_ms: int
    value:        Value
    next_offset:  Offset   # caller advances cursor here
```

### 3d. `decode_from(mv, offset) -> DecodedRecord`

```python
def decode_from(mv: memoryview, offset: Offset) -> DecodedRecord:
    end_header = offset + RECORD_HEADER_SIZE
    if end_header > len(mv):
        raise CorruptRecordError(
            f"Truncated header at offset {offset}: "
            f"need {RECORD_HEADER_SIZE}B, have {len(mv) - offset}B"
        )

    key_len, val_len, seq, timestamp_ms = _RECORD_HEADER.unpack_from(mv, offset)

    body_start = end_header
    key_end    = body_start + key_len
    val_end    = key_end    + val_len
    crc_end    = val_end    + CRC_SIZE

    if crc_end > len(mv):
        raise CorruptRecordError(
            f"Truncated body at offset {offset}: "
            f"need {crc_end}B total, have {len(mv)}B"
        )

    # Verify CRC — covers header + body
    expected_crc = crc_unpack(mv, val_end)
    if not verify(mv[offset:val_end], expected_crc):
        raise CorruptRecordError(
            f"CRC mismatch at offset {offset}: "
            f"stored={expected_crc}, computed={compute(mv[offset:val_end])}"
        )

    key   = bytes(mv[body_start : key_end])
    value = bytes(mv[key_end    : val_end])

    return DecodedRecord(
        key=key, seq=seq, timestamp_ms=timestamp_ms,
        value=value, next_offset=crc_end,
    )
```

### 3e. `iter_block(mv, block_offset, block_end) -> Iterator[DecodedRecord]`

```python
def iter_block(
    mv:           memoryview,
    block_offset: Offset,
    block_end:    Offset,
) -> Iterator[DecodedRecord]:
    cursor = block_offset
    while cursor < block_end:
        record = decode_from(mv, cursor)
        yield record
        cursor = record.next_offset
```

### 3f. Index entry encoding (for `SparseIndex`)

```python
def encode_index_entry(key: Key, block_offset: Offset) -> bytes:
    header = _IDX_HEADER.pack(len(key), block_offset)
    return header + key


def decode_index_entries(data: bytes) -> list[tuple[Key, Offset]]:
    entries = []
    pos = 0
    while pos < len(data):
        key_len, block_offset = _IDX_HEADER.unpack_from(data, pos)
        pos += IDX_HEADER_SIZE
        key  = data[pos : pos + key_len]
        pos += key_len
        entries.append((key, block_offset))
    return entries
```

---

## 4. Bloom filter — `app/bloom/filter.py`

### 4a. Purpose

Probabilistic membership test. Every `SSTableReader` holds one. `may_contain(key)` returning `False` prevents any disk access for that file.

### 4b. Implementation

```python
# app/bloom/filter.py
from __future__ import annotations

import ctypes
import math
import struct
from typing import Final

import mmh3

from app.types import Key, BloomFilterProtocol

# Filter file header (16 bytes, ctypes, _pack_ = 1)
class _BloomHeader(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("magic",      ctypes.c_uint32),    # 4B — 0xBF00BF00
        ("num_bits",   ctypes.c_uint32),    # 4B
        ("num_hashes", ctypes.c_uint8),     # 1B — k
        ("_pad",       ctypes.c_uint8 * 7), # 7B
    ]

BLOOM_MAGIC:       Final[int] = 0xBF00BF00
BLOOM_HEADER_SIZE: Final[int] = ctypes.sizeof(_BloomHeader)  # 16


class BloomFilter:
    """
    mmh3-backed Bloom filter.
    Serialisable to / from bytes for persistence in filter.bin.
    """

    def __init__(self, n: int = 1_000_000, fpr: float = 0.01) -> None:
        """
        n   — expected number of elements
        fpr — target false positive rate (default 1%)
        """
        self.fpr       = fpr
        self._n        = n
        self._m        = self._optimal_bits(n, fpr)      # bit array size
        self._k        = self._optimal_hashes(self._m, n) # number of hash functions
        self._bits     = bytearray(math.ceil(self._m / 8))

    # ── core operations ──────────────────────────────────────────────────

    def add(self, key: Key) -> None:
        for seed in range(self._k):
            bit = mmh3.hash(key, seed, signed=False) % self._m
            self._bits[bit >> 3] |= 1 << (bit & 7)

    def may_contain(self, key: Key) -> bool:
        for seed in range(self._k):
            bit = mmh3.hash(key, seed, signed=False) % self._m
            if not (self._bits[bit >> 3] & (1 << (bit & 7))):
                return False
        return True

    # ── serialisation ────────────────────────────────────────────────────

    def to_bytes(self) -> bytes:
        header = _BloomHeader(
            magic      = BLOOM_MAGIC,
            num_bits   = self._m,
            num_hashes = self._k,
        )
        return bytes(header) + bytes(self._bits)

    @classmethod
    def from_bytes(cls, data: bytes) -> BloomFilter:
        if len(data) < BLOOM_HEADER_SIZE:
            raise ValueError(f"Bloom filter data too short: {len(data)}B")
        header = _BloomHeader.from_buffer_copy(data[:BLOOM_HEADER_SIZE])
        if header.magic != BLOOM_MAGIC:
            raise ValueError(f"Bad Bloom magic: {header.magic:#010x}")

        bf        = cls.__new__(cls)
        bf.fpr    = 0.01   # not stored in header — use default
        bf._n     = 0      # not stored — only needed at construction
        bf._m     = header.num_bits
        bf._k     = header.num_hashes
        bf._bits  = bytearray(data[BLOOM_HEADER_SIZE:])
        return bf

    # ── optimal parameter calculation ─────────────────────────────────

    @staticmethod
    def _optimal_bits(n: int, fpr: float) -> int:
        return max(1, int(-n * math.log(fpr) / (math.log(2) ** 2)))

    @staticmethod
    def _optimal_hashes(m: int, n: int) -> int:
        return max(1, round(m / n * math.log(2)))
```

**Key facts:**
- `_pack_ = 1` on `_BloomHeader` is mandatory — without it, ctypes pads the struct and `from_buffer_copy` reads wrong bytes
- Uses `mmh3` (MurmurHash3) — fast, low collision, already in dependencies
- `from_bytes` uses `from_buffer_copy` (not `from_buffer`) — safe because we don't need to share memory with the file

---

## 5. Sparse index — `app/index/sparse.py`

### 5a. Purpose

Maps `first_key_of_block → block_offset`. Loaded fully into memory at `SSTableReader` open time. Provides O(log n) floor and ceil lookups for the read path.

### 5b. Implementation

```python
# app/index/sparse.py
from __future__ import annotations

import bisect
from typing import Final

from app.common.encoding import encode_index_entry, decode_index_entries
from app.types import Key, Offset


class SparseIndex:
    """
    In-memory sorted index: first_key_of_block → byte offset in data.bin.
    One entry per data block. Supports floor (bisect_right) and ceil (bisect_left) lookups.
    """

    def __init__(self) -> None:
        self._keys:    list[Key]    = []
        self._offsets: list[Offset] = []

    # ── mutation ──────────────────────────────────────────────────────────

    def add(self, first_key: Key, block_offset: Offset) -> None:
        """Append in sorted order — called by SSTableWriter at each block boundary."""
        self._keys.append(first_key)
        self._offsets.append(block_offset)

    # ── lookups ───────────────────────────────────────────────────────────

    def floor_offset(self, key: Key) -> Offset | None:
        """
        Returns the offset of the block whose first_key is the largest key <= `key`.
        This is the block that *might* contain `key`.
        Returns None if `key` is smaller than all index entries.
        """
        i = bisect.bisect_right(self._keys, key)
        if i == 0:
            return None
        return self._offsets[i - 1]

    def ceil_offset(self, key: Key) -> Offset | None:
        """
        Returns the offset of the block whose first_key is the smallest key >= `key`.
        Used by iter_range() to find the starting block.
        Returns None if `key` is larger than all index entries.
        """
        i = bisect.bisect_left(self._keys, key)
        if i >= len(self._keys):
            return None
        return self._offsets[i]

    def next_block_offset(self, current_offset: Offset) -> Offset | None:
        """
        Given the offset of the current block, return the offset of the next block.
        Used by iter_range() to walk blocks forward.
        Returns None if current_offset is the last block.
        """
        try:
            idx = self._offsets.index(current_offset)
        except ValueError:
            return None
        if idx + 1 >= len(self._offsets):
            return None
        return self._offsets[idx + 1]

    def last_offset(self) -> Offset | None:
        """Returns offset of the last block, or None if empty."""
        return self._offsets[-1] if self._offsets else None

    def __len__(self) -> int:
        return len(self._keys)

    # ── serialisation ─────────────────────────────────────────────────────

    def to_bytes(self) -> bytes:
        parts: list[bytes] = []
        for key, offset in zip(self._keys, self._offsets):
            parts.append(encode_index_entry(key, offset))
        return b"".join(parts)

    @classmethod
    def from_bytes(cls, data: bytes) -> SparseIndex:
        idx = cls()
        for key, offset in decode_index_entries(data):
            idx._keys.append(key)
            idx._offsets.append(offset)
        return idx
```

**Rules:**
- `_keys` and `_offsets` are parallel lists — same index → same entry. Never separate them.
- `add()` must be called in ascending key order — `SSTableWriter` guarantees this
- `bisect` operates on `_keys` directly — list must be sorted

---

## 6. Block cache — `app/cache/block.py`

### 6a. Purpose

Shared LRU cache for decoded data blocks. Keyed by `(file_id, block_offset)`. Thread-safe via `RLock`. One instance shared across all `SSTableReader` objects.

### 6b. Implementation

```python
# app/cache/block.py
from __future__ import annotations

import threading
from typing import Final

from cachetools import LRUCache
from cachetools.keys import hashkey

from app.types import FileID, Offset

_DEFAULT_MAXSIZE: Final[int] = 256   # max number of blocks in cache


class BlockCache:
    """
    Thread-safe LRU block cache.
    Keyed by (file_id, block_offset). Values are raw block bytes.

    Thread safety: RLock wraps every operation — cachetools.LRUCache is NOT
    thread-safe on its own (see github.com/tkem/cachetools#issue-105).
    """

    def __init__(self, maxsize: int = _DEFAULT_MAXSIZE) -> None:
        self._cache: LRUCache[tuple[str, int], bytes] = LRUCache(maxsize=maxsize)
        self._lock  = threading.RLock()

    def get(self, file_id: FileID, block_offset: Offset) -> bytes | None:
        with self._lock:
            return self._cache.get(hashkey(file_id, block_offset))

    def put(self, file_id: FileID, block_offset: Offset, block: bytes) -> None:
        with self._lock:
            self._cache[hashkey(file_id, block_offset)] = block

    def invalidate(self, file_id: FileID) -> None:
        """Evict all blocks belonging to `file_id`. Called before SSTable deletion."""
        with self._lock:
            keys_to_remove = [
                k for k in self._cache
                if k[0] == file_id
            ]
            for k in keys_to_remove:
                del self._cache[k]

    def invalidate_all(self, file_ids: list[FileID]) -> None:
        """Evict all blocks for a list of file IDs. Called after compaction."""
        with self._lock:
            keys_to_remove = [
                k for k in self._cache
                if k[0] in set(file_ids)
            ]
            for k in keys_to_remove:
                del self._cache[k]

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._cache)

    @property
    def maxsize(self) -> int:
        return self._cache.maxsize
```

**Rules:**
- `invalidate(file_id)` must be called before `shutil.rmtree(sstable_dir)` — stale blocks must be evicted before the file disappears
- `RLock` (reentrant) — same thread can acquire twice without deadlock, which is safer than `Lock` for complex call chains

---

## 7. SSTable registry — `app/sstable/registry.py`

### 7a. Purpose

Reference-counted registry of open `SSTableReader` instances. Ensures a reader is never closed while another thread is reading from it. A reader is only physically closed when its ref count reaches zero and it has been marked for deletion.

### 7b. Implementation

```python
# app/sstable/registry.py
from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING

from app.types import FileID

if TYPE_CHECKING:
    from app.sstable.reader import SSTableReader


class SSTableRegistry:
    """
    Ref-counted registry for open SSTableReader instances.

    - register(file_id, reader) — add a new reader
    - open_reader(file_id) — context manager: increments ref count on enter,
      decrements on exit; physically closes if pending deletion and ref == 0
    - mark_for_deletion(file_id) — schedule deletion; deferred until ref count == 0
    - all_file_ids() — iterate known file IDs
    """

    def __init__(self) -> None:
        self._readers:        dict[FileID, SSTableReader] = {}
        self._ref_counts:     dict[FileID, int]           = {}
        self._pending_delete: set[FileID]                 = set()
        self._lock            = threading.Lock()

    def register(self, file_id: FileID, reader: SSTableReader) -> None:
        with self._lock:
            self._readers[file_id]    = reader
            self._ref_counts[file_id] = 0

    @contextmanager
    def open_reader(self, file_id: FileID):
        with self._lock:
            if file_id not in self._readers:
                raise KeyError(f"Unknown SSTable: {file_id}")
            self._ref_counts[file_id] += 1
            reader = self._readers[file_id]
        try:
            yield reader
        finally:
            with self._lock:
                self._ref_counts[file_id] -= 1
                self._maybe_close(file_id)

    def mark_for_deletion(self, file_id: FileID) -> None:
        with self._lock:
            self._pending_delete.add(file_id)
            self._maybe_close(file_id)

    def _maybe_close(self, file_id: FileID) -> None:
        """Must be called under self._lock."""
        if (
            file_id in self._pending_delete
            and self._ref_counts.get(file_id, 0) == 0
        ):
            reader = self._readers.pop(file_id, None)
            self._ref_counts.pop(file_id, None)
            self._pending_delete.discard(file_id)
            if reader is not None:
                try:
                    reader.close()
                except Exception:
                    pass   # cleanup never raises

    def all_file_ids(self) -> list[FileID]:
        with self._lock:
            return list(self._readers.keys())

    def get(self, file_id: FileID) -> SSTableReader | None:
        with self._lock:
            return self._readers.get(file_id)

    def close_all(self) -> None:
        with self._lock:
            for file_id, reader in list(self._readers.items()):
                try:
                    reader.close()
                except Exception:
                    pass
            self._readers.clear()
            self._ref_counts.clear()
            self._pending_delete.clear()
```

**Rules:**
- `open_reader()` is a context manager — always use `with registry.open_reader(fid) as reader`
- `_maybe_close()` is only called under `_lock` — never directly
- `close_all()` does not raise — per guidelines §6b, cleanup never propagates exceptions

---

## 8. Config module — `app/common/config.py`

### 8a. Purpose

Single source of truth for all tunables. Per guidelines §13: every timeout, concurrency limit, and buffer size is configurable via environment variable with a sensible default. Never scatter `os.getenv()` calls across the codebase.

```python
# app/common/config.py
"""
All configurable tunables for lsm-kv.
Override any value via environment variable at startup.
"""
from __future__ import annotations

import os

# ── MemTable ──────────────────────────────────────────────────────────────

MAX_MEMTABLE_SIZE: int = int(os.getenv(
    "LSM_MAX_MEMTABLE_SIZE", str(64 * 1024 * 1024)   # 64MB
))

IMMUTABLE_QUEUE_MAX: int = int(os.getenv(
    "LSM_IMMUTABLE_QUEUE_MAX", "4"
))

# ── SSTable ───────────────────────────────────────────────────────────────

BLOCK_SIZE_DEFAULT: int = int(os.getenv(
    "LSM_BLOCK_SIZE", "4096"                           # 4KB
))

BLOOM_FPR: float = float(os.getenv(
    "LSM_BLOOM_FPR", "0.01"                            # 1% false positive rate
))

# ── Compaction ────────────────────────────────────────────────────────────

L0_COMPACTION_THRESHOLD: int = int(os.getenv(
    "LSM_L0_COMPACTION_THRESHOLD", "4"
))

COMPACTION_CHECK_INTERVAL: float = float(os.getenv(
    "LSM_COMPACTION_CHECK_INTERVAL", "0.5"             # seconds
))

# ── Concurrency ───────────────────────────────────────────────────────────

SSTABLE_OPEN_CONCURRENCY: int = int(os.getenv(
    "LSM_SSTABLE_OPEN_CONCURRENCY", "8"
))

L0_READ_CONCURRENCY: int = int(os.getenv(
    "LSM_L0_READ_CONCURRENCY", "4"                     # = L0_COMPACTION_THRESHOLD
))

FLUSH_WORKER_COUNT: int = int(os.getenv(
    "LSM_FLUSH_WORKER_COUNT", "1"                      # one flush at a time
))

# ── Cache ─────────────────────────────────────────────────────────────────

BLOCK_CACHE_MAXSIZE: int = int(os.getenv(
    "LSM_BLOCK_CACHE_MAXSIZE", "256"                   # max number of blocks
))

# ── WAL ───────────────────────────────────────────────────────────────────

WAL_SYNC_ON_APPEND: bool = os.getenv(
    "LSM_WAL_SYNC_ON_APPEND", "true"
).lower() == "true"

# ── Level size limits (bytes) ─────────────────────────────────────────────

LEVEL_MAX_SIZE: dict[int, int] = {
    0: L0_COMPACTION_THRESHOLD * MAX_MEMTABLE_SIZE,    # 256MB
    1: 4 * L0_COMPACTION_THRESHOLD * MAX_MEMTABLE_SIZE, # 1GB
    2: 10 * 4 * L0_COMPACTION_THRESHOLD * MAX_MEMTABLE_SIZE,  # 10GB
    3: 100 * 4 * L0_COMPACTION_THRESHOLD * MAX_MEMTABLE_SIZE, # 100GB
}
```

**Rules:**
- All values have defaults — nothing crashes at startup unless a value is genuinely required and missing
- All env var names are prefixed `LSM_` — no collision with other tools
- Import this module at the boundary (engine construction), pass typed values down — inner functions never call `os.getenv()` directly

---

## 9. Module `__init__.py` re-exports

Per rules.md §1: every subpackage exposes a public API via `__init__.py`. Callers import from the package, not the inner file.

### `app/common/__init__.py`
```python
from app.common.errors import (
    LSMError, CorruptRecordError, CorruptSSTableError,
    OutOfOrderKeyError, WriterSealedError, ReaderClosedError,
    MemTableFullError, SnapshotEmptyError, EngineClosed,
    WALCorruptError, WALTruncateError, IncompleteSSTableError,
)
from app.common.crc import compute as crc_compute, verify as crc_verify, CRC_SIZE
from app.common.encoding import (
    encode_record, decode_from, iter_block, DecodedRecord,
    RECORD_HEADER_SIZE, RECORD_FOOTER_SIZE, RECORD_OVERHEAD,
)

__all__ = [
    "LSMError", "CorruptRecordError", "CorruptSSTableError",
    "OutOfOrderKeyError", "WriterSealedError", "ReaderClosedError",
    "MemTableFullError", "SnapshotEmptyError", "EngineClosed",
    "WALCorruptError", "WALTruncateError", "IncompleteSSTableError",
    "crc_compute", "crc_verify", "CRC_SIZE",
    "encode_record", "decode_from", "iter_block", "DecodedRecord",
    "RECORD_HEADER_SIZE", "RECORD_FOOTER_SIZE", "RECORD_OVERHEAD",
]
```

### `app/bloom/__init__.py`
```python
from app.bloom.filter import BloomFilter
__all__ = ["BloomFilter"]
```

### `app/index/__init__.py`
```python
from app.index.sparse import SparseIndex
__all__ = ["SparseIndex"]
```

### `app/cache/__init__.py`
```python
from app.cache.block import BlockCache
__all__ = ["BlockCache"]
```

### `app/sstable/__init__.py`
```python
from app.sstable.meta import SSTableMeta
from app.sstable.registry import SSTableRegistry
__all__ = ["SSTableMeta", "SSTableRegistry"]
```

---

## 10. Test checklist (per phase)

### Phase 1 — errors.py tests
- [ ] All exception classes are subclasses of `LSMError`
- [ ] Each exception carries a meaningful message
- [ ] `IncompleteSSTableError` carries `file_id` as an attribute

### Phase 2 — crc.py tests
- [ ] `compute(data)` returns a u32 (0 <= result < 2^32)
- [ ] Same data always produces same CRC
- [ ] Single bit flip in data changes CRC
- [ ] `pack` + `unpack` round-trips correctly
- [ ] `verify(data, expected)` returns False for corrupted data

### Phase 3 — encoding.py tests
- [ ] `encode_record` + `decode_from` round-trip for arbitrary key/value
- [ ] `decode_from` raises `CorruptRecordError` on CRC mismatch
- [ ] `decode_from` raises `CorruptRecordError` on truncated header
- [ ] `decode_from` raises `CorruptRecordError` on truncated body
- [ ] `next_offset` arithmetic: `next_offset == offset + 28 + len(key) + len(value)`
- [ ] `iter_block` yields all records in a block in order
- [ ] `iter_block` handles a block with a single record
- [ ] `encode_index_entry` + `decode_index_entries` round-trip

### Phase 4 — BloomFilter tests
- [ ] `may_contain(key)` returns True for all added keys (no false negatives)
- [ ] False positive rate approximately matches target `fpr` over 10K random keys
- [ ] `to_bytes()` + `from_bytes()` round-trip: same results on all queries
- [ ] `_BloomHeader` has `_pack_ = 1` — `ctypes.sizeof` equals 16
- [ ] Bad magic in `from_bytes` raises `ValueError`
- [ ] Empty filter serialises and deserialises correctly

### Phase 5 — SparseIndex tests
- [ ] `add()` + `floor_offset(key)` returns correct offset for known key
- [ ] `floor_offset` returns None when key is below all entries
- [ ] `ceil_offset(key)` returns smallest first_key >= key
- [ ] `ceil_offset` returns None when key is above all entries
- [ ] `next_block_offset` returns correct next offset
- [ ] `next_block_offset` returns None for last block
- [ ] `to_bytes()` + `from_bytes()` round-trip with binary keys

### Phase 6 — BlockCache tests
- [ ] `put()` then `get()` returns same bytes
- [ ] `get()` returns None for unknown key
- [ ] LRU eviction: oldest block evicted when maxsize exceeded
- [ ] `invalidate(file_id)` removes all blocks for that file
- [ ] `invalidate_all([...])` removes blocks for all listed files
- [ ] Thread safety: 8 concurrent threads put/get without panic or corruption

### Phase 7 — SSTableRegistry tests
- [ ] `register()` then `open_reader()` yields the registered reader
- [ ] `open_reader()` increments ref count on enter, decrements on exit
- [ ] `mark_for_deletion()` closes reader immediately if ref count is 0
- [ ] `mark_for_deletion()` defers close until last reader context exits
- [ ] `close_all()` does not raise even if readers raise on close
- [ ] `open_reader()` for unknown file_id raises `KeyError`

### Phase 8 — config.py tests
- [ ] All values have defaults — no `KeyError` when no env vars are set
- [ ] `LSM_MAX_MEMTABLE_SIZE` env var overrides default
- [ ] `LEVEL_MAX_SIZE` computed values are consistent with base constants
