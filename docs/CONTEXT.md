# lsm-kv — Project Context & Design Decisions
> Source of truth for Claude Code sessions. Update as decisions evolve.

---

## 1. Project overview

| Field | Value |
|---|---|
| Name | lsm-kv |
| Type | Python library / portfolio project |
| Python | 3.12 (current target) |
| Package manager | uv |
| Layout | src-style — all code under `./app/` |
| Entry point | `main.py` (REPL via prompt_toolkit) |
| Publish target | PyPI |

---

## 2. Architecture — layered dependency graph

Strict bottom-up. No layer imports from a layer above it.

```
types.py
  └── wire/  (headers.py · footer.py · kv.py)
        ├── bloom/
        ├── index/
        ├── cache/
        ├── memtable/  (active.py · immutable.py)
        ├── wal/
        ├── sstable/  (writer · reader · meta)
        ├── compaction/  (task · leveled)
        └── engine/        ← manager sub-package
              ├── memtable_manager.py   (imports memtable/, wal/)
              ├── wal_manager.py        (imports wal/)
              └── sstable_manager.py    (imports sstable/, compaction/, cache/)
                    └── engine.py  ← imports engine/ managers only; never imports lower layers directly
```

---

## 3. Directory structure (`./app/`)

```
app/
├── __init__.py
├── engine.py              # LSMEngine — cross-cutting coordination only (put/get/delete/open/close/recover)
├── types.py               # Key, Value, SeqNum, Offset, TOMBSTONE, constants, Protocols
│
├── engine/                # manager sub-package — internal to engine layer
│   ├── __init__.py
│   ├── memtable_manager.py  # MemTableManager — owns _active, _immutable_q, _write_lock, _seq, backpressure
│   ├── sstable_manager.py   # SSTableManager  — owns _registry, _cache, _compactor, _flush_lock
│   └── wal_manager.py       # WALManager      — owns WALWriter, append/fsync/replay/truncate
│
├── wire/
│   ├── headers.py         # struct.Struct('>II') KV header + struct.Struct('>IQ') index entry
│   ├── footer.py          # ctypes _SSTableFooter (56B) + _BloomHeader (16B) + read_footer()
│   └── kv.py              # KVEntry dataclass + encode() / decode_from() / iter_block()
│
├── memtable/
│   ├── active.py          # ActiveMemTable — wraps SkipList, freeze() on size threshold
│   └── immutable.py       # ImmutableMemTable — frozen snapshot queue, awaits flush
│
├── wal/
│   └── writer.py          # WALWriter — append+fsync; WALEntry dataclass; msgpack; replay()
│
├── sstable/
│   ├── writer.py          # SSTableWriter — data blocks + sparse index + bloom + footer
│   ├── reader.py          # SSTableReader — mmap open, read_footer, floor/ceil, block cache
│   └── meta.py            # SSTableMeta dataclass + msgpack encode/decode
│
├── index/
│   └── sparse.py          # SparseIndex — bisect floor_offset()/ceil_offset(); to/from bytes
│
├── bloom/
│   └── filter.py          # BloomFilter — mmh3-backed; add/may_contain; to/from bytes
│
├── compaction/
│   ├── task.py            # CompactionTask dataclass + msgpack encode/decode
│   └── leveled.py         # LeveledCompactor — skeleton: level layout, manifest mgmt, trigger logic (merge impl. TBD)
│
└── cache/
    └── block.py           # BlockCache — cachetools LRU keyed (file_id, block_offset); RLock
```

---

## 4. Wire format decisions

### 4a. KV data blocks — mmap path (struct)

```
[ 4B key_len: u32 ][ 4B val_len: u32 ][ 8B seq: u64 ][ 8B timestamp_ms: u64 ][ key bytes ][ value bytes ][ 4B crc32: u32 ]
  ^──────────────── struct.Struct('>IIQq')  RECORD_HEADER_SIZE = 24 ────────^                             ^── 4B footer ──^
```

- Big-endian (`>`) — deterministic across architectures
- `decode_from(mv, offset)` operates on `memoryview` — zero copy
- `next_offset = offset + 24 + key_len + val_len + 4`  (header + body + CRC footer)
- CRC32 covers header + body bytes — verified on every read, raises `CorruptRecordError` on mismatch
- `seq` — authoritative ordering field for MVCC and compaction tombstone GC; always use `seq` for deduplication, never `timestamp_ms`
- `timestamp_ms` — Unix epoch milliseconds (`u64`) at write time; enables TTL, human-readable audit, and time-based queries; wall-clock only — not a tiebreaker
- `timestamp_ms` is set by the engine at `put()` time using `time.time_ns() // 1_000_000` — not by lower layers
- msgpack was rejected here: variable framing breaks O(1) seek within blocks

### 4b. Sparse index entries (struct)

```
[ 4B key_len: u32 ][ 8B block_offset: u64 ][ key bytes ]
  struct.Struct('>IQ')   IDX_ENTRY_HEADER_SIZE = 12
```

- Loaded fully into memory at `SSTableReader` open time
- `bisect_right` → floor block / `bisect_left` → ceil block

### 4c. Fixed ctypes overlays (`_pack_ = 1` mandatory)

| Struct | Size | Fields |
|---|---|---|
| `_RecordHeader` | 24B | key_len(4) + val_len(4) + seq(8) + timestamp_ms(8) — `struct.Struct('>IIQq')` |
| `_RecordFooter` | 4B | crc32(4) — `struct.Struct('>I')` |
| `_BloomHeader` | 16B | magic(4) + num_bits(4) + num_hashes(1) + _pad(7) |
| `_SSTableFooter` | 56B | magic(8) + data_end(8) + index_offset(8) + index_len(4) + bloom_offset(8) + bloom_len(4) + seq_min(8) + seq_max(8) |

> `index_offset` and `bloom_offset` in `_SSTableFooter` are reserved — index and filter are now separate sibling files (`index.bin`, `filter.bin`). `data_end` still marks where records end within `data.bin`.

- `_pack_ = 1` is **mandatory** — without it, platform padding silently corrupts mmap reads
- `from_buffer(mv[offset:])` — zero copy, struct shares memory with mmap
- Footer is always the last `FOOTER_SIZE` bytes — read it first to get all section offsets
- `FOOTER_MAGIC = int.from_bytes(b"LSMKV001", "big")`

### 4d. Streaming path (msgpack)

| Type | Encoding |
|---|---|
| `WALEntry` | `packb((seq, timestamp_ms, op, key, value))` — `op` is `OpType` IntEnum (1=PUT, 2=DELETE); `Unpacker` for streaming replay |
| `SSTableMeta` | `packb(asdict(dataclass))` |
| `CompactionTask` | partial serialise — exclude runtime-only fields (`done: bool`) — merge impl. out of scope |

- msgpack lives **only** in `wal/` and `sstable/meta.py` — never in the hot read path
- Always `use_bin_type=True` on pack, `raw=True` on unpack

---

## 5. Type system (`types.py`)

### Primitive aliases (PEP 695 `type` statement — Python 3.12+)

| Alias | Type | Purpose |
|---|---|---|
| `Key` | `bytes` | raw byte key |
| `Value` | `bytes` | raw byte value |
| `SeqNum` | `int` | monotonically increasing write sequence number (Python `int` is arbitrary-precision; on-disk wire format uses `u64` in SSTable headers, msgpack adaptive encoding in WAL) |
| `Offset` | `int` | byte offset within a file |
| `BlockSize` | `int` | data block size in bytes |
| `FileID` | `str` | UUID4 hex — identifies one SSTable file on disk |
| `SnapshotID` | `str` | UUID4 hex — identifies one `ImmutableMemTable` snapshot |
| `TableID` | `str` | UUID4 hex — identifies one `ActiveMemTable` instance |
| `Level` | `int` | compaction level (0 = freshest) |

### Constants

| Constant | Value | Purpose |
|---|---|---|
| `TOMBSTONE` | `b"\x00__tomb__\x00"` | deleted key sentinel |
| `BLOCK_SIZE_DEFAULT` | `4096` | 4 KB default block size |
| `MAX_MEMTABLE_SIZE` | `67108864` | 64 MB freeze threshold |
| `L0_COMPACTION_THRESHOLD` | `4` | L0 file count that triggers compaction |
| `IMMUTABLE_QUEUE_MAX` | `4` | max frozen memtable snapshots queued before backpressure |
| `COMPACTION_CHECK_INTERVAL` | `0.5` | seconds between compaction worker wake-ups |
| `PARALLELISM_MODE` | `"multiprocessing"` | fixed on 3.12 — `ProcessPoolExecutor` always used for compaction |

### Enums

| Enum | Values | Purpose |
|---|---|---|
| `OpType` | `PUT = 1`, `DELETE = 2` | Operation type stored in each WAL entry for unambiguous replay. `IntEnum` so values serialise directly as integers in msgpack. |

### Protocols (`@runtime_checkable`)

- `BloomFilterProtocol` — `add(key)` / `may_contain(key)` / `to_bytes()` / `from_bytes(data)` classmethod
- `KVIteratorProtocol` — `seek(key)` / `__iter__` / `__next__ → tuple[Key, Value]`

---

## 6. Parallelism strategy

> **Scope:** Python 3.12 only. 3.13t free-threaded mode (no-GIL + `ThreadPoolExecutor` for compaction) is out of scope for the current implementation — noted as a future upgrade path in §13b.

| Operation | Executor | Reason |
|---|---|---|
| Compaction (CPU-bound) | `ProcessPoolExecutor(max_workers=2)` | bypass GIL for merge-sort — executor chosen, merge impl. out of scope |
| MemTable flush (I/O-bound) | `asyncio` + `loop.run_in_executor` | offload blocking write, don't stall event loop |
| WAL writes | single thread, sequential | parallelising breaks durability guarantees |
| SSTable reads | `ThreadPoolExecutor` via `asyncio.gather` | I/O-bound fan-out — GIL is released during disk I/O syscalls, so threads run concurrently on 3.12 |

`PARALLELISM_MODE = "multiprocessing"` is a fixed constant on 3.12 — no runtime detection needed. `LeveledCompactor` always uses `ProcessPoolExecutor`.

---

## 7. Dependencies

### Runtime

| Package | Purpose |
|---|---|
| `mmh3` | MurmurHash3 for `BloomFilter` — fast, low collision |
| `msgpack` | WAL entries + SSTable metadata — streaming binary serialisation |
| `cachetools` | `LRUCache` for `BlockCache` — pure Python, thread-safe with `RLock` wrapper |
| `structlog` | Structured logging for compaction / flush events |
| `prompt_toolkit` | REPL shell — `lsm-kv>` prompt with history + arrow keys |

`sortedcontainers` removed — `ActiveMemTable` now backed by the custom `SkipList` in `app/memtable/skiplist.py`. No external sorted container dependency needed.

### Dev

| Package | Purpose |
|---|---|
| `pytest` | test runner |
| `pytest-cov` | coverage reports |
| `hypothesis` | property-based tests — Bloom filter FPR, round-trip encoding |
| `ruff` | lint + format (replaces flake8 + black + isort) |
| `mypy` | strict static type checking |
| `pre-commit` | ruff + mypy hooks on every commit |

---

## 8. Key design rules for Claude Code

### Wire format
- Nothing outside `wire/` ever manually packs or unpacks bytes
- `struct.Struct` objects are **module-level constants** — compiled once at import, not per call
- `decode_from()` always returns `DecodedKV(entry, next_offset)` as a `NamedTuple` — never a bare tuple
- All `ctypes.Structure` definitions **must** have `_pack_ = 1`
- msgpack: `use_bin_type=True` on pack, `raw=True` on unpack — always
- CRC32 is always computed with `binascii.crc32()` over `header_bytes + key + value`
- On CRC mismatch raise `CorruptRecordError` — never silently skip or return partial data
- `seq` is the **only** ordering field — never use `timestamp_ms` for deduplication or MVCC decisions
- `timestamp_ms` is always set by `LSMEngine` at `put()` time — lower layers accept it, never generate it
- `timestamp_ms` uses `time.time_ns() // 1_000_000` — set once per `put()` call and carried through WAL and SSTable unchanged

### On-disk layout
- Root data dir is configurable — never hardcoded; passed into `LSMEngine` at construction
- WAL file is always `<data_root>/wal/wal.log` — single file, never sharded
- Global registry is always `<data_root>/sstable/metadata.json`
- Level manifest is always `<data_root>/sstable/L<N>/manifest.json`
- Manifests are written atomically: write to `.tmp` then `os.replace()` — never partial writes
- Per-SSTable directory name is the `FileID` (UUID4 hex) — no other naming scheme
- `meta.json` keys and values are base64-encoded when they represent raw bytes
- `PARALLELISM_MODE` is a fixed string constant `"multiprocessing"` — no runtime detection on 3.12

### MemTable
- `ActiveMemTable` owns the freeze decision — not the engine
- `ImmutableMemTable` is read-only by contract — no locking needed for reads
- Flush is async — engine does not block writes while flush is in progress
- `freeze()` calls `skiplist.snapshot()` — point-in-time copy, no lock held during SSTable write

### SSTable
- `SSTableWriter` is write-once — `finish()` seals the file, writer is discarded
- `SSTableReader` holds one open mmap per file — `close()` releases fd and mmap
- Sparse index and Bloom filter are loaded into memory at open time, not per-read
- `BlockCache` is a single shared instance in `engine.py`, passed into each reader
- Compaction works with `SSTableMeta` until it needs to open files — cheap planning phase

### General
- `types.py` imports nothing from within the project — ever
- No layer imports from a layer above it
- `TOMBSTONE` comparison is always `value == TOMBSTONE` — never key-based
- `SeqNum` is always set by the engine — lower layers accept it, never generate it
- `FileID` is always a UUID4 hex string — generated at `SSTableWriter` construction
- `SnapshotID` is always a UUID4 hex string — generated at `ActiveMemTable.freeze()` and carried into `ImmutableMemTable` and then `SSTableMeta`
- `TableID` is always a UUID4 hex string — generated at `ActiveMemTable.__init__()`, replaced each freeze cycle
- The audit chain `TableID → SnapshotID → FileID` must be preserved end-to-end — no step may drop the ID

---

## 10. On-disk data layout

All persistent data lives under a single configurable **root data directory** (default: `./data/`).

```
data/                                  ← root data directory
├── wal/
│   └── wal.log                        ← single append-only WAL file (msgpack frames)
│
└── sstable/
    ├── metadata.json                  ← global SSTable registry (levels + file counts)
    │
    ├── L0/                            ← level directories  (L0, L1, L2, …)
    │   ├── manifest.json              ← level-scoped manifest (all SSTables on this level)
    │   │
    │   ├── <file_id>/                 ← one directory per SSTable (UUID4 hex name)
    │   │   ├── data.bin               ← KV records (length-prefixed, CRC-checked)
    │   │   ├── index.bin              ← sparse index (first_key → block_offset entries)
    │   │   ├── filter.bin             ← serialised Bloom filter bytes
    │   │   └── meta.json              ← per-SSTable metadata (see §11c)
    │   │
    │   └── <file_id>/
    │       └── …
    │
    ├── L1/
    │   ├── manifest.json
    │   └── …
    │
    └── L2/
        └── …
```

### 10a. `wal/wal.log`

- Single file, append-only, never randomly accessed
- Each entry is a msgpack-framed `WALEntry`: `(seq: int, timestamp_ms: int, op: int, key: bytes, value: bytes)` — `op` is `OpType` IntEnum (1=PUT, 2=DELETE)
- Replayed sequentially at startup to rebuild the active `MemTable`
- Truncated only after a successful MemTable flush to SSTable

### 10b. `sstable/metadata.json` — global registry

Written by the engine whenever levels change (new flush, compaction completes).

```json
{
  "version": 1,
  "levels": [
    { "level": 0, "sstable_count": 3, "max_sstables": 4 },
    { "level": 1, "sstable_count": 10, "max_sstables": 10 },
    { "level": 2, "sstable_count": 0,  "max_sstables": 100 }
  ],
  "updated_at": "<iso8601 timestamp>"
}
```

### 10c. `L<N>/manifest.json` — level manifest

One per level directory. Updated atomically (write to `.tmp`, rename) on every flush or compaction that touches this level.

```json
{
  "level": 0,
  "sstables": [
    {
      "file_id": "<uuid4-hex>",
      "min_key": "<base64-encoded bytes>",
      "max_key": "<base64-encoded bytes>",
      "seq_min": 0,
      "seq_max": 419,
      "size_bytes": 4194304,
      "record_count": 51200,
      "created_at": "<iso8601 timestamp>"
    }
  ]
}
```

- `min_key` / `max_key` are base64-encoded because keys are raw bytes (not valid JSON strings)
- Used by the engine for key-range overlap checks during compaction planning — no files opened
- Written atomically: `manifest.tmp` → `manifest.json` via `os.replace()`

### 10d. Per-SSTable files

| File | Format | Purpose |
|---|---|---|
| `data.bin` | binary, length-prefixed + CRC | KV records in sorted key order |
| `index.bin` | binary, struct-packed | sparse index: `(key_len u32, block_offset u64, key bytes)` per entry |
| `filter.bin` | binary, custom header + bit array | serialised Bloom filter |
| `meta.json` | JSON | per-SSTable metadata (see §11c) |

---

## 11. Record and file models

### 11a. KV record layout in `data.bin`

Every KV record has a **header**, a **body**, and a **footer** (CRC check).

```
┌──────────────────────────────────────────────────────────────────────────┐
│  RECORD HEADER  (24 bytes, fixed)                                        │
│  [ 4B key_len: u32 ][ 4B val_len: u32 ]                                 │
│  [ 8B seq: u64 ][ 8B timestamp_ms: u64 ]                                │
├──────────────────────────────────────────────────────────────────────────┤
│  RECORD BODY  (key_len + val_len bytes, variable)                        │
│  [ key bytes ][ value bytes ]                                            │
├──────────────────────────────────────────────────────────────────────────┤
│  RECORD FOOTER  (4 bytes, fixed)                                         │
│  [ 4B crc32: u32 ]  ← CRC of header + body bytes                        │
└──────────────────────────────────────────────────────────────────────────┘
  Total = 24 + key_len + val_len + 4
```

**struct definitions:**

```python
_RECORD_HEADER = struct.Struct(">IIQq")  # key_len u32, val_len u32, seq u64, timestamp_ms i64  — 24B
_RECORD_FOOTER = struct.Struct(">I")     # crc32 u32                                            —  4B
RECORD_HEADER_SIZE = 24
RECORD_FOOTER_SIZE = 4
```

`timestamp_ms` uses signed `q` (`i64`) rather than `Q` (`u64`) — Python's `time.time_ns()` returns a signed int and signed arithmetic avoids surprises when computing deltas. Values are always positive (milliseconds since Unix epoch).

**CRC scope:** CRC32 covers `header_bytes + key_bytes + value_bytes` (everything except the 4-byte CRC field itself). Computed with `binascii.crc32()`. On read, recompute and compare — raise `CorruptRecordError` on mismatch. `timestamp_ms` is inside the header so it is automatically covered.

**Cursor arithmetic:**
```
next_offset = offset + RECORD_HEADER_SIZE + key_len + val_len + RECORD_FOOTER_SIZE
            = offset + 28 + key_len + val_len
```

**Field semantics:**

| Field | Type | Set by | Purpose |
|---|---|---|---|
| `key_len` | `u32` | `SSTableWriter` | byte length of key |
| `val_len` | `u32` | `SSTableWriter` | byte length of value (including `TOMBSTONE`) |
| `seq` | `u64` | `LSMEngine` at `put()` | authoritative write ordering — always use for deduplication and MVCC |
| `timestamp_ms` | `i64` | `LSMEngine` at `put()` via `time.time_ns() // 1_000_000` | wall-clock write time — for TTL, audit, time-based queries; not a tiebreaker |
| `crc32` | `u32` | `SSTableWriter` | corruption detection — covers all bytes above |

**Ordering rule:** `seq` is the only field used to determine which of two records for the same key is newer. `timestamp_ms` must never be used as a tiebreaker — wall clocks are not monotonic.

**Tombstone encoding:** `value = TOMBSTONE` (`b"\x00__tomb__\x00"`), `val_len = len(TOMBSTONE)`. No special header flag needed — tombstone detection is always `value == TOMBSTONE`.

### 11b. `index.bin` — sparse index wire format

One entry per data block (default block = 4096 bytes). Entries are written in sorted key order.

```
┌──────────────────────────────────────────────────────┐
│  INDEX ENTRY  (12 + key_len bytes)                   │
│  [ 4B key_len: u32 ][ 8B block_offset: u64 ]         │
│  [ key bytes ]                                       │
└──────────────────────────────────────────────────────┘
```

```python
_IDX_ENTRY_HEADER = struct.Struct(">IQ")   # key_len u32 + block_offset u64  — 12B
```

Loaded fully into memory when an `SSTableReader` opens a file. Supports `bisect_right` (floor) and `bisect_left` (ceil) lookups for the read path.

### 11c. `meta.json` — per-SSTable metadata file

Written by `SSTableWriter.finish()`. Never modified after creation.

```json
{
  "file_id":      "<uuid4-hex>",
  "snapshot_id":  "<uuid4-hex>",
  "level":        0,
  "size_bytes":   4194304,
  "record_count": 51200,
  "block_count":  1024,
  "min_key":      "<base64>",
  "max_key":      "<base64>",
  "seq_min":      0,
  "seq_max":      419,
  "bloom_fpr":    0.01,
  "created_at":   "<iso8601 timestamp>",
  "data_file":    "data.bin",
  "index_file":   "index.bin",
  "filter_file":  "filter.bin"
}
```

| Field | Purpose |
|---|---|
| `file_id` | UUID4 hex — the SSTable's own identity; directory name on disk |
| `snapshot_id` | UUID4 hex — the `ImmutableMemTable` this file was flushed from; trace link in the audit chain |
| `record_count` | total KV records including tombstones |
| `block_count` | number of data blocks — equals number of sparse index entries |
| `min_key` / `max_key` | base64-encoded — used for range overlap checks without opening data file |
| `seq_min` / `seq_max` | sequence number range — compaction uses `seq_min` for tombstone GC decisions |
| `bloom_fpr` | false positive rate the filter was constructed with (for diagnostics) |
| `data_file` / `index_file` / `filter_file` | explicit filenames — allows renaming without breaking readers |

### 11d. `filter.bin` — Bloom filter file

```
┌──────────────────────────────────────────────────────┐
│  BLOOM HEADER  (16 bytes, ctypes _BloomHeader)       │
│  [ 4B magic: u32 = 0xBF00BF00 ]                     │
│  [ 4B num_bits: u32 ]                                │
│  [ 1B num_hashes: u8 ]                               │
│  [ 7B _pad ]                                         │
├──────────────────────────────────────────────────────┤
│  BIT ARRAY  (ceil(num_bits / 8) bytes)               │
└──────────────────────────────────────────────────────┘
```

Loaded fully into memory at `SSTableReader` open time. `may_contain(key)` is called before any disk access — a `False` result skips the SSTable entirely.

### 11e. `data.bin` — file-level footer (ctypes `_SSTableFooter`)

The last 56 bytes of `data.bin` are a fixed-size footer read first on open:

```
┌────────────────────────────────────────────────────────────┐
│  _SSTableFooter  (56 bytes, ctypes, _pack_ = 1)           │
│  [ 8B magic: u64 = LSMKV001 ]                             │
│  [ 8B data_end: u64 ]      ← end of record region         │
│  [ 8B index_offset: u64 ]  ← NOT USED (index.bin separate)│
│  [ 4B index_len: u32 ]                                     │
│  [ 8B bloom_offset: u64 ]  ← NOT USED (filter.bin sep.)   │
│  [ 4B bloom_len: u32 ]                                     │
│  [ 8B seq_min: u64 ]                                       │
│  [ 8B seq_max: u64 ]                                       │
└────────────────────────────────────────────────────────────┘
```

> Note: because index and filter are now separate files (`index.bin`, `filter.bin`), `index_offset` and `bloom_offset` in the footer are informational / reserved. The engine opens companion files directly by name. `data_end` remains meaningful — it marks where the record region ends and the footer begins within `data.bin`.

---

## 13. MemTable backing store — `SkipList`

### 13a. Why SkipList, not SortedDict

`SortedDict` is not thread-safe. Every `__setitem__` performs two non-atomic operations (hash map update + sorted list insert). Between those two steps the structure is internally inconsistent — any concurrent reader sees corrupt state. Wrapping it in a single `RLock` is correct but creates a global bottleneck: every `put()` and `get()` serialises on one lock even when they touch completely different key ranges.

The custom `SkipList` in `app/memtable/skiplist.py` uses **fine-grained per-node locking**. This is not lock-free in the strict CAS sense — Python exposes no atomic compare-and-swap primitive to userspace — but contention is bounded to nodes at the insertion boundary. Concurrent writes into non-overlapping key ranges are fully parallel.

### 13b. Why not truly lock-free, and why 3.13t is out of scope

True lock-free algorithms require hardware CAS (compare-and-swap). Python has no CAS primitive in stdlib or ctypes that works safely across versions. "Lock-free skip list in pure Python" is a misnomer — any correct implementation uses either the GIL as an implicit lock or explicit per-node locks.

**3.13t upgrade path (out of scope for current implementation):**
On 3.13t with no-GIL, the per-node locking model in this skip list becomes genuinely parallel — two threads inserting into non-overlapping key ranges run on separate CPU cores with zero contention. Additionally, `ProcessPoolExecutor` for compaction could be replaced with `ThreadPoolExecutor`, eliminating pickle serialisation overhead. These are noted here as the intended future upgrade path. No runtime detection (`sys._is_gil_enabled()`) is implemented in the current 3.12 codebase.

### 13c. Concurrency model

| Operation | Lock behaviour |
|---|---|
| `get(key)` | No lock acquired — reads `fully_linked` + `marked` flags only |
| `put(key, ...)` | Locks predecessor nodes at insertion level only |
| `delete(key, ...)` | Locks the target node only |
| `__iter__()` | No lock — skips nodes where `not fully_linked or marked` |
| `snapshot()` | No lock — `list(self)` materialises iterator |

### 13d. Key correctness invariants

- **`fully_linked` flag** — set as the final step of insert, after all forward pointers are wired. A node is invisible to readers until this is `True`. Prevents half-inserted nodes from being observed.
- **`marked` flag** — set as the first step of delete. Node is logically deleted immediately; physical removal is deferred. Prevents ABA: a deleted node that gets reinserted while a reader holds a pointer to it cannot corrupt the reader's traversal.
- **Lock ordering** — predecessor nodes are always locked in ascending `id()` order. Eliminates deadlock between concurrent inserts regardless of key order.
- **Validation after locking** — after locking predecessors, the insert path re-checks that predecessors still point to the expected successors. If not, it unlocks and retries. This handles the case where a concurrent insert or delete changed the structure between `_find()` and the lock acquisition.
- **`__iter__` is safe for concurrent reads** — boolean flag reads (`fully_linked`, `marked`) are atomic at the CPython bytecode level under the GIL. Iteration is safe concurrently with `put()` and `delete()`.

### 13e. Constants

```python
MAX_LEVEL:   int   = 16       # log2(max expected entries ~65536)
PROBABILITY: float = 0.5      # geometric level distribution
```

### 13f. Public API

```python
skiplist.put(key: Key, seq: SeqNum, value: Value) -> None
skiplist.get(key: Key) -> tuple[SeqNum, Value] | None
skiplist.delete(key: Key, seq: SeqNum) -> bool
skiplist.__iter__() -> Iterator[tuple[Key, SeqNum, Value]]   # sorted, lock-free
skiplist.snapshot() -> list[tuple[Key, SeqNum, Value]]       # for flush
skiplist.size_bytes -> int
```

### 13g. File location

```
app/memtable/skiplist.py    ← SkipList + _Node
app/memtable/active.py      ← ActiveMemTable wraps SkipList
```

`ActiveMemTable` is the public interface. `SkipList` is an implementation detail — never imported directly by layers above `memtable/`.

---

## 14. Engine layer — three-class design

The original `LSMEngine` monolith is split into three manager classes plus the engine coordinator. Each manager owns a coherent slice of state. `LSMEngine` owns only the three managers and the cross-cutting methods that require coordinating across them.

**Import rule:** `engine.py` imports only from `engine/`. It never imports directly from `memtable/`, `wal/`, `sstable/`, or `compaction/`. All lower-layer access is mediated through the manager classes.

**Visibility rule:** The three manager classes in `engine/` are internal to the engine layer. Nothing outside `engine.py` imports from `engine/` directly.

---

### 14a. `MemTableManager` — `app/engine/memtable_manager.py`

Owns all in-memory write-side state. Has no knowledge of SSTables, WAL, or disk.

**State:**

```python
class MemTableManager:
    _active:         ActiveMemTable            # live mutable SkipList-backed memtable
    _immutable_q:    deque[ImmutableMemTable]  # frozen snapshots, newest at left (index 0)
    _write_lock:     threading.RLock           # serialises put() and maybe_freeze()
    _seq:            int                       # monotonic sequence counter
    _seq_lock:       threading.Lock            # guards _seq increment only
    _queue_not_full: threading.Condition       # backpressure — wait when queue >= max
    _flush_event:    threading.Event           # signals flush worker to wake
```

`ActiveMemTable` carries a `table_id: TableID` (UUID4 hex) generated at construction. A new `table_id` is created every time `maybe_freeze()` instantiates a replacement `ActiveMemTable` — each write buffer has a unique identity for its entire lifetime.

`ImmutableMemTable` carries a `snapshot_id: SnapshotID` (UUID4 hex) assigned at freeze time. The `snapshot_id` is generated inside `maybe_freeze()` from the `_active.table_id` at the moment of freeze — it is the same UUID, not a new one, establishing the `TableID → SnapshotID` link explicitly.

**Public API:**

```python
def put(self, key: Key, seq: SeqNum, timestamp_ms: int, value: Value) -> None
    # Writes key into _active under _write_lock
    # Does NOT append to WAL — that is the engine's responsibility

def get(self, key: Key) -> tuple[SeqNum, Value] | None
    # Checks _active first, then _immutable_q newest->oldest
    # No lock acquired — SkipList.get() and deque iteration are safe under GIL

def next_seq(self) -> SeqNum
    # Atomically increments _seq under _seq_lock, returns new value

def maybe_freeze(self) -> ImmutableMemTable | None
    # Called by engine after put() — checks if _active >= MAX_MEMTABLE_SIZE
    # If yes:
    #   snapshot_id = _active.table_id            ← carry the TableID forward
    #   snapshot    = ImmutableMemTable(_active.freeze(), snapshot_id)
    #   _immutable_q.appendleft(snapshot)
    #   _active     = ActiveMemTable()            ← fresh table_id generated here
    #   _flush_event.set()
    #   return snapshot
    # If no:  returns None
    # Called while engine holds _write_lock — freeze is atomic with the triggering put()
    # Applies backpressure via _queue_not_full.wait() if queue is full

def peek_oldest(self) -> ImmutableMemTable | None
    # Returns _immutable_q[-1] without removing — None if queue is empty
    # Called by engine's _flush() before writing SSTable

def pop_oldest(self) -> None
    # Removes _immutable_q[-1] — called by engine AFTER SSTable is registered
    # Notifies _queue_not_full to unblock any stalled maybe_freeze()

def queue_len(self) -> int
    # Returns len(_immutable_q) — used by flush worker to check if work remains

def restore(self, entries: list[WALEntry]) -> None
    # Called by engine._recover() only
    # Replays WAL entries in seq order into _active — no lock needed (single-threaded recovery)

def size_bytes(self) -> int
    # Returns _active.size_bytes — used by engine for stats
```

**Invariants `MemTableManager` enforces:**
- Keys arrive in any order — SkipList handles sorting
- `_seq` is strictly monotonic — no two puts share a seq number
- `_immutable_q` is bounded by `IMMUTABLE_QUEUE_MAX` — backpressure enforced in `maybe_freeze()`
- `pop_oldest()` is only safe to call after the engine has registered the corresponding SSTable
- Every `ActiveMemTable` instance has a unique `table_id` for its lifetime
- Every `ImmutableMemTable` carries the `snapshot_id` equal to the `table_id` of the `ActiveMemTable` it was frozen from

---

### 14b. `SSTableManager` — `app/engine/sstable_manager.py`

Owns all on-disk read-side state. Has no knowledge of the memtable, WAL, or sequence numbers.

**State:**

```python
class SSTableManager:
    _registry:    SSTableRegistry    # ref-counted open SSTableReaders
    _cache:       BlockCache         # shared LRU, passed into every reader at open time
    _compactor:   LeveledCompactor   # skeleton — level layout + manifest mgmt (merge TBD)
    _flush_lock:  threading.RLock    # one flush at a time
    _data_root:   Path
```

**Public API:**

```python
@classmethod
def load(cls, data_root: Path, cache: BlockCache) -> SSTableManager
    # Reads metadata.json + all level manifests
    # Opens SSTableReader for every known SSTable and registers in _registry
    # Returns fully initialised SSTableManager

def get(self, key: Key) -> tuple[SeqNum, Value] | None
    # Searches L0 (newest->oldest) then L1, L2, ...
    # L0: checks every file (ranges may overlap), newest creation time first
    # L1+: checks at most one candidate file per level (non-overlapping ranges)
    # Returns (seq, value) or None — tombstone detection is the engine's responsibility

def flush(self, snapshot: ImmutableMemTable, file_id: FileID) -> SSTableMeta
    # Acquires _flush_lock
    # Creates SSTableWriter, iterates snapshot.items() in sorted order, calls writer.finish()
    # snapshot.snapshot_id is passed into SSTableMeta — establishes SnapshotID → FileID link
    # Opens SSTableReader for the new file, registers in _registry
    # Updates level manifest via _compactor.register_sstable(meta)
    # Returns SSTableMeta — caller (engine) uses seq_min to truncate WAL
    # Does NOT pop from immutable_q — that is the engine's responsibility

def max_seq_seen(self) -> SeqNum
    # Returns max(meta.seq_max for all registered SSTables)
    # Called by engine._recover() to set the starting seq counter

def mark_for_deletion(self, file_ids: list[FileID]) -> None
    # Marks files in registry for deferred deletion
    # Actual deletion happens when ref_count reaches zero
    # Skeleton — called by compaction path (not yet implemented)

def close_all(self) -> None
    # Closes all open SSTableReader mmap handles
    # Called by engine.close()
```

**Invariants `SSTableManager` enforces:**
- A reader is never closed while another thread holds a reference — `SSTableRegistry` ref-counting handles this
- A file is never deleted while an SSTableReader mmap is open on it
- `flush()` is serialised by `_flush_lock` — two concurrent flushes cannot interleave
- `meta.json` written last in `SSTableWriter.finish()` — its presence is the SSTable completeness signal

---

### 14c. `WALManager` — `app/engine/wal_manager.py`

Thin wrapper around `WALWriter`. No logic beyond serialising concurrent access.

**State:**

```python
class WALManager:
    _wal:      WALWriter       # owns the open wal.log file handle
    _wal_lock: threading.Lock  # one writer at a time
```

**Public API:**

```python
def append(self, entry: WALEntry) -> None
    # Acquires _wal_lock, appends msgpack-encoded entry, fsyncs, releases
    # Called by engine.put() before memtable write — durability before visibility

def replay(self) -> list[WALEntry]
    # Reads wal.log sequentially via msgpack.Unpacker
    # Returns entries sorted by seq — WAL may have out-of-order appends on crash
    # Called by engine._recover() only — not thread-safe, called before workers start

def truncate_before(self, seq: SeqNum) -> None
    # Removes all WAL entries with entry.seq <= seq
    # Called by engine._flush() after SSTable is registered and immutable_q popped
    # Never called before those two preconditions are met

def close(self) -> None
    # Acquires _wal_lock, fsyncs, closes file handle
```

---

### 14d. `LSMEngine` — `app/engine.py`

Reduced to three data members and cross-cutting coordination methods. Contains no state from the lower layers directly.

**State:**

```python
class LSMEngine:
    _mem:        MemTableManager   # all in-memory state
    _sst:        SSTableManager    # all on-disk state
    _wal:        WALManager        # WAL append / replay / truncate
    _data_root:  Path
    _closed:     bool
```

**Cross-cutting methods — each delegates entirely to managers:**

**`put(key, value)`:**
```
acquire _mem._write_lock
    seq          = _mem.next_seq()
    timestamp_ms = time.time_ns() // 1_000_000        ← wall-clock, set once per put()
    _wal.append(WALEntry(seq, timestamp_ms, key, value))   <- WALManager
    _mem.put(key, seq, timestamp_ms, value)                <- MemTableManager
    snapshot = _mem.maybe_freeze()              <- MemTableManager
    if snapshot: _flush_event.set()
release _mem._write_lock
```

**`get(key)`:**
```
result = _mem.get(key)                          <- MemTableManager (active + immutable_q)
if result is None:
    result = _sst.get(key)                      <- SSTableManager (all levels)
if result is None: return None
seq, value = result
return None if value == TOMBSTONE else value
```

**`delete(key)`:**
```
identical to put() with value = TOMBSTONE
```

**`_flush()`:**
```
snapshot = _mem.peek_oldest()                   <- ImmutableMemTable with snapshot_id set
if snapshot is None: return
file_id = uuid.uuid4().hex
meta = _sst.flush(snapshot, file_id)            <- SSTableManager: snapshot.snapshot_id → meta.snapshot_id
_mem.pop_oldest()                               <- MemTableManager (after SSTable registered)
_wal.truncate_before(meta.seq_min)              <- WALManager
# audit chain complete: snapshot.snapshot_id == meta.snapshot_id, meta.file_id == file_id
```

**`_recover()`:**
```
entries     = _wal.replay()                     <- WALManager
max_on_disk = _sst.max_seq_seen()              <- SSTableManager
to_replay   = [e for e in entries if e.seq > max_on_disk]
_mem.restore(to_replay)                         <- MemTableManager
```

---

### 14e. `_compact()` — background compaction worker *(skeleton — implementation out of scope)*

The compaction worker runs in a daemon `threading.Thread`. It wakes periodically and checks the L0 file count against `L0_COMPACTION_THRESHOLD`.

**Skeleton — what the worker must do (implementation deferred):**

```
loop:
    sleep(COMPACTION_CHECK_INTERVAL)

    if _sst._compactor.l0_file_count() < L0_COMPACTION_THRESHOLD:
        continue                          <- nothing to do yet

    # [COMPACTION IMPLEMENTATION — OUT OF SCOPE]
    # All compaction steps go through _sst — engine only triggers:
    #   1. _sst: build CompactionTask (select input files, output level, seq_cutoff)
    #   2. _sst: run merge in ProcessPoolExecutor subprocess
    #   3. _sst: register new SSTableReader in registry
    #   4. _sst: mark old files for deletion (deferred by ref-count)
    #   5. _sst: swap level manifest atomically (os.replace())
    #   6. _sst: invalidate old file_ids from BlockCache
```

**What is decided:**
- All compaction state mutation goes through `_sst` — engine only triggers the worker
- Subprocess receives only file paths and returns only `SSTableMeta`
- `CompactionTask` is serialisable — crash mid-compaction is recoverable

**What is NOT yet decided:**
- Merge algorithm (k-way heap vs other)
- Tombstone GC condition (seq-based threshold logic)
- Level sizing policy (L1 max size, Ln multiplier)
- Partial vs full L0 compaction

---

### 14f. The locks — design rationale

| Lock | Lives in | Scope | Held during |
|---|---|---|---|
| `_write_lock` | `MemTableManager` | `RLock` | `put()`, `delete()`, `maybe_freeze()` |
| `_seq_lock` | `MemTableManager` | `Lock` | `next_seq()` — `_seq += 1` only |
| `_queue_not_full` | `MemTableManager` | `Condition` | backpressure wait in `maybe_freeze()` |
| `_flush_lock` | `SSTableManager` | `RLock` | `flush()` — one flush at a time |
| `_wal_lock` | `WALManager` | `Lock` | `append()`, `truncate_before()`, `close()` |

**No lock crosses a manager boundary.** `_mem._write_lock` is never held when `_sst._flush_lock` is acquired. Enforced structurally — the engine calls managers sequentially, never holding one manager's lock while calling into another.

**Reads never acquire any engine-level lock.** `_mem.get()` uses SkipList's per-node locking internally. `_sst.get()` uses `SSTableRegistry`'s internal ref-count lock only for reader open/close, not for the actual read.

---

### 14g. `open()` — startup and recovery

```python
@classmethod
def open(cls, data_root: Path) -> LSMEngine:
    engine            = cls.__new__(cls)
    engine._data_root = Path(data_root)
    engine._closed    = False

    cache         = BlockCache(maxsize=256)
    engine._sst   = SSTableManager.load(data_root, cache)
    engine._wal   = WALManager.open(data_root / "wal" / "wal.log")
    engine._mem   = MemTableManager()

    engine._recover()
    engine._start_background_workers()
    return engine
```

**`_recover()` detail:**

```
entries     = _wal.replay()                         <- all WAL entries, sorted by seq
max_on_disk = _sst.max_seq_seen()                  <- highest seq in any SSTable
to_replay   = [e for e in entries if e.seq > max_on_disk]
_mem.restore(to_replay)                             <- replays into fresh _active
```

After recovery, `_mem._active` contains exactly the writes durable in WAL but not yet in any SSTable. `_mem._seq` is set to `max(max_on_disk, max(e.seq for e in to_replay))` so the next write gets a strictly higher seq than anything on disk.

---

### 14h. `close()` — clean shutdown

```python
def close(self) -> None:
    if self._closed:
        return
    self._closed = True
    self._stop_background_workers()     # join daemon threads
    self._mem.maybe_freeze()            # push remaining active memtable into queue
    self._flush_all()                   # synchronously drain entire immutable_q
    self._wal.close()                   # WALManager fsync + close
    self._sst.close_all()               # close all open mmap handles
```

---

### 14i. What `LSMEngine` does NOT do

- Does not own `_active`, `_immutable_q`, `_seq`, or any write-lock — that is `MemTableManager`
- Does not own `_registry`, `_cache`, `_compactor`, or `_flush_lock` — that is `SSTableManager`
- Does not own the WAL file handle or WAL lock — that is `WALManager`
- Does not know about `struct`, `mmap`, `ctypes`, or byte encoding — that is `wire/`
- Does not know how blocks are split or how the sparse index works — that is `sstable/`
- Does not manage the Bloom filter lifecycle — `SSTableWriter` and `SSTableReader` own that
- Does not directly open, read, or write any file
- Does not parse or construct msgpack frames — that is `wal/writer.py` and `sstable/meta.py`

A method belongs in `engine.py` only if it requires calling two or more managers in a specific sequence. If it touches only one manager, it belongs in that manager.

---

### 14j. ID audit chain — `TableID → SnapshotID → FileID`

Every write buffer, snapshot, and on-disk file has a unique identity. The three IDs form a traceable chain from the moment a key is written to the moment it is permanently on disk.

```
ActiveMemTable  (table_id: TableID)
        │
        │  ActiveMemTable.freeze() called inside maybe_freeze()
        │  snapshot_id = table_id  ← same UUID, explicit carry-forward
        ▼
ImmutableMemTable  (snapshot_id: SnapshotID)
        │
        │  SSTableManager.flush(snapshot, file_id)
        │  meta.snapshot_id = snapshot.snapshot_id
        ▼
SSTableMeta  (file_id: FileID, snapshot_id: SnapshotID)
        │
        │  written to meta.json, level manifest, SSTableRegistry
        ▼
Disk:  data/sstable/L0/<file_id>/meta.json
```

**ID generation rules:**

| ID | Type | Generated at | Carried to |
|---|---|---|---|
| `table_id` | `TableID` | `ActiveMemTable.__init__()` | `ImmutableMemTable.snapshot_id` at freeze |
| `snapshot_id` | `SnapshotID` | `ActiveMemTable.freeze()` — equals `table_id` | `SSTableMeta.snapshot_id` at flush |
| `file_id` | `FileID` | `LSMEngine._flush()` — `uuid.uuid4().hex` | `SSTableMeta.file_id`, directory name, `SSTableRegistry` key, `BlockCache` key prefix |

**Why `snapshot_id == table_id`:**

Using the same UUID avoids generating a new random value and makes the link unambiguous — there is exactly one `ImmutableMemTable` per `ActiveMemTable` lifecycle. A new `table_id` is only created when `maybe_freeze()` instantiates the replacement `ActiveMemTable`.

**Uses of the chain:**

- **Logging:** flush worker logs `snapshot_id` when it starts flushing and `file_id` when done — a single UUID links the log lines
- **Debugging:** if a key is missing, the chain tells you whether it was in the active table, the snapshot queue, or which SSTable should have it
- **Recovery:** `_recover()` replays WAL into `_active` — after recovery `_active.table_id` is a fresh UUID; the recovered data carries no prior `table_id`
- **Compaction:** `CompactionTask` lists `input FileIDs` — the `snapshot_id` in each `SSTableMeta` traces which flush produced each input file



## 15. Async flush — detailed design

### 15a. The three concurrent actors

At any point in time, three independent actors may be active simultaneously:

```
Writer thread(s)  → put() / delete() — calls _mem.put(), may call _mem.maybe_freeze()
Flush worker      → pops from _mem._immutable_q, writes SSTable to disk via _sst.flush()
Reader thread(s)  → get() — scans _mem._active + _mem._immutable_q + SSTables via _sst.get()
```

The flush worker runs on a daemon `threading.Thread`. It is completely decoupled from the write path — a write does not wait for a flush to complete. The interaction is signalled via a `threading.Event` (`_mem._flush_event`) set by `_mem.maybe_freeze()` and waited on by the flush worker.

---

### 15b. State transitions during async flush

There are four distinct states the engine passes through during a flush cycle. The key invariant is that **data is visible to reads at every state** — there is never a window where a key exists in neither `_mem._immutable_q` nor the SSTable registry.

**State A — normal operation:**
```
_mem._active       = SkipList with current writes
_mem._immutable_q  = []  (or prior snapshots still being flushed)
SSTables      = [L0a, L0b, ...]
```

**State B — immediately after `_mem.maybe_freeze()`, still under `_mem._write_lock`:**
```
_mem._active       = fresh empty SkipList  ← writes resume here immediately
_mem._immutable_q  = [snap0]              ← snapshot pushed, _flush_event signalled
SSTables           = [L0a, L0b, ...]      ← unchanged
```
`_mem._write_lock` is released at the end of `_mem.maybe_freeze()`. New writes go into the fresh `_mem._active` with no stall from this point.

**State C — flush in progress (concurrent with new writes):**
```
_mem._active       = SkipList accumulating new writes
_mem._immutable_q  = [snap0]              ← still present — readable by _mem.get()
SSTables           = [L0a, L0b, L0c...]   ← L0c being written via _sst.flush() right now
```
This is the critical window. `snap0` remains in `_mem._immutable_q` for the entire duration of the SSTable write. Reads that need data from `snap0` find it via `_mem.get()`. `_sst._flush_lock` is held but `_mem._write_lock` is not — writes proceed normally.

**State D — flush complete, under `_sst._flush_lock`:**
```
_mem._active       = SkipList with accumulated writes
_mem._immutable_q  = []                   ← snap0 removed AFTER L0c is in registry
SSTables      = [L0a, L0b, L0c]     ← L0c now registered and readable
```
The pop from `_mem._immutable_q` happens only after `registry.register()` confirms L0c is fully readable. This ordering is non-negotiable — inverting it creates a read gap.

---

### 15c. The peek-then-pop pattern — why pop order matters

The naive implementation pops from the queue before writing the SSTable:

```python
# WRONG — creates a read gap
table = immutable_q.pop()      ← snap0 removed from queue
writer.finish()                ← writing to disk... crash here?
registry.register(...)         ← snap0 never reaches SSTable
```

If the process crashes between pop and register, snap0 is lost — it is neither in the queue (already popped) nor in any SSTable (never finished). On recovery, `_recover()` replays the WAL, but only if WAL truncation has not yet happened. If WAL truncation occurred before the crash, the data is permanently lost.

The correct pattern is peek-write-register-pop:

```python
# CORRECT — no read gap, no data loss window
acquire _sst._flush_lock
    table = _mem.peek_oldest()           ← peek at oldest, do not remove
    ...write SSTable...
    meta = writer.finish()            ← all four files on disk
    reader = SSTableReader(...)
    registry.register(file_id, reader) ← SSTable now readable
    immutable_q.pop()                 ← NOW safe to remove from queue
    wal.truncate_before(meta.seq_min) ← NOW safe to truncate WAL
    _queue_not_full.notify_all()      ← wake any stalled writers
release _sst._flush_lock
```

During the write phase (`...write SSTable...`), a concurrent `get()` that needs data from `snap0` will find it in `immutable_q[-1]`. After pop, the same `get()` will find L0c in the registry. There is no point in time where the data is unreachable.

---

### 15d. Write path impact — three cases

**Case 1: Normal write, queue has capacity**

Zero impact on write latency. `put()` acquires `_mem._write_lock`, appends to WAL via `_wal`, updates `_mem._active`, releases `_mem._write_lock`. The flush worker runs independently in its own thread. `_mem._write_lock` and `_sst._flush_lock` are never held simultaneously.

```
put() latency = WAL append + fsync + SkipList.put()
              ≈ 0.5–2ms (dominated by fsync)
```

**Case 2: Write triggers freeze**

The write that crosses `MAX_MEMTABLE_SIZE` calls `_mem.maybe_freeze()` while the engine holds `_mem._write_lock`. The stall window is:

```
stall = O(n) skip list traversal to produce sorted snapshot
      ≈ 5–15ms for 64MB memtable at ~100B avg record size (~650K records)
```

All other concurrent writes queue behind `_mem._write_lock` during this window. After `_mem.maybe_freeze()` returns, `_mem._write_lock` is released and all queued writes proceed immediately — they write into the fresh empty `_mem._active`.

**Case 3: Queue full — backpressure**

If `len(_mem._immutable_q) >= IMMUTABLE_QUEUE_MAX` (4 by default) when `_mem.maybe_freeze()` is called, the engine applies backpressure. `put()` stalls on a condition variable while holding `_mem._write_lock`:

```python
# MemTableManager.maybe_freeze() — internal implementation
def maybe_freeze(self):
    while len(self._immutable_q) >= IMMUTABLE_QUEUE_MAX:
        self._queue_not_full.wait(self._write_lock)
        # releases _write_lock while waiting, reacquires before continuing
    snapshot = self._active.freeze()
    self._immutable_q.appendleft(snapshot)
    self._active = ActiveMemTable()
    self._flush_event.set()
```

Releasing `_mem._write_lock` during the wait means:
- Other write threads that are not themselves trying to freeze can proceed
- Read threads are never blocked regardless

The flush worker (via `_mem.pop_oldest()`) calls `_queue_not_full.notify_all()` after each successful pop. The stalled `_mem.maybe_freeze()` wakes, reacquires `_mem._write_lock`, and re-checks the condition before proceeding.

**Backpressure is intentional.** Without it, a burst of writes faster than the flush worker can handle would grow `_mem._immutable_q` without bound, exhausting heap memory. The 4-table limit caps worst-case in-flight memory at `4 × MAX_MEMTABLE_SIZE = 256MB`.

---

### 15e. Read path impact — the invariant that makes it safe

The read path scans in strict newest-to-oldest order:

```
1. _mem._active          → current writes (newest)
2. _mem._immutable_q     → left to right (newest snapshot to oldest)
3. SSTables L0      → newest file to oldest file within L0
4. SSTables L1+     → one candidate file per level (non-overlapping ranges)
```

This order guarantees correctness during every flush state:

| Flush state | `_mem._active` | `_mem._immutable_q` | SSTables | Read correct? |
|---|---|---|---|---|
| A (normal) | has data | empty | L0a, L0b | Yes |
| B (just frozen) | empty | [snap0] | L0a, L0b | Yes — snap0 in queue |
| C (flush in progress) | new data | [snap0] | L0a, L0b | Yes — snap0 still in queue |
| D (flush complete) | new data | empty | L0a, L0b, L0c | Yes — snap0 now in L0c |

At no state is data unreachable. The transition from State C to State D is the only moment that requires careful sequencing — handled by the peek-then-pop pattern in §15c.

**Multiple snapshots in queue:**

If writes are faster than flushes, the queue may hold multiple snapshots:

```
_mem._immutable_q = [snap2, snap1, snap0]
                newest           oldest
```

The read path scans left to right — snap2 is checked first. If a key was updated in snap2, its value shadows snap1 and snap0. This is correct — the newest write wins. The flush worker pops from the right (snap0 first), producing SSTables in write-time order. L0 files created later always contain more recent data than L0 files created earlier.

**TOMBSTONE short-circuit:**

If any layer (including an immutable snapshot) returns `TOMBSTONE` for a key, the read immediately returns `None` — it does not continue scanning older snapshots or SSTables. This is correct: the tombstone was written after the value, so the value is deleted.

---

### 15f. `_immutable_q` data structure — `deque`

`collections.deque` is the correct data structure:

- `appendleft(snapshot)` — O(1), newest at index 0 (left)
- `pop()` — O(1), removes oldest from right
- `[-1]` peek — O(1), reads oldest without removing
- Left-to-right iteration — newest to oldest, correct for read path
- Individual operations are atomic under CPython 3.12 GIL — safe for concurrent reads

The peek (`_mem.peek_oldest()`), write SSTable via `_sst.flush()`, pop via `_mem.pop_oldest()` sequence spans multiple operations and is NOT atomic as a unit. `_sst._flush_lock` serialises the write+register half; the pop and WAL truncation follow after.

Reads iterate `_immutable_q` without holding any lock. A concurrent `appendleft` (new freeze) during iteration is safe — `deque` iteration on CPython 3.12 is not interrupted by a single `appendleft` at the opposite end. A concurrent `pop` during iteration is also safe because `_mem.pop_oldest()` is only called after `_sst.flush()` completes, and the pop removes the rightmost element — iteration proceeds left to right, so a right-side removal does not invalidate the iterator. If iteration has already passed the popped element, no problem. If it has not yet reached it, it will simply stop there.

---

### 15g. WAL truncation — the coordination point

WAL truncation is the moment the engine reclaims WAL disk space. The rule is strict:

```
WAL truncation is safe only after:
    1. SSTableWriter.finish() has completed (data.bin, index.bin, filter.bin, meta.json all fsynced)
    2. registry.register() has made the SSTable readable
    3. immutable_q.pop() has removed the snapshot from the queue
```

All three conditions are met before `wal.truncate_before(meta.seq_min)` is called. The seq_min value is the smallest sequence number in the flushed snapshot — all WAL entries with `seq <= seq_min` are now durably on disk as an SSTable.

**Crash scenario analysis:**

| Crash point | WAL state | SSTable state | Recovery action |
|---|---|---|---|
| During SSTable write | intact | incomplete (no meta.json) | _recover() replays WAL; incomplete SSTable ignored |
| After finish(), before register() | intact | complete on disk | _recover() replays WAL; LeveledCompactor.load() finds and registers the orphaned SSTable |
| After register(), before pop() | intact | complete + registered | Normal startup; WAL replayed but seq already in SSTable — deduplication by seq handles this |
| After pop(), before truncate() | intact | complete + registered | Normal startup; WAL has redundant entries, they are replayed and deduped |
| After truncate() | truncated | complete + registered | Normal startup; truncated WAL entries are gone but are in SSTable |

The only unrecoverable scenario is: WAL truncated AND SSTable write incomplete. The peek-then-pop pattern and the truncation ordering in §15c make this structurally impossible.

---

### 15h. Constants governing flush behaviour

| Constant | Value | Location | Purpose |
|---|---|---|---|
| `MAX_MEMTABLE_SIZE` | 67108864 (64MB) | `types.py` | Threshold that triggers `_mem.maybe_freeze()` |
| `IMMUTABLE_QUEUE_MAX` | 4 | `types.py` | Max snapshots in queue before backpressure |
| `COMPACTION_CHECK_INTERVAL` | 0.5 (seconds) | `types.py` | How often compact worker wakes to check L0 |

`IMMUTABLE_QUEUE_MAX = 4` means worst-case in-flight memory is `4 × 64MB = 256MB` in addition to the active memtable. Total peak memtable memory = `5 × 64MB = 320MB`. Size this according to available heap.

---

## 12. REPL interface (`main.py`)

`prompt_toolkit` `PromptSession` with `InMemoryHistory` + `AutoSuggestFromHistory`.

| | |
|---|---|
| Prompt | `lsm-kv>` (green bold via `Style.from_dict`) |
| Commands | `put <key> <value>` / `get <key>` / `del <key>` / `compact` / `stats` / `help` / `exit` |
| Ctrl+C | clears current line — does not exit |
| Ctrl+D | exits cleanly via `EOFError` |