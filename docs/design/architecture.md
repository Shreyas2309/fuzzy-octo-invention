# System Architecture

lsm-kv is a Log-Structured Merge Tree key-value store built in Python 3.12+. It provides durable, crash-safe storage with an async-first API. The engine coordinates six subsystems — WAL, memtable, SSTable, flush pipeline, compaction, and observability — through a strict layered dependency model where no layer imports from a layer above it.

## Layered Dependency Model

The codebase is organized into six tiers, from leaf modules with no internal dependencies up to the top-level engine coordinator:

| Tier | Modules | Depends on |
|------|---------|------------|
| 0 | `types.py`, `common/crc.py`, `common/uuid7.py`, `common/errors.py` | Nothing (stdlib only) |
| 1 | `bloom/`, `cache/`, `index/`, `common/rwlock.py`, `common/merge_iterator.py` | Tier 0 |
| 2 | `memtable/`, `wal/` | Tier 0–1 |
| 3 | `sstable/`, `engine/config.py`, `engine/seq_generator.py`, `engine/wal_manager.py` | Tier 0–2 |
| 4 | `engine/memtable_manager.py`, `engine/sstable_manager.py`, `compaction/` | Tier 0–3 |
| 5 | `engine/flush_pipeline.py`, `engine/compaction_manager.py` | Tier 0–4 |
| 6 | `engine/lsm_engine.py` | All tiers |

This strict ordering prevents circular imports and makes each layer independently testable.

## Write Path

Every write follows this sequence atomically under a single lock:

```
engine.put(key, value)
    │
    ├─ 1. SeqGenerator.next()         → monotonic sequence number
    ├─ 2. WALManager.sync_append()    → durable on disk (fsync)
    ├─ 3. MemTableManager.put()       → insert into active skip list
    └─ 4. MemTableManager.maybe_freeze()
              │
              ├─ threshold not reached → return
              └─ threshold reached → freeze active table
                    │
                    ├─ snapshot skip list → ImmutableMemTable
                    ├─ push to immutable queue
                    ├─ create fresh ActiveMemTable
                    └─ signal FlushPipeline
```

Atomicity guarantee: all four steps happen under `_mem.write_lock`. Either all succeed or the caller sees the exception.

## Read Path

Reads scan from newest to oldest data:

```
engine.get(key)
    │
    ├─ 1. Active memtable       → O(log n) skip list lookup
    ├─ 2. Immutable queue       → newest-first scan (0–4 snapshots)
    ├─ 3. L0 SSTables           → all files checked (overlapping ranges)
    │      └─ bloom → index bisect → block scan
    └─ 4. L1, L2, L3 SSTables  → one file per level, sequential
           └─ bloom → index bisect → block scan
```

The first match with the highest sequence number wins. Tombstones indicate deleted keys — the engine returns `None`.

## Recovery Model

On startup, the engine replays the WAL to rebuild in-memory state:

1. Load config and open WAL file
2. Load all SSTables from disk via manifest
3. Determine `max_seq_seen` across all SSTables
4. Replay WAL entries with `seq > max_seq_seen` into fresh memtable
5. Restore sequence generator to highest seen seq
6. Start flush pipeline and compaction manager

Entries already flushed to SSTables are skipped. The WAL is the source of truth for unflushed data.

## Thread and Async Model

| Component | Model | Reason |
|-----------|-------|--------|
| MemTable operations | Sync (threading locks) | Pure in-memory, microseconds |
| WAL append | Sync under lock, offloaded via `to_thread` for async callers | Must fsync before returning |
| Flush pipeline | Async daemon task | Non-blocking I/O, parallel writes |
| Compaction merge | Subprocess (`ProcessPoolExecutor`) | CPU-bound, bypasses GIL |
| SSTable reads | Sync (mmap) | Memory-mapped, no syscall after page cache warm |
| Config access | Sync (RLock) | Thread-safe attribute reads |

## Architecture Diagram

![Architecture diagram](images/architecture.svg)