# Compaction Feature — Complete Implementation Specification
## lsm-kv · github.com/cvp2004/fuzzy-octo-invention · Base commit: 279efa3

> This document is the single source of truth for the compaction feature.
> Claude Code should read it entirely before entering plan mode.
> Implement phases in strict order. Run verification at the end of each phase.

---

## PART 1 — CONTEXT AND CURRENT STATE

### 1.1 What exists today (commit 279efa3)

The codebase has a fully working write path, flush pipeline, and point-read path. The following components are confirmed present and correct:

**Foundation**
- `app/types.py` — type aliases, `TOMBSTONE`, `L0_COMPACTION_THRESHOLD=10`, `Level` alias
- `app/common/errors.py` — 14-exception hierarchy, all inheriting `LSMError`
- `app/common/encoding.py` — `encode_record`, `decode_from`, `iter_block`, `DecodedRecord`
- `app/common/crc.py` — `compute`, `pack`, `unpack`, `verify`
- `app/common/uuid7.py` — `uuid7_hex()` generating time-ordered UUIDv7 strings

**Storage layer**
- `app/bloom/filter.py` — `BloomFilter` with `add`, `may_contain`, `to_bytes`, `from_bytes`
- `app/index/sparse.py` — `SparseIndex` with `floor_offset`, `ceil_offset`, `next_offset_after` (O(log n) bisect), ascending-key validation in `add()`
- `app/cache/block.py` — `BlockCache` with `get`, `put`, `invalidate(file_id)` — **no `invalidate_all` yet**
- `app/sstable/meta.py` — `SSTableMeta` frozen dataclass, `to_json`/`from_json`
- `app/sstable/writer.py` — `SSTableWriter` OPEN→FINISHING→DONE state machine, async `finish()` (L0), sync `finish_sync()` (L1+)
- `app/sstable/reader.py` — `SSTableReader` with async `open()`, `get()` with block cache, `scan_all()` — **no `iter_sorted()` yet**
- `app/sstable/registry.py` — `SSTableRegistry` ref-counted with `mark_for_deletion()`, deferred cleanup

**Engine layer**
- `app/engine/seq_generator.py` — standalone `SeqGenerator`
- `app/engine/config.py` — `LSMConfig` with keys: `l0_compaction_threshold=10`, `compaction_check_interval=0.5`, `flush_max_workers=2`
- `app/engine/wal_manager.py` — async `WALManager` wrapping sync `WALWriter`
- `app/engine/memtable_manager.py` — `MemTableManager` with `snapshot_queue()`, `set_flush_notify()`
- `app/engine/sstable_manager.py` — `SSTableManager` with `_state_lock`, L0-only manifest, `commit()`, `get()`
- `app/engine/flush_pipeline.py` — `FlushPipeline` with event-chain ordered commits, `batch_abort`, asyncio bridge
- `app/engine/lsm_engine.py` — `LSMEngine` orchestrating all managers

**What does NOT exist yet:**
- `app/common/rwlock.py` — `AsyncRWLock`
- `app/common/merge_iterator.py` — `KWayMergeIterator`
- `app/compaction/` package — `task.py`, `worker.py`
- `app/engine/compaction_manager.py` — `CompactionManager`
- L1 support in `SSTableManager` — `_l1_file_id`, `commit_compaction_async()`, updated `get()`
- `iter_sorted()` on `SSTableReader`
- `invalidate_all()` on `BlockCache`
- Level RW locks in `SSTableManager`

### 1.2 The three LSM correctness invariants

Every design decision in this document follows from these three rules:

**Invariant 1 — No read gap:** At every moment, every durably written key must be reachable by `get()`. A key must never be absent from both the old SSTables (not yet deleted) and the new SSTable (not yet registered) simultaneously.

**Invariant 2 — Highest seq wins:** For duplicate keys across input files, only the record with the highest `seq` appears in the output. `seq` is the sole tiebreaker — `timestamp_ms` must never be used for ordering.

**Invariant 3 — Tombstones preserved until safe:** A tombstone for key K can only be dropped if no older version of K exists in any other level. The `seq_cutoff` mechanism enforces this: tombstones with `seq < seq_cutoff` are GC'd; others are preserved.

---

## PART 2 — ARCHITECTURE

### 2.1 Trigger mechanism — flush-driven, not daemon-driven

Compaction is **not** a daemon with a polling loop. It is triggered directly by the flush pipeline after each successful SSTable commit. This is strictly better than polling:

- **Zero wasted polls** — the only moment L0 file count ever increases is after a flush commit
- **Immediate response** — compaction starts within one event loop iteration of threshold crossing
- **Simpler** — no `run()` loop, no `stop()`, no shutdown coordination

The trigger point is `FlushPipeline._commit_slot()`, after `self._sst.commit(...)`:

```python
# In FlushPipeline._commit_slot(), after WAL truncation:
if self._compaction is not None:
    asyncio.create_task(self._compaction.check_and_compact())
```

`create_task` (not `await`) — the flush commit completes immediately; compaction runs as an independent async task.

Additionally, `LSMEngine.open()` triggers a check after recovery, in case L0 already has ≥ threshold files from a previous run.

### 2.2 Compaction scheduling — level reservation set

Two scheduling rules:

**Rule 1 — Per-level exclusion:** At most one compaction job per source level at any time. A new L0→L1 job cannot start while one is already running.

**Rule 2 — Adjacent-level exclusion:** Two jobs that share a level cannot run simultaneously. L0→L1 and L1→L2 conflict because L1 is both the output of the first and the input of the second. L1→L2 and L3→L4 do not conflict — no shared level.

**General conflict rule:** Jobs A and B conflict if `{A.src, A.dst} ∩ {B.src, B.dst} ≠ ∅`.

**Implementation:** A `_active_levels: set[Level]` tracks all levels currently involved in a running job. Check-and-reserve is atomic under `_reservation_lock` (an `asyncio.Lock`):

```
L0→L1 running:  _active_levels = {0, 1}
L1→L2 tries:    1 in active → BLOCKED ✓
L3→L4 tries:    neither in active → ALLOWED, runs concurrently ✓
```

### 2.3 Input snapshot — new flushes don't interfere

When a compaction job starts, `_build_task()` captures the exact set of input file IDs under `_state_lock`. After that point, new L0 flushes prepend new file IDs to `_l0_order` — they are not in `task.input_file_ids` and the subprocess never sees them.

After compaction completes, `commit_compaction_async()` removes exactly `task.input_file_ids` from `_l0_order`, leaving any newer files untouched:

```
At task creation:  _l0_order = [L0d, L0c, L0b, L0a]
                   task.input_file_ids = [L0d, L0c, L0b, L0a]  ← snapshot

During compaction: new flush commits L0e
                   _l0_order = [L0e, L0d, L0c, L0b, L0a]

After commit:      _l0_order = [L0e]  ← only non-compacted files remain ✓
```

### 2.4 Read–compaction safety — per-level AsyncRWLock

The write lock is held **only** during `commit_compaction_async()` — the brief in-memory commit phase after the merge has already completed on disk. It is **not** held during the subprocess merge or during SSTable file writes.

```
Compaction timeline — when the write lock is held vs not:

t=0  _build_task()           snapshot _l0_order under _state_lock (microseconds)
     [LOCK NOT HELD]

t=1  subprocess merge        reads L0 + old L1 files, writes new L1 directory
     [LOCK NOT HELD]         can take seconds — get() proceeds freely

t=2  SSTableReader.open()    opens new L1 reader
     [LOCK NOT HELD]         new file invisible to get() until Step 1 below

t=3  commit_compaction_async():
       acquire L0.write_lock  ←──── WRITE LOCK ACQUIRED HERE
       acquire L1.write_lock
         waits for in-flight get() scans on L0 or L1 to finish
         new get() calls on those levels block here
       Step 1: register L1 reader
       Step 2: write manifest
       Step 3: mark L0 for deletion
       Step 4: update _l0_order, _l1_file_id
       Step 5: invalidate cache
       release L1.write_lock
       release L0.write_lock  ←──── WRITE LOCK RELEASED HERE
     [LOCK NOT HELD]

Total time under write lock: ~1–5ms
```

**What the write lock guarantees:** No `get()` is mid-scan on level N when we atomically swap its contents. It does **not** block reads during the merge itself — that is intentional. The subprocess reads from files that are still in the registry and fully accessible.

**What the read lock protects in `get()`:** The read lock is held for the duration of scanning one level — released between L0 and L1 scans. This means a compaction touching only L1 does not block L0 reads.

The write lock scope is as narrow as possible — milliseconds, not seconds. The seconds-long merge runs with no lock at all.

### 2.5 L1 is always fully replaced — never appended

When L0 compacts into L1, the output **merges with the existing L1 file** and replaces it entirely. A new L1 file is never added alongside the old one. This is mandatory for correctness:

```
Inputs to every L0→L1 compaction:
    ALL current L0 files          (overlapping key ranges, multiple files)
  + existing L1 file (if any)     (the previous compaction output)

Output:
    ONE new L1 file               (fully merged, deduplicated, sorted)

After commit:
    Old L0 files   → marked for deletion
    Old L1 file    → marked for deletion
    New L1 file    → registered as the sole L1 SSTable
```

**Why including old L1 is required:**

If the old L1 contains `key=b"foo", seq=100, value=b"old"` and L0 contains a tombstone `key=b"foo", seq=600`, compacting L0 alone produces a new L1 with just the tombstone. The old L1 remains as a second L1 file with the un-deleted value. Now two L1 files overlap — the single-file-per-level invariant is broken, reads become incorrect, and tombstone GC silently resurrects deleted keys.

By including the old L1 in the merge, the `KWayMergeIterator` produces the correct single output: the tombstone at seq=600 wins, the old value at seq=100 is deduplicated away.

**Tombstone cutoff** when old L1 is included: `seq_cutoff = old_l1.seq_min`. Tombstones with `seq < old_l1.seq_min` predate every record in the old L1, which is now fully consumed. They have nothing left to shadow — safe to GC. If no L1 exists (first compaction), `seq_cutoff = 0` — preserve all tombstones conservatively.

### 2.6 Compaction task tracking

Two levels of tracking — neither requires on-disk persistence:

**In-memory runtime state** in `CompactionManager`:
- `_active_levels: set[Level]` — which levels are currently reserved (drives scheduling)
- `_active_jobs: dict[tuple[Level, Level], asyncio.Task]` — maps level pair to running task (observability, shutdown)

Both are rebuilt from scratch on every startup — correct because no jobs survive a process restart.

**On-disk state** — the manifest is sufficient. No "compaction in progress" marker is needed. Every crash scenario is recoverable through the `meta.json`-as-completeness-signal and manifest-as-authoritative-state already established:
- Crash during merge → partial L1 dir (no `meta.json`) → skipped by `load()`
- Crash after merge, before commit → orphan L1 loaded alongside old L0 → highest seq wins
- Crash after commit → manifest shows new L1, old L0 cleaned up on next startup

**Optional audit trail** — `compaction.log` in the data root. Append-only structured JSON, one line per event (`started`, `committed`). Never read by the engine. No correctness dependency. Useful for tailing live compaction activity.

### 2.7 Subprocess isolation

The actual merge runs in a `ProcessPoolExecutor` subprocess via `asyncio.to_thread`. This means:
- The event loop stays alive during the entire merge — puts, gets, and flushes are unaffected
- The merge runs on a separate CPU core with its own GIL instance
- A subprocess crash cannot corrupt engine state
- The subprocess receives only serialisable data (`CompactionTask` dataclass with paths as strings)
- The subprocess opens its own file handles (`_open_reader_sync`) — it does not use the registry

### 2.8 The re-trigger after compaction completes

After a job finishes and releases its level reservations, `CompactionManager` calls `check_and_compact()` again. This handles the case where L0 refilled while the previous compaction was running:

```
Compaction runs:       merging [L0a, L0b, L0c, L0d]
Concurrent flushes:    L0e, L0f, L0g, L0h added (each triggers check, all skipped — level locked)
Compaction finishes:   _active_levels cleared
Re-trigger fires:      l0_count=4 ≥ threshold → second compaction starts ✓
```

---

## PART 3 — COMMIT SEQUENCE AND CRASH SAFETY

### 3.1 The non-negotiable commit ordering

Inside `commit_compaction_async()`, under write locks on both src and dst levels:

```
Step 1: registry.register(new_l1_file_id, reader)     ← L1 becomes readable
Step 2: manifest.write(new_layout)                     ← durable on disk (os.replace)
Step 3: registry.mark_for_deletion(old_l0_file_ids)   ← deferred, ref-counted
Step 4: update _l0_order, _l1_file_id in memory       ← in-memory state
Step 5: cache.invalidate() for each old L0 file        ← evict stale blocks
```

**Step 1 must come before Step 3.** If inverted, there is a window where L0 files are removed from the manifest but L1 is not yet registered — a `get()` in that window would find nothing (phantom miss).

### 3.2 Crash recovery for every scenario

| Crash point | On-disk state | Recovery action |
|---|---|---|
| During subprocess write | L1 dir has `data.bin` but no `meta.json` | `load()` skips it (no `meta.json` = incomplete). Old L0 files in manifest → loaded normally. |
| After `finish_sync()`, before Step 1 | L1 dir complete on disk, not in manifest | L1 loaded as orphan (sorted by UUIDv7). Old L0 still in manifest → also loaded. Highest-seq wins. |
| After Step 1, before Step 2 | L1 registered in memory (lost on crash) | Recovery loads manifest (old L0 files). L1 dir has `meta.json` → loaded as orphan. Both loaded, dedup by seq. |
| After Step 2, before Step 3 | Manifest shows L1 + smaller L0. Old L0 dirs still on disk. | Manifest is authoritative. L1 loaded. Old L0 dirs on disk but not in manifest → orphan path. Safe. |
| After full commit | Clean | Normal startup. |

---

## PART 4 — COMPLETE FILE LIST AND CHANGES

### New files to create

```
app/common/rwlock.py              — AsyncRWLock
app/common/merge_iterator.py      — KWayMergeIterator, MergeRecord
app/compaction/__init__.py        — (empty)
app/compaction/task.py            — CompactionTask dataclass
app/compaction/worker.py          — run_compaction(), _open_reader_sync()
app/engine/compaction_manager.py  — CompactionManager
tests/test_rwlock.py
tests/test_merge_iterator.py
tests/test_compaction_worker.py
tests/test_compaction_manager.py
```

### Existing files to modify

```
app/common/errors.py              — add CompactionError
app/cache/block.py                — add invalidate_all(file_ids)
app/sstable/reader.py             — add iter_sorted()
app/engine/sstable_manager.py     — extend Manifest, add L1 state + rwlocks,
                                    add commit_compaction_async(), update get()
app/engine/flush_pipeline.py      — add compaction param, fire trigger in _commit_slot
app/engine/lsm_engine.py          — wire CompactionManager, startup check, no close() changes
app/engine/config.py              — add compaction_max_workers key
```

---

## PART 5 — DETAILED IMPLEMENTATION (per file)

### Phase 1: `app/common/rwlock.py`

```python
"""AsyncRWLock — async readers-writer lock with writer preference.

Writer preference: once a writer is waiting, new readers block.
This prevents writer starvation under continuous read load.

Usage:
    lock = AsyncRWLock()

    async with lock.read_lock():
        # multiple readers can be here simultaneously
        ...

    async with lock.write_lock():
        # exclusive access — waits for readers, blocks new ones
        ...
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator


class AsyncRWLock:
    def __init__(self) -> None:
        self._condition      = asyncio.Condition()
        self._readers        = 0       # active reader count
        self._writer_active  = False   # True while a writer holds the lock
        self._writer_waiting = 0       # writers waiting to acquire

    @contextlib.asynccontextmanager
    async def read_lock(self) -> AsyncIterator[None]:
        """Acquire for reading. Blocked while any writer is waiting or active."""
        async with self._condition:
            await self._condition.wait_for(
                lambda: not self._writer_active and self._writer_waiting == 0
            )
            self._readers += 1
        try:
            yield
        finally:
            async with self._condition:
                self._readers -= 1
                if self._readers == 0:
                    self._condition.notify_all()

    @contextlib.asynccontextmanager
    async def write_lock(self) -> AsyncIterator[None]:
        """Acquire for writing. Exclusive — waits for all readers to finish."""
        async with self._condition:
            self._writer_waiting += 1
            await self._condition.wait_for(
                lambda: self._readers == 0 and not self._writer_active
            )
            self._writer_waiting -= 1
            self._writer_active = True
        try:
            yield
        finally:
            async with self._condition:
                self._writer_active = False
                self._condition.notify_all()
```

### Phase 2: `app/common/merge_iterator.py`

```python
"""KWayMergeIterator — merge N sorted iterators into one globally sorted stream.

Used by:
  - compaction: merge multiple L0 SSTables into one L1 file
  - range queries (future): merge memtable + SSTable results

Ordering:   sorted by key ascending
Dedup:      for equal keys, highest seq wins — lower-seq records dropped
Tombstones: configurable via skip_tombstones and seq_cutoff
"""

from __future__ import annotations

import heapq
from collections.abc import Iterator
from dataclasses import dataclass, field

from app.types import Key, SeqNum, Value, TOMBSTONE


@dataclass(order=False)
class MergeRecord:
    """One record in the merge heap."""
    key:          Key
    seq:          SeqNum
    timestamp_ms: int
    value:        Value
    source_idx:   int

    def __lt__(self, other: MergeRecord) -> bool:
        # Min-heap: smallest key first.
        # For equal keys: highest seq first (newest = highest priority = smallest in heap).
        if self.key != other.key:
            return self.key < other.key
        return self.seq > other.seq  # higher seq = higher priority = smaller in heap

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MergeRecord):
            return NotImplemented
        return self.key == other.key and self.seq == other.seq


class KWayMergeIterator:
    """
    Merge N sorted iterators into one globally sorted stream.

    Parameters
    ----------
    iterators:
        List of iterators each yielding (Key, SeqNum, int, Value) in
        strictly ascending key order.
    seq_cutoff:
        Tombstones with seq < seq_cutoff are dropped entirely (GC).
        Pass 0 to preserve all tombstones (safe default).
    skip_tombstones:
        If True, tombstoned keys are omitted from output (for range queries).
        If False, tombstones above seq_cutoff are preserved (for compaction).
    deduplicate:
        If True (default), only the highest-seq record per key is yielded.
    """

    def __init__(
        self,
        iterators:       list[Iterator[tuple[Key, SeqNum, int, Value]]],
        seq_cutoff:      SeqNum = 0,
        skip_tombstones: bool   = False,
        deduplicate:     bool   = True,
    ) -> None:
        self._iters          = list(iterators)
        self._seq_cutoff     = seq_cutoff
        self._skip_tombstones = skip_tombstones
        self._deduplicate    = deduplicate
        self._heap:           list[MergeRecord] = []
        self._last_key:       Key | None = None

    def _init_heap(self) -> None:
        for idx, it in enumerate(self._iters):
            self._push_next(idx)

    def _push_next(self, source_idx: int) -> None:
        try:
            key, seq, ts, value = next(self._iters[source_idx])
            heapq.heappush(
                self._heap,
                MergeRecord(key=key, seq=seq, timestamp_ms=ts,
                            value=value, source_idx=source_idx),
            )
        except StopIteration:
            pass

    def __iter__(self) -> Iterator[tuple[Key, SeqNum, int, Value]]:
        self._init_heap()

        while self._heap:
            rec = heapq.heappop(self._heap)

            # Dedup: skip lower-seq records for an already-yielded key
            if self._deduplicate and rec.key == self._last_key:
                self._push_next(rec.source_idx)
                continue

            self._last_key = rec.key

            # Tombstone handling
            if rec.value == TOMBSTONE:
                if self._skip_tombstones:
                    # Range query: key is deleted — skip entirely
                    self._push_next(rec.source_idx)
                    continue
                if rec.seq < self._seq_cutoff:
                    # Compaction GC: safe to drop
                    self._push_next(rec.source_idx)
                    continue
                # Compaction: preserve tombstone (seq >= cutoff or cutoff == 0)

            yield (rec.key, rec.seq, rec.timestamp_ms, rec.value)
            self._push_next(rec.source_idx)
```

### Phase 3: `app/compaction/task.py`

```python
"""CompactionTask — serialisable description of one compaction job.

All fields are primitive types (str, int, list, dict) so the task
can be pickled across the ProcessPoolExecutor subprocess boundary.
No live objects, no file handles, no locks.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.types import FileID, Level, SeqNum


@dataclass(frozen=True)
class CompactionTask:
    task_id:         str               # uuid7 hex — for logging and audit chain
    input_file_ids:  list[FileID]      # files to merge, newest first
    input_dirs:      dict[FileID, str] # file_id → absolute directory path (str, not Path)
    output_file_id:  FileID            # uuid7 hex — the new output file
    output_dir:      str               # absolute path for output SSTable directory
    output_level:    Level             # 1 for L0→L1
    seq_cutoff:      SeqNum            # tombstones with seq < this are GC'd (0 = keep all)
```

### Phase 4: `app/compaction/worker.py`

```python
"""Compaction subprocess worker.

run_compaction() is a pure function — receives file paths, writes one
output SSTable, returns SSTableMeta. No asyncio, no shared state, no locks.
Runs in a ProcessPoolExecutor subprocess with its own GIL instance.

_open_reader_sync() replicates SSTableReader.open() without asyncio,
for use where no event loop exists.
"""

from __future__ import annotations

import mmap
import os
from pathlib import Path

from app.bloom.filter import BloomFilter
from app.common.merge_iterator import KWayMergeIterator
from app.compaction.task import CompactionTask
from app.index.sparse import SparseIndex
from app.sstable.meta import SSTableMeta
from app.sstable.reader import SSTableReader
from app.sstable.writer import SSTableWriter


def run_compaction(task: CompactionTask) -> SSTableMeta:
    """
    Pure function: merge all input SSTables into one output SSTable.

    Called in a subprocess — no event loop, no registry, no cache.
    Opens its own file handles via _open_reader_sync().
    Uses finish_sync() because blocking is fine in a subprocess.
    """
    readers: list[SSTableReader] = []
    try:
        for file_id in task.input_file_ids:
            sst_dir = Path(task.input_dirs[file_id])
            reader = _open_reader_sync(sst_dir, file_id)
            readers.append(reader)

        merge_iter = KWayMergeIterator(
            iterators       = [r.iter_sorted() for r in readers],
            seq_cutoff      = task.seq_cutoff,
            skip_tombstones = False,  # compaction preserves tombstones above cutoff
            deduplicate     = True,
        )

        output_dir = Path(task.output_dir)
        writer = SSTableWriter(
            directory   = output_dir,
            file_id     = task.output_file_id,
            snapshot_id = task.task_id,  # task_id as provenance in audit chain
            level       = task.output_level,
        )

        for key, seq, ts, value in merge_iter:
            writer.put(key, seq, ts, value)

        # finish_sync() — blocking is fine in subprocess
        return writer.finish_sync()

    finally:
        for reader in readers:
            reader.close()


def _open_reader_sync(directory: Path, file_id: str) -> SSTableReader:
    """Open an SSTableReader synchronously — no event loop required."""
    meta = SSTableMeta.from_json((directory / "meta.json").read_text())
    index = SparseIndex.from_bytes((directory / meta.index_file).read_bytes())
    bloom = BloomFilter.from_bytes((directory / meta.filter_file).read_bytes())

    data_path = directory / meta.data_file
    fd = os.open(str(data_path), os.O_RDONLY)
    try:
        size = os.fstat(fd).st_size
        mm: mmap.mmap | None = (
            mmap.mmap(fd, 0, access=mmap.ACCESS_READ) if size > 0 else None
        )
    except Exception:
        os.close(fd)
        raise

    return SSTableReader(
        directory=directory,
        file_id=file_id,
        meta=meta,
        index=index,
        bloom=bloom,
        cache=None,   # no block cache in subprocess
        mm=mm,
        fd=fd,
    )
```

### Phase 5: `app/engine/compaction_manager.py`

```python
"""CompactionManager — flush-triggered compaction with level-reservation scheduling.

No daemon loop. Triggered by FlushPipeline after each SSTable commit.
Concurrent non-conflicting jobs run in parallel; conflicting jobs are skipped
and re-tried after the in-progress job finishes.

Scheduling rules:
  - _active_levels tracks all levels currently in a running job.
  - A job (src→dst) can only start if neither src nor dst is in _active_levels.
  - check-and-reserve is atomic under _reservation_lock.
  - After each job, check_and_compact() re-runs to handle any accumulated backlog.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING

from app.common.uuid7 import uuid7_hex
from app.compaction.task import CompactionTask
from app.compaction.worker import run_compaction
from app.observability import get_logger
from app.sstable.meta import SSTableMeta
from app.sstable.reader import SSTableReader
from app.types import Level

if TYPE_CHECKING:
    from app.engine.config import LSMConfig
    from app.engine.sstable_manager import SSTableManager

logger = get_logger(__name__)


class CompactionManager:

    def __init__(
        self,
        sst:       SSTableManager,
        config:    LSMConfig,
        data_root: Path,
    ) -> None:
        self._sst             = sst
        self._config          = config
        self._data_root       = data_root
        self._reservation_lock = asyncio.Lock()
        self._active_levels:  set[Level] = set()
        # Tracks running tasks by level pair — for observability and shutdown.
        # Lost on restart (correct — no jobs survive a restart).
        self._active_jobs: dict[tuple[Level, Level], asyncio.Task[None]] = {}

    # ── public entry point ─────────────────────────────────────────────

    async def check_and_compact(self) -> None:
        """
        Entry point. Called after every flush commit and after each
        completed compaction job.

        Finds all eligible jobs, reserves their levels atomically,
        and launches eligible ones concurrently as independent tasks.
        Conflicting or already-running levels are skipped — they will
        be retried when the in-progress job calls check_and_compact()
        on completion.
        """
        jobs = self._find_eligible_jobs()
        if not jobs:
            return

        for src, dst in jobs:
            reserved = await self._try_reserve(src, dst)
            if reserved:
                task = asyncio.create_task(self._run_job(src, dst))
                self._active_jobs[(src, dst)] = task  # track for observability

    # ── job lifecycle ──────────────────────────────────────────────────

    async def _run_job(self, src: Level, dst: Level) -> None:
        """Run one compaction job from src to dst level."""
        try:
            await self._run_one_compaction(src, dst)
        except Exception:
            logger.exception(
                "Compaction job failed",
                src_level=src, dst_level=dst,
            )
        finally:
            await self._release(src, dst)
            self._active_jobs.pop((src, dst), None)  # remove when done

        # Re-trigger: handles the case where L0 refilled during this compaction
        await self.check_and_compact()

    async def _run_one_compaction(self, src: Level, dst: Level) -> None:
        task = self._build_task(src, dst)
        if task is None:
            return

        logger.info(
            "Compaction started",
            task_id=task.task_id,
            src_level=src, dst_level=dst,
            input_count=len(task.input_file_ids),
            seq_cutoff=task.seq_cutoff,
        )
        self._append_compaction_log("started", task)

        # Phase 1: run merge in subprocess — event loop stays free
        new_meta: SSTableMeta = await asyncio.to_thread(
            self._run_in_subprocess, task
        )

        logger.info(
            "Compaction merge done",
            task_id=task.task_id,
            output_records=new_meta.record_count,
            output_bytes=new_meta.size_bytes,
        )

        # Phase 2: open reader for output SSTable
        new_reader = await SSTableReader.open(
            Path(task.output_dir),
            task.output_file_id,
            self._sst._cache,
            level=dst,
        )

        # Phase 3: atomic commit (acquires write locks on src + dst levels)
        await self._sst.commit_compaction_async(task, new_meta, new_reader)

        self._append_compaction_log(
            "committed", task,
            output_records=new_meta.record_count,
            output_bytes=new_meta.size_bytes,
        )
        logger.info(
            "Compaction committed",
            task_id=task.task_id,
            l0_remaining=self._sst.l0_count,
        )

    # ── level reservation ──────────────────────────────────────────────

    def _find_eligible_jobs(self) -> list[tuple[Level, Level]]:
        """Return eligible (src, dst) pairs ordered by priority (lower levels first)."""
        jobs: list[tuple[Level, Level]] = []
        if self._sst.l0_count >= int(self._config.l0_compaction_threshold):
            jobs.append((0, 1))
        # Future: L1→L2, L2→L3 based on size thresholds
        return jobs

    async def _try_reserve(self, src: Level, dst: Level) -> bool:
        """Atomically check and reserve levels. Returns True if successful."""
        async with self._reservation_lock:
            if src in self._active_levels or dst in self._active_levels:
                logger.debug(
                    "Compaction reservation skipped — level in use",
                    src=src, dst=dst, active=sorted(self._active_levels),
                )
                return False
            self._active_levels.add(src)
            self._active_levels.add(dst)
            logger.debug(
                "Compaction levels reserved",
                src=src, dst=dst, active=sorted(self._active_levels),
            )
            return True

    async def _release(self, src: Level, dst: Level) -> None:
        """Release reserved levels."""
        async with self._reservation_lock:
            self._active_levels.discard(src)
            self._active_levels.discard(dst)
            logger.debug(
                "Compaction levels released",
                src=src, dst=dst, active=sorted(self._active_levels),
            )

    # ── task construction ──────────────────────────────────────────────

    def _build_task(self, src: Level, dst: Level) -> CompactionTask | None:
        """
        Snapshot current input files under _state_lock.
        New flushes after this point are not part of this compaction.

        CRITICAL: the existing L1 file (if any) is included in the merge
        inputs. Compaction always produces ONE new L1 file that fully
        replaces the old one — it is never appended alongside it.
        Including L1 in the merge ensures:
          - No data from the old L1 is lost
          - Tombstones in L0 correctly shadow old L1 values
          - The single-file-per-level invariant is maintained
        """
        with self._sst._state_lock:
            if src == 0:
                if not self._sst._l0_order:
                    return None

                # Start with all current L0 files (snapshot — new flushes
                # may prepend to _l0_order after this point; they are NOT
                # included and will be handled by the next compaction cycle)
                input_file_ids = list(self._sst._l0_order)
                input_dirs = {
                    fid: str(self._sst._l0_dirs[fid])
                    for fid in input_file_ids
                }

                # Include the existing L1 file so it is fully merged into
                # the new L1. Without this, the old L1 would survive as a
                # second L1 file, violating the one-file-per-level invariant
                # and breaking tombstone correctness.
                existing_l1 = self._sst._l1_file_id
                if existing_l1 is not None and self._sst._l1_dir is not None:
                    input_file_ids.append(existing_l1)
                    input_dirs[existing_l1] = str(self._sst._l1_dir)
            else:
                raise NotImplementedError(f"L{src}→L{dst} compaction not yet implemented")

            # Tombstone cutoff: seq_min of the existing L1 file.
            # Any tombstone with seq < seq_cutoff predates every record in
            # the old L1, which is now fully consumed — safe to GC.
            # If no L1 exists (first compaction), seq_cutoff=0: preserve all
            # tombstones conservatively.
            seq_cutoff = self._sst.level_seq_min(dst)

        output_file_id = uuid7_hex()
        output_dir = self._data_root / "sstable" / f"L{dst}" / output_file_id

        return CompactionTask(
            task_id        = uuid7_hex(),
            input_file_ids = input_file_ids,
            input_dirs     = input_dirs,
            output_file_id = output_file_id,
            output_dir     = str(output_dir),
            output_level   = dst,
            seq_cutoff     = seq_cutoff,
        )

    @staticmethod
    def _run_in_subprocess(task: CompactionTask) -> SSTableMeta:
        with ProcessPoolExecutor(max_workers=1) as pool:
            return pool.submit(run_compaction, task).result()

    # ── observability ──────────────────────────────────────────────────

    def _append_compaction_log(
        self, event: str, task: CompactionTask, **kwargs: object
    ) -> None:
        """
        Append one structured line to data_root/compaction.log.
        Append-only, never read by the engine — purely for operational
        observability. No correctness dependency.
        """
        import datetime, json
        entry = {
            "ts":      datetime.datetime.utcnow().isoformat(),
            "event":   event,
            "task_id": task.task_id,
            "src":     task.output_level - 1,
            "dst":     task.output_level,
            "inputs":  task.input_file_ids,
            "output":  task.output_file_id,
            **{k: v for k, v in kwargs.items()},
        }
        try:
            log_path = self._data_root / "compaction.log"
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except OSError as exc:
            logger.warning("Failed to write compaction.log", error=str(exc))

    @property
    def active_jobs(self) -> dict[tuple[Level, Level], asyncio.Task[None]]:
        """Currently running compaction jobs — for stats and shutdown."""
        return dict(self._active_jobs)
```

### Phase 6: Modifications to existing files

#### `app/common/errors.py` — add CompactionError

```python
class CompactionError(LSMError):
    """Compaction job failed — subprocess error or I/O failure."""
```

#### `app/cache/block.py` — add `invalidate_all`

```python
def invalidate_all(self, file_ids: list[FileID]) -> None:
    """Evict all blocks belonging to any of the given file IDs."""
    ids = set(file_ids)
    with self._lock:
        keys_to_remove = [k for k in self._cache if k[0] in ids]
        for k in keys_to_remove:
            del self._cache[k]
```

#### `app/sstable/reader.py` — add `iter_sorted`

```python
def iter_sorted(self) -> Iterator[tuple[Key, SeqNum, int, Value]]:
    """
    Yield all records in ascending key order without materialising.
    Used as input to KWayMergeIterator during compaction.
    More memory-efficient than scan_all() for large SSTables.
    """
    if self._mm is None or self._meta.size_bytes == 0:
        return
    mv = memoryview(self._mm)
    for rec in iter_block(mv, 0, self._meta.size_bytes):
        yield (rec.key, rec.seq, rec.timestamp_ms, rec.value)
```

Add `from collections.abc import Iterator` to imports.

#### `app/engine/sstable_manager.py` — full changes

**1. Extend `Manifest` to store L1:**

```python
# Manifest.read() → returns dict[str, object]
def read(self) -> dict[str, object]:
    if not self._path.exists():
        return {"l0_order": [], "l1_file": None}
    try:
        data = json.loads(self._path.read_text(encoding="utf-8"))
        # backward-compat: old files only have l0_order
        if "l0_order" not in data:
            data = {"l0_order": data if isinstance(data, list) else [], "l1_file": None}
        data.setdefault("l1_file", None)
        return data
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Manifest corrupt, rebuilding", error=str(exc))
        return {"l0_order": [], "l1_file": None}

# Manifest.write() accepts full layout dict
def write(self, layout: dict[str, object]) -> None:
    data = json.dumps(layout, indent=2) + "\n"
    # ... same temp + os.replace() pattern ...
```

**2. Add new state fields to `SSTableManager.__init__`:**

```python
from app.common.rwlock import AsyncRWLock

# In __init__:
self._l1_file_id: FileID | None = None
self._l1_dir:     Path | None   = None
self._level_rwlocks: dict[Level, AsyncRWLock] = {}
```

**3. Update `SSTableManager.load()` to load L1:**

```python
layout = manifest.read()
# l0_order loading stays the same (use layout["l0_order"])
# After loading L0 readers, also load L1 if present:
l1_file_id: FileID | None = layout.get("l1_file")
if l1_file_id:
    l1_dir = data_root / "sstable" / "L1" / l1_file_id
    if (l1_dir / "meta.json").exists():
        try:
            l1_reader = await SSTableReader.open(l1_dir, l1_file_id, cache, level=1)
            registry.register(l1_file_id, l1_reader)
            mgr._l1_file_id = l1_file_id
            mgr._l1_dir = l1_dir
            if l1_reader.meta.seq_max > mgr._max_seq:
                mgr._max_seq = l1_reader.meta.seq_max
        except Exception:
            logger.exception("Failed to open L1 SSTable", file_id=l1_file_id)
```

**4. Add `_level_lock()` helper:**

```python
def _level_lock(self, level: Level) -> AsyncRWLock:
    if level not in self._level_rwlocks:
        self._level_rwlocks[level] = AsyncRWLock()
    return self._level_rwlocks[level]
```

**5. Add `level_seq_min()` helper:**

```python
def level_seq_min(self, level: Level) -> SeqNum:
    """Return seq_min of the existing SSTable at `level`, or 0 if none."""
    if level == 1 and self._l1_file_id is not None:
        try:
            with self._registry.open_reader(self._l1_file_id) as reader:
                return reader.meta.seq_min
        except KeyError:
            pass
    return 0
```

**6. Update `get()` to hold read locks and check L1:**

```python
async def get(self, key: Key) -> tuple[SeqNum, int, Value] | None:
    with self._state_lock:
        l0_snapshot = list(self._l0_order)
        l1_file_id  = self._l1_file_id

    best: tuple[SeqNum, int, Value] | None = None

    # L0: hold read lock while scanning — allows concurrent reads,
    # blocks compaction commit that needs write lock
    async with self._level_lock(0).read_lock():
        for file_id in l0_snapshot:
            try:
                with self._registry.open_reader(file_id) as reader:
                    result = reader.get(key)
            except KeyError:
                continue
            if result is not None and (best is None or result[0] > best[0]):
                best = result

    # L1: hold read lock while scanning
    if l1_file_id is not None:
        async with self._level_lock(1).read_lock():
            try:
                with self._registry.open_reader(l1_file_id) as reader:
                    result = reader.get(key)
            except KeyError:
                result = None
            if result is not None and (best is None or result[0] > best[0]):
                best = result

    return best
```

**7. Add `commit_compaction_async()`:**

```python
async def commit_compaction_async(
    self,
    task:       CompactionTask,
    new_meta:   SSTableMeta,
    new_reader: SSTableReader,
) -> None:
    """
    Atomically commit a compaction result.

    Acquires write locks on src and dst levels — waits for any in-flight
    reads on those levels to finish before proceeding.

    Ordering is non-negotiable:
      1. Register L1 reader  (L1 becomes readable)
      2. Write manifest      (durable)
      3. Mark L0 for deletion (deferred by ref-count)
      4. Update in-memory state
      5. Evict stale cache blocks
    """
    src_level = task.output_level - 1
    dst_level = task.output_level

    # Acquire write locks on both levels — always in ascending order (no deadlock)
    async with self._level_lock(src_level).write_lock():
        async with self._level_lock(dst_level).write_lock():

            with self._state_lock:
                # Step 1: Register new L1 reader
                self._registry.register(new_meta.file_id, new_reader)

                # Step 2: Write manifest atomically
                new_l0 = [f for f in self._l0_order
                           if f not in set(task.input_file_ids)]
                old_l1 = self._l1_file_id
                self._manifest.write({
                    "l0_order": new_l0,
                    "l1_file":  new_meta.file_id,
                })

                # Step 3: Mark old files for deferred deletion
                for fid in task.input_file_ids:
                    self._registry.mark_for_deletion(fid)
                if old_l1 is not None:
                    self._registry.mark_for_deletion(old_l1)

                # Step 4: Update in-memory state
                self._l0_order   = new_l0
                self._l1_file_id = new_meta.file_id
                self._l1_dir     = Path(task.output_dir)
                if new_meta.seq_max > self._max_seq:
                    self._max_seq = new_meta.seq_max

                # Step 5: Evict stale cache blocks for all removed files
                self._cache.invalidate_all(task.input_file_ids)
                # Note: task.input_file_ids already includes old_l1 (if it existed),
                # so one invalidate_all call covers both L0 files and the old L1.

    logger.info(
        "Compaction commit done",
        l0_removed=len(task.input_file_ids),
        l0_remaining=len(self._l0_order),
        l1_file=new_meta.file_id,
    )
```

#### `app/engine/flush_pipeline.py` — add compaction trigger

**In `__init__`:**
```python
from app.engine.compaction_manager import CompactionManager  # TYPE_CHECKING

def __init__(
    self,
    mem: MemTableManager,
    sst: SSTableManager,
    wal: WALManager,
    max_workers: int = 2,
    compaction: CompactionManager | None = None,   # NEW
) -> None:
    ...
    self._compaction = compaction
```

**In `_commit_slot()`, after WAL truncation:**
```python
# Trigger compaction check — non-blocking, independent task
if self._compaction is not None:
    asyncio.create_task(self._compaction.check_and_compact())

slot.my_committed.set()
```

#### `app/engine/lsm_engine.py` — wire CompactionManager

**In `__init__`:**
```python
self._compaction: CompactionManager | None = None
```

**In `open()`, after creating the flush pipeline:**
```python
from app.engine.compaction_manager import CompactionManager

engine._compaction = CompactionManager(
    sst       = engine._sst,
    config    = engine._config,
    data_root = engine._data_root,
)
engine._pipeline = FlushPipeline(
    mem        = engine._mem,
    sst        = engine._sst,
    wal        = engine._wal,
    max_workers= max_workers,
    compaction = engine._compaction,  # wire in
)
engine._pipeline_task = asyncio.create_task(engine._pipeline.run())
engine._pipeline_task.add_done_callback(engine._on_pipeline_done)

# Startup check: L0 may already be at threshold after recovery
if engine._sst.l0_count >= int(engine._config.l0_compaction_threshold):
    asyncio.create_task(engine._compaction.check_and_compact())
```

**In `close()` — no changes needed.** The flush pipeline drains completely. Any in-flight compaction task either completes or is abandoned (it holds no write locks that block shutdown since `close_all()` on the registry handles the rest).

#### `app/engine/config.py` — add `compaction_max_workers`

Add to `_DEFAULTS`:
```python
"compaction_max_workers": 1,   # subprocess slots; 1 per NVMe is usually optimal
```

---

## PART 6 — INTERACTION SCENARIOS

### Scenario A: Normal compaction — first run (no existing L1)
```
10 flushes complete → l0_count=10 → threshold hit
_build_task():
    input_file_ids = [L0j...L0a]   (10 L0 files)
    no existing L1 → not included
    seq_cutoff = 0                  (no L1 → preserve all tombstones)
Subprocess merges 10 L0 files → new L1_v1 written
commit_compaction_async():
    registers L1_v1
    manifest = { l0_order: [], l1_file: "L1_v1" }
    marks L0a..L0j for deletion
Result: L0 empty, L1_v1 is the sole L1 file ✓

### Scenario A2: Second compaction run (L1 already exists)
10 more flushes complete → l0_count=10 → threshold hit
_build_task():
    input_file_ids = [L0t...L0k, L1_v1]  (10 L0 files + existing L1)
    seq_cutoff = L1_v1.seq_min            (tombstones older than L1 are GC'd)
Subprocess merges all 11 inputs → new L1_v2 written
    KWayMergeIterator deduplicates across all 11 files
    L0 tombstones shadow old L1 values correctly
commit_compaction_async():
    registers L1_v2
    manifest = { l0_order: [], l1_file: "L1_v2" }
    marks L0k..L0t AND L1_v1 for deletion
Result: L0 empty, L1_v2 is the sole L1 file (fully merged replacement) ✓
```

### Scenario B: Concurrent L0→L1 and new flush batch
```
Compaction running: _active_levels = {0, 1}
New flush batch arrives: 4 more flushes commit
  Each commit fires check_and_compact()
  Each sees level 0 in _active_levels → reservation fails → skipped
Compaction finishes → _release(0,1) → re-trigger
  → l0_count=4 → _try_reserve(0,1) succeeds → second compaction starts ✓
```

### Scenario C: L0→L1 and L1→L2 (future)
```
L0→L1 running: _active_levels = {0, 1}
L1→L2 tries: 1 in active → blocked
L0→L1 finishes → _release(0,1) → re-trigger
  → check_and_compact finds L1→L2 needed → _try_reserve(1,2) → succeeds ✓
```

### Scenario D: L1→L2 and L3→L4 (future, parallel)
```
L1→L2 running: _active_levels = {1, 2}
L3→L4 check: neither 3 nor 4 in active → _try_reserve(3,4) succeeds
Both run concurrently on separate CPU cores ✓
```

### Scenario E: Concurrent get() and compaction commit
```
get(b"foo") starts:
  acquires L0 read_lock → _readers[L0] = 1
  snapshots l0_order = [L0a, L0b, ..., L0j]
  begins scanning L0a...

commit_compaction_async() starts:
  acquires L0 write_lock:
    _writer_waiting = 1 → new reads on L0 block
    wait for _readers[L0] == 0 → WAITS

get(b"foo") finishes L0 scan:
  releases L0 read_lock → _readers[L0] = 0 → notifies condition

commit_compaction_async() proceeds:
  holds L0 write_lock, acquires L1 write_lock
  registers L1, writes manifest, marks L0 for deletion
  releases both write locks

New get(b"bar") starts:
  acquires L0 read_lock (no writer waiting) → proceeds
  snapshot: l0_order=[] (cleared), l1_file_id=L1_new
  scans L1 → finds key ✓
```

---

## PART 7 — COMPLETE LOCK INVENTORY

| Primitive | Lives in | Type | Protects | Rule |
|---|---|---|---|---|
| `_write_lock` | `MemTableManager` | `threading.RLock` | `put()`, `maybe_freeze()` | Never hold across `await` |
| `_seq_lock` | `SeqGenerator` | `threading.Lock` | `_val += 1` | Never hold across `await` |
| `_queue_not_full` | `MemTableManager` | `threading.Condition` | Backpressure wait | — |
| `_state_lock` | `SSTableManager` | `threading.Lock` | `_l0_order`, `_l1_file_id`, `_max_seq` | Never hold across `await` |
| `_wal_lock` | `WALManager` | `threading.Lock` | WAL file ops | Never hold across `await` |
| `_registry._lock` | `SSTableRegistry` | `threading.Lock` | Ref counts + readers | Never hold across `await` (held briefly) |
| `_semaphore` | `FlushPipeline` | `asyncio.Semaphore` | Concurrent flush writes | No threading lock while held |
| `prev_committed` / `my_committed` | `FlushSlot` | `asyncio.Event` | Flush commit ordering | — |
| `_reservation_lock` | `CompactionManager` | `asyncio.Lock` | `_active_levels` set | No threading lock while awaiting |
| `_level_rwlocks[N]` | `SSTableManager` | `AsyncRWLock` | Per-level read/write access | Never hold `_state_lock` while awaiting write_lock |
| `_level_rwlocks[N]._condition` | `AsyncRWLock` | `asyncio.Condition` | Reader/writer counts | — |

**Golden rule:** No `threading.Lock` is ever held when `await`-ing any async primitive.

**Lock ordering for compaction commit:**
1. `_level_lock(src).write_lock()` — outer
2. `_level_lock(dst).write_lock()` — inner (always ascending level order)
3. `_state_lock` — innermost (threading, held briefly)

---

## PART 8 — IMPLEMENTATION ORDER AND PHASES

```
Phase 1:  app/common/rwlock.py               + tests/test_rwlock.py
Phase 2:  app/common/merge_iterator.py       + tests/test_merge_iterator.py
Phase 3:  app/compaction/__init__.py
          app/compaction/task.py
          app/compaction/worker.py           + tests/test_compaction_worker.py
Phase 4:  app/common/errors.py              (add CompactionError)
          app/cache/block.py               (add invalidate_all)
          app/sstable/reader.py            (add iter_sorted)
Phase 5:  app/engine/sstable_manager.py    (Manifest, L1 state, rwlocks,
                                            commit_compaction_async, get update)
          tests/test_sstable_manager.py    (add compaction tests)
Phase 6:  app/engine/compaction_manager.py + tests/test_compaction_manager.py
Phase 7:  app/engine/flush_pipeline.py     (add compaction param + trigger)
          app/engine/lsm_engine.py         (wire CompactionManager)
          app/engine/config.py             (add compaction_max_workers)
          tests/test_lsm_engine.py         (add end-to-end compaction tests)
```

Do not start phase N+1 until phase N passes all its tests.

---

## PART 9 — VERIFICATION CHECKLIST

Claude Code must verify every item in this checklist after completing all phases.

### Phase 1 — AsyncRWLock
- [ ] Multiple concurrent readers can hold simultaneously (no blocking between them)
- [ ] Writer waits until all active readers release
- [ ] New readers block while writer is waiting (writer preference — no starvation)
- [ ] After writer releases, readers proceed
- [ ] `read_lock` and `write_lock` are proper async context managers

### Phase 2 — KWayMergeIterator
- [ ] Two sources, no overlap → output is globally sorted
- [ ] Two sources, same key, different `seq` → highest `seq` wins
- [ ] Same key in 3 sources → only highest-seq version yielded
- [ ] `TOMBSTONE` with `skip_tombstones=True` → key absent from output
- [ ] `TOMBSTONE` with `seq < seq_cutoff` → tombstone dropped (GC'd)
- [ ] `TOMBSTONE` with `seq >= seq_cutoff` → tombstone preserved
- [ ] Empty source → ignored, other sources merge correctly
- [ ] All sources empty → iterator yields nothing
- [ ] `deduplicate=False` → all records yielded including duplicates

### Phase 3 — Compaction worker
- [ ] Merge 2 L0 files, non-overlapping keys → all keys in L1
- [ ] Merge 2 L0 files, overlapping keys → only highest-seq per key in L1
- [ ] Merge L0 files + existing L1 file → output contains records from all inputs
- [ ] L0 tombstone shadows same key in old L1 — deleted key absent from output (skip_tombstones=False but deduped)
- [ ] Tombstone with `seq_cutoff=0` → tombstone preserved in output
- [ ] Tombstone with `seq < seq_cutoff` → dropped from output
- [ ] Output `meta.json` written last (completeness signal intact)
- [ ] Reader count equals input count (L0 files + optional L1); all closed in `finally`
- [ ] `_open_reader_sync` opens file without event loop

### Phase 5 — SSTableManager changes
- [ ] `load()` reads `l1_file` from manifest; opens L1 reader if present
- [ ] `load()` backward-compatible with old manifest files (no `l1_file` key)
- [ ] `level_seq_min(1)` returns `seq_min` of L1 if present, else 0
- [ ] `invalidate_all(file_ids)` evicts blocks for all listed files
- [ ] `commit_compaction_async()` acquires write locks on both levels
- [ ] `commit_compaction_async()` registers L1 BEFORE marking L0 for deletion (no read gap)
- [ ] After commit, `_l0_order` contains only non-compacted files
- [ ] After commit, `_l1_file_id` is updated
- [ ] Manifest written atomically; old file_ids removed, new L1 added
- [ ] `get()` acquires L0 read lock for L0 scan, L1 read lock for L1 scan
- [ ] `get()` checks L1 after L0; returns highest-seq result overall
- [ ] Concurrent `get()` and `commit_compaction_async()` → `get()` sees consistent state

### Phase 6 — CompactionManager
- [ ] `check_and_compact()` is a no-op when `l0_count < threshold`
- [ ] `_try_reserve(0, 1)` returns False if level 0 or 1 already active
- [ ] `_try_reserve(0, 1)` and `_try_reserve(3, 4)` both succeed concurrently
- [ ] `_try_reserve(0, 1)` and `_try_reserve(1, 2)` conflict — second returns False
- [ ] `_release()` removes both levels from `_active_levels`
- [ ] After `_run_job()` finishes, `check_and_compact()` is called again
- [ ] `_build_task()` captures snapshot of `_l0_order` under `_state_lock`
- [ ] `_build_task()` includes existing L1 file in `input_file_ids` when L1 exists
- [ ] `_build_task()` does NOT include L1 when no L1 exists yet
- [ ] New flush after `_build_task()` does not appear in `task.input_file_ids`
- [ ] Job failure releases reservation and re-triggers check
- [ ] `_active_jobs` populated on task creation, removed in `finally`
- [ ] `active_jobs` property returns copy of `_active_jobs`
- [ ] `compaction.log` appended on `started` and `committed` events

### Phase 7 — End-to-end
- [ ] Write entries until `l0_count >= threshold` → compaction runs automatically
- [ ] After compaction: `l0_count` reduced, `l1_file` set in manifest
- [ ] `get()` returns correct values immediately after compaction
- [ ] Deleted keys remain deleted after compaction (tombstone in L1)
- [ ] Second compaction run merges L0 + existing L1 into new single L1 (old L1 deleted)
- [ ] Key written before first compaction and updated after → latest value survives second compaction
- [ ] Close engine and reopen → L1 loaded from manifest, reads correct
- [ ] Concurrent `put()` during compaction → no stall, no data loss
- [ ] Concurrent `get()` during compaction → always returns correct value
- [ ] L0 refills during compaction → second compaction triggered on completion
- [ ] `stats()` reflects updated `l0_sstable_count` after compaction
- [ ] `disk` REPL command shows L1 file after compaction
- [ ] `compaction.log` file exists after compaction with `started` and `committed` entries
- [ ] `pytest -v tests/ passes` with no failures after all phases complete