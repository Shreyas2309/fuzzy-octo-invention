# MemTable Layer — Implementation Specification
> Pass this document to Claude Code in **plan mode** before implementing any file.
> Implement in the order given in §0. Each phase is independently testable.

---

## 0. Implementation order

```
Phase 1 → app/types.py                          (type aliases + constants)
Phase 2 → app/memtable/skiplist.py              (_Node + SkipList)
Phase 3 → app/memtable/active.py                (ActiveMemTable)
Phase 4 → app/memtable/immutable.py             (ImmutableMemTable)
Phase 5 → app/engine/memtable_manager.py        (MemTableManager)
Phase 6 → tests/                                (unit tests per phase)
```

Do not start Phase N+1 until Phase N passes its tests.

---

## 1. Type aliases — `app/types.py`

### 1a. Purpose

`types.py` is the bottom of the dependency graph. It imports **nothing** from within the project — only stdlib. Every other module imports from here.

### 1b. Required definitions

```python
from __future__ import annotations

import os
import sys
import threading
from collections.abc import Iterator
from typing import Final, Protocol, TypeAlias, runtime_checkable

# ── Primitive aliases ─────────────────────────────────────────────────────

Key:        TypeAlias = bytes    # raw byte key — all keys are bytes
Value:      TypeAlias = bytes    # raw byte value — all values are bytes
SeqNum:     TypeAlias = int      # monotonically increasing write sequence number
Offset:     TypeAlias = int      # byte offset within a file
BlockSize:  TypeAlias = int      # data block size in bytes
FileID:     TypeAlias = str      # UUID4 hex — identifies one SSTable file on disk
SnapshotID: TypeAlias = str      # UUID4 hex — identifies one ImmutableMemTable snapshot
TableID:    TypeAlias = str      # UUID4 hex — identifies one ActiveMemTable instance
Level:      TypeAlias = int      # compaction level (0 = freshest)

# ── Constants ─────────────────────────────────────────────────────────────
# Tunables are configurable via environment variables with sensible defaults
# (async-io-guidelines §13). Centralised here as the single source of truth.

TOMBSTONE:               Final[bytes] = b"\x00__tomb__\x00"
BLOCK_SIZE_DEFAULT:      Final[int]   = int(os.getenv("LSM_BLOCK_SIZE", "4096"))
MAX_MEMTABLE_SIZE:       Final[int]   = int(os.getenv("LSM_MAX_MEMTABLE_SIZE", "67108864"))       # 64 MB
IMMUTABLE_QUEUE_MAX:     Final[int]   = int(os.getenv("LSM_IMMUTABLE_QUEUE_MAX", "4"))
L0_COMPACTION_THRESHOLD: Final[int]   = int(os.getenv("LSM_L0_COMPACTION_THRESHOLD", "4"))
COMPACTION_CHECK_INTERVAL: Final[float] = float(os.getenv("LSM_COMPACTION_CHECK_INTERVAL", "0.5"))  # seconds
PARALLELISM_MODE:        Final[str]   = "multiprocessing"  # fixed on Python 3.12 — not configurable

# ── Protocols ─────────────────────────────────────────────────────────────

@runtime_checkable
class BloomFilterProtocol(Protocol):
    def add(self, key: Key) -> None: ...
    def may_contain(self, key: Key) -> bool: ...
    def to_bytes(self) -> bytes: ...
    @classmethod
    def from_bytes(cls, data: bytes) -> BloomFilterProtocol: ...

@runtime_checkable
class KVIteratorProtocol(Protocol):
    def seek(self, key: Key) -> None: ...
    def __iter__(self) -> Iterator[tuple[Key, Value]]: ...
    def __next__(self) -> tuple[Key, Value]: ...
```

### 1c. Rules
- No third-party imports — stdlib + `os` only
- `TypeAlias` (not PEP 695 `type` statement) — target is Python 3.12
- `TOMBSTONE` comparison is always `value == TOMBSTONE` — never key-based
- `SeqNum` and `timestamp_ms` are always set by `LSMEngine` — lower layers accept them, never generate them
- Tunable constants (`MAX_MEMTABLE_SIZE`, `IMMUTABLE_QUEUE_MAX`, etc.) are read from environment variables with sensible defaults (async-io-guidelines §13) — centralised here as the single source of truth (rules.md §13)
- `TOMBSTONE` and `PARALLELISM_MODE` are not configurable — they are fixed protocol/platform constants

---

## 2. SkipList — `app/memtable/skiplist.py`

### 2a. Purpose

The SkipList is the in-memory sorted data structure backing `ActiveMemTable`. It provides O(log n) insert and lookup with fine-grained per-node locking, allowing concurrent writes into non-overlapping key ranges to proceed in parallel under Python 3.12's GIL.

### 2b. `_Node` class

```python
class _Node:
    __slots__ = ("key", "seq", "timestamp_ms", "value", "forward", "lock", "marked", "fully_linked")

    key:          Key
    seq:          SeqNum
    timestamp_ms: int       # Unix epoch milliseconds — carried from put()
    value:        Value     # TOMBSTONE sentinel for deleted keys
    forward:      list[_Node | None]   # forward[i] = next node at level i
    lock:         threading.Lock       # per-node lock — contention bounded to insertion boundary
    marked:       bool      # True = logically deleted, awaiting physical removal
    fully_linked: bool      # True = wired at all levels, safe for readers to observe
```

`_Node` is a private implementation detail. It is never exposed outside `skiplist.py`.

### 2c. SkipList constants

```python
MAX_LEVEL:   Final[int]   = 16    # max height — log2(~65536 max entries)
PROBABILITY: Final[float] = 0.5   # geometric level distribution
```

### 2d. SkipList class — state

```python
class SkipList:
    _head:     _Node          # sentinel head — key=b"", never returned to callers
    _level:    int            # current highest level in use (0-indexed)
    _size:      int            # estimated size in bytes (key + value per entry)
    _size_lock: threading.Lock # guards _size only
```

The head sentinel has `key=b""` which sorts before all real keys. It is `fully_linked=True` at construction.

### 2e. Internal helpers

**`_random_level() -> int`**
```
level = 0
while random.random() < PROBABILITY and level < MAX_LEVEL:
    level += 1
return level
```

**`_find(key: Key) -> tuple[list[_Node], list[_Node]]`**
```
Returns (preds, succs) at every level — purely navigational, acquires no lock.
pred = self._head
for level from self._level down to 0:
    curr = pred.forward[level]
    while curr is not None and curr.key < key:
        pred = curr
        curr = pred.forward[level]
    preds[level] = pred
    succs[level] = curr
return preds, succs
```

**`_lock_nodes(nodes: list[_Node]) -> contextmanager`**
```
Acquires locks on all nodes in ascending id() order to prevent deadlock.
Lock all, yield, unlock all in reverse order.
```

### 2f. Public API — `put(key, seq, timestamp_ms, value)`

```
top_level = _random_level()

retry loop:
    preds, succs = _find(key)

    check if key already exists (update path):
        scan succs[0..MAX_LEVEL] for a node where node.key == key
            and node.fully_linked == True and node.marked == False
        if found:
            with node.lock:
                if node.marked: continue retry   ← concurrently deleted, retry
                _size_lock: _size += len(value) - len(node.value)
                node.seq          = seq
                node.timestamp_ms = timestamp_ms
                node.value        = value
                return

    insert path:
        pred_nodes = preds[0 .. top_level]
        with _lock_nodes(pred_nodes):
            validate — each pred still points to the expected succ, neither is marked:
                if not valid: continue retry

            new_node = _Node(key, seq, timestamp_ms, value, top_level)
            for level in 0..top_level:
                new_node.forward[level] = succs[level]
                preds[level].forward[level] = new_node

            new_node.fully_linked = True   ← last step — now visible to readers

            if top_level > self._level:
                self._level = top_level

            with _size_lock:
                self._size += len(key) + len(value)
            return
```

**Critical:** `fully_linked = True` is set as the very last step after all forward pointers are wired. A reader that sees `fully_linked = False` skips the node entirely — it is invisible. This prevents half-inserted nodes from being observed.

### 2g. Public API — `get(key) -> tuple[SeqNum, Value] | None`

```
preds, succs = _find(key)   ← no lock acquired
node = succs[0]
if node is not None and node.key == key and node.fully_linked and not node.marked:
    return (node.seq, node.value)
return None
```

`get()` does not return `timestamp_ms` — only `seq` is needed for MVCC and deduplication on the read path (CONTEXT.md §8: "`seq` is the only ordering field"). `timestamp_ms` is preserved internally in the node and exposed via `__iter__()` / `snapshot()` for the flush path.

No lock is ever acquired during `get()`. The `fully_linked` and `marked` boolean reads are atomic under CPython 3.12's GIL.

### 2h. Public API — `delete(key, seq, timestamp_ms) -> bool`

Logical delete only — writes `TOMBSTONE` as the value, sets `marked = True`.

```
preds, succs = _find(key)
node = succs[0]

if node is None or node.key != key or not node.fully_linked:
    # key not found — insert a tombstone so delete propagates to SSTables
    put(key, seq, timestamp_ms, TOMBSTONE)
    return False

with node.lock:
    if node.marked:
        return False   ← concurrently deleted by another thread
    node.marked       = True
    node.seq          = seq
    node.timestamp_ms = timestamp_ms
    node.value        = TOMBSTONE
return True
```

Physical removal (unlinking from forward pointers) is deferred. The node stays in place but `marked = True` makes it invisible to readers and skipped by `__iter__`. Physical removal happens if/when compaction rebuilds the SSTable.

### 2i. Public API — `__iter__() -> Iterator[tuple[Key, SeqNum, int, Value]]`

```
curr = self._head.forward[0]
while curr is not None:
    if curr.fully_linked and not curr.marked:
        yield curr.key, curr.seq, curr.timestamp_ms, curr.value
    curr = curr.forward[0]
```

Yields `(key, seq, timestamp_ms, value)` in sorted key order. No lock acquired. Safe to call concurrently with `put()` and `delete()` — boolean flag reads are atomic under GIL.

### 2j. Public API — `snapshot() -> list[tuple[Key, SeqNum, int, Value]]`

```python
return list(self)   # materialises iterator
```

Used by `ActiveMemTable.freeze()` to produce a stable point-in-time copy for flushing.

### 2k. Property — `size_bytes -> int`

Returns `self._size`. Guards access with `_size_lock` for a consistent read.

### 2l. Error handling

- `put()` and `delete()` must never leave the SkipList in a half-modified state. If an exception occurs after locks are acquired, all locks must be released in `finally` blocks (rules.md §7 — temporary state mutation).
- Cleanup paths (`_lock_nodes.__exit__`) must never propagate exceptions — a cleanup failure must not mask the real error (async-io-guidelines §6b).
- No bare `except:` — catch specific exceptions or `except Exception as e` with log-then-reraise (rules.md §9).

### 2m. Correctness invariants the SkipList must preserve

1. `fully_linked` is set **last** after all forward pointers are wired — no half-inserted node is ever visible
2. `marked` is set **first** in delete — a node is logically gone before any structural change
3. Lock ordering by `id()` — eliminates deadlock for any concurrent insert combination
4. Validation after locking — if any predecessor was concurrently modified between `_find()` and lock acquisition, retry from scratch
5. `_find()` never acquires locks — it is a pure traversal

---

## 3. ActiveMemTable — `app/memtable/active.py`

### 3a. Purpose

`ActiveMemTable` is the public interface to the live mutable memtable. It wraps `SkipList` and adds:
- `table_id: TableID` for the audit chain
- `freeze()` to produce an `ImmutableMemTable` snapshot
- `size_bytes` threshold checking (delegated to SkipList)

### 3b. State

```python
class ActiveMemTable:
    table_id:  TableID    # UUID4 hex — generated at __init__, unique per instance
    _skiplist: SkipList   # the backing sorted data structure
```

### 3c. Construction

```python
def __init__(self) -> None:
    self.table_id = uuid.uuid4().hex
    self._skiplist = SkipList()
```

A fresh `table_id` is generated every time `ActiveMemTable()` is instantiated — including every time `MemTableManager.maybe_freeze()` creates a replacement table.

### 3d. Public API

```python
def put(self, key: Key, seq: SeqNum, timestamp_ms: int, value: Value) -> None:
    self._skiplist.put(key, seq, timestamp_ms, value)

def get(self, key: Key) -> tuple[SeqNum, Value] | None:
    return self._skiplist.get(key)

def delete(self, key: Key, seq: SeqNum, timestamp_ms: int) -> bool:
    return self._skiplist.delete(key, seq, timestamp_ms)

def freeze(self) -> list[tuple[Key, SeqNum, int, Value]]:
    # Returns sorted snapshot — snapshot_id is assigned by the caller (MemTableManager)
    return self._skiplist.snapshot()

@property
def size_bytes(self) -> int:
    return self._skiplist.size_bytes
```

### 3e. Rules
- `ActiveMemTable` never generates `seq` or `timestamp_ms` — these arrive from the caller
- `freeze()` returns a sorted list, not a reference to the live SkipList — the list is immutable after this call
- `ActiveMemTable` has no knowledge of `ImmutableMemTable`, `MemTableManager`, or any flush mechanism

---

## 4. ImmutableMemTable — `app/memtable/immutable.py`

### 4a. Purpose

`ImmutableMemTable` holds a frozen, read-only, sorted snapshot of an `ActiveMemTable`. It is created by `MemTableManager.maybe_freeze()` and lives in `_immutable_q` until it is flushed to an SSTable and popped.

### 4b. State

```python
class ImmutableMemTable:
    snapshot_id: SnapshotID                         # UUID4 hex — equals the table_id of the source ActiveMemTable
    _data:       list[tuple[Key, SeqNum, int, Value]]  # sorted by key, immutable after construction
    _index:      dict[Key, int]                     # key → position in _data for O(1) point lookup
```

### 4c. Construction

```python
def __init__(
    self,
    snapshot_id: SnapshotID,
    data: list[tuple[Key, SeqNum, int, Value]],
) -> None:
    self.snapshot_id = snapshot_id
    self._data       = data                          # must be sorted by key ascending
    self._index      = {entry[0]: i for i, entry in enumerate(data)}
```

`_data` is a sorted list produced by `ActiveMemTable.freeze()` (which calls `SkipList.snapshot()`). No copy is made — `ImmutableMemTable` takes ownership of the list.

### 4d. Public API

```python
def get(self, key: Key) -> tuple[SeqNum, Value] | None:
    # O(1) point lookup via _index — returns (seq, value) only; timestamp_ms
    # is not exposed on the read path (seq is the sole ordering field)
    idx = self._index.get(key)
    if idx is None:
        return None
    _, seq, _, value = self._data[idx]
    return (seq, value)

def items(self) -> Iterator[tuple[Key, SeqNum, int, Value]]:
    # Yields (key, seq, timestamp_ms, value) in sorted key order
    # Used by SSTableManager.flush() to iterate in write order
    return iter(self._data)

@property
def size_bytes(self) -> int:
    return sum(len(k) + len(v) for k, _, _, v in self._data)

@property
def seq_min(self) -> SeqNum:
    return min(seq for _, seq, _, _ in self._data) if self._data else 0

@property
def seq_max(self) -> SeqNum:
    return max(seq for _, seq, _, _ in self._data) if self._data else 0

def __len__(self) -> int:
    return len(self._data)
```

### 4e. Rules
- `ImmutableMemTable` is read-only after construction — no `put()`, `delete()`, or mutation methods
- No lock is needed for reads — the object is immutable
- `seq_min` and `seq_max` are used by `WALManager.truncate_before()` after flush
- `snapshot_id` is the same UUID as the `table_id` of the `ActiveMemTable` it was frozen from — this is the `TableID → SnapshotID` link in the audit chain

---

## 5. MemTableManager — `app/engine/memtable_manager.py`

### 5a. Purpose

`MemTableManager` owns all in-memory write-side state. It is the single point of coordination between the active memtable, the immutable queue, backpressure, and the flush signal. It has no knowledge of SSTables, WAL, or disk.

### 5b. State

```python
class MemTableManager:
    _active:         ActiveMemTable
    _immutable_q:    deque[ImmutableMemTable]   # newest at left (index 0), oldest at right
    _write_lock:     threading.RLock            # serialises put() and maybe_freeze()
    _seq:            int                        # global monotonic sequence counter, starts at 0
    _seq_lock:       threading.Lock             # guards _seq only
    _queue_not_full: threading.Condition        # backpressure condition — wait when queue >= IMMUTABLE_QUEUE_MAX
    _flush_event:    threading.Event            # set by maybe_freeze(), waited on by flush worker
```

### 5c. Construction

```python
def __init__(self) -> None:
    self._active         = ActiveMemTable()
    self._immutable_q    = deque()
    self._write_lock     = threading.RLock()
    self._seq            = 0
    self._seq_lock       = threading.Lock()
    self._queue_not_full = threading.Condition(self._write_lock)
    self._flush_event    = threading.Event()
```

`_queue_not_full` is a `Condition` built on top of `_write_lock` — waiting on it releases `_write_lock` temporarily, which is essential for the backpressure mechanism.

### 5d. `next_seq() -> SeqNum`

```python
def next_seq(self) -> SeqNum:
    with self._seq_lock:
        self._seq += 1
        return self._seq
```

Separate from `_write_lock` — seq can be incremented without holding the write lock during recovery.

### 5e. `put(key, seq, timestamp_ms, value) -> None`

```python
def put(self, key: Key, seq: SeqNum, timestamp_ms: int, value: Value) -> None:
    # Called by engine while holding _write_lock — no re-acquisition needed here
    self._active.put(key, seq, timestamp_ms, value)
```

The engine acquires `_write_lock` before calling `put()`. `MemTableManager.put()` does not acquire any lock internally — it relies on the engine's lock.

### 5f. `get(key) -> tuple[SeqNum, Value] | None`

```python
def get(self, key: Key) -> tuple[SeqNum, Value] | None:
    # 1. Check active memtable
    result = self._active.get(key)
    if result is not None:
        return result

    # 2. Check immutable queue newest → oldest (left → right)
    for table in self._immutable_q:
        result = table.get(key)
        if result is not None:
            return result

    return None
```

No lock acquired. `SkipList.get()` is safe under GIL. `deque` iteration from left to right is safe under GIL — a concurrent `appendleft` at the opposite end does not interrupt iteration.

**TOMBSTONE is NOT resolved here.** `get()` returns the raw value including `TOMBSTONE`. The engine (`LSMEngine.get()`) is responsible for checking `value == TOMBSTONE` and returning `None` to the caller.

### 5g. `maybe_freeze() -> ImmutableMemTable | None`

This is the most important method in `MemTableManager`. Called by the engine while holding `_write_lock`.

```python
def maybe_freeze(self) -> ImmutableMemTable | None:
    # Called under _write_lock — do not re-acquire

    if self._active.size_bytes < MAX_MEMTABLE_SIZE:
        return None   ← threshold not reached

    # Backpressure: wait if queue is full
    while len(self._immutable_q) >= IMMUTABLE_QUEUE_MAX:
        self._queue_not_full.wait()
        # wait() releases _write_lock, reacquires before returning
        # other put() calls may proceed while we wait
        # flush worker drains queue and calls _queue_not_full.notify_all()

    # Freeze the active table
    snapshot_id = self._active.table_id    # carry TableID → SnapshotID
    data        = self._active.freeze()    # O(n) sorted snapshot
    snapshot    = ImmutableMemTable(snapshot_id, data)

    self._immutable_q.appendleft(snapshot) # newest at left
    self._active = ActiveMemTable()         # fresh table, new table_id
    self._flush_event.set()                 # wake flush worker

    logger.info("MemTable frozen",
                extra={"snapshot_id": snapshot_id, "size_bytes": sum(len(k) + len(v) for k, _, _, v in data),
                        "entry_count": len(data), "queue_len": len(self._immutable_q)})
    return snapshot
```

### 5h. `peek_oldest() -> ImmutableMemTable | None`

```python
def peek_oldest(self) -> ImmutableMemTable | None:
    if not self._immutable_q:
        return None
    return self._immutable_q[-1]   # oldest = rightmost
```

Called by the engine's flush worker **before** writing the SSTable. The snapshot stays in the queue during the entire SSTable write — it is still readable by `get()` during this window.

### 5i. `pop_oldest() -> None`

```python
def pop_oldest(self) -> None:
    if self._immutable_q:
        removed = self._immutable_q.pop()        # remove rightmost (oldest)
        logger.debug("Snapshot popped from queue",
                     extra={"snapshot_id": removed.snapshot_id, "queue_remaining": len(self._immutable_q)})
    with self._queue_not_full:
        self._queue_not_full.notify_all()   # unblock any stalled maybe_freeze()
```

Called by the engine **only after** the SSTable has been written and registered in `SSTableRegistry`. Never call before that — doing so creates a read gap where data is in neither the queue nor the SSTable.

### 5j. `queue_len() -> int`

```python
def queue_len(self) -> int:
    return len(self._immutable_q)
```

Used by the flush worker to determine if more work remains after processing one snapshot.

### 5k. `restore(entries: list[WALEntry]) -> None`

```python
def restore(self, entries: list[WALEntry]) -> None:
    # Called by engine._recover() only — single-threaded, no lock needed
    # entries must be sorted by seq ascending before calling
    for entry in entries:
        self._active.put(entry.key, entry.seq, entry.timestamp_ms, entry.value)
        self._seq = max(self._seq, entry.seq)
```

Replays WAL entries into the fresh active memtable on startup. After `restore()`, `_seq` is at least as large as the highest seq in the replayed entries.

### 5l. `size_bytes -> int`

```python
@property
def size_bytes(self) -> int:
    return self._active.size_bytes
```

---

## 6. Async flush trigger — how the flush worker integrates

This section describes how the flush signal flows from a write all the way to disk. The flush worker itself lives in `LSMEngine` but is documented here because its triggering mechanism is owned by `MemTableManager`.

### 6a. The signal: `_flush_event`

`_flush_event` is a `threading.Event` owned by `MemTableManager`. It has two states:
- **Set** — at least one snapshot is waiting in `_immutable_q`
- **Clear** — queue is empty, flush worker should sleep

`maybe_freeze()` calls `_flush_event.set()` after pushing a snapshot. The flush worker calls `_flush_event.wait()` in a loop. After draining the queue, the flush worker calls `_flush_event.clear()`.

### 6b. Flush worker loop (lives in `LSMEngine`, triggered by `MemTableManager`)

```python
# This runs in a daemon threading.Thread — started by LSMEngine.open()
def _flush_worker(self) -> None:
    while not self._stop_event.is_set():
        self._mem._flush_event.wait(timeout=1.0)   # wake on signal or 1s timeout
        self._mem._flush_event.clear()

        while self._mem.queue_len() > 0:
            self._flush_one()                       # process oldest snapshot

def _flush_one(self) -> None:
    snapshot = self._mem.peek_oldest()
    if snapshot is None:
        return

    file_id = uuid.uuid4().hex
    logger.info("Flush started", extra={"snapshot_id": snapshot.snapshot_id, "file_id": file_id})

    meta    = self._sst.flush(snapshot, file_id)   # SSTableManager writes to disk

    self._mem.pop_oldest()                          # NOW safe to remove
    self._wal.truncate_before(meta.seq_min)         # NOW safe to truncate WAL

    logger.info("Flush complete", extra={"file_id": file_id, "seq_min": meta.seq_min,
                                          "seq_max": meta.seq_max, "queue_remaining": self._mem.queue_len()})
```

### 6c. Flush state machine

```
State A → normal write
    _active      has data
    _immutable_q is empty
    SSTables     unchanged
    ──────────────────────────────────────────────────────────
    Trigger: _active.size_bytes >= MAX_MEMTABLE_SIZE

State B → immediately after maybe_freeze() (still under _write_lock)
    _active      fresh empty ActiveMemTable (new table_id)
    _immutable_q [snap0]  — _flush_event is set
    SSTables     unchanged
    ──────────────────────────────────────────────────────────
    _write_lock released → new writes resume immediately

State C → flush in progress (concurrent with new writes)
    _active      accumulating new writes
    _immutable_q [snap0]  — still readable via _mem.get()
    SSTables     L0c being written by SSTableManager
    ──────────────────────────────────────────────────────────
    Key invariant: snap0 is visible to readers throughout State C

State D → flush complete (under _sst._flush_lock)
    _active      has accumulated writes
    _immutable_q empty — snap0 removed AFTER L0c registered
    SSTables     L0c now in registry and readable
```

### 6d. Backpressure flow

```
Writer calls put() → maybe_freeze() → len(_immutable_q) >= IMMUTABLE_QUEUE_MAX

    Writer blocks here:
        _queue_not_full.wait()          ← releases _write_lock temporarily
        (flush worker drains oldest snapshot → pop_oldest() → notify_all())
        _queue_not_full.wait() returns  ← reacquires _write_lock
        re-check condition              ← loop in case queue still full

    Writer proceeds:
        freeze active → push snapshot → replace active → set _flush_event
        return from maybe_freeze()
        release _write_lock
```

Memory ceiling: `IMMUTABLE_QUEUE_MAX × MAX_MEMTABLE_SIZE = 4 × 64MB = 256MB` plus the current active memtable = **320MB worst case**.

---

## 7. Read path — end to end through `MemTableManager`

### 7a. `LSMEngine.get(key)` delegates to managers in order

```python
def get(self, key: Key) -> Value | None:
    # 1. Check memtable layer (active + immutable queue)
    result = self._mem.get(key)

    # 2. If not found, check SSTable layer
    if result is None:
        result = self._sst.get(key)

    # 3. Resolve result
    if result is None:
        return None
    seq, value = result
    return None if value == TOMBSTONE else value
```

### 7b. `MemTableManager.get(key)` scan order

```
1. _active.get(key)             → SkipList.get() — no lock, newest data
2. for snap in _immutable_q:    → left (newest) to right (oldest)
       snap.get(key)            → dict lookup O(1)
       if found: return
3. return None                  → not in memtable layer
```

### 7c. Why this order is correct

Newer writes shadow older writes. The active memtable always has the most recent write for a key. Immutable snapshots are in freeze-time order (newest left). SSTables are searched newest-first (L0 newest → L0 oldest → L1 → L2).

Any layer that returns `TOMBSTONE` as the value causes the engine to return `None` immediately — the caller never sees the tombstone sentinel. `timestamp_ms` is never part of the `get()` return — `seq` is the sole ordering field for MVCC (CONTEXT.md §8).

### 7d. No lock on the read path

- `SkipList.get()` — reads `fully_linked` and `marked` flags only, no lock
- `deque` iteration — safe under GIL; `appendleft` at the opposite end does not interrupt left-to-right iteration
- `ImmutableMemTable.get()` — dict lookup on immutable data, no lock

Reads never block writers and writers never block reads (except during the brief freeze in `maybe_freeze()`, which blocks `put()` calls only, not `get()` calls).

---

## 8. Write path — end to end through `MemTableManager`

### 8a. `LSMEngine.put(key, value)` — full sequence

```python
def put(self, key: Key, value: Value) -> None:
    with self._mem._write_lock:
        seq          = self._mem.next_seq()
        timestamp_ms = time.time_ns() // 1_000_000

        # WAL first — durability before visibility
        self._wal.append(WALEntry(seq, timestamp_ms, key, value))

        # Memtable write
        self._mem.put(key, seq, timestamp_ms, value)

        # Check if freeze is needed
        self._mem.maybe_freeze()
```

### 8b. `LSMEngine.delete(key)` — identical with TOMBSTONE

```python
def delete(self, key: Key) -> None:
    with self._mem._write_lock:
        seq          = self._mem.next_seq()
        timestamp_ms = time.time_ns() // 1_000_000
        self._wal.append(WALEntry(seq, timestamp_ms, key, TOMBSTONE))
        self._mem.put(key, seq, timestamp_ms, TOMBSTONE)
        self._mem.maybe_freeze()
```

### 8c. Locking protocol for writes

```
_write_lock is acquired by LSMEngine.put() / delete()
    _seq_lock is acquired and released inside next_seq()         ← nested, very brief
    _wal.append() called                                         ← WAL has its own _wal_lock
    _mem.put() called                                            ← no lock inside
    _mem.maybe_freeze() called:
        if freeze needed:
            _queue_not_full.wait() may block (releases _write_lock temporarily)
            reacquires _write_lock
            _active.freeze() → _immutable_q.appendleft()
            _active = ActiveMemTable()
            _flush_event.set()
_write_lock released
```

`_write_lock` is an `RLock` — the engine can re-acquire it safely if needed. In practice it is only held once per `put()` call.

---

## 9. WAL entry format (for context — implemented in `wal/writer.py`)

`WALEntry` carries `timestamp_ms` alongside `seq`. The MemTableManager's `restore()` expects entries with this shape:

```python
@dataclass(slots=True, frozen=True)
class WALEntry:
    seq:          SeqNum
    timestamp_ms: int
    key:          Key
    value:        Value   # TOMBSTONE for deletes

    @property
    def is_tombstone(self) -> bool:
        return self.value == TOMBSTONE
```

msgpack encoding: `packb((seq, timestamp_ms, key, value), use_bin_type=True)`

---

## 10. ID audit chain

Every write buffer, snapshot, and SSTable has a unique identity forming a traceable chain:

```
ActiveMemTable.table_id  (TableID — UUID4 hex)
        │  ActiveMemTable.freeze() inside maybe_freeze()
        │  snapshot_id = table_id   ← same UUID, explicit carry-forward
        ▼
ImmutableMemTable.snapshot_id  (SnapshotID)
        │  SSTableManager.flush(snapshot, file_id)
        │  meta.snapshot_id = snapshot.snapshot_id
        ▼
SSTableMeta.file_id + SSTableMeta.snapshot_id
```

**ID generation rules:**

| ID | Generated at | Value |
|---|---|---|
| `table_id` | `ActiveMemTable.__init__()` | `uuid.uuid4().hex` — fresh UUID each time |
| `snapshot_id` | `MemTableManager.maybe_freeze()` | `= _active.table_id` at moment of freeze |
| `file_id` | `LSMEngine._flush_one()` | `uuid.uuid4().hex` — new UUID per SSTable |

---

## 11. File locations, imports, and module requirements

```
app/
├── types.py                        ← Phase 1 — no internal imports
├── memtable/
│   ├── __init__.py                 ← re-exports ActiveMemTable, ImmutableMemTable
│   ├── skiplist.py                 ← Phase 2 — imports types only
│   ├── active.py                   ← Phase 3 — imports skiplist, types
│   └── immutable.py                ← Phase 4 — imports types
└── engine/
    ├── __init__.py                 ← re-exports MemTableManager
    └── memtable_manager.py         ← Phase 5 — imports active, immutable, types
```

**Import rules (rules.md §1, §12):**
- `skiplist.py` imports from `types.py` only
- `active.py` imports from `skiplist.py` and `types.py` only
- `immutable.py` imports from `types.py` only
- `memtable_manager.py` imports from `active.py`, `immutable.py`, and `types.py` only
- Nothing outside `engine/memtable_manager.py` imports from `engine/` — `MemTableManager` is internal to the engine layer
- Within `memtable/`, use **relative imports** (`.skiplist`, `.active`). Across packages, use absolute imports (`from app.types import ...`) — rules.md §12

**`__init__.py` re-exports (rules.md §1):**

Callers import from the package, not the inner file:

```python
# app/memtable/__init__.py
from .active import ActiveMemTable
from .immutable import ImmutableMemTable

__all__ = ["ActiveMemTable", "ImmutableMemTable"]
```

```python
# app/engine/__init__.py
from .memtable_manager import MemTableManager

__all__ = ["MemTableManager"]
```

**Module-level requirements (rules.md §8, §18):**

Every `.py` file in this spec must include:

1. **Module-level docstring** — describes what the module does, what it exports, and any constraints.
2. **Logger at module level** — `from app.observability import get_logger; logger = get_logger(__name__)`. Never create loggers inside functions or methods. The project uses `structlog` (CONTEXT.md §7) behind an internal `app.observability` facade (rules.md §8).
3. **No `print()` in production code** — use `logger.*` only.

**Structured logging (async-io-guidelines §10, rules.md §8):**

All log calls must use structured `extra={}` dicts. Never embed variable data in f-string log messages:

```python
# CORRECT
logger.info("MemTable frozen", extra={"snapshot_id": snapshot_id, "size_bytes": size, "entries": count})
logger.warning("Backpressure active", extra={"queue_len": len(self._immutable_q), "max": IMMUTABLE_QUEUE_MAX})

# WRONG
logger.info(f"Frozen memtable {snapshot_id} with {count} entries")
```

**Function classification (async-io-guidelines §2):**

| Function | Classification | Rationale |
|---|---|---|
| `SkipList.put/get/delete/__iter__` | **Pure computation / sync** | In-memory data structure manipulation, no I/O |
| `ActiveMemTable.*` | **Pure computation / sync** | Thin wrapper over SkipList |
| `ImmutableMemTable.*` | **Pure computation / sync** | Read-only in-memory data, no I/O |
| `MemTableManager.put/get/maybe_freeze` | **Pure computation / sync** | In-memory operations only |
| `MemTableManager.restore` | **Pure computation / sync** | Replays entries into memory (I/O is done by WALManager) |
| Flush worker (`_flush_one`) | **I/O-bound** | Writes SSTable to disk — runs in daemon thread |

None of these functions should be `async def` — they perform no I/O. The flush worker is I/O-bound but runs on a dedicated daemon `threading.Thread` (CONTEXT.md §15a).

---

## 12. Test checklist (per phase)

### Phase 2 — SkipList tests
- [ ] Insert single key, get returns it
- [ ] Insert multiple keys, `__iter__` returns sorted order
- [ ] Update existing key — seq and value change, position unchanged
- [ ] Delete key — `get()` returns `None`, `__iter__` skips it
- [ ] Delete non-existent key — tombstone inserted
- [ ] `snapshot()` returns sorted list, not affected by subsequent inserts
- [ ] `size_bytes` tracks correctly through inserts and updates
- [ ] Concurrent inserts from 4 threads — no panic, result is sorted
- [ ] `fully_linked` invariant — node not visible until all levels wired
- [ ] `marked` invariant — node invisible after delete, before physical removal

### Phase 3 — ActiveMemTable tests
- [ ] `table_id` is unique across instances
- [ ] `freeze()` returns sorted snapshot
- [ ] After `freeze()`, source SkipList can still be written (freeze makes a copy)
- [ ] `size_bytes` matches SkipList

### Phase 4 — ImmutableMemTable tests
- [ ] `get()` finds existing key in O(1)
- [ ] `get()` returns `None` for missing key
- [ ] `items()` yields in sorted order
- [ ] `seq_min` / `seq_max` correct
- [ ] `snapshot_id` matches the `table_id` passed at construction

### Phase 5 — MemTableManager tests
- [ ] `put()` then `get()` returns same value
- [ ] `get()` checks active first, then immutable queue newest→oldest
- [ ] `maybe_freeze()` triggers at `MAX_MEMTABLE_SIZE`
- [ ] After freeze, new writes go into fresh active table
- [ ] `peek_oldest()` returns oldest snapshot without removing
- [ ] `pop_oldest()` removes oldest snapshot
- [ ] Backpressure: `maybe_freeze()` blocks when queue >= `IMMUTABLE_QUEUE_MAX`
- [ ] `pop_oldest()` unblocks stalled `maybe_freeze()` via `_queue_not_full`
- [ ] `restore()` replays entries into active, sets `_seq` correctly
- [ ] `queue_len()` accurate across concurrent operations
- [ ] Read path never acquires any engine-level lock — verified by inspection