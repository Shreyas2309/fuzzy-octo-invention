# Bug Report — lsm-kv Deep Code Audit

**Date:** 2026-03-21
**Scope:** Every source file in `app/` audited line-by-line
**Findings:** 19 bugs (3 CRITICAL, 8 HIGH, 6 MEDIUM, 2 LOW)

---

## Severity Guide

| Level | Meaning |
|-------|---------|
| **CRITICAL** | Can cause deadlock, data loss, or crash under normal operation |
| **HIGH** | Correctness bug, resource leak, or crash under specific (but realistic) conditions |
| **MEDIUM** | Performance issue, design smell, or bug requiring unusual conditions to trigger |
| **LOW** | Cosmetic, theoretical, or by-design trade-off |

---

## BUG-01 — CRITICAL: FlushPipeline Event Chain Deadlock

### Location
`app/engine/flush_pipeline.py` — lines 146–152

### Current Code
```python
async def _flush_slot(self, slot: FlushSlot) -> None:
    async with self._semaphore:
        meta, reader = await self._write_sstable(slot)  # can raise

    await self._commit_slot(slot, meta, reader)  # sets slot.my_committed
```

### Problem
If `_write_sstable()` raises an exception (disk full, I/O error, encoding failure), `_flush_slot()` exits immediately. `_commit_slot()` is never called, so `slot.my_committed` is **never set**. Any downstream slot whose `prev_committed` points to this slot's `my_committed` will call `await slot.prev_committed.wait()` and **hang forever**.

```
Timeline:
  Slot 0: writes OK → commits → sets my_committed ✅
  Slot 1: write FAILS → exception bubbles up → my_committed never set ❌
  Slot 2: awaits slot_1.my_committed.wait() → DEADLOCK FOREVER ♾️
```

The exception is caught by `asyncio.gather(..., return_exceptions=True)` at line 130, logged at line 133–140, and then silently discarded. The pipeline continues running, but all future flush batches that chain off the failed slot are permanently stuck.

### Proposed Fix
Wrap `_flush_slot()` in `try/finally` to guarantee the event is always signalled:

```python
async def _flush_slot(self, slot: FlushSlot) -> None:
    """Phase 1: write SSTable (parallel). Phase 2: commit (serialized)."""
    try:
        async with self._semaphore:
            meta, reader = await self._write_sstable(slot)

        await self._commit_slot(slot, meta, reader)
    except Exception:
        logger.exception(
            "Flush slot failed, signalling downstream",
            file_id=slot.file_id,
            position=slot.position,
        )
        raise
    finally:
        # ALWAYS signal downstream slots, even on failure.
        # Without this, downstream slots deadlock forever.
        slot.my_committed.set()
```

Also remove the redundant `slot.my_committed.set()` at the end of `_commit_slot()` (line 206), since `finally` now handles it.

### Impact if Not Fixed
Any I/O error during a single flush permanently freezes all subsequent flushes. The immutable queue fills up, backpressure kicks in, and eventually `FreezeBackpressureTimeout` halts all writes. The engine becomes read-only.

---

## BUG-02 — CRITICAL: MemTableManager.get() Iterates Queue Without Lock

### Location
`app/engine/memtable_manager.py` — lines 75–83

### Current Code
```python
def get(self, key: Key) -> tuple[SeqNum, Value] | None:
    result = self._active.get(key)
    if result is not None:
        return result

    for i, table in enumerate(self._immutable_q):   # ← NO LOCK
        result = table.get(key)
        if result is not None:
            return result
    return None
```

### Problem
`get()` iterates `self._immutable_q` (a `collections.deque`) without holding any lock. Concurrently, `maybe_freeze()` modifies the deque via `appendleft()` at line 148 (under `_write_lock`), and `pop_oldest()` modifies it via `pop()` at line 180.

While CPython's GIL prevents a segfault, deque iteration during concurrent modification can:
- **Skip entries** — an `appendleft()` shifts indices, causing the iterator to jump over an element
- **See inconsistent state** — a `pop()` removes the tail while the iterator is approaching it

This means `get()` can **miss a key that exists** in the immutable queue, returning `None` when it should return a value. The engine then falls through to SSTables, which may return stale data or `None`.

### Proposed Fix
Take a snapshot copy of the deque before iterating:

```python
def get(self, key: Key) -> tuple[SeqNum, Value] | None:
    result = self._active.get(key)
    if result is not None:
        return result

    # Snapshot the queue to avoid concurrent-modification issues.
    # deque copy is O(n) but n ≤ immutable_queue_max_len (default 4).
    snapshot = list(self._immutable_q)

    for i, table in enumerate(snapshot):
        result = table.get(key)
        if result is not None:
            logger.debug(
                "MemTableManager get",
                key=key,
                source=f"immutable_{i}",
            )
            return result

    logger.debug("MemTableManager get", key=key, source="miss")
    return None
```

**Why not acquire `_write_lock`?** The write lock is an `RLock` held by the engine during the entire write path (`put` → WAL append → memtable put → maybe_freeze`). Acquiring it in `get()` would serialize reads behind writes, destroying read throughput. A snapshot copy of a 4-element deque is ~100ns — negligible.

### Impact if Not Fixed
Intermittent phantom misses on `get()` during concurrent writes. Hard to reproduce, harder to debug. Data appears lost but is actually present in the immutable queue.

---

## BUG-03 — CRITICAL: peek_at_depth() TOCTOU Race

### Location
`app/engine/memtable_manager.py` — lines 167–175

### Current Code
```python
def peek_at_depth(self, depth: int) -> ImmutableMemTable | None:
    if depth >= len(self._immutable_q):     # ← CHECK
        return None
    return self._immutable_q[-(depth + 1)]  # ← USE
```

### Problem
Between the length check (line 173) and the index access (line 175), another thread can call `pop_oldest()` which does `self._immutable_q.pop()`. If the deque shrinks from length 3 to length 2 between the two lines, and `depth=2`, the access `self._immutable_q[-3]` raises `IndexError`.

This is called by `FlushPipeline._dispatch_all()` at line 105, which iterates depths 0 through `queue_len - 1`. A concurrent `_commit_slot()` calling `pop_oldest()` can trigger the race.

### Proposed Fix
Acquire `_write_lock` around the check-and-access:

```python
def peek_at_depth(self, depth: int) -> ImmutableMemTable | None:
    with self._write_lock:
        if depth >= len(self._immutable_q):
            return None
        return self._immutable_q[-(depth + 1)]
```

Alternatively, since `peek_at_depth` is only called from the flush pipeline (single async task), the fix from BUG-02 (snapshot copy) could also solve this — the pipeline could take a full snapshot of the queue once and iterate over the copy.

### Impact if Not Fixed
`IndexError` crash in the flush pipeline. Since the pipeline runs as a daemon task with no restart logic, this permanently stops all flushes. Same cascade as BUG-01: queue fills → backpressure → writes stall.

---

## BUG-04 — HIGH: SSTableRegistry close_all() Use-After-Close

### Location
`app/sstable/registry.py` — lines 69–73

### Current Code
```python
def close_all(self) -> None:
    with self._lock:
        for file_id in list(self._readers):
            self._cleanup(file_id)  # closes reader
```

### Problem
`close_all()` is called during engine shutdown (`LSMEngine.close()`). If a `get()` is in progress and has an active `open_reader()` context, that context holds a reference to a reader. `close_all()` closes the reader immediately via `_cleanup()`. The in-flight `get()` then calls `reader.get(key)` on a closed mmap → crash or undefined behavior.

The refcount check in `_cleanup()` (line 62) does NOT prevent this because `close_all()` bypasses the `mark_for_deletion()` path and calls `_cleanup()` directly for every reader, ignoring refcounts.

### Proposed Fix
`close_all()` should only close readers with refcount 0, and mark the rest for deferred cleanup:

```python
def close_all(self) -> None:
    """Close all readers. Readers with active refs are deferred."""
    with self._lock:
        for file_id in list(self._readers):
            if self._refcounts.get(file_id, 0) == 0:
                self._cleanup(file_id)
            else:
                self._marked.add(file_id)
                logger.warning(
                    "Reader deferred (in use)",
                    file_id=file_id,
                    refcount=self._refcounts[file_id],
                )
```

In practice, the engine should also wait for in-flight reads to complete before calling `close_all()`. The flush pipeline stop + short sleep is usually sufficient.

---

## BUG-05 — HIGH: L0 SSTable Ordering by Random UUID

### Location
`app/engine/sstable_manager.py` — lines 61, 88–90

### Current Code
```python
for child in sorted(l0_dir.iterdir()):  # sorts by directory name
    ...
# Sort L0 by created_at descending (newest first) via meta
# We'll use the directory listing order for now — UUIDs are time-based
l0_order.reverse()
```

### Problem
Directories are named `uuid.uuid4().hex` — UUIDv4 is **random**, not time-ordered. The comment "UUIDs are time-based" is incorrect (that would be UUIDv1 or UUIDv7). Sorting random hex strings gives arbitrary order, not chronological.

Currently `get()` scans ALL L0 files and takes the highest-seq result, so this doesn't cause incorrect reads. But it means:
1. The `_l0_order` list is unreliable for any future "newest-first, early termination" optimization
2. Startup recovery could compute an incorrect `_max_seq` if iteration short-circuits (it doesn't today, but it's fragile)

### Proposed Fix
Sort by `meta.created_at` after loading all SSTables:

```python
# After loading all SSTables into l0_order + l0_dirs:
# Sort by created_at descending (newest first)
meta_map: dict[FileID, SSTableMeta] = {}
for fid in l0_order:
    with registry.open_reader(fid) as reader:
        meta_map[fid] = reader.meta

l0_order.sort(
    key=lambda fid: meta_map[fid].created_at,
    reverse=True,  # newest first
)
```

---

## BUG-06 — HIGH: SSTableReader File Descriptor Leak on Open Failure

### Location
`app/sstable/reader.py` — lines 83–99

### Current Code
```python
fd = os.open(str(data_path), os.O_RDONLY)
size = os.fstat(fd).st_size
if size == 0:
    mm = mmap.mmap(-1, 1)
else:
    mm = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)

reader = cls(directory=directory, ..., mm=mm, fd=fd)
```

### Problem
If `os.fstat(fd)` or `mmap.mmap(fd, ...)` raises an exception (e.g., `PermissionError`, `OSError`), the `fd` is never closed. It hasn't been stored in `self._fd` yet (that happens inside `cls(...)` at line 90), so the `close()` method can't clean it up. The exception propagates, leaking the file descriptor.

Over many failed opens (e.g., during recovery with corrupted files), this exhausts the process file descriptor limit.

### Proposed Fix
Wrap the mmap section in try/except:

```python
fd = os.open(str(data_path), os.O_RDONLY)
try:
    size = os.fstat(fd).st_size
    if size == 0:
        mm = mmap.mmap(-1, 1)
    else:
        mm = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)
except Exception:
    os.close(fd)
    raise

reader = cls(directory=directory, ..., mm=mm, fd=fd)
```

---

## BUG-07 — HIGH: Empty SSTable Anonymous mmap

### Location
`app/sstable/reader.py` — lines 85–86

### Current Code
```python
if size == 0:
    mm = mmap.mmap(-1, 1)  # anonymous 1-byte mmap
```

### Problem
For empty `data.bin` files (0 bytes), the code creates an anonymous 1-byte memory region unrelated to the actual file. Issues:
1. `mmap(-1, 1)` behavior is platform-dependent (Unix-only anonymous mapping)
2. A 1-byte corrupted file is treated as "empty", masking corruption
3. `close()` calls `os.close(self._fd)` which closes the real fd, but `self._mm.close()` closes the anonymous mmap — they're disconnected

The code works accidentally because `iter_block(mv, 0, 0)` never iterates (the `while pos < block_end` loop exits immediately when block_end is 0).

### Proposed Fix
Use `None` for the mmap and guard accesses:

```python
# In open():
if size == 0:
    mm = None
else:
    mm = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)

# Store as: self._mm: mmap.mmap | None

# In get():
if self._mm is None:
    return None  # empty SSTable has no records

mv = memoryview(self._mm)
# ... rest of get() logic

# In close():
if self._mm is not None:
    with contextlib.suppress(Exception):
        self._mm.close()
```

---

## BUG-08 — HIGH: Concurrent _l0_order Modification Without Lock

### Location
`app/engine/sstable_manager.py` — lines 134–146

### Current Code
```python
def commit(self, file_id, reader, sst_dir):
    self._registry.register(file_id, reader)
    self._l0_order.insert(0, file_id)  # ← NO LOCK
    self._l0_dirs[file_id] = sst_dir
    if reader.meta.seq_max > self._max_seq:  # ← NO LOCK
        self._max_seq = reader.meta.seq_max
```

### Problem
Multiple concurrent flush slots can call `commit()` simultaneously. `list.insert()` while another thread iterates `_l0_order` in `get()` (line 154) is unsafe — CPython's GIL makes individual operations atomic, but `insert()` can trigger a list resize that invalidates iteration state.

`self._max_seq` has a read-modify-write race: Thread A reads `_max_seq=100`, Thread B reads `_max_seq=100`, Thread A writes `_max_seq=150`, Thread B writes `_max_seq=120` → final value 120 (lost update).

### Proposed Fix
Add a `threading.Lock` to guard all mutable state:

```python
class SSTableManager:
    def __init__(self, ...):
        ...
        self._lock = threading.Lock()

    def commit(self, file_id, reader, sst_dir):
        self._registry.register(file_id, reader)
        with self._lock:
            self._l0_order.insert(0, file_id)
            self._l0_dirs[file_id] = sst_dir
            if reader.meta.seq_max > self._max_seq:
                self._max_seq = reader.meta.seq_max

    async def get(self, key):
        with self._lock:
            order_snapshot = list(self._l0_order)

        for file_id in order_snapshot:
            ...
```

---

## BUG-09 — HIGH: BlockCache is Dead Code

### Location
`app/sstable/reader.py` — `self._cache` stored at line 42 but never read in `get()`

### Current Code
```python
def __init__(self, ..., cache: BlockCache | None, ...):
    self._cache = cache  # stored but never used

def get(self, key):
    ...
    mv = memoryview(self._mm)
    for rec in iter_block(mv, block_offset, block_end):  # always scans mmap
        ...
```

### Problem
The `BlockCache` is instantiated in `SSTableManager`, passed to every `SSTableReader`, but **never consulted** during reads. Every `get()` does a fresh mmap scan of the data block. The cache is pure dead code.

### Proposed Fix
Check cache before scanning, populate on miss:

```python
def get(self, key):
    if not self._bloom.may_contain(key):
        return None

    block_offset = self._index.floor_offset(key)
    if block_offset is None:
        return None

    block_end = self._find_block_end(block_offset)

    # Check cache first
    block_data: bytes | None = None
    if self._cache is not None:
        block_data = self._cache.get(self._file_id, block_offset)

    if block_data is not None:
        mv = memoryview(block_data)
        scan_start, scan_end = 0, len(block_data)
    else:
        mv = memoryview(self._mm)
        scan_start, scan_end = block_offset, block_end
        # Populate cache
        if self._cache is not None:
            self._cache.put(
                self._file_id, block_offset,
                bytes(self._mm[block_offset:block_end]),
            )

    best = None
    for rec in iter_block(mv, scan_start, scan_end):
        if rec.key == key:
            if best is None or rec.seq > best[0]:
                best = (rec.seq, rec.timestamp_ms, rec.value)
        elif rec.key > key:
            break

    return best
```

---

## BUG-10 — HIGH: SSTableWriter Double-Finish Crash

### Location
`app/sstable/writer.py` — lines 137–159

### Current Code
```python
async def finish(self) -> SSTableMeta:
    self._flush_remaining()  # checks _State.OPEN, but state set to DONE only in _finalize()
    ...
    await asyncio.gather(...)  # async gap between check and state change
    ...
    return self._finalize()  # sets _State.DONE here

def _flush_remaining(self):
    if self._state is not _State.OPEN:
        raise SSTableWriteError("Writer is not in OPEN state")
    ...
    self._data_fd.close()  # closes file descriptor
```

### Problem
If two coroutines call `finish()` concurrently:
1. Both enter `_flush_remaining()` and pass the `_State.OPEN` check
2. First coroutine closes `self._data_fd`
3. Second coroutine tries to flush/close an already-closed fd → `ValueError: I/O operation on closed file`

The `_State.DONE` is only set in `_finalize()` which runs AFTER the async bloom/index writes. The window between the state check and state set is wide (entire bloom/index write duration).

### Proposed Fix
Add a `FINISHING` state and set it immediately:

```python
class _State(enum.Enum):
    OPEN = "OPEN"
    FINISHING = "FINISHING"
    DONE = "DONE"

async def finish(self) -> SSTableMeta:
    if self._state is not _State.OPEN:
        raise SSTableWriteError("Writer is not in OPEN state")
    self._state = _State.FINISHING  # immediate transition, before any I/O

    self._flush_block()
    self._data_fd.flush()
    os.fsync(self._data_fd.fileno())
    self._data_fd.close()
    ...
```

Since `SSTableWriter` is not shared across threads (one writer per flush slot), a lock isn't necessary — just the state guard.

---

## BUG-11 — HIGH: BloomFilter.from_bytes() Crashes on Truncated Data

### Location
`app/bloom/filter.py` — lines 66–74

### Current Code
```python
@classmethod
def from_bytes(cls, data: bytes) -> BloomFilter:
    num_hashes, bit_count, seed, _ = _HEADER_STRUCT.unpack_from(data)  # line 68
    obj = cls.__new__(cls)
    obj._num_hashes = num_hashes
    obj._bit_count = bit_count
    obj._seed = seed
    obj._bits = bytearray(data[_HEADER_SIZE:])  # line 73
    return obj
```

### Problem
1. **Truncated header:** If `data` has fewer than 16 bytes, `_HEADER_STRUCT.unpack_from(data)` raises `struct.error` — unhandled, crashes the caller (SSTableReader.open)
2. **Truncated bit array:** If the header claims `bit_count = 1_000_000` but `data` only has 100 bytes after the header, `self._bits` will be too small. Later, `may_contain()` computes `idx = h % bit_count` which can be up to 999,999, then `self._bits[idx >> 3]` → `IndexError` (index 124,999 on a 84-byte array)

This can happen when:
- A filter.bin file is truncated by a crash during write
- Disk corruption
- The SSTable directory is complete (meta.json exists) but filter.bin was only partially written before crash

### Proposed Fix
Validate before deserializing:

```python
@classmethod
def from_bytes(cls, data: bytes) -> BloomFilter:
    if len(data) < _HEADER_SIZE:
        raise CorruptRecordError(
            f"Bloom filter data too short: {len(data)} < {_HEADER_SIZE}"
        )

    num_hashes, bit_count, seed, _ = _HEADER_STRUCT.unpack_from(data)
    expected_bytes = (bit_count + 7) // 8
    actual_bytes = len(data) - _HEADER_SIZE

    if actual_bytes < expected_bytes:
        raise CorruptRecordError(
            f"Bloom filter bit array truncated: "
            f"need {expected_bytes} bytes, have {actual_bytes}"
        )

    obj = cls.__new__(cls)
    obj._num_hashes = num_hashes
    obj._bit_count = bit_count
    obj._seed = seed
    obj._bits = bytearray(data[_HEADER_SIZE : _HEADER_SIZE + expected_bytes])
    return obj
```

---

## BUG-12 — MEDIUM: SSTableManager.get() is Sequential, Not Parallel

### Location
`app/engine/sstable_manager.py` — lines 150–163

### Current Code
```python
async def get(self, key):
    best = None
    for file_id in self._l0_order:       # serial loop
        with self._registry.open_reader(file_id) as reader:
            result = reader.get(key)      # blocking mmap read
        if result is not None and (best is None or result[0] > best[0]):
            best = result
    return best
```

### Problem
The design docs (ASYNC_IO_INTEGRATION.md) specify concurrent L0 fan-out via `asyncio.gather`. The current implementation iterates serially. With N L0 files, read latency is O(N) instead of O(1) wall-clock time.

### Proposed Fix
Fan out reads concurrently:

```python
async def get(self, key):
    with self._lock:
        order_snapshot = list(self._l0_order)

    if not order_snapshot:
        return None

    async def _check_one(file_id: FileID) -> tuple[SeqNum, int, Value] | None:
        try:
            with self._registry.open_reader(file_id) as reader:
                return reader.get(key)
        except KeyError:
            return None

    results = await asyncio.gather(*[_check_one(fid) for fid in order_snapshot])

    best = None
    for result in results:
        if result is not None and (best is None or result[0] > best[0]):
            best = result
    return best
```

Note: Since `reader.get()` is sync (mmap), `asyncio.gather` won't help unless wrapped in `to_thread`. For pure mmap reads (microseconds), the serial loop may actually be faster than the overhead of task scheduling. The fix matters more when block cache misses trigger real I/O.

---

## BUG-13 — MEDIUM: FlushPipeline Errors Logged But Not Propagated

### Location
`app/engine/flush_pipeline.py` — lines 133–140 and `app/engine/lsm_engine.py` — line 156

### Problem
1. **Slot failures silently discarded:** `_dispatch_all()` catches exceptions via `return_exceptions=True` and logs them, but doesn't retry, re-queue the snapshot, or stop the pipeline.
2. **WAL truncation failures swallowed:** Line 191–197 catches WAL truncate errors and logs them, but continues. The WAL grows unbounded.
3. **Pipeline task crash undetected:** `asyncio.create_task(pipeline.run())` at `lsm_engine.py:156` has no done callback. If `run()` exits with an unhandled exception, the engine doesn't know — flushes silently stop.

### Proposed Fix
1. Add a done callback in the engine:
```python
engine._pipeline_task = asyncio.create_task(engine._pipeline.run())
engine._pipeline_task.add_done_callback(engine._on_pipeline_exit)

def _on_pipeline_exit(self, task: asyncio.Task) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.critical("FlushPipeline crashed", error=str(exc))
```

2. On slot failure, re-queue the snapshot instead of losing it (or at minimum, leave it in the immutable queue so it's retried on next dispatch).

---

## BUG-14 — MEDIUM: FlushPipeline Polls threading.Event via asyncio.sleep

### Location
`app/engine/flush_pipeline.py` — lines 84–89

### Current Code
```python
async def _wait_for_work(self):
    flush_event = self._mem.flush_event  # threading.Event
    while not flush_event.is_set() and not self._stop_event.is_set():
        await asyncio.sleep(0.05)  # 50ms polling interval
    flush_event.clear()
```

### Problem
The `flush_event` is a `threading.Event` but the consumer is an async loop. The 50ms polling interval wastes CPU and adds up to 50ms latency between a freeze event and flush dispatch.

### Proposed Fix
Replace with `asyncio.Event` and signal from the freeze path via `loop.call_soon_threadsafe`:

```python
# In MemTableManager:
self._flush_event_async: asyncio.Event | None = None  # set by pipeline

# In FlushPipeline.__init__:
self._flush_async = asyncio.Event()
mem._flush_event_async = self._flush_async

# In MemTableManager.maybe_freeze(), after appendleft:
if self._flush_event_async is not None:
    loop = asyncio.get_event_loop()
    loop.call_soon_threadsafe(self._flush_event_async.set)

# In FlushPipeline:
async def _wait_for_work(self):
    await self._flush_async.wait()
    self._flush_async.clear()
```

This eliminates polling entirely — the pipeline wakes up immediately when a freeze occurs.

---

## BUG-15 — MEDIUM: SparseIndex.next_offset_after() is O(n)

### Location
`app/index/sparse.py` — lines 63–68

### Current Code
```python
def next_offset_after(self, offset):
    for o in self._offsets:  # linear scan
        if o > offset:
            return o
    return None
```

### Problem
Called on every `SSTableReader.get()` via `_find_block_end()`. For an SSTable with 10,000 blocks (40MB at 4KB blocks), every read does a linear scan of 10,000 offsets.

### Proposed Fix
```python
def next_offset_after(self, offset: Offset) -> Offset | None:
    idx = bisect.bisect_right(self._offsets, offset)
    if idx < len(self._offsets):
        return self._offsets[idx]
    return None
```

O(log n) instead of O(n). For 10,000 blocks: ~14 comparisons instead of 10,000.

---

## BUG-16 — MEDIUM: SparseIndex.add() Doesn't Validate Key Order

### Location
`app/index/sparse.py` — lines 22–25

### Current Code
```python
def add(self, first_key, block_offset):
    self._keys.append(first_key)
    self._offsets.append(block_offset)
```

### Problem
No validation that keys are in ascending order. `floor_offset()` and `ceil_offset()` use `bisect` which assumes sorted input. If `add()` is called out of order (due to a bug in the caller), lookups return silently incorrect results.

### Proposed Fix
```python
def add(self, first_key: Key, block_offset: Offset) -> None:
    if self._keys and first_key <= self._keys[-1]:
        raise ValueError(
            f"Keys must be ascending: {first_key!r} <= {self._keys[-1]!r}"
        )
    self._keys.append(first_key)
    self._offsets.append(block_offset)
```

---

## BUG-17 — MEDIUM: SkipList Concurrent Same-Key Update Window

### Location
`app/memtable/skiplist.py` — lines 155–173

### Problem
When two threads insert the same key, the update path checks `node.fully_linked and not node.marked` **before** acquiring `node.lock`. Between the check and lock acquisition, another thread could mark the node (delete it). The code re-checks `node.marked` after acquiring the lock (line 165) and retries if marked, so correctness is maintained. No data loss occurs.

### Proposed Fix
No code change needed. The retry loop handles this correctly. Document the invariant:

```python
# NOTE: The check at line 160-162 is intentionally lock-free.
# If the node is marked between check and lock, the re-check
# at line 165 detects it and triggers a retry via `break`.
```

---

## BUG-18 — MEDIUM: Undocumented Lock Ordering (write_lock → _wal_lock)

### Location
`app/engine/lsm_engine.py` — line 181, 194 + `app/engine/wal_manager.py` — lines 60–62

### Problem
The engine acquires `_mem.write_lock` (line 181), then calls `_wal.sync_append()` which acquires `_wal_lock` (line 61). This is a nested lock: `write_lock → _wal_lock`. No reverse acquisition path exists today, so no deadlock occurs. But the ordering is undocumented and fragile — any future code that acquires `_wal_lock` then `write_lock` would deadlock.

### Proposed Fix
Document the lock ordering in both files:

```python
# In lsm_engine.py:
# LOCK ORDER: _mem.write_lock → _wal._wal_lock (never reverse)

# In wal_manager.py:
# NOTE: _wal_lock may be acquired while caller holds _mem.write_lock.
# Never acquire _mem.write_lock while holding _wal_lock.
```

Alternatively, remove `_wal_lock` entirely when called from the engine, since `write_lock` already serializes all WAL writes. Keep `_wal_lock` only for standalone async calls.

---

## BUG-19 — LOW: Empty SSTable Writes Placeholder Metadata

### Location
`app/sstable/writer.py` — lines 194–197

### Current Code
```python
min_key=self._min_key or b"",
max_key=self._max_key or b"",
seq_min=self._seq_min or 0,
seq_max=self._seq_max or 0,
```

### Problem
If `finish()` is called with zero records (no `put()` calls), metadata uses placeholder values: empty keys and seq 0. This could confuse future compaction logic that uses `min_key`/`max_key` for range overlap checks, or `seq_min`/`seq_max` for freshness ordering.

### Proposed Fix
Reject empty SSTables:
```python
def _flush_remaining(self):
    if self._state is not _State.OPEN:
        raise SSTableWriteError("Writer is not in OPEN state")
    if self._record_count == 0:
        raise SSTableWriteError("Cannot finish an empty SSTable")
    ...
```

---

## BUG-20 — LOW: SkipList Delete Inserts Phantom Tombstones

### Location
`app/memtable/skiplist.py` — lines 247–250

### Problem
Deleting a key that doesn't exist in the skiplist still inserts a TOMBSTONE entry. This is **correct by LSM design** — the tombstone must propagate to SSTables so that older versions of the key are shadowed. However, it inflates skiplist memory if many non-existent keys are deleted.

### Proposed Fix
No code change. This is by design. Document the trade-off:

```python
# NOTE: Tombstones are inserted even for non-existent keys.
# This is required for correctness: the tombstone must propagate
# to SSTables during flush to shadow any older versions of the
# key that may exist in lower levels.
```

---

## Summary

| Bug | Severity | Component | One-Line Description |
|-----|----------|-----------|---------------------|
| BUG-01 | **CRITICAL** | FlushPipeline | Write exception → event chain deadlock → all flushes freeze |
| BUG-02 | **CRITICAL** | MemTableManager | Lockless queue iteration → phantom read misses |
| BUG-03 | **CRITICAL** | MemTableManager | peek_at_depth TOCTOU → IndexError crash in pipeline |
| BUG-04 | HIGH | SSTableRegistry | close_all() closes readers in use → use-after-close |
| BUG-05 | HIGH | SSTableManager | L0 ordering by random UUID → unreliable order |
| BUG-06 | HIGH | SSTableReader | fd leak if mmap fails during open |
| BUG-07 | HIGH | SSTableReader | Empty SSTable uses platform-dependent anonymous mmap |
| BUG-08 | HIGH | SSTableManager | Concurrent _l0_order mutation → list corruption |
| BUG-09 | HIGH | SSTableReader | BlockCache instantiated but never consulted |
| BUG-10 | HIGH | SSTableWriter | Concurrent finish() → double-close crash |
| BUG-11 | HIGH | BloomFilter | from_bytes() crashes on truncated/corrupt data |
| BUG-12 | MEDIUM | SSTableManager | Sequential L0 reads instead of parallel fan-out |
| BUG-13 | MEDIUM | FlushPipeline | Errors logged not propagated; task crash undetected |
| BUG-14 | MEDIUM | FlushPipeline | 50ms busy-wait polling on threading.Event |
| BUG-15 | MEDIUM | SparseIndex | next_offset_after() O(n) linear scan |
| BUG-16 | MEDIUM | SparseIndex | add() doesn't validate ascending key order |
| BUG-17 | MEDIUM | SkipList | Same-key update lock window (handled by retry) |
| BUG-18 | MEDIUM | WAL/Engine | Nested lock ordering undocumented |
| BUG-19 | LOW | SSTableWriter | Empty SSTable writes placeholder metadata |
| BUG-20 | LOW | SkipList | Delete inserts phantom tombstone (by design) |

### Fix Priority

1. **Immediate (blocks correctness):** BUG-01, BUG-02, BUG-03
2. **Soon (prevents crashes/leaks):** BUG-04, BUG-06, BUG-07, BUG-08, BUG-10, BUG-11
3. **Next sprint (performance + completeness):** BUG-05, BUG-09, BUG-12, BUG-13, BUG-15
4. **Low priority (hardening):** BUG-14, BUG-16, BUG-17, BUG-18, BUG-19, BUG-20
