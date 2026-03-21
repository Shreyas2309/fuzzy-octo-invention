# Parallel Flush Workers — Implementation Specification
> Replaces the single-flush-worker design in §15 of CONTEXT.md.
> Pass to Claude Code in plan mode before implementing `app/engine/flush_pipeline.py`.

---

## 0. Why parallelism is safe — and where it isn't

The immutable queue has one inviolable rule: **data must never be unreachable**. At every point in time, any key that has been durably written must be findable by `get()` — either in `_mem._immutable_q` or in the SSTable registry. Never in neither.

This means two things can be parallelised freely, and one thing cannot:

| Phase | Parallel? | Reason |
|---|---|---|
| SkipList snapshot iteration (`snapshot.items()`) | Yes | Read-only, each snapshot is independent |
| SSTable disk write (`SSTableWriter.finish()`) | Yes | Each writes to a separate directory |
| Bloom filter + index build (inside `finish()`) | Yes | Already concurrent per writer via `asyncio.gather` |
| Registry register + `pop_oldest()` + WAL truncate | **No — strictly ordered** | Determines read visibility and WAL safety |

The insight: **writes are embarrassingly parallel, commits must be serialised in oldest-first order.** This is the pipeline model.

---

## 1. The pipeline model

```
Snapshot queue (newest → oldest):   [snap2]  [snap1]  [snap0]

Worker A picks snap0 (oldest):  ──[write]──────────────[commit A]──►
Worker B picks snap1:              ──[write]──────────[wait for A]──[commit B]──►
Worker C picks snap2:                 ──[write]─────────────────[wait for B]──[commit C]──►

Time ─────────────────────────────────────────────────────────────────────────────────────►
```

Each worker:
1. **Picks** a snapshot by position — worker 0 picks oldest, worker 1 picks second-oldest, etc.
2. **Writes** the SSTable to disk — fully concurrent, no coordination
3. **Waits** for the previous worker to commit — ordered by snapshot age
4. **Commits** — registers reader, pops from queue, truncates WAL
5. **Signals** the next worker that it can commit

The disk write is the slow part (~200–500ms). The commit is the fast part (<5ms). By making commits a dependency chain, we guarantee oldest-first ordering without any global lock during the expensive disk I/O.

---

## 2. The ordering mechanism — `asyncio.Event` chain

Each snapshot in the pipeline carries two events:

```python
@dataclass
class FlushSlot:
    snapshot:       ImmutableMemTable
    file_id:        FileID
    prev_committed: asyncio.Event   # set when the previous snapshot commits
    my_committed:   asyncio.Event   # set when this snapshot commits
```

When the pipeline dispatcher assigns snapshots to workers, it threads the events together:

```
slot0.prev_committed = already_set_event   ← no predecessor, can commit immediately
slot0.my_committed   = Event()

slot1.prev_committed = slot0.my_committed  ← must wait for slot0
slot1.my_committed   = Event()

slot2.prev_committed = slot1.my_committed  ← must wait for slot1
slot2.my_committed   = Event()
```

This forms a commit chain. Each worker blocks on `prev_committed.wait()` before committing — but this wait only starts after its disk write is complete, so it does not delay the write.

---

## 3. Component: `FlushPipeline` — `app/engine/flush_pipeline.py`

### 3a. State

```python
class FlushPipeline:
    _mem:            MemTableManager
    _sst:            SSTableManager
    _wal:            WALManager
    _max_workers:    int                # configurable — default 2
    _write_sem:      asyncio.Semaphore  # bounds concurrent disk writes
    _stop_event:     asyncio.Event      # set by engine.close()
    _last_committed: asyncio.Event      # tracks the tail of the commit chain
    _chain_lock:     asyncio.Lock       # serialises chain extension (dispatch only)
```

### 3b. Construction

```python
def __init__(
    self,
    mem:         MemTableManager,
    sst:         SSTableManager,
    wal:         WALManager,
    max_workers: int = 2,
) -> None:
    self._mem         = mem
    self._sst         = sst
    self._wal         = wal
    self._max_workers = max_workers
    self._write_sem   = asyncio.Semaphore(max_workers)
    self._stop_event  = asyncio.Event()
    self._chain_lock  = asyncio.Lock()

    # Bootstrap: a pre-set event so the first slot has no predecessor to wait for
    self._last_committed = asyncio.Event()
    self._last_committed.set()
```

`_last_committed` is the tail of the commit chain. It is always the `my_committed` event of the most recently dispatched slot. When the pipeline is empty it is pre-set so the first slot has no wait.

### 3c. Main worker loop

```python
async def run(self) -> None:
    """
    Main flush worker coroutine. Runs as a daemon task.
    Wakes on _flush_event, dispatches all queued snapshots concurrently,
    then sleeps until the next signal.
    """
    while not self._stop_event.is_set():
        # Wait for flush signal — timeout so we check stop_event periodically
        try:
            await asyncio.wait_for(
                asyncio.to_thread(self._mem._flush_event.wait),
                timeout=1.0,
            )
        except asyncio.TimeoutError:
            continue

        self._mem._flush_event.clear()
        await self._dispatch_all()

async def _dispatch_all(self) -> None:
    """
    Dispatch all currently queued snapshots as concurrent flush tasks.
    Each task gets a slot in the commit chain — ordering is established
    at dispatch time, before any disk write begins.
    """
    tasks: list[asyncio.Task] = []

    async with self._chain_lock:
        # Snapshot the current queue depth — dispatch exactly this many
        depth = self._mem.queue_len()
        for i in range(depth):
            snapshot = self._mem.peek_at_depth(i)
            if snapshot is None:
                break

            file_id = uuid.uuid4().hex

            # Build commit chain: this slot waits for the previous to commit
            prev_committed    = self._last_committed
            my_committed      = asyncio.Event()
            self._last_committed = my_committed

            slot = FlushSlot(
                snapshot       = snapshot,
                file_id        = file_id,
                prev_committed = prev_committed,
                my_committed   = my_committed,
                position       = i,
            )
            tasks.append(asyncio.create_task(self._flush_slot(slot)))

    if tasks:
        # Wait for all dispatched tasks — errors are logged, not raised
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(
                    "Flush task failed",
                    extra={"position": i, "error": str(result)},
                )
```

### 3d. `_flush_slot` — per-snapshot worker

```python
async def _flush_slot(self, slot: FlushSlot) -> None:
    """
    Two-phase flush for one snapshot:
      Phase 1: write SSTable to disk (concurrent, bounded by _write_sem)
      Phase 2: commit in chain order (serialised by prev_committed.wait())
    """
    logger.info(
        "Flush started",
        extra={
            "snapshot_id": slot.snapshot.snapshot_id,
            "file_id":     slot.file_id,
            "position":    slot.position,
        },
    )

    # ── Phase 1: write ────────────────────────────────────────────────────
    # Bounded by semaphore — at most _max_workers writes concurrently
    async with self._write_sem:
        try:
            meta, reader = await self._write_sstable(slot)
        except Exception as e:
            logger.error(
                "SSTable write failed",
                extra={
                    "snapshot_id": slot.snapshot.snapshot_id,
                    "file_id":     slot.file_id,
                    "error":       str(e),
                },
            )
            # Signal my_committed even on failure so the chain does not deadlock
            slot.my_committed.set()
            raise

    # ── Phase 2: commit ───────────────────────────────────────────────────
    # Wait for the previous slot to commit first — strict oldest-first ordering
    await slot.prev_committed.wait()

    try:
        await self._commit_slot(slot, meta, reader)
    except Exception as e:
        logger.error(
            "Flush commit failed",
            extra={
                "snapshot_id": slot.snapshot.snapshot_id,
                "file_id":     slot.file_id,
                "error":       str(e),
            },
        )
        slot.my_committed.set()   # unblock next slot even on failure
        raise

    logger.info(
        "Flush complete",
        extra={
            "snapshot_id": slot.snapshot.snapshot_id,
            "file_id":     slot.file_id,
            "seq_min":     meta.seq_min,
            "seq_max":     meta.seq_max,
        },
    )
```

### 3e. `_write_sstable` — Phase 1

```python
async def _write_sstable(
    self,
    slot: FlushSlot,
) -> tuple[SSTableMeta, SSTableReader]:
    """
    Write one SSTable to disk. Fully concurrent — no ordering constraints.
    Uses async finish() so Bloom filter and index are built concurrently.
    """
    directory = self._sst._data_root / "sstable" / "L0" / slot.file_id

    writer = SSTableWriter(
        directory   = directory,
        file_id     = slot.file_id,
        snapshot_id = slot.snapshot.snapshot_id,
        level       = 0,
    )

    # Iterate snapshot — sync loop, in-memory, no I/O
    for key, seq, timestamp_ms, value in slot.snapshot.items():
        writer.put(key, seq, timestamp_ms, value)

    # async finish: Bloom filter + index built concurrently with data block flush
    meta = await writer.finish()

    # Open reader — L0: index + Bloom loaded concurrently
    reader = await SSTableReader.open(
        directory, slot.file_id, self._sst._cache, level=0
    )
    return meta, reader
```

### 3f. `_commit_slot` — Phase 2

```python
async def _commit_slot(
    self,
    slot:   FlushSlot,
    meta:   SSTableMeta,
    reader: SSTableReader,
) -> None:
    """
    Commit one flush in strict oldest-first order.
    Precondition: slot.prev_committed is set (previous snapshot committed).

    Sequence (all three steps must happen in this order):
      1. register SSTable — makes it readable
      2. pop snapshot     — removes it from the immutable queue
      3. truncate WAL     — reclaims WAL space
    """
    # 1. Register reader — SSTable is now visible to _sst.get()
    self._sst._registry.register(meta.file_id, reader)
    self._sst._compactor.register_sstable(meta)

    # 2. Pop snapshot — safe because SSTable is now registered
    self._mem.pop_oldest()

    # 3. Truncate WAL — safe because snapshot is no longer in queue
    await self._wal.truncate_before(meta.seq_min)

    # 4. Signal next slot in commit chain
    slot.my_committed.set()
```

---

## 4. Changes to `MemTableManager`

The pipeline needs to peek at any position in the queue, not just the oldest. Add `peek_at_depth()`:

```python
def peek_at_depth(self, depth: int) -> ImmutableMemTable | None:
    """
    Return the snapshot at position `depth` from the oldest end.
    depth=0 → oldest (same as peek_oldest())
    depth=1 → second oldest
    Returns None if depth >= queue length.

    Used by FlushPipeline._dispatch_all() to assign slots without removing.
    """
    if depth >= len(self._immutable_q):
        return None
    # _immutable_q: newest at index 0 (left), oldest at right
    # depth 0 = oldest = rightmost = index -(depth+1)
    return self._immutable_q[-(depth + 1)]
```

`pop_oldest()` is unchanged — still removes from the right (oldest). The pipeline calls it exactly once per slot in the commit phase, in chain order, so the queue drains correctly.

---

## 5. Changes to `LSMEngine`

Replace `_flush_one()` and the single flush worker with `FlushPipeline`:

```python
class LSMEngine:
    _mem:      MemTableManager
    _sst:      SSTableManager
    _wal:      WALManager
    _seq:      SeqGenerator
    _pipeline: FlushPipeline      # ← replaces single flush worker
    _data_root: Path
    _closed:    bool
```

### 5a. `open()` — start the pipeline

```python
@classmethod
async def open(cls, data_root: Path) -> LSMEngine:
    engine            = cls.__new__(cls)
    engine._data_root = Path(data_root)
    engine._closed    = False

    cache         = BlockCache(maxsize=BLOCK_CACHE_MAXSIZE)
    engine._sst   = await SSTableManager.load(data_root, cache)
    engine._wal   = WALManager.open(data_root / "wal" / "wal.log")
    engine._mem   = MemTableManager()
    engine._seq   = SeqGenerator()

    engine._pipeline = FlushPipeline(
        mem         = engine._mem,
        sst         = engine._sst,
        wal         = engine._wal,
        max_workers = FLUSH_MAX_WORKERS,   # from config.py, default 2
    )

    engine._recover()
    asyncio.create_task(engine._pipeline.run())   # daemon task
    return engine
```

### 5b. `close()` — drain the pipeline

```python
async def close(self) -> None:
    if self._closed:
        return
    self._closed = True

    # Stop the pipeline loop
    self._pipeline._stop_event.set()

    # Freeze any remaining active memtable
    self._mem.maybe_freeze()

    # Synchronously drain the queue — flush everything to disk before exit
    while self._mem.queue_len() > 0:
        await self._pipeline._dispatch_all()

    # Wait for all in-flight tasks to complete
    # (asyncio.gather on all running pipeline tasks)

    self._wal.close()
    self._sst.close_all()
```

---

## 6. Correctness proof — why ordering is maintained

### 6a. Read gap invariant

At every point in time, every durable key is findable. This holds because:

- A snapshot is added to `_mem._immutable_q` before `_flush_event` is set
- During Phase 1 (write), the snapshot remains at its position in the queue — `peek_at_depth()` does not remove it
- During Phase 2 (commit), `register()` is called before `pop_oldest()`
- `register()` makes the SSTable visible to `_sst.get()` before the snapshot disappears from `_mem.get()`

There is no window where a key is unreachable.

### 6b. WAL truncation safety

WAL truncation is safe only when all three conditions hold:
1. SSTable files are fully on disk (`writer.finish()` completed)
2. SSTable is registered and readable (`registry.register()` called)
3. Snapshot is no longer in the queue (`pop_oldest()` called)

In `_commit_slot()`, these happen in exactly this order: register → pop → truncate. The commit chain ensures this sequence happens for slot N before slot N+1's commit begins.

**The crash safety argument:**
- Crash during Phase 1: snapshot still in queue. Recovery replays WAL. Incomplete SSTable directory has no `meta.json` — ignored at startup.
- Crash during Phase 2, after register, before pop: snapshot still in queue. Recovery replays WAL. SSTable already registered by `LeveledCompactor.load()` at startup. WAL replay deduplicates by seq.
- Crash after pop, before truncate: WAL intact. Recovery replays entries with seq > max_seq_seen. They are replayed and deduplicated.
- Crash after truncate: WAL safely truncated. SSTable has all data.

### 6c. Out-of-order completion

Worker B may finish writing before Worker A. The commit chain handles this:

```
snap0 (oldest):  ──[write: 400ms]──[wait: 0ms (no predecessor)]──[commit A]──►
snap1 (newer):   ──[write: 200ms]──[wait: 200ms for A]────────────────────────[commit B]──►
```

snap1 finishes writing 200ms before snap0. It blocks on `prev_committed.wait()` for 200ms while snap0 writes. Total elapsed time for snap1's commit: 400ms (same as snap0) + commit overhead. No data loss, no ordering violation.

The wait time is bounded by the difference in write times between adjacent snapshots, not by the total number of snapshots.

---

## 7. State machine with two workers

```
Queue at dispatch:  [snap1(newer)]  [snap0(oldest)]

Dispatch creates:
  slot0: prev=pre_set_event,        my=Event_A
  slot1: prev=Event_A,              my=Event_B

_last_committed = Event_B

Worker picks slot0:
  Phase 1: write snap0 to disk (concurrent, under semaphore)
  Phase 2: prev=pre_set_event → already set, no wait
           commit: register L0a, pop snap0, truncate WAL to snap0.seq_min
           set Event_A

Worker picks slot1:
  Phase 1: write snap1 to disk (concurrent with slot0's Phase 1)
  Phase 2: prev=Event_A → wait until slot0 commits
           commit: register L0b, pop snap1, truncate WAL to snap1.seq_min
           set Event_B

Timeline:
slot0: ──[write 300ms]──────────────────[commit]──────►
slot1:     ──[write 200ms]──[wait 100ms]────────[commit]──►
           ^concurrent write         ^ordered commit
```

After both slots complete:
- Queue is empty
- L0a and L0b are in registry
- WAL truncated to snap1.seq_min
- Event_B is set (next dispatch starts fresh)

---

## 8. Edge cases

### 8a. New snapshot arrives while pipeline is running

`_flush_event` is set again by `maybe_freeze()`. The next iteration of `run()` wakes and calls `_dispatch_all()`, which picks up any snapshots added after the current batch. The commit chain for the new batch starts from `_last_committed` — which is the `my_committed` of the last slot of the previous batch. Ordering is preserved across batches.

### 8b. Single snapshot in queue

Degenerate case — one slot, `prev_committed` is pre-set, commits immediately after write. Same as the single-worker design, no overhead.

### 8c. Write failure in Phase 1

If `_write_sstable()` raises, the slot:
1. Sets `my_committed` (so the chain does not deadlock)
2. Re-raises the exception (caught by `asyncio.gather`, logged)
3. Does NOT call `pop_oldest()` — snapshot stays in queue

On the next `_flush_event`, the pipeline will re-attempt to dispatch the same snapshot (it is still at the head of the queue). This is correct — transient disk errors are retried automatically.

The WAL is not truncated — the snapshot's data remains recoverable.

### 8d. Commit failure in Phase 2

Rare — would indicate a programming error (e.g., registry already contains the file_id). The slot sets `my_committed` to unblock the chain and re-raises. The queue is left inconsistent — this is a fatal error that should crash the engine and trigger recovery.

### 8e. `close()` called while pipeline is running

`_stop_event.set()` stops the main loop after the current `_dispatch_all()` completes. `close()` then calls `_dispatch_all()` synchronously in a loop until the queue is empty — all remaining snapshots are flushed before shutdown.

---

## 9. Configuration

```python
# app/common/config.py — add these

FLUSH_MAX_WORKERS: int = int(os.getenv("LSM_FLUSH_MAX_WORKERS", "2"))
# Recommended values:
#   NVMe SSD: 2 (write pipeline, near-100% disk utilisation)
#   SATA SSD: 2
#   HDD:      1 (concurrent sequential writes compete for head)
#   Memory-only test: 4+ (no real I/O bottleneck)
```

---

## 10. Lock and synchronisation inventory

The parallel pipeline introduces two new async primitives. Full inventory:

| Primitive | Lives in | Type | Protects |
|---|---|---|---|
| `_write_lock` | `MemTableManager` | `threading.RLock` | `put()`, `maybe_freeze()` |
| `_seq_lock` | `SeqGenerator` | `threading.Lock` | `_val += 1` |
| `_queue_not_full` | `MemTableManager` | `threading.Condition` | backpressure wait |
| `_flush_lock` | `SSTableManager` | `threading.RLock` | manifest update only (brief) |
| `_wal_lock` | `WALManager` | `threading.Lock` | WAL file operations |
| `_write_sem` | `FlushPipeline` | `asyncio.Semaphore` | concurrent disk writes |
| `_chain_lock` | `FlushPipeline` | `asyncio.Lock` | chain extension at dispatch |
| `prev_committed` | `FlushSlot` | `asyncio.Event` | commit ordering between slots |
| `my_committed` | `FlushSlot` | `asyncio.Event` | signals next slot to commit |
| `_stop_event` | `FlushPipeline` | `asyncio.Event` | graceful shutdown |

**No threading lock is ever held when an asyncio primitive is awaited.** `_write_lock`, `_wal_lock`, etc. are released before any `await`. The async primitives (`_write_sem`, `prev_committed`) are only awaited in async context where no threading lock is held.

---

## 11. Changes to existing CONTEXT.md sections

The following CONTEXT.md sections need updating after this implementation:

### §14a MemTableManager — add `peek_at_depth()`
```
def peek_at_depth(self, depth: int) -> ImmutableMemTable | None
    # Returns snapshot at position `depth` from the oldest end (depth=0 = oldest)
    # Does not remove — used by FlushPipeline to assign slots without popping
```

### §14d LSMEngine state — replace flush fields
```python
# REMOVE: (implicit single flush worker)
# ADD:
_pipeline: FlushPipeline   # manages parallel flush workers
```

### §15 Async flush — replace §15a actor description
```
Writer thread(s)  → put() / delete() — calls _mem.put(), may call _mem.maybe_freeze()
FlushPipeline     → dispatches concurrent _flush_slot tasks, ordered commit chain
Reader thread(s)  → get() — scans _mem._active + _mem._immutable_q + SSTables
```

### §14f locks table — add pipeline primitives
Add the `_write_sem`, `_chain_lock`, slot events to the table.

---

## 12. Implementation order

```
Step 1 → app/engine/flush_pipeline.py     FlushSlot dataclass + FlushPipeline class
Step 2 → app/engine/memtable_manager.py   add peek_at_depth()
Step 3 → app/engine/engine.py             replace _flush_one with FlushPipeline
Step 4 → app/common/config.py             add FLUSH_MAX_WORKERS
Step 5 → tests/test_flush_pipeline.py     tests (see §13)
```

---

## 13. Test checklist

### Correctness
- [ ] Single snapshot: flushes correctly, WAL truncated, queue empty after
- [ ] Two snapshots dispatched together: both reach disk, committed oldest-first
- [ ] Three snapshots: all three flushed, commit order matches snapshot age (oldest seq first)
- [ ] Out-of-order completion: slower snapshot (snap0) finishes after faster (snap1) — snap0 still commits first
- [ ] Read gap: during Phase 1, snapshot is still in `_mem._immutable_q` and readable
- [ ] Read gap: after Phase 2 commit, snapshot removed from queue and SSTable readable — no gap
- [ ] WAL truncation: happens only after pop, not before
- [ ] New snapshot during flush: picked up by next dispatch cycle, correct chain ordering

### Failure handling
- [ ] Phase 1 failure: snapshot stays in queue, next dispatch retries it
- [ ] Phase 1 failure: `my_committed` is set — chain does not deadlock
- [ ] Phase 1 failure of slot0: slot1 still commits (via `prev_committed.wait()` unblocked)
- [ ] Semaphore bounding: with `max_workers=2`, at most 2 writes happen concurrently

### Concurrency
- [ ] 4 concurrent writers + 2 flush workers: no data loss, no duplicates after close
- [ ] Rapid freeze + flush: queue depth stays bounded at HARD_LIMIT
- [ ] `close()` drains queue before returning: all snapshots flushed

### Recovery
- [ ] Crash during Phase 1: recovery replays WAL, incomplete SSTable directory ignored
- [ ] Crash after register, before pop: SSTable loaded at startup, WAL deduplicates
- [ ] Crash after truncate: data only in SSTable, recovery works correctly