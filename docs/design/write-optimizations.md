# Write-Path Optimizations

This document catalogs every significant design decision in lsm-kv and explains how each one contributes to a write-optimized LSM architecture. Sections are ordered by impact on write throughput — the highest-impact architectural decisions first, supporting optimizations later. Each section provides enough context for generating presentation slides — including the problem, the chosen solution, what alternatives exist, and the concrete benefit.

---

## 1. L0 Multiple Files / L1+ Single File: Write-Optimized Level Organization

**Problem**: In a traditional leveled LSM-Tree, every level maintains a single sorted run. When a memtable is flushed, the resulting SSTable must be *merged* into the existing L0 sorted run before the flush can complete. This merge-on-write design means the write path is blocked by compaction-like I/O — the very thing compaction was supposed to defer.

**Decision**: lsm-kv splits level organization into two distinct strategies:

| Level | Files | Key Ranges | Flush Cost | Read Cost |
|-------|-------|------------|------------|-----------|
| **L0** | Up to 10 SSTables | **Overlapping** — same key can exist in multiple files | **O(1)**: just write a new file, no merge | Must check **all** L0 files |
| **L1–L3** | Exactly 1 SSTable per level | **Non-overlapping** — fully merged and deduplicated | N/A (produced by compaction) | Check **one** file per level |

![Architecture Diagram](images/sst_org_optimization_1.jpeg)
<!-- ![Architecture Diagram](images/sst_org_optimization_2.jpeg) -->

**How this optimizes writes**:

The flush pipeline writes a new L0 SSTable and *returns immediately*. No merge, no sorting against existing L0 data, no blocking on compaction. The write path never touches existing SSTables — it only creates new files.

```
Traditional LSM (merge-on-flush):
  freeze memtable → merge with L0 sorted run → replace L0 → done
  └── blocked on merge I/O (100–500ms for 64MB)

lsm-kv (append-to-L0):
  freeze memtable → write new L0 file → register in manifest → done
  └── no merge; flush is pure sequential write (~50ms for 64MB)
```

This is the foundational write-throughput optimization. Everything else — the parallel flush pipeline, the subprocess compaction, the level reservations — builds on this separation between "fast L0 append" and "deferred background merge."

**The read-side tradeoff**: Since L0 files have overlapping key ranges, `get()` must check *all* L0 files (not just one). This is mitigated by:

1. **Bloom filters** — each L0 file has its own bloom filter. A negative bloom check skips the entire file in microseconds.
2. **Bounded L0 count** — compaction triggers at `l0_compaction_threshold` (default 10). L0 never grows unbounded.
3. **Newest-first ordering** — L0 files are scanned newest-first. For hot keys (recently written), the first file often has the answer.

**Compaction benefit**: When L0 reaches the threshold, *all* L0 files are merged into a single L1 file in one pass. L1+ levels always hold exactly one file, so subsequent compactions (L1→L2, L2→L3) are simple two-file merges — no fan-out, no multi-way coordination.

**Alternative considered**: Universal compaction (all levels allow multiple files). Rejected because it increases read amplification at every level and makes point lookups expensive deep in the tree. The L0-multiple/L1+-single hybrid gives the write speed of universal compaction at L0 with the read efficiency of leveled compaction at L1+.

---

## 2. Parallel Flush Pipeline with Ordered Commits

**Problem**: Flushing memtable snapshots to SSTables is I/O-bound (~200–500ms per 64MB SSTable). Sequential flushing wastes disk bandwidth when multiple snapshots are queued.

**Decision**: Two-phase flush with parallel writes and serialized commits:

| Phase | Parallelism | What happens |
|-------|-------------|-------------|
| **Phase 1: Write** | Parallel (up to `flush_max_workers`, default 2) | SSTableWriter produces data.bin + index.bin + filter.bin + meta.json |
| **Phase 2: Commit** | Serialized (oldest-first via event chain) | Register reader → pop queue → truncate WAL → trigger compaction |

The ordering mechanism is an `asyncio.Event` chain:

```
slot0.prev_committed = pre_set_event       ← commits immediately
slot0.my_committed   = Event_A

slot1.prev_committed = Event_A             ← waits for slot0
slot1.my_committed   = Event_B
```

**Why it matters**: Disk writes are the bottleneck. Parallelizing them doubles throughput on NVMe. But commits must be FIFO to maintain the immutable queue invariant — the oldest snapshot must be committed first so `pop_oldest()` removes the correct entry.

**Failure handling**: If any slot fails, `batch_abort` is set — all downstream slots skip their commits. The failed snapshot stays in the queue and is retried on the next dispatch cycle. `my_committed` is always set in a `finally` block to prevent event chain deadlock.

**Operational Complexity Comparison** (K = queued snapshots, W = max_workers, T_io = single SSTable write time):

| Approach | Flush Latency (K snapshots) | Disk Utilization | Commit Ordering |
|----------|---------------------------|------------------|-----------------|
| **lsm-kv (parallel write + serial commit)** | **T_io × ceil(K/W)** — parallel bounded by W | **~100%** with W=2 on NVMe | Guaranteed FIFO via event chain |
| Sequential flush (one at a time) | T_io × K — fully serial | ~50% on NVMe (idle between flushes) | Trivially ordered |
| Fully parallel (no ordering) | T_io × 1 (all concurrent) | ~100% | **Broken** — manifest non-deterministic, queue pops unordered |
| Batched flush (group then write) | T_io + merge cost | Variable | Requires post-merge sort |

With W=2 and K=4 queued snapshots: sequential takes 4 × T_io, parallel takes 2 × T_io — a **2x throughput improvement** with correctness preserved by the event chain. The commit phase is O(1) per slot (register + pop + truncate), so the serialization overhead is negligible (<5ms vs 200–500ms write time).

---

## 3. Subprocess Compaction (GIL Bypass)

**Problem**: Compaction merges large SSTables — a CPU-bound operation. On CPython 3.12 with GIL enabled, threading cannot achieve true parallelism for this workload.

**Decision**: Compaction runs in a `ProcessPoolExecutor` subprocess:

```
Main Process (asyncio)              Subprocess (own GIL)
─────────────────────               ─────────────────────
CompactionManager                   run_compaction(task)
  _build_task() → CompactionTask      open input SSTables
  asyncio.to_thread(                   KWayMergeIterator
    pool.submit(run_compaction)          dedup + GC tombstones
  )                                    SSTableWriter.finish_sync()
  ← SSTableMeta result                return SSTableMeta
  commit_compaction_async()
```

**Why it matters**: The merge is CPU-bound (decode records, compare keys, re-encode). Running it in a subprocess with its own GIL means the main event loop continues accepting writes and reads uninterrupted.

**CompactionTask serialization**: The task is a frozen dataclass with only primitive fields (str, int, list, dict, float) — no file handles, no locks, no live objects. Safe to send across the process boundary.

**Non-blocking dispatch**: `asyncio.create_task()` launches the job and returns immediately. The engine doesn't wait for compaction to finish.

**Operational Complexity Comparison** (M = total records to merge, T_merge = merge time):

| Approach | Write Path Blocking | Merge Parallelism | Event Loop Impact |
|----------|--------------------|--------------------|-------------------|
| **lsm-kv (ProcessPoolExecutor subprocess)** | **Zero** — fire-and-forget dispatch | **True parallelism** — own GIL, own memory space | **None** — event loop free for put/get |
| In-thread compaction (GIL) | Zero (background thread) | **None** — GIL serializes with writes | CPU-bound merge competes with writes for GIL |
| Synchronous compaction (inline) | **O(M)** — blocks until merge completes | N/A | **Blocked** — no writes during merge |
| Async compaction (to_thread) | Zero (offloaded) | **None** — still one GIL | Moderate — thread pool contention with WAL fsync |

The subprocess approach is the only option that achieves true parallelism on CPython 3.12. A 64MB L0→L1 merge (~650K records) takes ~500ms of CPU. With in-thread compaction, this 500ms competes with the write path for GIL time slices. With subprocess, both run at full speed simultaneously.

---

## 4. Write Path: Atomic Under a Single Lock

**Problem**: Traditional LSMs either use separate locks per component (WAL lock, memtable lock) risking inconsistency, or a coarse global lock that blocks reads during writes.

**Decision**: A single `_mem.write_lock` (RLock) protects the entire atomic write sequence:

1. `SeqGenerator.next()` — monotonic sequence number
2. `WALManager.sync_append()` — encode + write + fsync (durability)
3. `MemTableManager.put()` — skip list insert (visibility)
4. `MemTableManager.maybe_freeze()` — threshold check + possible freeze

**Why it matters**: The WAL is fsynced *before* the memtable is updated. If the process crashes after fsync but before memtable put, recovery replays the WAL entry. If it crashes before fsync, the write was never acknowledged — no data loss in either case.

**Alternative considered**: Per-component locks with two-phase commit. Rejected because it adds complexity without throughput gain — the bottleneck is fsync latency (~1ms), not lock contention.

**Lock ordering rule**: `_mem.write_lock` → `_wal_lock`. Never reversed. Enforced by code structure — the engine acquires write_lock, then sync_append acquires wal_lock internally.

**Operational Complexity Comparison** (T_fsync = WAL fsync latency, T_put = memtable insert):

| Approach | Write Latency | Consistency Guarantee | Read Blocking |
|----------|--------------|----------------------|---------------|
| **lsm-kv (single atomic lock)** | **T_fsync + T_put** — sequential under one lock | **Full** — WAL durable before memtable visible | **None** — readers are lock-free |
| Per-component locks (WAL lock + memtable lock) | T_fsync ‖ T_put — potentially parallel | Requires 2PC or ordering protocol | Depends on lock design |
| Global RWLock (read/write lock) | T_fsync + T_put — same latency | Full | **Readers block during writes** |
| Lock-free CAS (compare-and-swap) | T_fsync + T_put — plus retry overhead | Requires careful memory ordering | None |

The single-lock approach has the same write latency as alternatives (T_fsync dominates at ~1ms, T_put is microseconds) but is simpler and doesn't block readers. Per-component locks add coordination complexity for no throughput gain because fsync is the bottleneck, not lock contention.

---

## 5. Concurrent Skip List with Lock-Free Reads

**Problem**: The memtable needs to support concurrent writes from multiple threads while allowing lock-free reads for the hot read path.

**Decision**: A probabilistic skip list with three concurrency mechanisms:

| Mechanism | Who | How |
|-----------|-----|-----|
| Per-node locks | Writers | Lock only predecessor nodes at insertion boundary |
| Lock-free reads | Readers | Check `fully_linked` and `marked` boolean flags (GIL-atomic) |
| Deterministic lock ordering | Writers | Acquire locks in ascending `id()` order to prevent deadlock |

**Why it matters**: Readers (the `get()` path) never block. The `fully_linked` flag is the visibility barrier — it's set to `True` only after all forward pointers are linked, making the node atomically visible to concurrent readers.

**Alternative comparison**:

| Structure | Sorted Iteration | Point Lookup | Concurrent Writes | Lock-Free Reads |
|-----------|-----------------|--------------|-------------------|-----------------|
| **Skip List** | O(n) | O(log n) | Per-node locks | Yes |
| Hash Map | Requires sort | O(1) avg | Global lock | Yes |
| B-Tree | O(n) | O(log n) | Complex rebalancing | No |

Skip list wins because SSTable flush requires sorted iteration, and lock-free reads are critical for throughput.

**Safety bound**: 64 retries before raising `SkipListInsertError`. Prevents live-lock under extreme contention.

**Operational Complexity Comparison** (N = entries, C = concurrent writers, R = concurrent readers):

| Structure | Write | Read | Freeze (sorted snapshot) | Write Contention (C writers) | Read Contention (R readers) |
|-----------|-------|------|-------------------------|-----------------------------|-----------------------------|
| **Skip List (lsm-kv)** | **O(log N)** per-node lock | **O(log N)** lock-free | **O(N)** lock-free iteration | **O(C × log N)** — disjoint keys don't contend | **Zero** — no locks |
| B-Tree + RWLock | O(log N) write-locked | O(log N) read-locked | O(N) read-locked | O(C × log N) — serialized by global write lock | O(R × log N) — blocked during writes |
| Hash Map + sort-on-freeze | **O(1)** amortized | **O(1)** | **O(N log N)** sort required | O(C) — global lock | Zero (if lock-free hash) |
| Red-Black Tree + RWLock | O(log N) | O(log N) | O(N) | Rebalancing holds lock longer | Blocked during rebalance |

The skip list's unique advantage: **writes are O(log N) with per-node locking** (concurrent writes to disjoint keys proceed without contention) **and reads are O(log N) with zero locking** (GIL-atomic flag checks). The hash map alternative has O(1) writes but pays O(N log N) at freeze time — for a 64MB memtable with ~650K entries, that's a significant freeze stall.

---

## 6. Backpressure: Bounded Immutable Queue

**Problem**: If the flush pipeline falls behind, frozen memtable snapshots accumulate in memory without bound, eventually causing OOM.

**Decision**: The immutable queue has a maximum depth (`immutable_queue_max_len`, default 4). When full, `maybe_freeze()` blocks the writer on a `Condition.wait()` with a 60-second timeout:

```
Writer calls put() → threshold reached → maybe_freeze()
  └─ queue full? → wait(60s) → FreezeBackpressureTimeout if still full
```

**Why it matters**: Backpressure propagates to the caller — if the disk can't keep up, writes slow down rather than consuming unbounded memory. The timeout detects stuck flush pipelines (e.g., disk failure) and raises an actionable error.

**Operational Complexity Comparison** (Q = queue depth, M = memtable size, D = disk throughput):

| Approach | Memory Usage | Write Stall Behavior | Failure Detection |
|----------|-------------|---------------------|-------------------|
| **lsm-kv (bounded queue + timeout)** | **O(Q × M)** — bounded at 4 × 64MB = 256MB max | **Controlled** — blocks writer, resumes when space frees | **60s timeout** → FreezeBackpressureTimeout |
| Unbounded queue (no limit) | **O(∞)** — grows until OOM | **None** — writes never stall, then process crashes | OOM kill (uncontrolled) |
| Drop-oldest (evict unflushed) | O(Q × M) — bounded | None — but **loses data** silently | No detection needed (data lost) |
| Rate limiting (token bucket) | O(Q × M) — bounded | Smooth degradation | Latency spike detection |

The bounded queue with timeout is the only approach that provides: (1) bounded memory (no OOM), (2) no data loss (snapshot stays in queue), and (3) actionable failure detection (timeout raises a named exception the caller can handle).

---

## 7. SSTable Four-File Layout with Completeness Signal

**Problem**: A crash during SSTable write must not produce a corrupted table that poisons reads on recovery.

**Decision**: Each SSTable is a directory with four files, written in a specific order:

```
1. data.bin      ← sorted records in blocks (written during put() calls)
2. index.bin     ← sparse index (written during finish())
3. filter.bin    ← bloom filter (written during finish(), concurrent with index)
4. meta.json     ← metadata (written LAST — completeness signal)
```

**Why it matters**: `meta.json` is the completeness signal. If it exists, all other files are guaranteed to be fully written and fsynced. If it's missing (crash during write), the SSTable directory is ignored on recovery. No corruption, no partial reads.

For L0 SSTables, `finish()` writes bloom and index **concurrently** via `asyncio.gather()` — parallelizing two I/O operations that are independent.

---

## 8. WAL Design: Msgpack Framing with Per-Write Fsync

**Problem**: The WAL must be durable (survive process crash), compact (minimize I/O), and recoverable (detect corruption).

**Decision**: Each WAL entry is framed as:

```
[4B payload length][msgpack(seq, ts, op, key, value)][4B CRC32]
```

- **Msgpack**: Compact binary encoding (vs JSON), fast to encode/decode
- **Length prefix**: Enables fast forward scanning during replay
- **CRC32 per frame**: Detects corruption at frame granularity — a partial write from a crash is detected and replay stops cleanly
- **Fsync per write**: Every `append()` calls `fd.flush()` + `os.fsync()` before returning

**Why it matters**: Per-write fsync is the conservative choice — every acknowledged write is durable. During recovery, entries already flushed to SSTables are filtered by sequence number (`seq > sst_max_seq`), avoiding redundant replay.

---

## 9. Level Reservation for Conflict-Free Compaction

**Problem**: Multiple compaction jobs can run concurrently, but adjacent-level jobs conflict. An L0→L1 job reads L1, so a concurrent L1→L2 job writing to L1 would cause data corruption.

**Decision**: A set-based reservation system tracks active levels:

```python
_active_levels: set[Level] = set()

# Reserve: blocked if either level is already active
if src in _active_levels or dst in _active_levels:
    return False  # skip this job, try next cycle
_active_levels.add(src)
_active_levels.add(dst)
```

**Conflict matrix** (with `max_levels = 3`):

| Running Job | Reserves | Blocks | Can Parallel With |
|-------------|----------|--------|-------------------|
| L0→L1 | {0, 1} | L0→L1, L1→L2 | L2→L3 |
| L1→L2 | {1, 2} | L0→L1, L1→L2, L2→L3 | Nothing |
| L2→L3 | {2, 3} | L1→L2, L2→L3 | L0→L1 |

**Why it matters**: Only truly independent jobs run in parallel. After a job completes, `check_and_compact()` is re-triggered to pick up deferred work — cascading compactions happen naturally.

---

## 10. Bloom Filter: Data-Derived Sizing with Environment-Aware FPR

**Problem**: A bloom filter that's too large wastes memory and build time. One that's too small has excessive false positives, causing unnecessary disk reads.

**Decision**: Two parameters, sourced differently:

| Parameter | Source | Value |
|-----------|--------|-------|
| `bloom_n` (expected items) | **Actual data count** — `len(snapshot)` for flush, `sum(record_counts)` for compaction | Exact |
| `bloom_fpr` (false positive rate) | **Config** — `bloom_fpr_dev = 0.05`, `bloom_fpr_prod = 0.01` | Environment-aware |

**Why it matters**: Using the actual record count produces an optimally sized filter — no over-provisioning, no under-provisioning. The FPR tradeoff is:

- **Dev (5%)**: Smaller filters, faster builds, good enough for small test datasets
- **Prod (1%)**: Larger filters, fewer false disk reads on production workloads

The optimal bit count and hash count are derived from standard formulas:
- Bit count: `m = -n × ln(fpr) / ln²(2)`
- Hash count: `k = (m/n) × ln(2)`

---

## 11. Lazy Loading with Cross-Reader Cache Reuse

**Problem**: Opening an SSTable eagerly (loading bloom filter + sparse index at open time) is wasteful when most L0 files are compacted away before being queried.

**Decision**: Bloom and index are loaded **lazily on first `get()` call**, not at open time:

1. Check `BlockCache` for cached bytes (sentinel offsets: -1 for bloom, -2 for index)
2. On cache miss, read from disk (`filter.bin`, `index.bin`)
3. Populate cache for future readers
4. Deserialize into `BloomFilter` and `SparseIndex` objects

**Why it matters**: At startup, the engine opens all SSTables but only mmaps `data.bin`. The bloom and index are deferred until actually needed. After the first read, they're cached — even if the reader is closed and a new one opened for the same file (common after engine restart), the cache hit avoids disk I/O entirely.

---

## 12. Three-Tier Block Cache with Differential Eviction

**Problem**: A single LRU cache treats all entries equally, but bloom filters and indexes are far more valuable than individual data blocks — they're reused across many queries.

**Decision**: Three separate LRU caches with independent capacities:

| Tier | Sentinel Offset | Default Capacity | Eviction Priority |
|------|----------------|-------------------|-------------------|
| Data blocks | >= 0 | 256 entries | Evicted first |
| Sparse indexes | -2 | 64 entries | Medium retention |
| Bloom filters | -1 | 64 entries | Evicted last |

Routing is automatic — the `offset` parameter determines which tier receives the entry.

**Why it matters**: Under heavy read load, data blocks churn through the cache rapidly. Without tiering, they would evict bloom filters and indexes, forcing expensive re-loads. With tiering, blooms and indexes stay resident while data blocks rotate through their own pool.

---

## 13. Runtime-Mutable Configuration

**Problem**: Production workloads change over time. Fixed thresholds require restart to tune.

**Decision**: All thresholds are stored in `config.json` and read at the point of use (not cached):

```python
# In MemTableManager.maybe_freeze():
if self._config.is_prod:
    return self._active.size_bytes >= self._config.max_memtable_bytes
```

Updating a config value:
```python
engine.update_config("max_memtable_entries", 50)  # takes effect on next put()
```

The update is persisted atomically (temp file + `os.replace()`) and logged.

**Why it matters**: No restart needed to tune freeze thresholds, compaction triggers, bloom FPR, or cache sizes. The dev/prod mode switch (`config.set("env", "prod")`) changes multiple behaviors at once — freeze trigger type, bloom FPR, compaction threshold scaling.

---

## 14. Recovery: Seq-Based WAL Filtering

**Problem**: After a crash, the WAL may contain entries that were already flushed to SSTables before the crash. Replaying them is redundant and wastes time.

**Decision**: Recovery filters WAL entries by sequence number:

```
1. sst_max_seq = SSTableManager.max_seq_seen()     ← highest seq in all SSTables
2. entries = WAL.replay()                            ← all entries in WAL
3. unflushed = [e for e in entries if e.seq > sst_max_seq]
4. MemTableManager.restore(unflushed)               ← replay only unflushed
5. SeqGenerator.restore(max_unflushed_seq)           ← ensure next seq is higher
```

**Why it matters**: On a store with millions of entries, the WAL might have thousands of entries from the last few seconds. But most are already in SSTables. Seq-based filtering skips them in O(n) with no disk I/O — just integer comparison.

---

## 15. AsyncRWLock with Writer Preference

**Problem**: During compaction commits, the SSTableManager needs exclusive access to a level's file list. But reads are far more frequent than compaction commits. A reader-preference lock would starve the compaction commit under continuous read load.

**Decision**: Writer-preference async RWLock — once a writer is waiting, new readers block:

```
Reader arrives → if writer_waiting > 0: block
Writer arrives → wait for readers == 0, then proceed
```

Each compaction level has its own lock. This means:
- L0 reads and L2→L3 compaction proceed simultaneously (different locks)
- L0 reads block briefly during L0→L1 commit (same lock, < 5ms)

**Why it matters**: Compaction must complete promptly to prevent L0 buildup. Writer preference ensures compaction commits aren't starved, while per-level granularity limits reader blocking to only the affected level.

---

## 16. mmap Reads with Zero-Copy Block Scanning

**Problem**: Reading SSTable records involves I/O. Traditional approaches copy data from kernel to user space, then deserialize each record.

**Decision**: `data.bin` is memory-mapped at open time. Reads use `memoryview` slicing — no copy until cache insertion:

```python
mv = memoryview(self._mm)           # zero-copy view into mmap
for rec in iter_block(mv, start, end):  # decode records on-the-fly
    if rec.key == key:
        return (rec.seq, rec.timestamp_ms, rec.value)
```

Cache insertion is the only copy:
```python
self._cache.put(file_id, offset, bytes(self._mm[offset:end]))
```

**Why it matters**: After the OS page cache is warm, mmap reads are effectively memory reads — no syscall, no copy. The `iter_block()` generator decodes records lazily and stops early when `rec.key > key` (records are sorted).

---

## 17. Manifest: Atomic Persistence via Temp + Replace

**Problem**: The manifest tracks L0 ordering and per-level file assignments. A corrupt manifest after crash means all SSTables become orphaned.

**Decision**: Manifest written via temp file + `os.replace()`:

```python
tmp = tempfile.NamedTemporaryFile(dir=manifest_dir, ...)
tmp.write(json.dumps(layout))
os.fsync(tmp.fileno())
os.replace(tmp_path, manifest_path)  # atomic on POSIX
```

**Why it matters**: `os.replace()` is atomic on POSIX filesystems. Either the old manifest or the new one is visible — never a partial write. If crash occurs before replace, the old manifest is intact. If after, the new one is fully written and fsynced.

On startup, the manifest is reconciled with what's actually on disk — orphan files are adopted, missing files are skipped.

---

## Summary: Write-Path Optimization Stack

The following table summarizes how each decision contributes to write throughput, ordered by impact:

| # | Layer | Optimization | Effect on Writes |
|---|-------|-------------|-----------------|
| 1 | **Level Architecture** | **L0 multiple files / L1+ single file** | **Flush is pure append — no merge-on-write** |
| 2 | **Flush** | **Parallel SSTable writes + ordered commits** | **2x disk throughput on NVMe** |
| 3 | **Compaction** | **Subprocess execution (GIL bypass)** | **Event loop stays free during merge** |
| 4 | API | Single atomic write lock | Minimal critical section; durability before visibility |
| 5 | Memtable | Concurrent skip list (lock-free reads) | Writers don't contend with readers |
| 6 | Freeze | Backpressure queue (bounded depth) | Prevents OOM; propagates disk pressure to callers |
| 7 | SSTable | Four-file layout + async bloom/index writes | Crash-safe; L0 finish() parallelizes I/O |
| 8 | WAL | Msgpack framing + per-write fsync | Compact, durable, corruption-detectable |
| 9 | Compaction | Level reservation (conflict-free parallel jobs) | Non-adjacent compactions run simultaneously |
| 10 | Bloom | Data-derived sizing + env-aware FPR | Optimal filter per SSTable; dev speed vs prod accuracy |
| 11 | Cache | Lazy loading + cross-reader reuse | No I/O for unopened SSTables |
| 12 | Cache | Three-tier LRU (differential eviction) | Bloom/index survive data block churn |
| 13 | Config | Runtime mutability (no restart) | Live threshold tuning |
| 14 | Recovery | Seq-based WAL filtering | Skip already-flushed entries |
| 15 | Compaction | AsyncRWLock with writer preference | Compaction commits aren't starved |
| 16 | SSTable | mmap + zero-copy block scanning | Read optimization; no copy after page cache warm |
| 17 | Manifest | Atomic temp + os.replace | Crash-safe state persistence |
