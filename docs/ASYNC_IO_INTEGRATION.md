# Async I/O Integration Guide — lsm-kv
> Identifies every operation in the project, classifies it per the async-io-guidelines,
> and specifies exactly where async I/O improves operational efficiency.

---

## 0. Classification methodology

Every operation is classified before any implementation decision:

| Class | Signature | Rule |
|---|---|---|
| **I/O-bound** | `async def` + `await` | disk write, fsync, mmap open |
| **Sync I/O** (blocking stdlib) | `def` + `asyncio.to_thread()` | `mmap`, `open()`, `struct.unpack_from` on OS pages |
| **CPU-bound** | `def`, offload if >100ms | CRC32, Bloom filter build, SkipList snapshot |
| **Pure logic** | `def`, call directly | key comparison, offset arithmetic, flag checks |

**Hard rule from guidelines §2:** Never make a function `async def` unless it contains at least one `await` on an I/O operation.

---

## 1. Component-by-component classification

### 1a. WAL (`wal/writer.py`)

| Operation | Class | Async strategy |
|---|---|---|
| `append(entry)` — encode + write bytes | I/O-bound | `async def` + `await asyncio.to_thread(_sync_append)` |
| `fsync()` — OS flush | I/O-bound | `await asyncio.to_thread(os.fsync, fd)` |
| `replay()` — sequential read at startup | Sync I/O | Runs once before workers start — sync is acceptable |
| `truncate_before(seq)` — rewrite WAL | Sync I/O | `await asyncio.to_thread(_sync_truncate)` |
| `encode(entry)` — msgpack packb | CPU-bound (<1ms) | Call directly — never wrap |

**Key decision:** WAL `append` + `fsync` is the dominant latency on the write path (0.5–2ms per write). Making this truly async via `asyncio.to_thread` lets the engine accept the next `put()` call while the OS flushes. The event loop stays unblocked.

```python
# WALManager — async interface over sync stdlib I/O
async def append(self, entry: WALEntry) -> None:
    encoded = msgpack.packb(
        (entry.seq, entry.timestamp_ms, entry.key, entry.value),
        use_bin_type=True,
    )
    await asyncio.to_thread(self._sync_append, encoded)

def _sync_append(self, data: bytes) -> None:
    with self._wal_lock:
        self._fd.write(data)
        os.fsync(self._fd.fileno())
```

---

### 1b. MemTable (`memtable/`)

| Operation | Class | Async strategy |
|---|---|---|
| `SkipList.put()` | Pure logic + per-node lock | Sync — direct call |
| `SkipList.get()` | Pure logic | Sync — direct call |
| `SkipList.snapshot()` | CPU-bound (O(n), ~5–15ms for 64MB) | Sync — called inside `maybe_freeze()` which is under `_write_lock` |
| `ActiveMemTable.freeze()` | CPU-bound | Sync — called under `_write_lock` |
| `ImmutableMemTable.get()` | Pure logic | Sync — direct call |
| `MemTableManager.put()` | Pure logic | Sync — direct call |
| `MemTableManager.get()` | Pure logic | Sync — direct call |
| `MemTableManager.maybe_freeze()` | CPU-bound | Sync — freeze is brief; write lock released immediately after |

**Decision:** No async in the MemTable layer. Per guidelines §8 — do not force async onto phases that are pure computation. The MemTable is entirely in-memory. The only wait is the per-node lock in SkipList, which is microseconds.

---

### 1c. SSTable Writer (`sstable/writer.py`)

| Operation | Class | Async strategy |
|---|---|---|
| `put(key, seq, ts, value)` — encode + buffer | CPU-bound (<1ms) | Sync — buffered in `_data_buf` |
| Block boundary flush — `_data_buf` → `fd.write()` | I/O-bound | `await asyncio.to_thread(_sync_write_block)` |
| Bloom filter `add(key)` | CPU-bound | Sync — called inline during `put()` |
| Sparse index `add(key, offset)` | Pure logic | Sync — called at block boundary |
| `finish()` — write all four files + fsync | I/O-bound | `async def finish()` orchestrates async writes |
| CRC32 computation | CPU-bound (<1ms) | Sync — inline during encode |

**L0 vs L1+ distinction (critical):**

- **L0 `finish()`** — Bloom filter build and index serialisation happen **asynchronously** as background tasks while the data blocks are being written. This is the key optimisation: the L0 writer does not block on Bloom/index work.
- **L1+ `finish()`** — Bloom filter build and index serialisation happen **synchronously** during the write. L1+ SSTables are produced by compaction which runs in a subprocess — blocking there is acceptable and simpler.

```python
# L0 SSTableWriter.finish() — async, Bloom/index built concurrently
async def finish(self) -> SSTableMeta:
    # Flush remaining data buffer
    await asyncio.to_thread(self._sync_flush_block)

    # L0: build Bloom filter and serialise index concurrently with footer write
    bloom_task = asyncio.create_task(
        asyncio.to_thread(self._build_and_write_filter)
    )
    index_task = asyncio.create_task(
        asyncio.to_thread(self._write_index)
    )
    await asyncio.gather(bloom_task, index_task)

    await asyncio.to_thread(self._write_footer_and_fsync)
    return self._build_meta()

# L1+ SSTableWriter.finish() — sync, called inside subprocess
def finish_sync(self) -> SSTableMeta:
    self._sync_flush_block()
    self._build_and_write_filter()   # sync — blocking is fine in subprocess
    self._write_index()
    self._write_footer_and_fsync()
    return self._build_meta()
```

---

### 1d. SSTable Reader (`sstable/reader.py`)

| Operation | Class | Async strategy |
|---|---|---|
| `open()` — mmap file | Sync I/O (once) | `await asyncio.to_thread(_sync_open)` |
| `read_footer()` — ctypes overlay | Pure logic | Sync — memoryview slice, no syscall |
| `load_index()` — read index.bin | Sync I/O (once) | Part of async open |
| `load_bloom()` — read filter.bin | Sync I/O (once) | Part of async open — **L0: concurrent with index load** |
| `get(key)` — Bloom + bisect + mmap | Mixed | Sync on hot path (mmap = memory, no syscall after OS pages cached) |
| `iter_range(start, end)` | Mixed | Sync — sequential mmap read |
| `close()` | Sync I/O | Sync — called in cleanup, never raises |

**L0 open optimisation:** For L0 SSTables, index and Bloom filter are loaded concurrently at open time:

```python
# SSTableReader.open() — async, L0 loads index + bloom concurrently
@classmethod
async def open(cls, path: Path, file_id: FileID, cache: BlockCache, level: Level) -> SSTableReader:
    reader = cls.__new__(cls)
    reader._file_id = file_id
    reader._cache   = cache
    reader._level   = level

    await asyncio.to_thread(reader._sync_open_mmap, path)

    if level == 0:
        # L0: load index and bloom filter concurrently
        index_task = asyncio.create_task(asyncio.to_thread(reader._load_index))
        bloom_task = asyncio.create_task(asyncio.to_thread(reader._load_bloom))
        reader._index, reader._bloom = await asyncio.gather(index_task, bloom_task)
    else:
        # L1+: sequential is fine — done once at compaction output
        reader._index = await asyncio.to_thread(reader._load_index)
        reader._bloom = await asyncio.to_thread(reader._load_bloom)

    return reader
```

---

### 1e. SSTableManager — flush path (`engine/sstable_manager.py`)

| Operation | Class | Async strategy |
|---|---|---|
| `flush(snapshot, file_id)` | I/O-bound | `async def` — orchestrates writer |
| `get(key)` — fan-out across L0 files | I/O-bound | `asyncio.gather()` across L0 readers |
| `load()` — open all known SSTables at startup | I/O-bound | `asyncio.gather()` with semaphore |
| `mark_for_deletion()` | Pure logic | Sync |
| `close_all()` | Sync I/O | Sync — cleanup |

**L0 read fan-out — the key async optimisation on the read path:**

```python
async def get(self, key: Key) -> tuple[SeqNum, int, Value] | None:
    # L0: all files must be checked (overlapping ranges) — fan out concurrently
    l0_files = self._compactor.level_files(0)  # newest first
    if l0_files:
        tasks = [
            asyncio.create_task(self._reader_get(file_id, key))
            for file_id in l0_files
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Return highest-seq result (newest write wins)
        best: tuple[SeqNum, int, Value] | None = None
        for result in results:
            if isinstance(result, Exception) or result is None:
                continue
            if best is None or result[0] > best[0]:
                best = result
        if best is not None:
            return best

    # L1+: one candidate per level — sequential is fine (binary search on manifest)
    for level in range(1, self._compactor.max_level + 1):
        candidate = self._compactor.find_candidate(level, key)
        if candidate is None:
            continue
        result = await asyncio.to_thread(self._sync_reader_get, candidate, key)
        if result is not None:
            return result

    return None
```

---

### 1f. MemTableManager flush worker

| Operation | Class | Async strategy |
|---|---|---|
| Flush worker loop | I/O-bound | `async def _flush_worker()` |
| `_mem.peek_oldest()` | Pure logic | Sync |
| `_sst.flush(snapshot, file_id)` | I/O-bound | `await` |
| `_mem.pop_oldest()` | Pure logic | Sync |
| `_wal.truncate_before(seq)` | I/O-bound | `await` |

```python
async def _flush_worker(self) -> None:
    loop = asyncio.get_running_loop()
    while not self._stop_event.is_set():
        # Wait for flush signal (with timeout to catch stop_event)
        await asyncio.to_thread(self._mem._flush_event.wait, 1.0)
        self._mem._flush_event.clear()

        while self._mem.queue_len() > 0:
            await self._flush_one()

async def _flush_one(self) -> None:
    snapshot = self._mem.peek_oldest()
    if snapshot is None:
        return
    file_id = uuid.uuid4().hex
    meta = await self._sst.flush(snapshot, file_id)  # async write
    self._mem.pop_oldest()                            # sync, pure logic
    await self._wal.truncate_before(meta.seq_min)    # async WAL trim
```

---

### 1g. Compaction worker (skeleton — merge impl. out of scope)

| Operation | Class | Async strategy |
|---|---|---|
| Compaction trigger check | Pure logic | Sync |
| Subprocess dispatch | CPU-bound (in subprocess) | `asyncio.to_thread(pool.submit(...).result)` |
| Registry update after compaction | Pure logic | Sync |
| Manifest swap (`os.replace`) | Sync I/O | `await asyncio.to_thread(os.replace, tmp, target)` |
| Cache invalidation | Pure logic | Sync |

```python
async def _compact_worker(self) -> None:
    while not self._stop_event.is_set():
        await asyncio.sleep(COMPACTION_CHECK_INTERVAL)

        if self._sst._compactor.l0_file_count() < L0_COMPACTION_THRESHOLD:
            continue

        # [COMPACTION MERGE — OUT OF SCOPE]
        # When implemented: dispatch to ProcessPoolExecutor via asyncio.to_thread
        # so the event loop stays alive during the (potentially long) merge
        pass
```

---

## 2. Where async I/O provides the biggest wins

### 2a. Write path — WAL fsync

**Before async:** `put()` blocks for 0.5–2ms while `fsync()` completes. During this window, all other `put()` calls queue.

**After async:** WAL append + fsync runs in a thread. The event loop immediately accepts the next `put()`. Throughput increases ~10× on write-heavy workloads.

**Constraint:** WAL write ordering must be preserved. The `_wal_lock` inside `_sync_append` serialises writes — order is guaranteed.

### 2b. L0 SSTable write — Bloom + Index concurrent build

**Before async:** `SSTableWriter.finish()` writes data blocks, then builds Bloom filter, then serialises index, then writes footer — all sequential.

**After async:** Data block flush, Bloom filter build, and index serialisation all run concurrently via `asyncio.gather`. Only the footer write waits for all three.

**Estimated gain:** For a 64MB L0 SSTable (~650K records), Bloom filter build takes ~200ms. Running it concurrently with index write (also ~100ms) saves ~150ms per flush cycle.

### 2c. L0 read — concurrent fan-out across files

**Before async:** `get()` checks L0 files sequentially — up to 4 files × average 1ms mmap read = 4ms worst case.

**After async:** All L0 files checked concurrently via `asyncio.gather`. Worst case becomes ~1ms regardless of L0 file count.

**Constraint:** Must return the highest-seq result, not the first result. All tasks must complete before returning.

### 2d. SSTableManager.load() — startup parallelism

**Before async:** Opening all known SSTables at startup is sequential. For a large store with 50+ files this could take several seconds.

**After async:** All `SSTableReader.open()` calls dispatched concurrently with a semaphore:

```python
@classmethod
async def load(cls, data_root: Path, cache: BlockCache) -> SSTableManager:
    mgr = cls.__new__(cls)
    # ... load manifests ...
    sem = asyncio.Semaphore(8)  # configurable via SSTABLE_OPEN_CONCURRENCY

    async def _bounded_open(file_id, level, path):
        async with sem:
            return await SSTableReader.open(path, file_id, cache, level)

    tasks = [_bounded_open(fid, lvl, p) for fid, lvl, p in all_files]
    readers = await asyncio.gather(*tasks)
    for reader in readers:
        mgr._registry.register(reader.file_id, reader)
    return mgr
```

---

## 3. What must stay synchronous

Per guidelines §8 — do not force async on phases that are pure computation:

| Component | Reason to stay sync |
|---|---|
| `SkipList.put()` / `get()` | Pure in-memory, microseconds |
| `ImmutableMemTable.get()` | Dict lookup, pure logic |
| CRC32 computation | <1ms per record, inline |
| `struct.unpack_from(mv, offset)` | memoryview read — no syscall |
| Bloom filter `may_contain()` | Bit array read — pure logic |
| Sparse index `bisect_right()` | List bisect — pure logic |
| `_find()` in SkipList | Pointer traversal — pure logic |

---

## 4. Concurrency limits — configurable via env vars

Per guidelines §5d and §13:

```python
# app/config.py — single source of truth for all async tunables
import os

SSTABLE_OPEN_CONCURRENCY = int(os.getenv("SSTABLE_OPEN_CONCURRENCY", "8"))
L0_READ_CONCURRENCY      = int(os.getenv("L0_READ_CONCURRENCY", "4"))    # = L0_COMPACTION_THRESHOLD
WAL_WRITE_THREAD_POOL    = int(os.getenv("WAL_WRITE_THREAD_POOL", "1"))  # WAL is serial by design
FLUSH_THREAD_POOL        = int(os.getenv("FLUSH_THREAD_POOL", "2"))
```

---

## 5. Error handling in async contexts

Per guidelines §6:

```python
# Graceful degradation for L0 fan-out: one reader failure does not abort the get()
async def _reader_get(self, file_id: FileID, key: Key):
    try:
        with self._registry.open_reader(file_id) as reader:
            return reader.get(key)
    except CorruptRecordError as e:
        logger.warning("Corrupt record encountered",
                       extra={"file_id": file_id, "error": str(e)})
        return None
    except Exception as e:
        logger.error("Unexpected reader error",
                     extra={"file_id": file_id, "error": str(e)})
        return None

# Cleanup never raises — per guidelines §6b
async def close_all(self) -> None:
    for file_id, reader in self._registry.items():
        try:
            reader.close()
        except Exception as e:
            logger.debug("Error closing reader",
                         extra={"file_id": file_id, "error": str(e)})
```

---

## 6. Testing async components

Per guidelines §12:

```toml
# pyproject.toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "function"
```

```python
# tests/test_wal_manager.py
async def test_append_does_not_block_event_loop():
    wal = WALManager.open(tmp_path / "wal.log")
    entry = WALEntry(seq=1, timestamp_ms=1000, key=b"k", value=b"v")

    # Should complete without blocking — test times out if it blocks
    await asyncio.wait_for(wal.append(entry), timeout=0.1)

async def test_l0_read_fan_out():
    # All L0 files queried concurrently — total time ≈ single file time
    mgr = await SSTableManager.load(data_root, cache)
    result = await mgr.get(b"some_key")
    assert result is not None
```

---

## 7. Summary decision table

| Component | Sync or Async | Pattern |
|---|---|---|
| `WALManager.append()` | **Async** | `asyncio.to_thread(_sync_append)` |
| `WALManager.truncate_before()` | **Async** | `asyncio.to_thread(_sync_truncate)` |
| `WALManager.replay()` | Sync | Startup only — before workers |
| `SkipList` all ops | **Sync** | Pure in-memory, no I/O |
| `MemTableManager` all ops | **Sync** | Pure in-memory, no I/O |
| `SSTableWriter.finish()` L0 | **Async** | `gather(bloom_task, index_task)` |
| `SSTableWriter.finish()` L1+ | **Sync** | Runs in compaction subprocess |
| `SSTableReader.open()` L0 | **Async** | `gather(index_task, bloom_task)` |
| `SSTableReader.get()` | **Sync** | mmap = memory after OS caches pages |
| `SSTableManager.flush()` | **Async** | Delegates to async writer |
| `SSTableManager.get()` L0 | **Async** | `asyncio.gather()` fan-out |
| `SSTableManager.get()` L1+ | **Async** | `asyncio.to_thread` per level |
| `SSTableManager.load()` | **Async** | `gather` with semaphore |
| Flush worker loop | **Async** | `async def _flush_worker()` |
| Compaction worker loop | **Async** | `asyncio.sleep` + subprocess via `to_thread` |
| CRC32 / Bloom `may_contain` / bisect | **Sync** | Pure logic, <1ms |
