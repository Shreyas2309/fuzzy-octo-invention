# SSTable Layer — Implementation Specification
> Pass this document to Claude Code in **plan mode** before implementing any file.
> Implement phases in order. Each phase is independently testable.

---

## 0. Implementation order

```
Phase 1 → app/sstable/meta.py           (SSTableMeta dataclass + msgpack codec)
Phase 2 → app/sstable/writer.py         (SSTableWriter — L0 async, L1+ sync)
Phase 3 → app/sstable/reader.py         (SSTableReader — mmap + async open)
Phase 4 → app/engine/sstable_manager.py (SSTableManager — flush, get, load)
Phase 5 → tests/                        (unit tests per phase)
```

---

## 1. Level architecture — the critical design constraint

### 1a. L0 — multiple overlapping SSTables (async Bloom + Index)

- L0 holds up to `L0_COMPACTION_THRESHOLD` (4) SSTables simultaneously
- Key ranges **may overlap** between L0 files — two L0 files can both contain key `b"foo"`
- Every `get()` must check **all** L0 files and return the result with the highest `seq`
- Bloom filter and sparse index are built **asynchronously** during `finish()` — concurrent with the data block flush, not sequential after it
- At open time, Bloom filter and index are loaded **concurrently** via `asyncio.gather`

### 1b. L1+ — exactly one SSTable per level (sync Bloom + Index)

- Each level L1, L2, L3... holds **exactly one SSTable** — the merged, fully deduplicated file
- Key ranges are **non-overlapping** within a level — a binary search on the manifest is sufficient to find (or rule out) a candidate
- Bloom filter and sparse index are built **synchronously** during `finish_sync()` — runs inside the compaction subprocess where blocking is acceptable
- The SSTable is only visible to readers after `meta.json` is written — its existence is the completeness signal

### 1c. Level size limits

```
L0: max 4 SSTables (each up to MAX_MEMTABLE_SIZE = 64MB)
L1: 1 SSTable, max 256MB  (= 4 × MAX_MEMTABLE_SIZE)
L2: 1 SSTable, max 2.5GB  (= 10 × L1)
L3: 1 SSTable, max 25GB   (= 10 × L2)
```

---

## 2. `SSTableMeta` — `app/sstable/meta.py`

### 2a. Purpose

`SSTableMeta` is the in-memory and on-disk representation of a single SSTable's metadata. It lives in the level manifests, in `meta.json`, and in `SSTableRegistry`. It is the only thing `SSTableManager` reads to plan compaction — no data files are opened during planning.

### 2b. Dataclass definition

```python
@dataclass(slots=True, frozen=True)
class SSTableMeta:
    file_id:     FileID      # UUID4 hex — directory name on disk
    snapshot_id: SnapshotID  # UUID4 hex — ImmutableMemTable this was flushed from
    level:       Level       # 0, 1, 2, ...
    size_bytes:  int         # total bytes of data.bin
    record_count: int        # total KV records including tombstones
    block_count: int         # number of data blocks = number of sparse index entries
    min_key:     bytes       # raw bytes — smallest key in this file
    max_key:     bytes       # raw bytes — largest key in this file
    seq_min:     SeqNum      # smallest sequence number in this file
    seq_max:     SeqNum      # largest sequence number in this file
    bloom_fpr:   float       # false positive rate used when building Bloom filter
    created_at:  str         # ISO8601 timestamp
    data_file:   str         # always "data.bin"
    index_file:  str         # always "index.bin"
    filter_file: str         # always "filter.bin"
```

### 2c. Serialisation

`meta.json` on disk uses base64 for `min_key` and `max_key` (raw bytes are not valid JSON strings):

```python
import base64
import json
import msgpack
from dataclasses import asdict

def encode(self) -> bytes:
    d = asdict(self)
    d["min_key"] = base64.b64encode(self.min_key).decode()
    d["max_key"] = base64.b64encode(self.max_key).decode()
    return json.dumps(d, indent=2).encode()

@classmethod
def decode(cls, data: bytes) -> SSTableMeta:
    d = json.loads(data)
    d["min_key"] = base64.b64decode(d["min_key"])
    d["max_key"] = base64.b64decode(d["max_key"])
    return cls(**d)
```

`meta.json` is written last by `SSTableWriter.finish()`. Its presence on disk is the signal to the engine that the SSTable is complete and consistent. An SSTable directory without `meta.json` is treated as incomplete and ignored at startup.

---

## 3. `SSTableWriter` — `app/sstable/writer.py`

### 3a. Purpose and lifecycle

`SSTableWriter` is a write-once state machine. It accepts a stream of sorted KV pairs via `put()` and produces the four files (`data.bin`, `index.bin`, `filter.bin`, `meta.json`) atomically when `finish()` or `finish_sync()` is called.

```
State: OPEN → [put() × N] → FINISHING → DONE
```

After `finish()` / `finish_sync()` returns, the writer is discarded. It must not be used again.

### 3b. State

```python
class SSTableWriter:
    # Identity
    _file_id:     FileID
    _snapshot_id: SnapshotID
    _level:       Level
    _directory:   Path

    # Data block accumulation
    _fd:          BinaryIO         # open file handle for data.bin
    _data_buf:    bytearray        # current block buffer
    _block_offset: Offset          # byte offset where current block starts
    _last_key:    Key | None       # for key-order validation

    # Metadata accumulated during write
    _index:       SparseIndex      # built incrementally: first_key → block_offset
    _bloom:       BloomFilter      # all keys added during put()
    _min_key:     Key | None
    _max_key:     Key | None
    _seq_min:     SeqNum
    _seq_max:     SeqNum
    _record_count: int
    _block_count: int
    _total_bytes: int

    # State machine
    _sealed:      bool             # True after finish() — further puts raise WriterSealedError
```

### 3c. Construction

```python
def __init__(
    self,
    directory:   Path,
    file_id:     FileID,
    snapshot_id: SnapshotID,
    level:       Level,
    bloom_fpr:   float = 0.01,
) -> None:
    self._file_id     = file_id
    self._snapshot_id = snapshot_id
    self._level       = level
    self._directory   = directory
    directory.mkdir(parents=True, exist_ok=True)
    self._fd          = open(directory / "data.bin", "wb")
    self._data_buf    = bytearray()
    self._block_offset = 0
    self._last_key    = None
    self._index       = SparseIndex()
    self._bloom       = BloomFilter(fpr=bloom_fpr)
    self._min_key     = None
    self._max_key     = None
    self._seq_min     = 2**63
    self._seq_max     = 0
    self._record_count = 0
    self._block_count  = 0
    self._total_bytes  = 0
    self._sealed       = False
```

### 3d. `put(key, seq, timestamp_ms, value)` — sync for all levels

```python
def put(self, key: Key, seq: SeqNum, timestamp_ms: int, value: Value) -> None:
    if self._sealed:
        raise WriterSealedError("put() called on sealed writer")
    if self._last_key is not None and key <= self._last_key:
        raise OutOfOrderKeyError(f"key {key!r} <= previous {self._last_key!r}")

    # Encode record: 24B header + body + 4B CRC footer
    record = _encode_record(key, seq, timestamp_ms, value)

    # Track first key of this block for sparse index
    if not self._data_buf:
        self._first_key_of_block = key

    self._data_buf.extend(record)

    # Update metadata
    self._bloom.add(key)
    self._last_key    = key
    self._min_key     = self._min_key or key
    self._max_key     = key
    self._seq_min     = min(self._seq_min, seq)
    self._seq_max     = max(self._seq_max, seq)
    self._record_count += 1
    self._total_bytes  += len(record)

    # Flush block if full — AFTER writing the record that crosses the threshold
    if len(self._data_buf) >= BLOCK_SIZE_DEFAULT:
        self._flush_block_sync()
```

### 3e. `_flush_block_sync()` — internal

```python
def _flush_block_sync(self) -> None:
    if not self._data_buf:
        return
    self._index.add(self._first_key_of_block, self._block_offset)
    self._fd.write(self._data_buf)
    self._block_offset += len(self._data_buf)
    self._block_count  += 1
    self._data_buf      = bytearray()
```

**Important:** The last block (partial) is flushed inside `finish()` / `finish_sync()`, not inside `put()`. A block is only added to the sparse index at flush time, not at put time.

### 3f. `_encode_record(key, seq, timestamp_ms, value) -> bytes`

```
header  = struct.pack(">IIqq", len(key), len(value), seq, timestamp_ms)  # 24B
body    = key + value
crc     = struct.pack(">I", binascii.crc32(header + body) & 0xFFFFFFFF)   # 4B
return header + body + crc
```

### 3g. `finish()` — async, L0 only

```python
async def finish(self) -> SSTableMeta:
    if self._sealed:
        raise WriterSealedError()
    self._sealed = True

    # 1. Flush partial last block (sync — must complete before concurrent tasks)
    self._flush_block_sync()

    # 2. Write Bloom filter and index concurrently
    bloom_task = asyncio.create_task(
        asyncio.to_thread(self._write_filter_sync)
    )
    index_task = asyncio.create_task(
        asyncio.to_thread(self._write_index_sync)
    )
    await asyncio.gather(bloom_task, index_task)

    # 3. Write footer to data.bin, fsync all files
    await asyncio.to_thread(self._write_footer_and_fsync_sync)

    # 4. Write meta.json last — its existence = SSTable completeness
    meta = self._build_meta()
    await asyncio.to_thread(self._write_meta_sync, meta)

    return meta
```

### 3h. `finish_sync()` — sync, L1+ only (runs inside compaction subprocess)

```python
def finish_sync(self) -> SSTableMeta:
    if self._sealed:
        raise WriterSealedError()
    self._sealed = True

    self._flush_block_sync()
    self._write_filter_sync()     # sequential — acceptable in subprocess
    self._write_index_sync()
    self._write_footer_and_fsync_sync()
    meta = self._build_meta()
    self._write_meta_sync(meta)
    return meta
```

### 3i. Internal write helpers

**`_write_filter_sync()`:**
```
Serialise Bloom filter to filter.bin:
    header = _BloomHeader(magic, num_bits, num_hashes, _pad)
    write header bytes (16B)
    write bit array bytes
    fsync filter.bin
```

**`_write_index_sync()`:**
```
Serialise SparseIndex to index.bin:
    for each (first_key, block_offset) in index:
        write struct.pack(">IQ", len(first_key), block_offset)  # 12B
        write first_key bytes
    fsync index.bin
```

**`_write_footer_and_fsync_sync()`:**
```
Write _SSTableFooter (56B) to end of data.bin:
    footer = _SSTableFooter(
        magic        = FOOTER_MAGIC,
        data_end     = _block_offset,   # end of record region
        index_offset = 0,               # reserved — index is separate file
        index_len    = 0,               # reserved
        bloom_offset = 0,               # reserved — filter is separate file
        bloom_len    = 0,               # reserved
        seq_min      = _seq_min,
        seq_max      = _seq_max,
    )
    fd.write(bytes(footer))
    os.fsync(fd.fileno())
    fd.close()
```

**`_build_meta()`:**
```python
def _build_meta(self) -> SSTableMeta:
    return SSTableMeta(
        file_id      = self._file_id,
        snapshot_id  = self._snapshot_id,
        level        = self._level,
        size_bytes   = self._total_bytes,
        record_count = self._record_count,
        block_count  = self._block_count,
        min_key      = self._min_key or b"",
        max_key      = self._max_key or b"",
        seq_min      = self._seq_min if self._record_count else 0,
        seq_max      = self._seq_max,
        bloom_fpr    = self._bloom.fpr,
        created_at   = datetime.utcnow().isoformat(),
        data_file    = "data.bin",
        index_file   = "index.bin",
        filter_file  = "filter.bin",
    )
```

---

## 4. `SSTableReader` — `app/sstable/reader.py`

### 4a. Purpose

`SSTableReader` holds an open mmap on `data.bin` and serves `get()` and `iter_range()` queries. It is opened once, held in `SSTableRegistry`, and closed only when the SSTable is deleted (after compaction).

### 4b. State

```python
class SSTableReader:
    _file_id:  FileID
    _level:    Level
    _path:     Path          # SSTable directory
    _fd:       BinaryIO      # open file handle
    _mm:       mmap.mmap     # memory-mapped data.bin (ACCESS_READ)
    _mv:       memoryview    # view over _mm — for zero-copy slicing
    _footer:   _SSTableFooter
    _index:    SparseIndex   # loaded at open time
    _bloom:    BloomFilter   # loaded at open time
    _cache:    BlockCache    # shared with engine
    _closed:   bool
```

### 4c. `open()` — async classmethod

```python
@classmethod
async def open(
    cls,
    directory: Path,
    file_id:   FileID,
    cache:     BlockCache,
    level:     Level,
) -> SSTableReader:
    reader = cls.__new__(cls)
    reader._file_id = file_id
    reader._level   = level
    reader._path    = directory
    reader._cache   = cache
    reader._closed  = False

    # Open mmap — blocking I/O, offload to thread
    await asyncio.to_thread(reader._sync_open_mmap, directory / "data.bin")

    # Read footer from last 56B of mmap — pure memory, sync is fine
    reader._footer = read_footer(reader._mv)
    if reader._footer.magic != FOOTER_MAGIC:
        raise CorruptSSTableError(f"Bad magic in {file_id}")

    if level == 0:
        # L0: load index and Bloom filter concurrently
        idx_task   = asyncio.create_task(
            asyncio.to_thread(reader._sync_load_index, directory / "index.bin")
        )
        bloom_task = asyncio.create_task(
            asyncio.to_thread(reader._sync_load_bloom, directory / "filter.bin")
        )
        reader._index, reader._bloom = await asyncio.gather(idx_task, bloom_task)
    else:
        # L1+: sequential is fine — done once at compaction output registration
        reader._index = await asyncio.to_thread(
            reader._sync_load_index, directory / "index.bin"
        )
        reader._bloom = await asyncio.to_thread(
            reader._sync_load_bloom, directory / "filter.bin"
        )

    return reader
```

### 4d. `_sync_open_mmap(path)`

```python
def _sync_open_mmap(self, path: Path) -> None:
    self._fd = open(path, "rb")
    self._mm = mmap.mmap(self._fd.fileno(), 0, access=mmap.ACCESS_READ)
    self._mv = memoryview(self._mm)
```

### 4e. `_sync_load_index(path) -> SparseIndex`

```python
def _sync_load_index(self, path: Path) -> SparseIndex:
    data = path.read_bytes()
    return SparseIndex.from_bytes(data)
```

### 4f. `_sync_load_bloom(path) -> BloomFilter`

```python
def _sync_load_bloom(self, path: Path) -> BloomFilter:
    data = path.read_bytes()
    return BloomFilter.from_bytes(data)
```

### 4g. `get(key) -> tuple[SeqNum, int, Value] | None` — sync

```python
def get(self, key: Key) -> tuple[SeqNum, int, Value] | None:
    if self._closed:
        raise ReaderClosedError()

    # Step 1: Bloom filter — no I/O
    if not self._bloom.may_contain(key):
        return None

    # Step 2: Sparse index bisect — no I/O
    block_offset = self._index.floor_offset(key)
    if block_offset is None:
        return None

    # Step 3: Block cache lookup
    block = self._cache.get(self._file_id, block_offset)
    if block is None:
        # Cache miss — read from mmap (may trigger OS page fault)
        end = block_offset + BLOCK_SIZE_DEFAULT
        if end > len(self._mv):
            end = len(self._mv) - FOOTER_SIZE  # don't read into footer
        block = bytes(self._mv[block_offset:end])
        self._cache.put(self._file_id, block_offset, block)

    # Step 4: Scan block
    mv_block = memoryview(block)
    cursor   = 0
    while cursor < len(mv_block):
        decoded = decode_from(mv_block, cursor)
        if decoded.entry.key == key:
            e = decoded.entry
            return (e.seq, e.timestamp_ms, e.value)
        if decoded.entry.key > key:
            return None   # passed it — key is not in this block
        cursor = decoded.next_offset

    return None
```

`get()` is sync because:
- Bloom filter and bisect are pure logic
- mmap reads after OS page warmup are memory accesses, not syscalls
- Blocking on an mmap page fault is acceptable (<1ms typically) and unavoidable

### 4h. `iter_range(start, end) -> Iterator[tuple[Key, SeqNum, int, Value]]` — sync generator

```python
def iter_range(
    self,
    start: Key,
    end:   Key,
) -> Iterator[tuple[Key, SeqNum, int, Value]]:
    if self._closed:
        raise ReaderClosedError()

    # Find starting block via ceil_offset — smallest block whose first_key >= start
    block_offset = self._index.ceil_offset(start)
    if block_offset is None:
        # start is beyond all index entries — check from last block
        block_offset = self._index.last_offset()
        if block_offset is None:
            return

    # Walk blocks forward
    data_end = self._footer.data_end
    while block_offset < data_end:
        block    = self._cache.get(self._file_id, block_offset)
        if block is None:
            end_offset = min(block_offset + BLOCK_SIZE_DEFAULT, data_end)
            block      = bytes(self._mv[block_offset:end_offset])
            self._cache.put(self._file_id, block_offset, block)

        mv_block = memoryview(block)
        cursor   = 0
        block_exhausted = True

        while cursor < len(mv_block):
            decoded = decode_from(mv_block, cursor)
            entry   = decoded.entry
            cursor  = decoded.next_offset

            if entry.key < start:
                continue
            if entry.key > end:
                return   # past range end — stop entirely
            if not entry.value == TOMBSTONE:
                yield (entry.key, entry.seq, entry.timestamp_ms, entry.value)

        # Move to next block via sparse index
        next_offset = self._index.next_block_offset(block_offset)
        if next_offset is None:
            break
        block_offset = next_offset
```

### 4i. `close()` — sync

```python
def close(self) -> None:
    if self._closed:
        return
    self._closed = True
    try:
        self._mv.release()
    except Exception as e:
        logger.debug("Error releasing memoryview", extra={"error": str(e)})
    try:
        self._mm.close()
    except Exception as e:
        logger.debug("Error closing mmap", extra={"error": str(e)})
    try:
        self._fd.close()
    except Exception as e:
        logger.debug("Error closing fd", extra={"error": str(e)})
```

Per guidelines §6b: cleanup never raises. Each resource is closed separately with its own try/except.

---

## 5. `SSTableManager` — `app/engine/sstable_manager.py`

### 5a. Purpose

`SSTableManager` owns all on-disk SSTable state. It has no knowledge of the memtable or WAL. It is the single point for flush, read, level management, and compaction triggering (not merge — that is out of scope).

### 5b. State

```python
class SSTableManager:
    _registry:   SSTableRegistry     # file_id → SSTableReader, ref-counted
    _cache:      BlockCache           # shared LRU across all readers
    _compactor:  LeveledCompactor     # skeleton — level layout + manifest
    _flush_lock: threading.RLock      # one flush at a time
    _data_root:  Path
```

### 5c. `load()` — async classmethod

```python
@classmethod
async def load(cls, data_root: Path, cache: BlockCache) -> SSTableManager:
    mgr          = cls.__new__(cls)
    mgr._data_root  = data_root
    mgr._cache      = cache
    mgr._registry   = SSTableRegistry()
    mgr._flush_lock = threading.RLock()
    mgr._compactor  = LeveledCompactor.load(data_root)

    all_files = mgr._compactor.all_known_files()  # [(file_id, level, path)]

    sem = asyncio.Semaphore(SSTABLE_OPEN_CONCURRENCY)

    async def _bounded_open(file_id: FileID, level: Level, path: Path):
        async with sem:
            try:
                reader = await SSTableReader.open(path, file_id, cache, level)
                mgr._registry.register(file_id, reader)
            except Exception as e:
                logger.warning("Failed to open SSTable at startup",
                               extra={"file_id": file_id, "error": str(e)})

    tasks = [
        asyncio.create_task(_bounded_open(fid, lvl, p))
        for fid, lvl, p in all_files
    ]
    await asyncio.gather(*tasks)
    return mgr
```

### 5d. `flush(snapshot, file_id) -> SSTableMeta` — async

```python
async def flush(
    self,
    snapshot:    ImmutableMemTable,
    file_id:     FileID,
) -> SSTableMeta:
    directory = self._data_root / "sstable" / "L0" / file_id

    writer = SSTableWriter(
        directory   = directory,
        file_id     = file_id,
        snapshot_id = snapshot.snapshot_id,
        level       = 0,
    )

    # Write all KV pairs — sync loop (in-memory iteration, no I/O per record)
    for key, seq, timestamp_ms, value in snapshot.items():
        writer.put(key, seq, timestamp_ms, value)

    # L0: async finish — Bloom + index built concurrently
    meta = await writer.finish()

    # Register reader
    reader = await SSTableReader.open(directory, file_id, self._cache, level=0)
    self._registry.register(file_id, reader)

    # Update level manifest
    self._compactor.register_sstable(meta)

    return meta
```

**Note:** `_flush_lock` is not held during the async `writer.finish()` — the write itself is the expensive part, and blocking the lock for its entire duration would serialise flush and compaction unnecessarily. The lock only protects manifest update:

```python
async def flush(self, snapshot, file_id):
    # ... write SSTable without holding flush_lock ...
    meta   = await writer.finish()
    reader = await SSTableReader.open(directory, file_id, self._cache, level=0)

    # Lock only for the manifest update — brief
    with self._flush_lock:
        self._registry.register(file_id, reader)
        self._compactor.register_sstable(meta)

    return meta
```

### 5e. `get(key) -> tuple[SeqNum, int, Value] | None` — async

```python
async def get(self, key: Key) -> tuple[SeqNum, int, Value] | None:
    # L0: concurrent fan-out — all files must be checked
    l0_files = self._compactor.level_files(0)  # list[FileID], newest first

    if l0_files:
        tasks = [
            asyncio.create_task(self._reader_get(fid, key))
            for fid in l0_files
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        best: tuple[SeqNum, int, Value] | None = None
        for r in results:
            if isinstance(r, Exception) or r is None:
                continue
            if best is None or r[0] > best[0]:   # higher seq = newer
                best = r
        if best is not None:
            return best

    # L1+: one candidate per level — sequential
    for level in range(1, self._compactor.max_level + 1):
        candidate_id = self._compactor.find_candidate(level, key)
        if candidate_id is None:
            continue
        result = await asyncio.to_thread(self._sync_reader_get, candidate_id, key)
        if result is not None:
            return result

    return None

async def _reader_get(self, file_id: FileID, key: Key):
    try:
        with self._registry.open_reader(file_id) as reader:
            return reader.get(key)
    except Exception as e:
        logger.warning("Reader error during get",
                       extra={"file_id": file_id, "error": str(e)})
        return None

def _sync_reader_get(self, file_id: FileID, key: Key):
    try:
        with self._registry.open_reader(file_id) as reader:
            return reader.get(key)
    except Exception as e:
        logger.warning("Reader error during get",
                       extra={"file_id": file_id, "error": str(e)})
        return None
```

### 5f. `max_seq_seen() -> SeqNum` — sync

```python
def max_seq_seen(self) -> SeqNum:
    return self._compactor.max_seq_seen()
```

Used by `LSMEngine._recover()` to set the starting seq counter after startup.

### 5g. `close_all()` — sync

```python
def close_all(self) -> None:
    for file_id in list(self._registry.all_file_ids()):
        try:
            reader = self._registry.get(file_id)
            reader.close()
        except Exception as e:
            logger.debug("Error closing reader",
                         extra={"file_id": file_id, "error": str(e)})
```

---

## 6. Compaction design constraints — write-optimised

### 6a. Write path must never be blocked by compaction

Compaction runs in a `ProcessPoolExecutor` subprocess. The engine's write path (`put()`, `delete()`, flush worker) must never wait for compaction to complete. The compaction worker runs on a separate daemon thread:

```
Write path:   put() → WAL → MemTable → freeze → flush worker → SSTable
Compaction:   separate daemon thread → ProcessPoolExecutor subprocess
              ← completely decoupled from write path
```

### 6b. L0 compaction — triggered, not blocking

When L0 reaches `L0_COMPACTION_THRESHOLD` (4 files), the compaction worker triggers a merge of all 4 L0 files into one L1 SSTable. This happens asynchronously:

```
L0: [file_a, file_b, file_c, file_d]   ← 4 files → trigger
         ↓  compaction subprocess (separate process)
L1: [merged_file]                       ← 1 file, fully deduplicated, L1 max size enforced
```

During compaction, new writes still go to L0. The engine may accumulate a 5th L0 file while compaction is running (brief window). The compaction check uses `>=` not `==`.

### 6c. L1+ compaction — replace the single file

Since L1, L2, L3 each hold exactly one SSTable, compaction at these levels replaces the single file with a merged file that incorporates L0 data (for L1→L1) or LN data (for LN→LN+1 overflow):

```
L1 overflow: merge [L1_file + oldest_L0_file] → new L1_file
L2 overflow: merge [L2_file + L1_file] → new L2_file, L1 now empty
```

### 6d. Tombstone GC — only at L1+ (implementation deferred)

Tombstones are dropped only when:
1. The key being deleted does not appear in any higher (newer) level
2. `entry.seq < compaction_cutoff_seq`

At L0, tombstones are **always preserved** — L0 files are not deduplicated. Dropping a tombstone at L0 could resurrect a value in L1+ that should be deleted.

### 6e. Flush lock scope — minimised for write throughput

The `_flush_lock` in `SSTableManager` is held **only** during manifest update (< 1ms), not during the SSTable write (potentially 200-500ms for a 64MB file). This ensures that:
- New flushes can be queued even while a previous flush is writing
- Only the manifest update itself is serialised

---

## 7. Test checklist (per phase)

### Phase 1 — SSTableMeta tests
- [ ] `encode()` produces valid JSON with base64 keys
- [ ] `decode()` round-trips through encode
- [ ] `min_key` / `max_key` survive base64 encode/decode for binary key data
- [ ] Missing `meta.json` is not loaded at startup (incomplete SSTable)

### Phase 2 — SSTableWriter tests
- [ ] `put()` raises `OutOfOrderKeyError` if key <= previous key
- [ ] `put()` raises `WriterSealedError` after `finish()`
- [ ] `finish()` / `finish_sync()` produces all four files: `data.bin`, `index.bin`, `filter.bin`, `meta.json`
- [ ] `meta.json` is written **last** — removing it leaves other files intact
- [ ] Last (partial) block is flushed by `finish()` — not dropped
- [ ] Sparse index has one entry per block, first key of each block
- [ ] Bloom filter `may_contain(key)` is True for all inserted keys
- [ ] CRC mismatch in data.bin detected by reader
- [ ] L0 `finish()` is async — Bloom + index written concurrently
- [ ] L1 `finish_sync()` is sync — sequential writes

### Phase 3 — SSTableReader tests
- [ ] `open()` loads footer, index, Bloom filter correctly
- [ ] `get()` returns correct value for existing key
- [ ] `get()` returns None for missing key
- [ ] `get()` returns None when Bloom filter says absent (no false negative)
- [ ] `get()` handles TOMBSTONE: returns `(seq, ts, TOMBSTONE)` — not None
- [ ] CRC mismatch raises `CorruptRecordError`
- [ ] `iter_range(start, end)` yields keys in sorted order within range
- [ ] `iter_range` skips tombstoned keys
- [ ] `close()` after `close()` is a no-op — no error
- [ ] L0 open: index and Bloom loaded concurrently (verify via timing or mock)
- [ ] L1+ open: index and Bloom loaded sequentially

### Phase 4 — SSTableManager tests
- [ ] `load()` opens all known SSTables at startup, bounded by semaphore
- [ ] `load()` silently skips incomplete SSTables (no `meta.json`)
- [ ] `flush()` writes SSTable, registers reader, updates manifest
- [ ] `flush()` carries `snapshot_id` into `SSTableMeta`
- [ ] `get()` checks all L0 files concurrently, returns highest-seq result
- [ ] `get()` returns TOMBSTONE from L0 even if L1 has a value (newer shadows older)
- [ ] `get()` for L1+: binary search on manifest, checks at most one file per level
- [ ] Startup with no SSTables: `load()` returns empty manager
- [ ] `close_all()` does not raise even if individual readers fail to close
- [ ] `max_seq_seen()` returns correct maximum across all levels
