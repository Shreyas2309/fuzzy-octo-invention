# Async I/O Coding Guidelines for Python Projects

> **Purpose:** Generic coding rules for a coding agent to follow when implementing async I/O in Python, extracted from real production patterns.
> **Date:** 2026-03-20

---

## 1. Foundational Principle

Async I/O exists to manage **wait time on external systems** (network, disk, databases), not to speed up computation. Every decision below follows from this: use `async`/`await` where the program is waiting for something external, keep everything else synchronous.

---

## 2. Classify Every Function Before Writing It

Before writing any function, determine its nature. The classification dictates the implementation:

| Classification | Signature | How to Call from Async | Real Example |
|---|---|---|---|
| **I/O-bound** — waits on network, disk, DB | `async def` with `await` on I/O calls | `await fn()` | Querying a database, calling a REST API via async client, writing to object storage |
| **Sync I/O** — uses a sync-only library for network/DB | Regular `def` | `await asyncio.to_thread(fn)` | A third-party SDK that only exposes blocking methods |
| **CPU-bound** — parsing, computation, transformation | Regular `def` | Direct call if fast (<100ms); `await asyncio.to_thread(fn)` if slow | Recursive type parsing, data reshaping, in-memory SQL |
| **Pure logic** — validation, mapping, formatting | Regular `def` | Direct call (never wrap) | Filter parsing, field renaming, string manipulation |

**Rule: Never make a function `async def` unless it contains at least one `await` on an I/O operation, or unless a protocol/interface requires the async signature.**

Example of correct classification:

```python
# PURE LOGIC — stays sync, called directly from async context
def build_query(filters: dict) -> str:
    return f"SELECT * FROM table WHERE {build_where_clause(filters)}"

# I/O-BOUND — async because it awaits a database call
async def extract_data(client, filters: dict) -> list:
    query = build_query(filters)          # sync call — fine, it's fast
    return await client.execute(query)    # I/O: await

# SYNC I/O — sync SDK, called via to_thread from async context
def list_resources(sdk_client) -> list:
    return sdk_client.list_all()          # blocking SDK call

# CPU-BOUND — heavy parsing, offloaded to thread when called from async
def parse_nested_structure(raw_text: str) -> dict:
    return _recursive_descent_parse(raw_text, depth=0)
```

---

## 3. Never Block the Event Loop

Any synchronous call that waits on I/O (network, disk) inside an `async def` blocks the **entire** event loop — no other coroutine can progress.

```python
# BAD: blocks the event loop for every call
async def get_data():
    response = requests.get(url)           # blocks everything
    return response.json()

# GOOD: offloads to thread pool
async def get_data():
    response = await asyncio.to_thread(requests.get, url)
    return response.json()

# GOOD: uses native async client
async def get_data():
    async with httpx.AsyncClient() as client:
        response = await client.get(url)   # non-blocking
        return response.json()
```

**One exception:** A sync call inside `async def` is tolerable for **one-time initialization** (e.g., fetching a token at startup). Document it with a comment explaining why.

```python
async def initialize(self, credentials):
    # One-time sync call during init — acceptable, blocks briefly
    token = requests.post(token_url, data=payload).json()["access_token"]
    self._token = token
```

---

## 4. Bridging Sync Libraries into Async Code

When a third-party library provides only synchronous methods, use these patterns depending on whether you need a single call or parallel calls.

### 4a. Single sync call — `asyncio.to_thread()`

Wrap the sync function so it runs in a thread and returns a future the event loop can await:

```python
async def fetch_items(self):
    # self.client.list_items() is sync — run in thread to keep event loop alive
    items = await asyncio.to_thread(self.client.list_items)
    return items
```

**Pattern: Define a sync helper, await it via `to_thread`.**

When the sync code needs multiple statements, extract a private `_sync_*` method:

```python
def _sync_fetch_and_process(self, category: str) -> list:
    """Sync helper: fetch from SDK and do light processing."""
    cursor = self.connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM {category}")
        return [dict(row) for row in cursor.fetchall()]
    finally:
        cursor.close()

async def extract(self, category: str) -> list:
    return await asyncio.to_thread(self._sync_fetch_and_process, category)
```

### 4b. Many parallel sync calls — `ThreadPoolExecutor` inside `asyncio.to_thread()`

When you need to parallelize multiple independent sync I/O calls:

```python
async def extract_all(self, categories: list) -> list:
    def _parallel_extract() -> list:
        all_results = []
        lock = threading.Lock()

        def _extract_one(category: str) -> list:
            try:
                return self.sync_client.fetch(category)
            except Exception as e:
                logger.warning("Failed", extra={"category": category, "error": str(e)})
                return []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_extract_one, c): c for c in categories}
            for future in as_completed(futures):
                result = future.result()
                with lock:
                    all_results.extend(result)
        return all_results

    return await asyncio.to_thread(_parallel_extract)
```

**Key conventions observed:**
- `ThreadPoolExecutor` is created inside the function (context manager), not at module level — this bounds its lifetime and prevents resource leaks.
- Shared mutable state (`all_results`) is protected with `threading.Lock`.
- Each thread function catches its own exceptions and returns an empty/default result instead of crashing the pool.
- `as_completed()` processes results as they finish, not in submission order.
- The whole sync block is wrapped in `asyncio.to_thread()` so the calling async function doesn't block.

### 4c. Cancellation-safe thread pools

When threads may hang (e.g., HTTP calls to unreachable hosts), shut down defensively:

```python
executor = ThreadPoolExecutor(max_workers=5)
try:
    futures = {executor.submit(fn, item): item for item in items}
    for future in as_completed(futures):
        try:
            result = future.result(timeout=60)  # per-future timeout
        except TimeoutError:
            logger.warning("Timed out", extra={"item": futures[future]})
finally:
    executor.shutdown(wait=False, cancel_futures=True)
```

---

## 5. Concurrency Control

**Never fire unbounded concurrent I/O.** External systems have rate limits, connection caps, and resource constraints. Always gate concurrency.

### 5a. `asyncio.Semaphore` — for async tasks

```python
concurrency = int(os.environ.get("MAX_CONCURRENT_REQUESTS", "20"))
semaphore = asyncio.Semaphore(concurrency)

async def bounded_call(item):
    async with semaphore:
        return await async_api_call(item)

results = await asyncio.gather(*[bounded_call(item) for item in items])
```

### 5b. `ThreadPoolExecutor(max_workers=N)` — for sync tasks in threads

```python
max_workers = int(os.environ.get("MAX_WORKERS", "20"))
with ThreadPoolExecutor(max_workers=max_workers) as pool:
    futures = [pool.submit(sync_call, item) for item in items]
```

### 5c. Sliding window with `asyncio.Semaphore` + `asyncio.gather`

For workflow-level fan-out where you dispatch many independent units but want to limit how many run at once:

```python
semaphore = asyncio.Semaphore(5)
tasks = []
for unit in work_units:
    async def _process(u=unit):
        async with semaphore:
            return await execute_work(u)
    tasks.append(_process())
results = await asyncio.gather(*tasks)
```

**Note the `u=unit` default argument** — this captures the loop variable by value. Without it, all closures would reference the same `unit` variable.

### 5d. Make all concurrency limits configurable

```python
MAX_WORKERS = int(os.getenv("EXTRACTION_MAX_WORKERS", "20"))
SEMAPHORE_SLOTS = int(os.getenv("FAN_OUT_CONCURRENCY", "5"))
```

Never hardcode concurrency values. Different environments (local dev, staging, production) have different resource constraints.

---

## 6. Error Handling in Async & Threaded Contexts

### 6a. Graceful degradation — skip and continue

When processing independent items where one failure shouldn't abort the rest:

```python
async def extract_all_levels(self) -> list:
    all_results = []
    for level in self.levels:
        try:
            results = await self.extract_level(level)
            all_results.extend(results)
        except Exception as e:
            logger.warning("Failed to extract level",
                          extra={"level": level, "error": str(e)})
            # Continue with remaining levels
    return all_results
```

### 6b. Cleanup never raises

Teardown/cleanup methods must never propagate exceptions — a cleanup failure should not mask the real error:

```python
def close(self) -> None:
    if self.connection:
        try:
            self.connection.close()
        except Exception as exc:
            logger.debug("Error closing connection", extra={"error": str(exc)})
        self.connection = None
```

### 6c. Thread-safe error propagation

When threads encounter certain errors that should affect other threads (e.g., "this entire group is permission-denied"), use a lock-protected shared set:

```python
denied_groups = set()
denied_lock = threading.Lock()

def _process(item, group):
    with denied_lock:
        if group in denied_groups:
            return []  # skip — already known to be denied
    try:
        return client.fetch(item)
    except PermissionDenied:
        with denied_lock:
            denied_groups.add(group)  # prevent further attempts for this group
        return []
```

### 6d. Exception-specific handling with typed errors

Differentiate between retryable and non-retryable errors. Log at appropriate severity:

```python
try:
    result = future.result(timeout=60)
except PermissionDenied as e:
    logger.warning("Permission denied, skipping group", extra={"group": group})
except NotFound:
    logger.debug("Resource not found, skipping", extra={"item": item})
except TimeoutError:
    logger.warning("Timed out", extra={"item": item, "timeout": 60})
except Exception as e:
    logger.warning("Unexpected error", extra={"item": item, "error": str(e)})
```

### 6e. Progress logging in loops

For long-running extraction/processing loops, log progress at regular intervals:

```python
total = len(items)
for i, item in enumerate(items):
    result = await process(item)
    if (i + 1) % 100 == 0 or (i + 1) == total:
        logger.info("Processing progress",
                    extra={"completed": i + 1, "total": total})
```

---

## 7. Buffered Async Writes

Never issue one I/O write per record. Buffer records and flush in batches:

```python
class BufferedWriter:
    def __init__(self, flush_fn, chunk_size: int = 50_000, on_flush=None):
        self._buffer = []
        self._chunk_size = chunk_size
        self._flush_fn = flush_fn
        self._on_flush = on_flush      # optional callback after each flush
        self._chunk_index = 0
        self._total_written = 0

    async def write(self, record) -> None:
        self._buffer.append(record)
        if len(self._buffer) >= self._chunk_size:
            await self.flush()

    async def flush(self) -> None:
        if not self._buffer:
            return
        await self._flush_fn(self._buffer, self._chunk_index)
        self._total_written += len(self._buffer)
        self._buffer.clear()
        self._chunk_index += 1
        if self._on_flush:
            await self._on_flush()  # heartbeat, progress update, etc.

    async def close(self) -> int:
        await self.flush()          # flush remaining
        return self._total_written
```

**Key design points:**
- `flush()` is a no-op on empty buffer (safe to call multiple times).
- `close()` calls `flush()` for remaining records, then returns total count.
- An optional callback (`on_flush`) allows callers to send heartbeats or progress updates between chunks.
- Chunk size is configurable (default 50,000 — tune based on record size and memory).

---

## 8. Sync Phases + Async Phases in Pipelines

When a pipeline has distinct phases where some are CPU-bound (in-memory) and others are I/O-bound (writing output), keep them in their natural domains:

```python
async def run_pipeline(self) -> dict:
    # Phase 1-3: Sync — in-memory computation, no benefit from async
    for processor in self.processors:
        processor.load()       # sync: load data into memory
        processor.enrich()     # sync: joins, transformations
        processor.prune()      # sync: filtering, deduplication

    # Phase 4: Async — I/O to external storage
    results = {}
    for processor in self.processors:
        count = await processor.transform_and_write()  # async: writes to storage
        results[processor.name] = count

    return results
```

**Don't force `async` onto phases that are pure computation.** An `async def` that never awaits anything gains nothing — it just adds overhead.

---

## 9. Protocols Define Async Contracts

Use `typing.Protocol` to define interfaces that may have multiple implementations — some natively async, some sync-wrapped:

```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class ClientProtocol(Protocol):
    async def load(self, credentials: dict) -> None: ...
    async def close(self) -> None: ...

class AsyncClient:
    """Natively async implementation."""
    async def load(self, credentials):
        self.session = await aiohttp.ClientSession().__aenter__()

    async def close(self):
        await self.session.close()

class SyncClientWrapper:
    """Wraps a sync SDK to satisfy the async protocol."""
    async def load(self, credentials):
        # One-time sync init — acceptable
        self._client = SyncSDK(credentials)

    async def close(self):
        self._client.disconnect()
```

**The caller doesn't need to know which implementation is used.** Both satisfy `ClientProtocol`. For sync methods that need to be called repeatedly, the caller wraps with `asyncio.to_thread()`:

```python
# Caller code — works with any ClientProtocol implementation
await client.load(credentials)
results = await asyncio.to_thread(client.fetch_data)  # safe for sync or async
```

---

## 10. Structured Logging in Async Contexts

Always use structured `extra={}` fields. Never embed variable data in log message strings:

```python
# CORRECT — parseable, searchable, consistent
logger.info("Extraction complete",
    extra={"entity_type": "catalogs", "row_count": len(df), "duration_ms": elapsed})

logger.warning("Failed to process item",
    extra={"item_id": item_id, "error": str(e)})

# WRONG — unparseable, inconsistent format
logger.info(f"Extracted {len(df)} catalogs in {elapsed}ms")
logger.warning(f"Failed: {e}")
```

**Why:** Structured logs can be queried in log aggregation systems (grep `entity_type=catalogs`). F-string logs require regex to parse.

**Log level conventions:**
- `debug` — skippable items, fallback paths, not-found responses
- `info` — stage start/complete, progress checkpoints, success counts
- `warning` — recoverable failures (item skipped, retrying, graceful degradation)
- `error` — unexpected failures that may need investigation

---

## 11. Entry Point and Event Loop

Use `asyncio.run()` as the single entry point. Never nest event loops or create them manually:

```python
async def main():
    await setup()
    await run()

if __name__ == "__main__":
    asyncio.run(main())
```

**Never:**
- `loop.run_until_complete()` — deprecated pattern
- `nest_asyncio.apply()` — hack for Jupyter, not production code
- `asyncio.get_event_loop()` for running coroutines — use `asyncio.run()`

---

## 12. Testing Async Code

Configure pytest-asyncio in auto mode so test functions can be `async def` without decorators:

```toml
# pyproject.toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "function"
```

```python
# Tests are just async functions — no @pytest.mark.asyncio needed
async def test_extract_returns_data():
    extractor = MyExtractor(mock_client)
    result = await extractor.extract()
    assert len(result) > 0

async def test_concurrent_extraction():
    results = await asyncio.gather(
        extractor.extract("a"),
        extractor.extract("b"),
    )
    assert all(len(r) > 0 for r in results)
```

---

## 13. Configuration via Environment Variables

All timeout, concurrency, and buffer size values should be configurable through environment variables with sensible defaults:

```python
import os

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "20"))
BUFFER_SIZE = int(os.getenv("BUFFER_SIZE", "50000"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))
```

Centralize these in a single module (e.g., `config.py` or `timeouts.py`) so they're discoverable and not scattered across the codebase:

```python
# timeouts.py — single source of truth for all tunables
import os

EXTRACT_TIMEOUT_SECONDS = int(os.getenv("EXTRACT_TIMEOUT_SECONDS", "2700"))
TRANSFORM_TIMEOUT_SECONDS = int(os.getenv("TRANSFORM_TIMEOUT_SECONDS", "14400"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "20"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "50000"))
SEMAPHORE_SLOTS = int(os.getenv("FAN_OUT_CONCURRENCY", "5"))
```

---

## 14. Liveness Callbacks for Long-Running Operations

When an async operation takes a long time (>60s), external systems (orchestrators, load balancers, health checks) may consider it dead. Use a callback pattern to emit liveness signals:

```python
class DataProcessor:
    def __init__(self, heartbeat_fn=None):
        self._heartbeat_fn = heartbeat_fn

    async def process(self, data):
        for chunk in self._chunk(data):
            await self._write(chunk)
            if self._heartbeat_fn:
                await self._heartbeat_fn()  # signal "still alive"
```

**The heartbeat is optional (default `None`) so the component works both with and without an orchestrator.** The caller injects it:

```python
async def _send_heartbeat():
    orchestrator.heartbeat()

processor = DataProcessor(heartbeat_fn=_send_heartbeat)
await processor.process(large_dataset)
```

---

## Decision Flowchart

```
Is the function doing I/O (network, file, database)?
├── YES
│   ├── Is the library async-native?
│   │   ├── YES → `async def` + `await library_call()`
│   │   └── NO (sync-only library)
│   │       ├── Called once (init/cleanup)? → sync inside `async def` is OK
│   │       └── Called in a hot path?
│   │           ├── Single call → `await asyncio.to_thread(sync_fn)`
│   │           └── Many parallel calls → `ThreadPoolExecutor` inside `asyncio.to_thread()`
│   │               └── Protect shared state with `threading.Lock`
│   └── Does it need concurrency control?
│       ├── Async calls → `asyncio.Semaphore` + `asyncio.gather()`
│       └── Sync calls → `ThreadPoolExecutor(max_workers=N)`
│           └── Make N configurable via env var
└── NO (pure computation)
    ├── Takes >100ms? → keep sync, offload with `asyncio.to_thread()` from async caller
    └── Takes <100ms? → keep sync, call directly (no wrapping needed)
```

---

## Summary: The 14 Rules

| # | Rule | One-liner |
|---|------|-----------|
| 1 | Classify before coding | Determine I/O vs CPU vs pure logic before choosing sync/async |
| 2 | Never block the event loop | No sync I/O inside `async def` on hot paths |
| 3 | Bridge sync libraries with `to_thread` | `await asyncio.to_thread(sync_fn)` for sync SDKs |
| 4 | Parallelize sync I/O with `ThreadPoolExecutor` | Create per-call, wrap in `to_thread`, protect shared state with Lock |
| 5 | Gate all concurrency | `Semaphore` for async, `max_workers` for threads, always configurable |
| 6 | Handle errors by context | Skip-and-continue for optional work, never raise in cleanup, lock shared state |
| 7 | Buffer writes | Never one I/O call per record; flush in chunks with optional callback |
| 8 | Keep sync phases sync | Don't force async on CPU-bound pipeline stages |
| 9 | Protocols define async contracts | `typing.Protocol` for swappable sync/async implementations |
| 10 | Structured logging always | `extra={}` dict, never f-strings in log messages |
| 11 | Single async entry point | `asyncio.run(main())`, never nested loops |
| 12 | Test with pytest-asyncio auto mode | `asyncio_mode = "auto"`, async test functions |
| 13 | Configure via env vars | All timeouts, concurrency, buffer sizes in one module |
| 14 | Liveness callbacks for long ops | Optional `heartbeat_fn` passed to nested components |
