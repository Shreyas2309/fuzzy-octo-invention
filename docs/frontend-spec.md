# lsm-kv Explorer

**Website + API Specification**  
*For project examiners — interactive demonstration of a production-grade LSM-Tree KV store*

---

## PART 1 — Product Vision

### 1.1  Purpose

lsm-kv Explorer is a web application that lets examiners interact with the live LSM-Tree key-value store engine through a browser. Every architectural component built in the project — the WAL, MemTable, SSTable files, compaction pipeline, bloom filters, block cache, and async RW locking — is directly visible, navigable, and demonstrable through the UI.

The application is not a static documentation site. It is a live control panel that drives the real LSMEngine Python process and shows its internal state updating in real time.

### 1.2  Design direction

Aesthetic: **dark industrial terminal** — the visual language of a database internals debugger. Deep navy backgrounds, monospace data everywhere, red accent for writes and destructive operations, green for reads and healthy state, amber for warnings. Typography mixes a geometric display face for headings with pure monospace for all data.

The examiner should feel they are looking **inside** the database — not at a dashboard about it. Internal state (seq numbers, file IDs, block offsets, bloom filter bytes) is displayed raw, not prettified.

### 1.3  What the examiner can do

| Capability | What it demonstrates |
| --- | --- |
| Write keys/values, delete keys, run batch loads | Write path: WAL → MemTable → freeze → flush pipeline |
| Watch MemTable fill in real time, see freeze trigger | SkipList, dual threshold (dev/prod), backpressure |
| Trigger manual flush, watch L0 SSTable appear | Parallel flush pipeline, event-chain ordered commits |
| Browse every SSTable's entries, bloom filter, sparse index | SSTable format: data.bin, index.bin, filter.bin, meta.json |
| Watch L0 count climb to threshold, compaction fire | CompactionManager, level reservation, subprocess isolation |
| Tail live structured logs in the browser | TCP log broadcast server on port 9009 |
| Run point lookups, see which level answered | Bloom filter hit/miss, bisect lookup, block cache hit/miss |
| View and update config at runtime | disk-persisted LSMConfig, runtime-mutable fields |
| Inspect crash recovery by restarting the engine | WAL replay, manifest reconciliation, orphan handling |
| See test suite results and static analysis badges | 211 tests, mypy strict, ruff, bandit |

## PART 2 — System Architecture

### 2.1  Component diagram

```

┌─────────────────────────────────────────────────────────────────┐
│                      Browser (React SPA)                        │
│  Dashboard │ MemTable │ SSTables │ Compaction │ Logs │ Config   │
└────────────────────────────┬────────────────────────────────────┘
                             │ HTTP/REST  +  WebSocket
┌────────────────────────────▼────────────────────────────────────┐
│                    FastAPI  (Python)                            │
│   /api/*  REST endpoints      /ws/logs  WebSocket proxy        │
└────────────────────────────┬────────────────────────────────────┘
                             │ in-process function calls
┌────────────────────────────▼────────────────────────────────────┐
│                    LSMEngine  (Python)                          │
│  WAL · MemTable · SSTableManager · FlushPipeline               │
│  CompactionManager · BlockCache · AsyncRWLock                  │
└───────────────────────┬─────────────────────────────────────────┘
                        │ TCP port 9009
              LogBroadcastServer  ──►  /ws/logs WebSocket

```

**Key design choices:** FastAPI runs in the same Python process as the LSMEngine — it calls engine methods directly with no serialisation overhead. The WebSocket log tail connects to the existing TCP broadcast server on port 9009. No message queue, no external services, no Docker required.

### 2.2  Technology choices

| Layer | Technology | Reason |
| --- | --- | --- |
| API server | FastAPI + uvicorn | Async-native, integrates with asyncio event loop the LSMEngine already runs on |
| Frontend | React 18 + Vite | Component model maps cleanly to engine subsystems; Vite dev server proxies API |
| Styling | Tailwind CSS + custom CSS vars | Utility classes for layout; CSS vars for the dark industrial theme |
| Real-time logs | WebSocket (browser) ↔ FastAPI ↔ TCP socket (log server) | Log server already broadcasts on port 9009; WS bridges it to browser |
| State management | React Query (TanStack) | Auto-polling for engine state; cache invalidation on mutations |
| Charts | Recharts | SSTable size over time, L0 file count, compaction history |
| Font (display) | JetBrains Mono (monospace), IBM Plex Sans (UI text) | Database terminal aesthetic |

## PART 3 — API Specification

> ℹ  Base URL: http://localhost:8000/api/v1   |   WebSocket: ws://localhost:8000/ws/logs

### 3.1  Engine lifecycle

| Method | Path | Description | Response |
| --- | --- | --- | --- |
| GET | `/engine/status` | Engine open/closed, data_root, log_port | `{open, data_root, log_port, uptime_s}` |
| POST | `/engine/open` | Open or re-open the engine | `{ok, data_root}` |
| POST | `/engine/close` | Graceful shutdown (drains flush pipeline) | `{ok}` |

### 3.2  Write operations

| Method | Path | Body | Response |
| --- | --- | --- | --- |
| POST | `/kv/put` | `{key: str, value: str}` | `{ok, seq, timestamp_ms}` |
| POST | `/kv/delete` | `{key: str}` | `{ok, seq, timestamp_ms}` |
| POST | `/kv/batch` | `{ops: [{op:'put'` 'del', key, value?}]} | `{ok, count, seq_range:[min,max]}` |

### 3.3  Read operations

| Method | Path | Query / Body | Response |
| --- | --- | --- | --- |
| GET | `/kv/get` | `?key=<str>` | `{found, value?, source, seq?, bloom_hit?}` |
| POST | `/kv/scan` | `{prefix: str, limit: int}` | `{entries:[{key,value,seq,source}], count}` |

```
The source field in the GET response tells the examiner exactly which component answered: active_memtable, immutable_0, l0:<file_id>, or l1:<file_id>. The bloom_hit boolean shows whether the Bloom filter returned a positive (and whether it was a false positive).
```

### 3.4  MemTable inspection

| Method | Path | Description | Response shape |
| --- | --- | --- | --- |
| GET | `/mem` | Summary: active + immutable queue | `{active:{...}, immutable:[...]}` |
| GET | `/mem/:table_id` | All entries in one memtable | `{type, table_id, entries:[{key,value,seq,ts,is_tombstone}]}` |
| POST | `/mem/flush` | Force-freeze active memtable | `{ok, flushed, snapshot_id}` |

### 3.5  SSTable inspection

| Method | Path | Description | Response shape |
| --- | --- | --- | --- |
| GET | `/disk` | All SSTables grouped by level | `{L0:[...], L1:{...}` null} |
| GET | `/disk/:file_id` | All records in one SSTable | `{file_id, level, record_count, entries:[...]}` |
| GET | `/disk/:file_id/meta` | SSTableMeta: sizes, seq range, key range | `{file_id, level, record_count, block_count, size_bytes, min_key, max_key, seq_min, seq_max, created_at}` |
| GET | `/disk/:file_id/bloom` | Bloom filter stats for one SSTable | `{file_id, hash_count, bit_count, approx_fpr, size_bytes}` |
| GET | `/disk/:file_id/index` | Sparse index entries for one SSTable | `{file_id, entries:[{first_key, block_offset}], entry_count}` |

### 3.6  Compaction

| Method | Path | Description | Response shape |
| --- | --- | --- | --- |
| GET | `/compaction/status` | Active jobs, reserved levels, last event | `{active_jobs:[{src,dst,task_id,started_at}], active_levels:[int], last_completed:{...}` null} |
| POST | `/compaction/trigger` | Manually force a compaction check | `{ok, triggered, reason}` |
| GET | `/compaction/history` | Last N events from compaction.log | `{events:[{ts,event,task_id,src,dst,inputs,output,output_records?}]}` |

### 3.7  Engine stats

| Method | Path | Description | Response shape |
| --- | --- | --- | --- |
| GET | `/stats` | Full engine snapshot | `{key_count, seq, wal_entry_count, active_table_id, active_size_bytes, immutable_queue_len, l0_sstable_count, l1_file_id` null, compaction_active} |
| GET | `/stats/history` | Time-series: seq, l0_count, mem_bytes — last 5 min | `{samples:[{ts, seq, l0_count, mem_bytes}]}` |
| GET | `/wal` | WAL entry count, current size, last seq | `{entry_count, size_bytes, last_seq, path}` |

### 3.8  Configuration

| Method | Path | Body | Response |
| --- | --- | --- | --- |
| GET | `/config` | — | Full config JSON object |
| PATCH | `/config` | `{key: str, value: int` float `str}` | `{ok, key, old_value, new_value}` |
| GET | `/config/schema` | — | All valid keys with types and current values |

### 3.9  WebSocket — live log stream

```

GET  ws://localhost:8000/ws/logs
 
// Server streams one JSON object per log event:
{
  "ts":       "2026-03-22T08:45:12.331Z",
  "level":    "info",            // debug | info | warning | error | critical
  "event":    "SSTable committed",
  "logger":   "app.engine.flush_pipeline",
  "file_id":  "019600ab...",     // any extra structlog fields
  "l0_count": 4
}
 
// Client can send a filter message:
{ "filter": "compaction" }    // server only streams lines matching filter
{ "filter": null }            // clear filter, stream all

```

### 3.10  Lookup trace endpoint — the examiner favourite

This is the most important endpoint for demonstrating internals. It executes a full lookup and returns a step-by-step trace of every decision made:

```

GET  /kv/trace?key=<str>
 
Response:
{
  "key": "hello",
  "found": true,
  "value": "world",
  "steps": [
    { "step": 1, "component": "active_memtable",  "result": "miss" },
    { "step": 2, "component": "immutable_0",      "result": "miss" },
    { "step": 3, "component": "l0:019600ab",
      "bloom_check": "negative",
      "result": "bloom_skip" },
    { "step": 4, "component": "l0:019600ac",
      "bloom_check": "positive",
      "bisect_offset": 4096,
      "block_cache_hit": false,
      "result": "found",
      "seq": 47 }
  ]
}

```

### 3.11  Error format

```

// All errors return HTTP 4xx/5xx with:
{
  "error": "key not found",
  "code":  "KEY_NOT_FOUND",
  "detail": "..."   // optional
}

```

## PART 4 — Frontend Specification

### 4.1  Application shell

Single-page application. Fixed left sidebar (240px) for navigation. Main content area fills the rest. No top navigation bar. A persistent status strip at the very bottom of the sidebar shows engine open/closed, uptime, and a pulsing green dot when the log stream is connected.

```

┌─────────────────────────────────────────────────────────────────┐
│  lsm-kv  [●]  SIDEBAR (240px)  │  MAIN CONTENT AREA           │
│  ─────────────────────────     │                               │
│  ◆ Dashboard                   │  <active page>                │
│  ◆ Write / Read                │                               │
│  ◆ MemTable                    │                               │
│  ◆ SSTables                    │                               │
│  ◆ Compaction                  │                               │
│  ◆ Lookup Trace                │                               │
│  ◆ Live Logs                   │                               │
│  ◆ Config                      │                               │
│  ─────────────────────────     │                               │
│  ◉ Engine: OPEN  12m 4s        │                               │
│  ◉ Logs: connected             │                               │
└─────────────────────────────────────────────────────────────────┘

```

### 4.2  Page 1 — Dashboard

**Purpose:** One-screen overview of the entire engine state. The examiner opens this first.

**Layout — 4 stat cards (top row):** Key count, Current seq, WAL entries, L0 SSTable count. Each card shows the value large (monospace), a label below, and a sparkline of the last 60 seconds.

**Layout — 3 panels (middle row):**

- [object Object] — Active memtable: entry count progress bar toward threshold, size in bytes. Below it, the immutable queue slots (up to 4) shown as filled/empty rectangles. Freeze trigger threshold marked.
- [object Object] — Visual representation of the LSM level structure. L0 shows N stacked rectangles (one per file, proportional to size). L1 shows one wide rectangle if it exists. Compaction arrow animates when active.
- [object Object] — Small line chart: writes/sec and reads/sec over the last 2 minutes. Sampled from the stats/history endpoint.

**Layout — live event feed (bottom):** Last 10 engine events from the log stream. Each line: timestamp, coloured level badge, event name, key fields. Auto-scrolls. Shows compaction started/committed events with a distinct amber colour.

### 4.3  Page 2 — Write / Read console

**Purpose:** Interactive KV operations — the main demonstration tool for examiners.

**Left panel — command input:**

- Tab bar: PUT | GET | DELETE | BATCH
- PUT: key input, value textarea, submit button. On success: shows seq number and timestamp_ms returned.
- GET: key input, submit. Shows value, source (e.g. 
- DELETE: key input, confirm checkbox, submit.
- BATCH: text area for multi-line ops (one per line: 

**Right panel — operation history:** Scrolling table of the last 50 operations in this session: timestamp, op type (colour coded), key, value (truncated), seq, latency in ms. Persistent for the session, cleared on page reload.

**Bottom strip — quick lookup:** Always-visible key input that shows the current value in real time as you type (500ms debounce). Shows source and whether it came from memtable or disk.

### 4.4  Page 3 — MemTable Inspector

**Purpose:** Show the examiner the in-memory write side of the engine in full detail.

```
Top section — Active MemTable: Table ID (truncated UUID, full on hover), entry count, size in bytes, progress bar to threshold. Two threshold lines marked: dev (entry count) and prod (size bytes). A Force Flush button triggers POST /mem/flush.
```

**Middle section — Immutable queue:** Up to 4 slots rendered as cards in a horizontal row. Each card shows: snapshot ID, entry count, seq_min..seq_max, size, tombstone count. Cards animate in from the left when a new snapshot is added. When the flush pipeline claims one, it animates out to the right with a fade.

**Bottom section — Entry browser:** Click any memtable card to expand a paginated table of its entries. Columns: seq, key, value (raw bytes decoded as UTF-8), is_tombstone (shown as a red ✗ badge). Tombstones styled with strikethrough value.

```
Auto-refresh: Polls GET /mem every 1 second. New entries appear with a brief highlight animation.
```

### 4.5  Page 4 — SSTable Browser

**Purpose:** Navigate the on-disk structure — the core data structures demonstration.

**Left panel — level tree (200px):** Collapsible tree: L0 → [file_id_1, file_id_2, ...], L1 → [file_id]. Each file shows record count and size. Click a file to load its detail in the right panel.

**Right panel — SSTable detail:** Four tabs:

- [object Object] — Paginated table of all records: seq, key, value, is_tombstone. Key range shown at top (min_key .. max_key). Export as JSON button.
- [object Object] — All SSTableMeta fields in a two-column key-value grid: file_id, level, snapshot_id (provenance), record_count, block_count, size_bytes, min_key, max_key, seq_min, seq_max, created_at, data_file, index_file, filter_file.
- [object Object] — Visual representation: bit_count, hash_count, approx false positive rate, size_bytes. A search box lets the examiner type a key and see whether the bloom filter returns positive or negative for it. Shows "POSITIVE (may exist)" or "NEGATIVE (definitely absent)" with colour coding. Demonstrates false positive probability interactively.
- [object Object] — Table of index entries: first_key → block_offset. Shows how many index entries exist vs total blocks. A key search shows which block offset the bisect would land on. Demonstrates the O(log n) block selection.

```
Refresh: Polls GET /disk every 2 seconds. New SSTables appear with a slide-in animation on the left panel.
```

### 4.6  Page 5 — Compaction

**Purpose:** Demonstrate the compaction pipeline — the most complex component.

```
Top section — status: Three status chips: L0 count (N / threshold, colour: green < 7, amber 7–9, red ≥ 10), Active jobs (0 or "L0→L1 running"), Reserved levels (e.g. {0, 1}). A Force Compaction Check button calls POST /compaction/trigger.
```

**Middle section — level diagram:** Animated diagram showing the merge in progress when a compaction is active. L0 files (rectangles) animated with a flowing-into-L1 animation. An arrow labeled with the task_id and input_count flows downward. L1 rectangle pulses amber while merge is running, turns solid green when committed.

**Right column — active job card:** When a job is running: task_id, src→dst, input file count (including old L1), start time, elapsed. Subprocess running indicator (spinner). Goes blank when no job is active.

```
Bottom section — compaction history: Table from GET /compaction/history. Columns: timestamp, event (started/committed, colour coded), task_id (truncated), L0→L1, input_count, output_records, duration (derived from start/committed pair). Shows how merged record counts compare to input record counts (deduplication ratio).
```

```
Auto-refresh: Polls GET /compaction/status every 500ms when a job is active, every 2s otherwise.
```

### 4.7  Page 6 — Lookup Trace

**Purpose:** The single most powerful demonstration tool. Shows the examiner exactly how a point lookup traverses every component.

**Input:** One key input field, prominent submit button labeled "Trace Lookup". Also a checkbox: "Show all levels even after find" (default off — stops at first match).

**Output — step-by-step trace:** Each step rendered as a horizontal card in a vertical sequence, connected by arrows. Card content varies by component:

| Component | Card shows |
| --- | --- |
| active_memtable | MISS badge (grey) or HIT badge (green) + seq + value |
| immutable_N | MISS or HIT, which snapshot depth it is |
| l0:<file_id> | Bloom result: NEGATIVE (red, skipped) or POSITIVE (amber, scanned). If scanned: bisect offset, block cache HIT/MISS, final result. |
| l1:<file_id> | Same as L0 but labeled L1 |

The trace renders progressively as the API streams the response — each step appears with a brief delay so the examiner can watch the lookup walk down the levels. At the bottom: final answer — value, source component, total latency in microseconds.

Edge cases shown: Key not found (all steps MISS, final result NIL). Tombstone found (shows DELETED in red with the seq). Bloom filter false positive (POSITIVE at bloom check, then MISS at actual scan — explicitly labeled "false positive").

### 4.8  Page 7 — Live Logs

**Purpose:** Real-time structured log stream — shows the engine talking to itself.

**Top controls:** Log level filter buttons: ALL | DEBUG | INFO | WARNING | ERROR. Text search filter input (client-side filter on the live buffer). Pause/Resume toggle. Clear buffer button.

**Log stream:** Scrolling terminal-style panel, dark background, monospace font. Each line:

- Timestamp (dim, small)
- Level badge: DEBUG (grey) | INFO (blue) | WARNING (amber) | ERROR (red) | CRITICAL (red bold)
- Logger name (dim, truncated: 
- Event string (bright white)
- Extra fields (dim, key=value pairs)

Interesting log lines highlighted specially:

| Log pattern | Highlight |
| --- | --- |
| `SSTable committed` | Green left border — flush completed |
| `Compaction started` | Amber left border + indent showing input_count and seq_cutoff |
| `Compaction committed` | Green left border + output_records |
| `Backpressure active` | Red background — write stall happening now |
| `Recovery` | Blue left border — startup sequence |
| `Flush slot error` | Red background — pipeline failure |

**Connection status:** Top-right of the panel — green dot + "streaming" or red dot + "reconnecting". Auto-reconnects on disconnect with exponential backoff.

### 4.9  Page 8 — Config

**Purpose:** Show and modify the runtime-mutable config — demonstrates disk-persisted configuration.

```
Layout: Two-column form. Left: key name + description. Right: current value with inline edit. Submit changes via PATCH /config.
```

| Config key | Type | UI control | Notes shown to examiner |
| --- | --- | --- | --- |
| `env` | string | Toggle: dev / prod | Dev: entry-count based freeze. Prod: size-based freeze. |
| `max_memtable_entries` | int | Number input + slider (2–100) | Only applies in dev mode |
| `max_memtable_size_mb` | int | Number input (1–512) | Only applies in prod mode |
| `immutable_queue_max_len` | int | Number input + slider (1–8) | Hard stall when full (backpressure) |
| `l0_compaction_threshold` | int | Number input + slider (2–20) | L0 file count that triggers compaction |
| `flush_max_workers` | int | Number input (1–4) | Parallel SSTable write concurrency |
| `block_size` | int | Number input (512–65536) | SSTable data block size in bytes |

When a config value is changed, the value in the sidebar stats strip updates immediately. The config page shows a banner: "Config saved to config.json — takes effect immediately" (no restart required).

## PART 5 — State Management & Polling

| Data | Source | Polling interval | Invalidation trigger |
| --- | --- | --- | --- |
| Engine stats (seq, key_count, l0_count) | `GET /stats` | 1s | Any write op, flush, compaction commit |
| MemTable state | `GET /mem` | 1s | Any write op, flush |
| SSTable list | `GET /disk` | 2s | Flush commit, compaction commit |
| Compaction status | `GET /compaction/status` | 500ms active / 2s idle | Automatic |
| Config | `GET /config` | On mount only | `PATCH /config`  response |
| Stats history (charts) | `GET /stats/history` | 10s | Never invalidated — append-only |
| Live logs | WebSocket | Push — no polling | Reconnect on disconnect |
| WAL info | `GET /wal` | 5s | Write ops |

## PART 6 — Backend Implementation Details

### 6.1  FastAPI server bootstrap

```

# server.py
import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.engine import LSMEngine
 
app = FastAPI(title='lsm-kv Explorer')
app.add_middleware(CORSMiddleware, allow_origins=['*'], ...)
 
engine: LSMEngine | None = None
 
@app.on_event('startup')
async def startup():
    global engine
    engine = await LSMEngine.open()
 
@app.on_event('shutdown')
async def shutdown():
    if engine: await engine.close()

```

### 6.2  WebSocket log bridge

```

import asyncio, socket
from fastapi import WebSocket
 
@app.websocket('/ws/logs')
async def log_websocket(ws: WebSocket):
    await ws.accept()
    # Connect to the LSM engine's TCP log broadcast server on 9009
    reader, writer = await asyncio.open_connection('127.0.0.1', 9009)
    filter_text = None
    try:
        async def read_logs():
            async for line in reader:
                decoded = line.decode().strip()
                if filter_text and filter_text not in decoded: continue
                await ws.send_text(decoded)
        async def read_client():
            nonlocal filter_text
            async for msg in ws.iter_json():
                filter_text = msg.get('filter')
        await asyncio.gather(read_logs(), read_client())
    finally:
        writer.close()

```

### 6.3  Lookup trace implementation

```
The trace endpoint calls each engine component in the same order as LSMEngine.get() but returns intermediate results rather than short-circuiting. It tracks Bloom filter decisions and block cache hits by temporarily wrapping the reader:

@router.get('/kv/trace')
async def trace_lookup(key: str):
    raw_key = key.encode()
    steps = []
    # 1. Active memtable
    result = engine._mem._active.get(raw_key)
    steps.append({'step':1,'component':'active_memtable',
                  'result':'hit' if result else 'miss', ...})
    # 2. Immutable snapshots
    for i, snap in enumerate(engine._mem._immutable_q):
        result = snap.get(raw_key)
        steps.append({'step':2+i,'component':f'immutable_{i}', ...})
    # 3. L0 SSTables — with bloom + bisect detail
    for fid in engine._sst._l0_order:
        with engine._sst._registry.open_reader(fid) as reader:
            bloom_hit = reader._bloom.may_contain(raw_key)
            if not bloom_hit:
                steps.append({'component':f'l0:{fid}',
                              'bloom_check':'negative','result':'bloom_skip'})
                continue
            offset = reader._index.floor_offset(raw_key)
            cache_hit = reader._cache.get(fid, offset) is not None
            val = reader.get(raw_key)
            steps.append({'bloom_check':'positive',
                          'bisect_offset':offset,'block_cache_hit':cache_hit,
                          'result':'found' if val else 'miss', ...})
    return {'key': key, 'found': ..., 'value': ..., 'steps': steps}

```

### 6.4  Stats history accumulator

The server maintains an in-memory ring buffer of stats samples, collected every second by a background task:

```

from collections import deque
import time
 
stats_history: deque = deque(maxlen=300)  # 5 minutes at 1s interval
 
@app.on_event('startup')
async def start_stats_collector():
    asyncio.create_task(_collect_stats())
 
async def _collect_stats():
    while True:
        if engine:
            s = engine.stats()
            stats_history.append({
                'ts': time.time(),
                'seq': s.seq,
                'l0_count': s.l0_sstable_count,
                'mem_bytes': s.active_size_bytes,
                'key_count': s.key_count,
            })
        await asyncio.sleep(1)

```

## PART 7 — Examiner Demo Script

This is a suggested walkthrough for the examiner. Each step demonstrates a specific architectural component.

| Step | Action | What to point out |
| --- | --- | --- |
| 1 | Open Dashboard. Explain the level diagram — all empty. | LSM structure: MemTable above disk, L0 files appear on flush, L1 is the compacted level. |
| 2 | Go to Write/Read. PUT 5 keys. Watch Dashboard seq counter increment. | Write path: WAL append (durability first), MemTable put (visibility second), all under write_lock. |
| 3 | GET one of the keys. Show source = active_memtable. | Read path: checks MemTable first before touching disk. |
| 4 | Go to MemTable page. Show the 5 entries in the active table. | SkipList: entries stored in sorted order by key. Show seq numbers increasing. |
| 5 | Click Force Flush. Watch a new SSTable appear on SSTables page. | Flush pipeline: freeze → parallel write → event-chain commit → manifest update. |
| 6 | Go to SSTables. Click the new file. Show Bloom Filter tab. | Bloom filter: search for a key that exists (positive) and one that doesn't (negative). Show FPR. |
| 7 | Show Sparse Index tab. Explain O(log n) bisect. | Block-based layout: each index entry is the first key of one block. Bisect finds the right block. |
| 8 | Go to Lookup Trace. Trace a key that is now in L0. | Full read path trace: memtable miss → bloom positive → bisect → block cache miss → found. |
| 9 | PUT enough keys to fill 10 SSTables (use batch). Watch compaction fire. | CompactionManager: flush-triggered (not daemon). Level reservation. Subprocess isolation. |
| 10 | Go to Compaction page during the merge. Show the animated level diagram. | L0 + old L1 → KWayMergeIterator → one new L1 file. Show input_count includes old L1. |
| 11 | After compaction: Trace the same key again. | Now answers from L1. Show bloom filter on L1 file. |
| 12 | Go to Live Logs. Show the compaction started / committed events. | Structured logging, TCP broadcast server, WebSocket bridge. |
| 13 | Go to Config. Change max_memtable_entries to 3. | Runtime-mutable config: takes effect immediately, persisted to config.json. |
| 14 | Write 3 more keys. Watch memtable freeze immediately. | Config change visible in real-time: threshold now 3, freeze triggers sooner. |
| 15 | Stop and restart the server. Show Dashboard recovers state. | Recovery: SSTable scan (max seq), WAL replay (unflushed entries), manifest reconciliation. |

## PART 8 — Project Structure

```

lsm-kv/
├── app/                   ← existing engine code (unchanged)
│   ├── engine/
│   ├── sstable/
│   ├── memtable/
│   └── ...
├── web/                   ← NEW: FastAPI server
│   ├── server.py          ← FastAPI app + engine lifecycle
│   ├── routers/
│   │   ├── kv.py          ← /kv/* endpoints
│   │   ├── mem.py         ← /mem/* endpoints
│   │   ├── disk.py        ← /disk/* endpoints
│   │   ├── compaction.py  ← /compaction/* endpoints
│   │   ├── stats.py       ← /stats/* endpoints
│   │   └── config.py      ← /config/* endpoints
│   └── ws/
│       └── logs.py        ← WebSocket log bridge
├── frontend/              ← NEW: React SPA
│   ├── src/
│   │   ├── pages/
│   │   │   ├── Dashboard.tsx
│   │   │   ├── WriteRead.tsx
│   │   │   ├── MemTable.tsx
│   │   │   ├── SSTables.tsx
│   │   │   ├── Compaction.tsx
│   │   │   ├── LookupTrace.tsx
│   │   │   ├── LiveLogs.tsx
│   │   │   └── Config.tsx
│   │   ├── components/    ← shared: StatCard, LevelDiagram, LogLine ...
│   │   ├── hooks/         ← useEngine, useCompaction, useLogs ...
│   │   └── api/           ← typed fetch wrappers for every endpoint
│   ├── index.html
│   ├── vite.config.ts     ← proxy /api and /ws to localhost:8000
│   └── package.json
└── pyproject.toml         ← add fastapi, uvicorn, websockets to deps

```

### 8.1  Running the full stack

```

# Terminal 1: start the API server (also starts LSMEngine)
uv run uvicorn web.server:app --reload --port 8000
 
# Terminal 2: start the React dev server
cd frontend && npm run dev    # serves on :5173, proxies :8000
 
# Open http://localhost:5173

```

## PART 9 — Implementation Order

| Phase | Deliverable | Enables |
| --- | --- | --- |
| 1 | `web/server.py`  — FastAPI shell, CORS, engine open/close,  `GET /stats` ,  `GET /engine/status` | Any frontend can verify the server is running |
| 2 | All remaining REST endpoints (kv, mem, disk, compaction, config, wal) | All data visible via curl / Postman before any UI |
| 3 | `web/ws/logs.py`  — WebSocket bridge to TCP log server | Live logs endpoint ready |
| 4 | `GET /kv/trace`  and stats history accumulator | Most complex endpoints done |
| 5 | Frontend shell: sidebar nav, status strip, React Query setup, theme/fonts | App structure before any real pages |
| 6 | Dashboard page + Write/Read page | Core demo pages first |
| 7 | MemTable + SSTables pages | Inspection pages |
| 8 | Compaction page + Lookup Trace page | Showcase pages |
| 9 | Live Logs + Config pages | Utility pages |
| 10 | Polish: animations, examiner demo script, README update | Presentation ready |