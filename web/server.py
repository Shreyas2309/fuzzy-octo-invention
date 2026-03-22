"""FastAPI server — wraps the LSMEngine for the web dashboard."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.engine import LSMEngine
from web.routers import kv, mem, disk, compaction, stats, config_routes, engine, terminal
from web.ws import logs

# ---------------------------------------------------------------------------
# Global engine + stats history
# ---------------------------------------------------------------------------

_engine: LSMEngine | None = None
_engine_opened_at: float = 0.0
stats_history: deque[dict[str, object]] = deque(maxlen=300)

# Write-amplification tracking
wa_user_bytes: int = 0  # cumulative user-written bytes (key+value per put/del)


def get_engine() -> LSMEngine:
    """Return the live engine instance. Raises if not open."""
    if _engine is None:
        raise RuntimeError("Engine is not open")
    return _engine


async def _collect_stats() -> None:
    """Background task: sample engine stats every 1s into ring buffer."""
    while True:
        if _engine is not None:
            try:
                s = _engine.stats()
                stats_history.append({
                    "ts": time.time(),
                    "seq": s.seq,
                    "l0_count": s.l0_sstable_count,
                    "mem_bytes": s.active_size_bytes,
                    "key_count": s.key_count,
                })
            except Exception:
                pass
        await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global _engine, _engine_opened_at
    _engine = await LSMEngine.open()
    _engine_opened_at = time.time()
    collector = asyncio.create_task(_collect_stats())
    yield
    collector.cancel()
    if _engine is not None:
        await _engine.close()
        _engine = None


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="lsm-kv Explorer", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routers
app.include_router(engine.router, prefix="/api/v1/engine", tags=["engine"])
app.include_router(kv.router, prefix="/api/v1/kv", tags=["kv"])
app.include_router(mem.router, prefix="/api/v1/mem", tags=["mem"])
app.include_router(disk.router, prefix="/api/v1/disk", tags=["disk"])
app.include_router(compaction.router, prefix="/api/v1/compaction", tags=["compaction"])
app.include_router(stats.router, prefix="/api/v1", tags=["stats"])
app.include_router(config_routes.router, prefix="/api/v1/config", tags=["config"])
app.include_router(terminal.router, prefix="/api/v1/terminal", tags=["terminal"])

# WebSocket
app.include_router(logs.router)
