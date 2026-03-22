"""Engine lifecycle endpoints."""

from __future__ import annotations

import time

from fastapi import APIRouter

import web.server as srv

router = APIRouter()


@router.get("/status")
async def engine_status() -> dict[str, object]:
    if srv._engine is None:
        return {"open": False, "data_root": None, "log_port": 0, "uptime_s": 0}
    e = srv.get_engine()
    uptime = round(time.time() - srv._engine_opened_at, 1)
    return {
        "open": True,
        "data_root": str(e.data_root),
        "log_port": e.log_port,
        "uptime_s": uptime,
    }


@router.post("/open")
async def engine_open() -> dict[str, object]:
    if srv._engine is not None:
        return {"ok": True, "data_root": str(srv._engine.data_root)}
    from app.engine import LSMEngine
    srv._engine = await LSMEngine.open()
    srv._engine_opened_at = time.time()
    return {"ok": True, "data_root": str(srv._engine.data_root)}


@router.post("/close")
async def engine_close() -> dict[str, object]:
    if srv._engine is None:
        return {"ok": True}
    await srv._engine.close()
    srv._engine = None
    return {"ok": True}
