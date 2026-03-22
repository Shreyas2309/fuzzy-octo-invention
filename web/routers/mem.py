"""MemTable inspection endpoints."""

from __future__ import annotations

from fastapi import APIRouter

import web.server as srv

router = APIRouter()


@router.get("")
async def mem_list() -> dict[str, object]:
    e = srv.get_engine()
    return e.show_mem()


@router.post("/flush")
async def mem_flush() -> dict[str, object]:
    e = srv.get_engine()
    flushed = await e.flush()
    result: dict[str, object] = {"ok": True, "flushed": flushed}
    if flushed:
        meta = e._mem.active_metadata
        result["snapshot_id"] = meta.table_id
    return result


@router.get("/{table_id}")
async def mem_detail(table_id: str) -> dict[str, object]:
    e = srv.get_engine()
    return e.show_mem(table_id)
