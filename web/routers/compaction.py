"""Compaction endpoints."""

from __future__ import annotations

import json

from fastapi import APIRouter

import web.server as srv

router = APIRouter()


@router.get("/status")
async def compaction_status() -> dict[str, object]:
    e = srv.get_engine()
    cm = e._compaction
    if cm is None:
        return {"active_jobs": [], "active_levels": [], "last_completed": None}

    active_jobs = []
    for (src, dst), task in cm.active_jobs.items():
        active_jobs.append({
            "src": src,
            "dst": dst,
            "task_id": task.get_name() if hasattr(task, "get_name") else str(id(task)),
            "started_at": None,
        })

    return {
        "active_jobs": active_jobs,
        "active_levels": sorted(cm._active_levels),
        "last_completed": None,
    }


@router.post("/trigger")
async def compaction_trigger() -> dict[str, object]:
    e = srv.get_engine()
    cm = e._compaction
    if cm is None:
        return {"ok": False, "triggered": False, "reason": "No compaction manager"}

    threshold = int(e.config.l0_compaction_threshold)
    l0_count = e._sst.l0_count
    await cm.check_and_compact()
    return {
        "ok": True,
        "triggered": l0_count >= threshold,
        "reason": f"L0 count: {l0_count}/{threshold}",
    }


@router.get("/history")
async def compaction_history() -> dict[str, object]:
    e = srv.get_engine()
    log_path = e.data_root / "compaction.log"
    events: list[dict[str, object]] = []
    if log_path.exists():
        try:
            for line in log_path.read_text(encoding="utf-8").strip().splitlines():
                if line.strip():
                    events.append(json.loads(line))
        except (json.JSONDecodeError, OSError):
            pass
    return {"events": events}
