"""Config endpoints."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

import web.server as srv

router = APIRouter()


class ConfigPatch(BaseModel):
    key: str
    value: int | float | str


@router.get("")
async def config_get() -> dict[str, object]:
    e = srv.get_engine()
    return e.config.to_dict()


@router.patch("")
async def config_patch(req: ConfigPatch) -> dict[str, object]:
    e = srv.get_engine()
    old, new = e.update_config(req.key, req.value)
    return {"ok": True, "key": req.key, "old_value": old, "new_value": new}


@router.get("/schema")
async def config_schema() -> dict[str, object]:
    e = srv.get_engine()
    current = e.config.to_dict()
    schema: list[dict[str, object]] = []
    for key, value in current.items():
        schema.append({
            "key": key,
            "type": type(value).__name__,
            "value": value,
        })
    return {"fields": schema}
