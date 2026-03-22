"""WebSocket log bridge — connects browser to TCP log server on port 9009."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

import web.server as srv

router = APIRouter()


@router.websocket("/ws/logs")
async def log_websocket(ws: WebSocket) -> None:
    await ws.accept()

    engine = srv._engine
    if engine is None or engine.log_port == 0:
        await ws.send_json({"error": "Log server not available"})
        await ws.close()
        return

    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", engine.log_port)
    except (ConnectionRefusedError, OSError):
        await ws.send_json({"error": "Cannot connect to log server"})
        await ws.close()
        return

    filter_text: str | None = None

    async def read_logs() -> None:
        nonlocal filter_text
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                decoded = line.decode(errors="replace").strip()
                if not decoded:
                    continue
                if filter_text and filter_text.lower() not in decoded.lower():
                    continue
                await ws.send_text(decoded)
        except (ConnectionError, asyncio.CancelledError):
            pass

    async def read_client() -> None:
        nonlocal filter_text
        try:
            while True:
                msg = await ws.receive_json()
                filter_text = msg.get("filter")
        except (WebSocketDisconnect, RuntimeError):
            pass

    try:
        await asyncio.gather(read_logs(), read_client())
    finally:
        writer.close()
