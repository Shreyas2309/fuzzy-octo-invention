"""TCP log broadcast server.

Runs in a daemon thread. Accepts TCP connections and broadcasts every
log line to all connected clients.  Clients that disconnect are silently
removed.

Usage (started automatically by ``configure_logging``):
    server = LogBroadcastServer(port=9009)
    server.start()       # background daemon thread
    server.broadcast(b"2026-03-20 ... WAL append ...\\n")
    server.stop()
"""

from __future__ import annotations

import contextlib
import os
import select
import socket
import threading

import structlog

_logger = structlog.get_logger(__name__)

DEFAULT_LOG_PORT: int = int(os.getenv("LSM_LOG_PORT", "9009"))


class LogBroadcastServer:
    """TCP server that broadcasts log lines to all connected clients."""

    def __init__(self, host: str = "127.0.0.1", port: int = DEFAULT_LOG_PORT) -> None:
        self._host = host
        self._port = port
        self._clients: list[socket.socket] = []
        self._clients_lock = threading.Lock()
        self._server_sock: socket.socket | None = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        """Return the port the server is listening on."""
        return self._port

    def start(self) -> None:
        """Start the broadcast server in a daemon thread."""
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self._host, self._port))
        self._server_sock.listen(8)
        self._server_sock.setblocking(False)

        self._thread = threading.Thread(
            target=self._accept_loop, daemon=True, name="log-broadcast",
        )
        self._thread.start()
        _logger.debug(
            "Log broadcast server started", host=self._host, port=self._port,
        )

    def _accept_loop(self) -> None:
        """Accept new client connections until stopped."""
        assert self._server_sock is not None
        while not self._stop_event.is_set():
            try:
                readable, _, _ = select.select(
                    [self._server_sock], [], [], 0.5,
                )
            except (OSError, ValueError):
                # Socket closed by stop() while we were in select()
                break
            if not readable:
                continue
            try:
                client, addr = self._server_sock.accept()
                client.setblocking(False)
                with self._clients_lock:
                    self._clients.append(client)
                _logger.debug("Log client connected", addr=addr)
            except OSError:
                break

    def broadcast(self, data: bytes) -> None:
        """Send *data* to all connected clients. Drop disconnected ones."""
        with self._clients_lock:
            alive: list[socket.socket] = []
            for client in self._clients:
                try:
                    client.sendall(data)
                    alive.append(client)
                except OSError:
                    with contextlib.suppress(OSError):
                        client.close()
            self._clients = alive

    def stop(self) -> None:
        """Shut down the server and disconnect all clients."""
        _logger.debug("Log broadcast server stopping", port=self._port)
        self._stop_event.set()
        if self._server_sock:
            self._server_sock.close()
        with self._clients_lock:
            client_count = len(self._clients)
            for client in self._clients:
                with contextlib.suppress(OSError):
                    client.close()
            self._clients.clear()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
            if self._thread.is_alive():
                _logger.warning(
                    "Log broadcast thread did not stop in time",
                    port=self._port,
                )
        _logger.info(
            "Log broadcast server stopped",
            port=self._port,
            clients_disconnected=client_count,
        )
