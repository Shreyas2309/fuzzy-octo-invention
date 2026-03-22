"""Centralized logging configuration for lsm-kv.

Configures structlog with two output targets:
    1. **File** — human-readable lines appended to ``<data_root>/logs/lsm-kv.log``
    2. **TCP broadcast** — streams log lines to connected clients on a port

Call :func:`configure_logging` once at startup (before any log calls).
All application code uses :func:`get_logger` from this module.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import structlog

from app.observability.log_server import LogBroadcastServer

# ---------------------------------------------------------------------------
# Module state — set by configure_logging()
# ---------------------------------------------------------------------------

_log_server: LogBroadcastServer | None = None
_configured: bool = False

LOG_LEVEL: str = os.getenv("LSM_LOG_LEVEL", "DEBUG").upper()

# ── ANSI colour codes ────────────────────────────────────────────────────

_RESET = "\033[0m"
_DIM = "\033[2m"
_BOLD = "\033[1m"
_CYAN = "\033[36m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_RED = "\033[31m"
_MAGENTA = "\033[35m"
_WHITE = "\033[37m"

# ── Level symbols for compact, scannable output ──────────────────────────

_LEVEL_SYMBOLS: dict[str, str] = {
    "DEBUG": "·",
    "INFO": "→",
    "WARNING": "⚠",
    "ERROR": "✗",
    "CRITICAL": "✗✗",
}

_LEVEL_COLORS: dict[str, str] = {
    "DEBUG": _DIM,
    "INFO": _GREEN,
    "WARNING": _YELLOW,
    "ERROR": _RED,
    "CRITICAL": _RED + _BOLD,
}

# ── Short module name mapping ────────────────────────────────────────────

_MODULE_SHORT: dict[str, str] = {
    "app.engine.lsm_engine": "engine",
    "app.engine.wal_manager": "wal-mgr",
    "app.engine.config": "config",
    "app.engine.memtable_manager": "mem-mgr",
    "app.engine.seq_generator": "seq-gen",
    "app.engine.sstable_manager": "sst-mgr",
    "app.engine.flush_pipeline": "flush",
    "app.wal.writer": "wal",
    "app.memtable.skiplist": "skip",
    "app.memtable.active": "memtbl",
    "app.memtable.immutable": "immtbl",
    "app.observability.log_server": "log-srv",
    "app.observability.logging": "log-cfg",
    "app.common.crc": "crc",
    "app.common.encoding": "encode",
    "app.common.uuid7": "uuid7",
    "app.bloom.filter": "bloom",
    "app.index.sparse": "index",
    "app.cache.block": "cache",
    "app.sstable.meta": "sst-meta",
    "app.sstable.writer": "sst-wr",
    "app.sstable.reader": "sst-rd",
    "app.sstable.registry": "sst-reg",
    "app.common.rwlock": "rwlock",
    "app.common.merge_iterator": "merge",
    "app.compaction.worker": "compact-w",
    "app.engine.compaction_manager": "compact",
}


def _short_module(name: str) -> str:
    """Return a short display name for a module path."""
    return _MODULE_SHORT.get(name, name.rsplit(".", maxsplit=1)[-1])


# ---------------------------------------------------------------------------
# Custom formatter — produces clean, aligned, readable lines
# ---------------------------------------------------------------------------


class _PrettyFormatter(logging.Formatter):
    """ANSI-coloured formatter for TCP log stream.

    Renders in a terminal as::

        14:30:01 → [engine  ]  Startup complete  keys=3
        (dimmed) (green) (cyan)   (level-coloured)
    """

    def format(self, record: logging.LogRecord) -> str:
        ts = self.formatTime(record, self.datefmt)
        symbol = _LEVEL_SYMBOLS.get(record.levelname, "?")
        level_color = _LEVEL_COLORS.get(record.levelname, "")
        module = _short_module(record.name)
        msg = record.getMessage()

        return (
            f"{_DIM}{ts}{_RESET} "
            f"{level_color}{symbol}{_RESET} "
            f"{_CYAN}[{module:<8s}]{_RESET}  "
            f"{level_color}{msg}{_RESET}"
        )


class _FileFormatter(logging.Formatter):
    """Longer format for log files with date, level name, and full module.

    2026-03-20 14:30:01.123  INFO   [engine]   Startup complete   keys=3 seq=5
    """

    def format(self, record: logging.LogRecord) -> str:
        ts = self.formatTime(record, self.datefmt)
        level = record.levelname
        module = _short_module(record.name)
        msg = record.getMessage()

        return f"{ts}  {level:<8s} [{module:<8s}]  {msg}"


# ---------------------------------------------------------------------------
# Custom handler that broadcasts to TCP clients
# ---------------------------------------------------------------------------


class _BroadcastHandler(logging.Handler):
    """Logging handler that forwards formatted records to the TCP server."""

    def __init__(self, server: LogBroadcastServer) -> None:
        super().__init__()
        self._server = server

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self._server.broadcast((msg + "\n").encode())
        except Exception:
            self.handleError(record)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def configure_logging(
    data_root: Path = Path("./data"),
    log_port: int | None = None,
) -> LogBroadcastServer | None:
    """Set up structured logging with file and TCP targets.

    Parameters
    ----------
    data_root:
        Root data directory. Log file is written to ``<data_root>/logs/lsm-kv.log``.
    log_port:
        TCP port for the log broadcast server.  ``None`` reads from
        ``LSM_LOG_PORT`` env var (default 9009).  Pass ``0`` to disable TCP.

    Returns
    -------
    The :class:`LogBroadcastServer` instance (or None if TCP is disabled).
    """
    global _log_server, _configured  # noqa: PLW0603

    if _configured:
        return _log_server

    # ── Resolve port ──────────────────────────────────────────────────────

    if log_port is None:
        log_port = int(os.getenv("LSM_LOG_PORT", "9009"))

    # ── File handler ──────────────────────────────────────────────────────

    log_dir = data_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "lsm-kv.log"

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(LOG_LEVEL)
    file_handler.setFormatter(_FileFormatter(datefmt="%Y-%m-%d %H:%M:%S"))

    # ── TCP broadcast handler ─────────────────────────────────────────────

    handlers: list[logging.Handler] = [file_handler]

    if log_port > 0:
        server = LogBroadcastServer(port=log_port)
        server.start()
        _log_server = server

        broadcast_handler = _BroadcastHandler(server)
        broadcast_handler.setLevel(LOG_LEVEL)
        broadcast_handler.setFormatter(_PrettyFormatter(datefmt="%H:%M:%S"))
        handlers.append(broadcast_handler)

    # ── Wire stdlib logging ───────────────────────────────────────────────

    root = logging.getLogger()
    root.setLevel(LOG_LEVEL)
    # Clear any existing handlers to avoid duplicates on re-configure
    root.handlers.clear()
    for h in handlers:
        root.addHandler(h)

    # ── Configure structlog to route through stdlib ───────────────────────

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    _configured = True
    return _log_server


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a structlog logger bound to *name*.

    All application modules should use::

        from app.observability import get_logger
        logger = get_logger(__name__)
    """
    return structlog.get_logger(name)  # type: ignore[no-any-return]


def get_log_server() -> LogBroadcastServer | None:
    """Return the active TCP broadcast server, or None."""
    return _log_server


def reset_logging() -> None:
    """Reset logging state so :func:`configure_logging` can be called again.

    Intended for tests only — stops the TCP server and clears handlers.
    """
    global _log_server, _configured  # noqa: PLW0603

    if _log_server:
        _log_server.stop()
        _log_server = None

    root = logging.getLogger()
    root.handlers.clear()
    _configured = False
