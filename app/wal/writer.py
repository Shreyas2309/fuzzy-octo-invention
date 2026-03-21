"""WAL writer — append-only, msgpack-framed write-ahead log.

Exports:
    WALEntry  — frozen dataclass representing a single log entry.
    WALWriter — synchronous WAL file writer with append, replay, and truncate.

All methods are synchronous. Async wrapping and lock management are
handled by WALManager in the engine layer.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

import msgpack  # pyright: ignore[reportMissingTypeStubs]
import structlog

from app.common.errors import WALCorruptError, WALTruncateError
from app.types import Key, OpType, SeqNum, Value

logger = structlog.get_logger(__name__)


def _encode_entry(entry: WALEntry) -> bytes:
    """Encode a WALEntry as msgpack bytes."""
    result: bytes = msgpack.packb(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportAssignmentType]
        (entry.seq, entry.timestamp_ms, int(entry.op), entry.key, entry.value),
        use_bin_type=True,
    )
    return result


# ---------------------------------------------------------------------------
# WALEntry — a single write-ahead log record
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class WALEntry:
    """A single write-ahead log entry.

    Fields are ordered to match the msgpack encoding:
    ``packb((seq, timestamp_ms, op, key, value), use_bin_type=True)``
    """

    seq: SeqNum
    timestamp_ms: int
    op: OpType
    key: Key
    value: Value

    @property
    def is_tombstone(self) -> bool:
        """Return True if this entry represents a deletion."""
        return self.op == OpType.DELETE


# ---------------------------------------------------------------------------
# WALWriter — synchronous append-only WAL file writer
# ---------------------------------------------------------------------------


class WALWriter:
    """Append-only WAL file writer with msgpack-framed entries.

    Owns the file handle for a single ``wal.log`` file.  All methods are
    synchronous — async wrapping is done by :class:`WALManager`.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._fd = open(path, "ab")  # noqa: SIM115
        except OSError as exc:
            logger.error("WAL file open failed", path=str(path), error=str(exc))
            raise
        logger.debug("WALWriter opened", path=str(path))

    @property
    def path(self) -> Path:
        """Return the WAL file path."""
        return self._path

    # ── write path ────────────────────────────────────────────────────────

    def append(self, entry: WALEntry) -> None:
        """Encode *entry* as msgpack, write to WAL, and fsync."""
        try:
            self._fd.write(_encode_entry(entry))
            self._fd.flush()
            os.fsync(self._fd.fileno())
        except OSError as exc:
            logger.error(
                "WAL append failed", seq=entry.seq, error=str(exc),
            )
            raise
        logger.debug(
            "WAL append",
            seq=entry.seq,
            op=entry.op.name,
            key_len=len(entry.key),
        )

    # ── replay path ───────────────────────────────────────────────────────

    def replay(self) -> list[WALEntry]:
        """Read the WAL file and return all entries sorted by seq.

        Returns an empty list when the file does not exist or is empty.
        Raises :class:`WALCorruptError` if decoding fails.
        """
        logger.debug("WAL replay start", path=str(self._path))

        if not self._path.exists() or self._path.stat().st_size == 0:
            logger.debug("WAL replay skipped (empty or missing)")
            return []

        entries: list[WALEntry] = []
        try:
            with open(self._path, "rb") as fd:
                unpacker = msgpack.Unpacker(fd, raw=True)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
                for item in unpacker:  # pyright: ignore[reportUnknownVariableType]
                    raw_seq, raw_ts, raw_op, raw_key, raw_value = item  # pyright: ignore[reportUnknownVariableType]
                    entries.append(
                        WALEntry(
                            seq=int(raw_seq),  # pyright: ignore[reportUnknownArgumentType]
                            timestamp_ms=int(raw_ts),  # pyright: ignore[reportUnknownArgumentType]
                            op=OpType(int(raw_op)),  # pyright: ignore[reportUnknownArgumentType]
                            key=bytes(raw_key),  # pyright: ignore[reportUnknownArgumentType]
                            value=bytes(raw_value),  # pyright: ignore[reportUnknownArgumentType]
                        )
                    )
        except (msgpack.UnpackException, ValueError, TypeError) as exc:
            raise WALCorruptError(
                f"Failed to decode WAL at {self._path}: {exc}"
            ) from exc

        entries.sort(key=lambda e: e.seq)
        logger.info("WAL replay complete", entry_count=len(entries))
        return entries

    # ── truncation ────────────────────────────────────────────────────────

    def truncate_before(self, seq: SeqNum) -> None:
        """Remove all entries with ``entry.seq <= seq``.

        Rewrites the WAL to a temp file, then atomically replaces the
        original via :func:`os.replace`.
        """
        logger.info("WAL truncate start", cutoff_seq=seq)
        try:
            all_entries = self.replay()
            kept = [e for e in all_entries if e.seq > seq]

            # Write kept entries to a temp file in the same directory
            tmp_fd = tempfile.NamedTemporaryFile(  # noqa: SIM115
                dir=self._path.parent, delete=False, mode="wb"
            )
            tmp_path = Path(tmp_fd.name)
            try:
                for entry in kept:
                    tmp_fd.write(_encode_entry(entry))
                os.fsync(tmp_fd.fileno())
                tmp_fd.close()

                # Atomic swap
                os.replace(tmp_path, self._path)
            except BaseException:
                tmp_fd.close()
                tmp_path.unlink(missing_ok=True)
                raise

            # Reopen the fd on the new file
            self._fd.close()
            self._fd = open(self._path, "ab")  # noqa: SIM115
        except OSError as exc:
            raise WALTruncateError(
                f"WAL truncation failed at {self._path}: {exc}"
            ) from exc

        logger.info(
            "WAL truncated", cutoff_seq=seq, remaining=len(kept)
        )

    # ── lifecycle ─────────────────────────────────────────────────────────

    def close(self) -> None:
        """Fsync and close the WAL file handle.

        Cleanup never raises — errors are logged as warnings.
        """
        logger.debug("WAL close start", path=str(self._path))
        try:
            os.fsync(self._fd.fileno())
            self._fd.close()
            logger.debug("WAL close done", path=str(self._path))
        except Exception as exc:
            logger.warning(
                "WAL close error (non-fatal)",
                path=str(self._path),
                error=str(exc),
            )
