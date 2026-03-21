"""SSTableRegistry — ref-counted registry of open SSTable readers.

Ensures readers are not closed while in use and that marked-for-deletion
readers are cleaned up once their refcount drops to zero.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager

from app.observability import get_logger
from app.sstable.reader import SSTableReader
from app.types import FileID

logger = get_logger(__name__)


class SSTableRegistry:
    """Thread-safe registry of open SSTable readers with ref counting."""

    def __init__(self) -> None:
        self._readers: dict[FileID, SSTableReader] = {}
        self._refcounts: dict[FileID, int] = {}
        self._marked: set[FileID] = set()
        self._lock = threading.Lock()

    def register(self, file_id: FileID, reader: SSTableReader) -> None:
        """Register an open reader."""
        with self._lock:
            self._readers[file_id] = reader
            self._refcounts[file_id] = 0
        logger.debug("Reader registered", file_id=file_id)

    @contextmanager
    def open_reader(self, file_id: FileID) -> Iterator[SSTableReader]:
        """Acquire a ref-counted handle to a reader."""
        with self._lock:
            reader = self._readers.get(file_id)
            if reader is None:
                raise KeyError(f"No reader for file_id={file_id}")
            self._refcounts[file_id] += 1

        try:
            yield reader
        finally:
            with self._lock:
                self._refcounts[file_id] -= 1
                if file_id in self._marked and self._refcounts[file_id] == 0:
                    self._cleanup(file_id)

    def mark_for_deletion(self, file_id: FileID) -> None:
        """Mark a reader for deletion. Cleaned up when refcount hits 0."""
        with self._lock:
            self._marked.add(file_id)
            if self._refcounts.get(file_id, 0) == 0:
                self._cleanup(file_id)

    def _cleanup(self, file_id: FileID) -> None:
        """Close and remove a reader (caller holds lock)."""
        reader = self._readers.pop(file_id, None)
        self._refcounts.pop(file_id, None)
        self._marked.discard(file_id)
        if reader is not None:
            reader.close()
            logger.info("Reader cleaned up", file_id=file_id)

    def close_all(self) -> None:
        """Close all readers immediately."""
        with self._lock:
            for file_id in list(self._readers):
                self._cleanup(file_id)
