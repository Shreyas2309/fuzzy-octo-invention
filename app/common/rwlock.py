"""AsyncRWLock — async readers-writer lock with writer preference.

Writer preference: once a writer is waiting, new readers block.
This prevents writer starvation under continuous read load.

Usage::

    lock = AsyncRWLock()

    async with lock.read_lock():
        # multiple readers can be here simultaneously
        ...

    async with lock.write_lock():
        # exclusive access — waits for readers, blocks new ones
        ...
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

from app.observability import get_logger

logger = get_logger(__name__)


class AsyncRWLock:
    """Async readers-writer lock with writer preference."""

    def __init__(self, name: str = "") -> None:
        self._name = name
        self._condition = asyncio.Condition()
        self._readers = 0
        self._writer_active = False
        self._writer_waiting = 0

    @contextlib.asynccontextmanager
    async def read_lock(self) -> AsyncIterator[None]:
        """Acquire for reading. Blocked while any writer is waiting or active."""
        async with self._condition:
            if self._writer_active or self._writer_waiting > 0:
                logger.debug(
                    "Read lock waiting",
                    lock=self._name,
                    writer_active=self._writer_active,
                    writers_waiting=self._writer_waiting,
                )
            await self._condition.wait_for(
                lambda: not self._writer_active and self._writer_waiting == 0
            )
            self._readers += 1
        try:
            yield
        finally:
            async with self._condition:
                self._readers -= 1
                if self._readers == 0:
                    self._condition.notify_all()

    @contextlib.asynccontextmanager
    async def write_lock(self) -> AsyncIterator[None]:
        """Acquire for writing. Exclusive — waits for all readers to finish."""
        async with self._condition:
            self._writer_waiting += 1
            if self._readers > 0 or self._writer_active:
                logger.debug(
                    "Write lock waiting",
                    lock=self._name,
                    active_readers=self._readers,
                    writer_active=self._writer_active,
                )
            await self._condition.wait_for(
                lambda: self._readers == 0 and not self._writer_active
            )
            self._writer_waiting -= 1
            self._writer_active = True
            logger.debug(
                "Write lock acquired",
                lock=self._name,
            )
        try:
            yield
        finally:
            async with self._condition:
                self._writer_active = False
                self._condition.notify_all()
                logger.debug(
                    "Write lock released",
                    lock=self._name,
                )
