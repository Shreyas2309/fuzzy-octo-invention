"""Tests for app.common.rwlock — AsyncRWLock."""

from __future__ import annotations

import asyncio

from app.common.rwlock import AsyncRWLock


class TestAsyncRWLock:
    async def test_multiple_concurrent_readers(self) -> None:
        lock = AsyncRWLock()
        entered: list[int] = []

        async def reader(n: int) -> None:
            async with lock.read_lock():
                entered.append(n)
                await asyncio.sleep(0.05)

        await asyncio.gather(reader(1), reader(2), reader(3))
        assert sorted(entered) == [1, 2, 3]

    async def test_writer_excludes_readers(self) -> None:
        lock = AsyncRWLock()
        log: list[str] = []

        async def writer() -> None:
            async with lock.write_lock():
                log.append("w_start")
                await asyncio.sleep(0.1)
                log.append("w_end")

        async def reader() -> None:
            await asyncio.sleep(0.02)  # let writer start first
            async with lock.read_lock():
                log.append("r")

        await asyncio.gather(writer(), reader())
        # Reader must wait for writer to finish
        assert log == ["w_start", "w_end", "r"]

    async def test_writer_waits_for_readers(self) -> None:
        lock = AsyncRWLock()
        log: list[str] = []

        async def reader() -> None:
            async with lock.read_lock():
                log.append("r_start")
                await asyncio.sleep(0.1)
                log.append("r_end")

        async def writer() -> None:
            await asyncio.sleep(0.02)  # let reader start first
            async with lock.write_lock():
                log.append("w")

        await asyncio.gather(reader(), writer())
        assert log == ["r_start", "r_end", "w"]

    async def test_writer_preference(self) -> None:
        """New readers block while a writer is waiting."""
        lock = AsyncRWLock()
        log: list[str] = []

        async def holder() -> None:
            async with lock.read_lock():
                log.append("h_start")
                await asyncio.sleep(0.15)
                log.append("h_end")

        async def writer() -> None:
            await asyncio.sleep(0.03)
            async with lock.write_lock():
                log.append("w")

        async def late_reader() -> None:
            await asyncio.sleep(0.05)  # after writer starts waiting
            async with lock.read_lock():
                log.append("lr")

        await asyncio.gather(holder(), writer(), late_reader())
        # Late reader must wait for writer (writer preference)
        assert log == ["h_start", "h_end", "w", "lr"]

    async def test_read_lock_is_async_context_manager(self) -> None:
        lock = AsyncRWLock()
        async with lock.read_lock():
            pass

    async def test_write_lock_is_async_context_manager(self) -> None:
        lock = AsyncRWLock()
        async with lock.write_lock():
            pass
