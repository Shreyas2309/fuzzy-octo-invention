"""Tests for app.common.uuid7 — UUIDv7 time-ordered IDs."""

from __future__ import annotations

import time

from app.common.uuid7 import uuid7_hex


class TestUUID7:
    def test_returns_32_hex_chars(self) -> None:
        uid = uuid7_hex()
        assert len(uid) == 32
        int(uid, 16)  # must be valid hex

    def test_unique(self) -> None:
        ids = {uuid7_hex() for _ in range(100)}
        assert len(ids) == 100

    def test_lexicographic_order_is_chronological(self) -> None:
        """IDs generated later sort higher (newer > older)."""
        a = uuid7_hex()
        time.sleep(0.002)  # 2ms gap ensures different timestamp
        b = uuid7_hex()
        assert b > a, f"Expected {b} > {a}"

    def test_version_bits(self) -> None:
        uid = uuid7_hex()
        # Version nibble is at position 12 (0-indexed), should be '7'
        assert uid[12] == "7"

    def test_batch_sorted(self) -> None:
        """A batch generated with small delays sorts correctly."""
        ids: list[str] = []
        for _ in range(10):
            ids.append(uuid7_hex())
            time.sleep(0.002)
        assert ids == sorted(ids)
