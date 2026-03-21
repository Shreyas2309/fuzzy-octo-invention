"""Tests for app.common.crc — CRC32 helper."""

from __future__ import annotations

from app.common import crc


class TestCRC:
    def test_compute_deterministic(self) -> None:
        data = b"hello world"
        assert crc.compute(data) == crc.compute(data)

    def test_compute_different_data(self) -> None:
        assert crc.compute(b"a") != crc.compute(b"b")

    def test_pack_unpack_roundtrip(self) -> None:
        val = crc.compute(b"test data")
        packed = crc.pack(val)
        assert len(packed) == crc.CRC_SIZE
        assert crc.unpack(packed) == val

    def test_unpack_with_offset(self) -> None:
        val = crc.compute(b"xyz")
        data = b"\x00\x00" + crc.pack(val)
        assert crc.unpack(data, offset=2) == val

    def test_verify_true(self) -> None:
        data = b"some payload"
        checksum = crc.compute(data)
        assert crc.verify(data, checksum) is True

    def test_verify_false(self) -> None:
        data = b"some payload"
        assert crc.verify(data, 0xDEADBEEF) is False

    def test_memoryview_input(self) -> None:
        data = b"memoryview test"
        mv = memoryview(data)
        assert crc.compute(mv) == crc.compute(data)
