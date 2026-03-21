"""CRC32 helper — centralises computation so it is never duplicated."""

from __future__ import annotations

import binascii
import struct
from typing import Final

_CRC_STRUCT: Final = struct.Struct(">I")  # big-endian u32
CRC_SIZE: Final[int] = 4


def compute(data: bytes | bytearray | memoryview) -> int:
    """Return CRC32 of *data* as an unsigned 32-bit integer."""
    return binascii.crc32(data) & 0xFFFFFFFF


def pack(crc: int) -> bytes:
    """Encode a CRC32 value as 4 big-endian bytes."""
    return _CRC_STRUCT.pack(crc)


def unpack(data: bytes | memoryview, offset: int = 0) -> int:
    """Decode a CRC32 value from 4 big-endian bytes at *offset*."""
    result: int = _CRC_STRUCT.unpack_from(data, offset)[0]
    return result


def verify(data: bytes | memoryview, expected_crc: int) -> bool:
    """Return True if CRC32 of *data* matches *expected_crc*."""
    return compute(data) == expected_crc
