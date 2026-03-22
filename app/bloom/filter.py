"""BloomFilter — mmh3-backed probabilistic membership test.

Used by SSTableReader to skip disk reads for keys that are
definitely not in an SSTable.
"""

from __future__ import annotations

import math
import struct
from typing import Final

import mmh3

from app.common import crc
from app.common.errors import CorruptRecordError
from app.types import Key

# ---------------------------------------------------------------------------
# Header: num_hashes(4) + bit_count(4) + seed(4) + reserved(4) = 16B
# ---------------------------------------------------------------------------

_HEADER_STRUCT: Final = struct.Struct(">IIII")
_HEADER_SIZE: Final[int] = _HEADER_STRUCT.size  # 16


class BloomFilter:
    """Fixed-size Bloom filter backed by mmh3 hashes."""

    def __init__(self, n: int = 1_000_000, fpr: float = 0.01) -> None:
        if n <= 0:
            n = 1
        if fpr <= 0 or fpr >= 1:
            fpr = 0.01

        # Optimal bit count: m = -n * ln(p) / (ln2)^2
        self._bit_count = max(1, int(-n * math.log(fpr) / (math.log(2) ** 2)))
        # Optimal hash count: k = (m/n) * ln2
        self._num_hashes = max(1, int((self._bit_count / n) * math.log(2)))
        self._seed: int = 42
        # Bit array stored as bytearray
        self._bits = bytearray((self._bit_count + 7) // 8)

    def add(self, key: Key) -> None:
        """Insert *key* into the filter."""
        for i in range(self._num_hashes):
            h = mmh3.hash(key, seed=self._seed + i, signed=False)
            idx = h % self._bit_count
            self._bits[idx >> 3] |= 1 << (idx & 7)

    def may_contain(self, key: Key) -> bool:
        """Return True if *key* might be present (false positives allowed)."""
        for i in range(self._num_hashes):
            h = mmh3.hash(key, seed=self._seed + i, signed=False)
            idx = h % self._bit_count
            if not (self._bits[idx >> 3] & (1 << (idx & 7))):
                return False
        return True

    def to_bytes(self) -> bytes:
        """Serialize the filter to bytes with CRC footer."""
        header = _HEADER_STRUCT.pack(
            self._num_hashes, self._bit_count, self._seed, 0,
        )
        payload = header + bytes(self._bits)
        return payload + crc.pack(crc.compute(payload))

    @classmethod
    def from_bytes(cls, data: bytes) -> BloomFilter:
        """Deserialize a filter from *data*.

        Raises :class:`CorruptRecordError` if data is truncated or
        CRC verification fails.
        """
        min_size = _HEADER_SIZE + crc.CRC_SIZE
        if len(data) < min_size:
            raise CorruptRecordError(
                f"Bloom filter data too short: "
                f"{len(data)} < {min_size}"
            )

        # Verify CRC (covers header + bit array)
        payload = data[: -crc.CRC_SIZE]
        stored_crc = crc.unpack(data, len(data) - crc.CRC_SIZE)
        if not crc.verify(payload, stored_crc):
            raise CorruptRecordError("Bloom filter CRC mismatch")

        num_hashes, bit_count, seed, _ = _HEADER_STRUCT.unpack_from(
            payload,
        )
        expected_bytes = (bit_count + 7) // 8
        actual_bytes = len(payload) - _HEADER_SIZE
        if actual_bytes < expected_bytes:
            raise CorruptRecordError(
                f"Bloom filter truncated: "
                f"need {expected_bytes}, have {actual_bytes}"
            )
        obj = cls.__new__(cls)
        obj._num_hashes = num_hashes
        obj._bit_count = bit_count
        obj._seed = seed
        obj._bits = bytearray(
            payload[_HEADER_SIZE : _HEADER_SIZE + expected_bytes],
        )
        return obj
