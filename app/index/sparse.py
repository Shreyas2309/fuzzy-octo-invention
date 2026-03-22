"""SparseIndex — maps first_key_of_block to block_offset.

Loaded into memory at SSTableReader open time.  Used for binary
search to find the block containing a given key.
"""

from __future__ import annotations

import bisect

from app.common import crc
from app.common.encoding import decode_index_entries, encode_index_entry
from app.common.errors import CorruptRecordError
from app.types import Key, Offset


class SparseIndex:
    """In-memory sparse index mapping block first-keys to offsets."""

    def __init__(self) -> None:
        self._keys: list[Key] = []
        self._offsets: list[Offset] = []

    def add(self, first_key: Key, block_offset: Offset) -> None:
        """Append a new index entry. Keys must be in ascending order."""
        if self._keys and first_key <= self._keys[-1]:
            raise ValueError(
                f"Keys must be ascending: {first_key!r} <= {self._keys[-1]!r}"
            )
        self._keys.append(first_key)
        self._offsets.append(block_offset)

    def floor_offset(self, key: Key) -> Offset | None:
        """Return the offset of the block whose first_key <= *key*.

        Uses bisect_right: finds rightmost entry with first_key <= key.
        """
        idx = bisect.bisect_right(self._keys, key) - 1
        if idx < 0:
            return None
        return self._offsets[idx]

    def ceil_offset(self, key: Key) -> Offset | None:
        """Return the offset of the block whose first_key >= *key*.

        Uses bisect_left: finds leftmost entry with first_key >= key.
        """
        idx = bisect.bisect_left(self._keys, key)
        if idx >= len(self._keys):
            return None
        return self._offsets[idx]

    def to_bytes(self) -> bytes:
        """Serialize all index entries to bytes with CRC footer."""
        parts: list[bytes] = []
        for key, offset in zip(self._keys, self._offsets, strict=True):
            parts.append(encode_index_entry(key, offset))
        payload = b"".join(parts)
        return payload + crc.pack(crc.compute(payload))

    @classmethod
    def from_bytes(cls, data: bytes) -> SparseIndex:
        """Deserialize from *data*. Verifies CRC integrity."""
        if len(data) < crc.CRC_SIZE:
            # Empty index (no entries, no CRC) is valid
            if len(data) == 0:
                return cls()
            raise CorruptRecordError(
                f"Index data too short for CRC: {len(data)}"
            )

        if len(data) == 0:
            return cls()

        payload = data[: -crc.CRC_SIZE]
        stored_crc = crc.unpack(data, len(data) - crc.CRC_SIZE)
        if not crc.verify(payload, stored_crc):
            raise CorruptRecordError("Index CRC mismatch")

        obj = cls()
        for key, offset in decode_index_entries(payload):
            obj._keys.append(key)
            obj._offsets.append(offset)
        return obj

    def next_offset_after(self, offset: Offset) -> Offset | None:
        """Return the first offset strictly greater than *offset*, or None."""
        idx = bisect.bisect_right(self._offsets, offset)
        if idx < len(self._offsets):
            return self._offsets[idx]
        return None

    def __len__(self) -> int:
        return len(self._keys)
