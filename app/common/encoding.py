"""Record encoding/decoding for SSTable data and index blocks.

All KV record serialisation lives here — used by SSTableWriter,
SSTableReader, and any future component reading ``data.bin``.
"""

from __future__ import annotations

import struct
from collections.abc import Iterator
from typing import Final, NamedTuple

from app.common import crc
from app.common.errors import CorruptRecordError
from app.types import Key, Offset, SeqNum, Value

# ---------------------------------------------------------------------------
# Record format: [key_len(4) | val_len(4) | seq(8) | ts(8)] key val crc(4)
# ---------------------------------------------------------------------------

_RECORD_HEADER: Final = struct.Struct(">IIqq")  # 4+4+8+8 = 24B
RECORD_HEADER_SIZE: Final[int] = _RECORD_HEADER.size  # 24
RECORD_FOOTER_SIZE: Final[int] = crc.CRC_SIZE  # 4
RECORD_OVERHEAD: Final[int] = RECORD_HEADER_SIZE + RECORD_FOOTER_SIZE  # 28

# ---------------------------------------------------------------------------
# Index entry format: [key_len(4) | block_offset(8)] key
# ---------------------------------------------------------------------------

_IDX_HEADER: Final = struct.Struct(">IQ")  # 4+8 = 12B
IDX_HEADER_SIZE: Final[int] = _IDX_HEADER.size  # 12


# ---------------------------------------------------------------------------
# Decoded record
# ---------------------------------------------------------------------------


class DecodedRecord(NamedTuple):
    """A single decoded data-block record."""

    key: Key
    seq: SeqNum
    timestamp_ms: int
    value: Value
    next_offset: Offset


# ---------------------------------------------------------------------------
# Encode / decode
# ---------------------------------------------------------------------------


def encode_record(
    key: Key, seq: SeqNum, timestamp_ms: int, value: Value,
) -> bytes:
    """Encode a single KV record with CRC footer."""
    key_len = len(key)
    val_len = len(value)
    header = _RECORD_HEADER.pack(key_len, val_len, seq, timestamp_ms)
    payload = header + key + value
    checksum = crc.pack(crc.compute(payload))
    return payload + checksum


def decode_from(mv: memoryview, offset: Offset) -> DecodedRecord:
    """Decode one record from *mv* starting at *offset*.

    Raises :class:`CorruptRecordError` on CRC mismatch or truncation.
    """
    end = len(mv)
    if offset + RECORD_HEADER_SIZE > end:
        raise CorruptRecordError(
            f"Truncated header at offset {offset} (need {RECORD_HEADER_SIZE}, "
            f"have {end - offset})"
        )

    key_len, val_len, seq, timestamp_ms = _RECORD_HEADER.unpack_from(mv, offset)

    record_end = offset + RECORD_HEADER_SIZE + key_len + val_len + RECORD_FOOTER_SIZE
    if record_end > end:
        raise CorruptRecordError(
            f"Truncated record at offset {offset} "
            f"(need {record_end - offset}, have {end - offset})"
        )

    payload_start = offset
    payload_end = offset + RECORD_HEADER_SIZE + key_len + val_len
    payload = mv[payload_start:payload_end]
    stored_crc = crc.unpack(mv, payload_end)

    if not crc.verify(payload, stored_crc):
        raise CorruptRecordError(
            f"CRC mismatch at offset {offset}: "
            f"computed={crc.compute(payload):#010x}, stored={stored_crc:#010x}"
        )

    key_start = offset + RECORD_HEADER_SIZE
    key = bytes(mv[key_start : key_start + key_len])
    val_start = key_start + key_len
    value = bytes(mv[val_start : val_start + val_len])

    return DecodedRecord(
        key=key,
        seq=seq,
        timestamp_ms=timestamp_ms,
        value=value,
        next_offset=record_end,
    )


def iter_block(
    mv: memoryview, block_offset: Offset, block_end: Offset,
) -> Iterator[DecodedRecord]:
    """Yield all records in the block bounded by [block_offset, block_end)."""
    pos = block_offset
    while pos < block_end:
        rec = decode_from(mv, pos)
        yield rec
        pos = rec.next_offset


# ---------------------------------------------------------------------------
# Index entry encode / decode
# ---------------------------------------------------------------------------


def encode_index_entry(key: Key, block_offset: Offset) -> bytes:
    """Encode a sparse-index entry: ``[key_len | block_offset] key``."""
    return _IDX_HEADER.pack(len(key), block_offset) + key


def decode_index_entries(data: bytes) -> list[tuple[Key, Offset]]:
    """Decode all sparse-index entries from *data*."""
    entries: list[tuple[Key, Offset]] = []
    mv = memoryview(data)
    pos = 0
    end = len(mv)
    while pos < end:
        if pos + IDX_HEADER_SIZE > end:
            break
        key_len, block_offset = _IDX_HEADER.unpack_from(mv, pos)
        key_start = pos + IDX_HEADER_SIZE
        key = bytes(mv[key_start : key_start + key_len])
        entries.append((key, block_offset))
        pos = key_start + key_len
    return entries
