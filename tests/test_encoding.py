"""Tests for app.common.encoding — record encode/decode."""

from __future__ import annotations

import pytest

from app.common import crc
from app.common.encoding import (
    RECORD_OVERHEAD,
    DecodedRecord,
    decode_from,
    decode_index_entries,
    encode_index_entry,
    encode_record,
    iter_block,
)
from app.common.errors import CorruptRecordError


class TestEncodeDecodeRecord:
    def test_roundtrip(self) -> None:
        key, seq, ts, val = b"mykey", 42, 1000, b"myvalue"
        encoded = encode_record(key, seq, ts, val)
        mv = memoryview(encoded)
        rec = decode_from(mv, 0)

        assert rec.key == key
        assert rec.seq == seq
        assert rec.timestamp_ms == ts
        assert rec.value == val
        assert rec.next_offset == len(encoded)

    def test_record_size(self) -> None:
        key, val = b"k", b"v"
        encoded = encode_record(key, 1, 100, val)
        assert len(encoded) == len(key) + len(val) + RECORD_OVERHEAD

    def test_empty_value(self) -> None:
        encoded = encode_record(b"key", 1, 100, b"")
        rec = decode_from(memoryview(encoded), 0)
        assert rec.value == b""
        assert rec.key == b"key"

    def test_corrupt_crc_raises(self) -> None:
        encoded = bytearray(encode_record(b"key", 1, 100, b"val"))
        # Flip last byte (part of CRC)
        encoded[-1] ^= 0xFF
        with pytest.raises(CorruptRecordError, match="CRC mismatch"):
            decode_from(memoryview(encoded), 0)

    def test_truncated_header_raises(self) -> None:
        with pytest.raises(CorruptRecordError, match="Truncated header"):
            decode_from(memoryview(b"\x00" * 10), 0)

    def test_truncated_body_raises(self) -> None:
        encoded = encode_record(b"key", 1, 100, b"value")
        truncated = encoded[: len(encoded) - 2]
        with pytest.raises(CorruptRecordError, match="Truncated record"):
            decode_from(memoryview(truncated), 0)


class TestIterBlock:
    def test_multiple_records(self) -> None:
        records = [
            (b"a", 1, 100, b"v1"),
            (b"b", 2, 200, b"v2"),
            (b"c", 3, 300, b"v3"),
        ]
        block = b"".join(encode_record(*r) for r in records)
        mv = memoryview(block)

        decoded = list(iter_block(mv, 0, len(block)))
        assert len(decoded) == 3
        assert decoded[0].key == b"a"
        assert decoded[1].key == b"b"
        assert decoded[2].key == b"c"


class TestIndexEntries:
    def test_roundtrip(self) -> None:
        entries = [(b"aaa", 0), (b"mmm", 4096), (b"zzz", 8192)]
        encoded = b"".join(encode_index_entry(k, o) for k, o in entries)
        decoded = decode_index_entries(encoded)
        assert decoded == entries

    def test_empty(self) -> None:
        assert decode_index_entries(b"") == []
