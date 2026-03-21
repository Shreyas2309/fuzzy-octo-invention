"""Tests for app.sstable.meta — SSTableMeta serialization."""

from __future__ import annotations

from app.sstable.meta import SSTableMeta


class TestSSTableMeta:
    def _make_meta(self) -> SSTableMeta:
        return SSTableMeta(
            file_id="abc123",
            snapshot_id="snap456",
            level=0,
            size_bytes=8192,
            record_count=100,
            block_count=2,
            min_key=b"aaa",
            max_key=b"zzz",
            seq_min=1,
            seq_max=100,
            bloom_fpr=0.01,
            created_at="2026-03-20T12:00:00+00:00",
            data_file="data.bin",
            index_file="index.bin",
            filter_file="filter.bin",
        )

    def test_to_json_from_json_roundtrip(self) -> None:
        meta = self._make_meta()
        json_str = meta.to_json()
        restored = SSTableMeta.from_json(json_str)

        assert restored.file_id == meta.file_id
        assert restored.snapshot_id == meta.snapshot_id
        assert restored.level == meta.level
        assert restored.size_bytes == meta.size_bytes
        assert restored.record_count == meta.record_count
        assert restored.block_count == meta.block_count
        assert restored.min_key == meta.min_key
        assert restored.max_key == meta.max_key
        assert restored.seq_min == meta.seq_min
        assert restored.seq_max == meta.seq_max
        assert restored.bloom_fpr == meta.bloom_fpr
        assert restored.data_file == meta.data_file

    def test_frozen(self) -> None:
        meta = self._make_meta()
        import dataclasses

        assert dataclasses.is_dataclass(meta)
        # frozen=True means setattr raises
        import pytest

        with pytest.raises(dataclasses.FrozenInstanceError):
            meta.file_id = "changed"  # type: ignore[misc]
