"""Tests for app.compaction.worker — run_compaction()."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.compaction.task import CompactionTask
from app.compaction.worker import run_compaction
from app.sstable.writer import SSTableWriter
from app.types import TOMBSTONE


def _write_sst(
    directory: Path,
    file_id: str,
    entries: list[tuple[bytes, int, int, bytes]],
    level: int = 0,
) -> None:
    """Helper: write an SSTable synchronously."""
    writer = SSTableWriter(
        directory=directory,
        file_id=file_id,
        snapshot_id=f"snap_{file_id}",
        level=level,
    )
    for key, seq, ts, val in entries:
        writer.put(key, seq, ts, val)
    writer.finish_sync()


class TestRunCompaction:
    def test_merge_non_overlapping(self, tmp_path: Path) -> None:
        d1 = tmp_path / "L0" / "f1"
        d2 = tmp_path / "L0" / "f2"
        _write_sst(d1, "f1", [(b"a", 1, 100, b"v1"), (b"b", 2, 100, b"v2")])
        _write_sst(d2, "f2", [(b"c", 3, 100, b"v3"), (b"d", 4, 100, b"v4")])

        out_dir = tmp_path / "L1" / "out"
        task = CompactionTask(
            task_id="t1",
            input_file_ids=["f1", "f2"],
            input_dirs={"f1": str(d1), "f2": str(d2)},
            output_file_id="out",
            output_dir=str(out_dir),
            output_level=1,
            seq_cutoff=0,
        )
        meta = run_compaction(task)
        assert meta.record_count == 4
        assert meta.min_key == b"a"
        assert meta.max_key == b"d"

    def test_merge_overlapping_dedup(self, tmp_path: Path) -> None:
        d1 = tmp_path / "L0" / "f1"
        d2 = tmp_path / "L0" / "f2"
        _write_sst(d1, "f1", [(b"k", 10, 100, b"old")])
        _write_sst(d2, "f2", [(b"k", 50, 200, b"new")])

        out_dir = tmp_path / "L1" / "out"
        task = CompactionTask(
            task_id="t2",
            input_file_ids=["f1", "f2"],
            input_dirs={"f1": str(d1), "f2": str(d2)},
            output_file_id="out",
            output_dir=str(out_dir),
            output_level=1,
            seq_cutoff=0,
        )
        meta = run_compaction(task)
        assert meta.record_count == 1
        assert meta.seq_max == 50

    def test_merge_with_existing_l1(self, tmp_path: Path) -> None:
        d1 = tmp_path / "L0" / "f1"
        dl1 = tmp_path / "L1" / "old_l1"
        _write_sst(d1, "f1", [(b"a", 5, 100, b"new_a")])
        _write_sst(
            dl1, "old_l1",
            [(b"a", 1, 50, b"old_a"), (b"z", 2, 50, b"old_z")],
            level=1,
        )

        out_dir = tmp_path / "L1" / "new_l1"
        task = CompactionTask(
            task_id="t3",
            input_file_ids=["f1", "old_l1"],
            input_dirs={"f1": str(d1), "old_l1": str(dl1)},
            output_file_id="new_l1",
            output_dir=str(out_dir),
            output_level=1,
            seq_cutoff=0,
        )
        meta = run_compaction(task)
        assert meta.record_count == 2  # a (seq=5 wins) + z
        assert meta.min_key == b"a"
        assert meta.max_key == b"z"

    def test_tombstone_preserved_zero_cutoff(self, tmp_path: Path) -> None:
        d1 = tmp_path / "L0" / "f1"
        _write_sst(d1, "f1", [(b"k", 10, 100, TOMBSTONE)])

        out_dir = tmp_path / "L1" / "out"
        task = CompactionTask(
            task_id="t4",
            input_file_ids=["f1"],
            input_dirs={"f1": str(d1)},
            output_file_id="out",
            output_dir=str(out_dir),
            output_level=1,
            seq_cutoff=0,
        )
        meta = run_compaction(task)
        assert meta.record_count == 1  # tombstone preserved

    def test_tombstone_gc_with_cutoff(self, tmp_path: Path) -> None:
        d1 = tmp_path / "L0" / "f1"
        _write_sst(
            d1, "f1",
            [(b"a", 5, 100, TOMBSTONE), (b"b", 15, 100, b"keep")],
        )

        out_dir = tmp_path / "L1" / "out"
        task = CompactionTask(
            task_id="t5",
            input_file_ids=["f1"],
            input_dirs={"f1": str(d1)},
            output_file_id="out",
            output_dir=str(out_dir),
            output_level=1,
            seq_cutoff=10,
        )
        meta = run_compaction(task)
        assert meta.record_count == 1  # tombstone seq=5 GC'd, b kept
        assert meta.min_key == b"b"

    def test_output_has_meta_json(self, tmp_path: Path) -> None:
        d1 = tmp_path / "L0" / "f1"
        _write_sst(d1, "f1", [(b"k", 1, 100, b"v")])

        out_dir = tmp_path / "L1" / "out"
        task = CompactionTask(
            task_id="t6",
            input_file_ids=["f1"],
            input_dirs={"f1": str(d1)},
            output_file_id="out",
            output_dir=str(out_dir),
            output_level=1,
            seq_cutoff=0,
        )
        run_compaction(task)
        assert (out_dir / "meta.json").exists()
        assert (out_dir / "data.bin").exists()
        assert (out_dir / "index.bin").exists()
        assert (out_dir / "filter.bin").exists()
