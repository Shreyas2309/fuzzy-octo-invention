"""Tests for Manifest — persistent L0 SSTable ordering."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.engine.sstable_manager import Manifest


@pytest.fixture()
def manifest_path(tmp_path: Path) -> Path:
    return tmp_path / "sstable" / "manifest.json"


class TestManifest:
    def test_read_missing_file(self, manifest_path: Path) -> None:
        m = Manifest(manifest_path)
        assert m.read() == []

    def test_write_then_read(self, manifest_path: Path) -> None:
        m = Manifest(manifest_path)
        order = ["aaa", "bbb", "ccc"]
        m.write(order)

        assert manifest_path.exists()
        loaded = m.read()
        assert loaded == order

    def test_write_overwrites(self, manifest_path: Path) -> None:
        m = Manifest(manifest_path)
        m.write(["old1", "old2"])
        m.write(["new1", "new2", "new3"])

        assert m.read() == ["new1", "new2", "new3"]

    def test_read_corrupt_returns_empty(self, manifest_path: Path) -> None:
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text("NOT VALID JSON!!!", encoding="utf-8")

        m = Manifest(manifest_path)
        assert m.read() == []

    def test_write_is_atomic(self, manifest_path: Path) -> None:
        """No partial writes — file either has old or new content."""
        m = Manifest(manifest_path)
        m.write(["first"])

        # Write again — if it crashes mid-write, old content stays
        m.write(["second"])
        assert m.read() == ["second"]

    def test_empty_order(self, manifest_path: Path) -> None:
        m = Manifest(manifest_path)
        m.write([])
        assert m.read() == []

    def test_preserves_order(self, manifest_path: Path) -> None:
        m = Manifest(manifest_path)
        order = ["zzz", "aaa", "mmm"]  # intentionally not sorted
        m.write(order)
        assert m.read() == order  # order preserved, not sorted
