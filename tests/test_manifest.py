"""Tests for Manifest — persistent multi-level SSTable ordering."""

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
        layout = m.read()
        assert layout["l0_order"] == []
        assert layout["levels"] == {}

    def test_write_then_read(self, manifest_path: Path) -> None:
        m = Manifest(manifest_path)
        layout: dict[str, object] = {
            "l0_order": ["aaa", "bbb", "ccc"],
            "levels": {},
        }
        m.write(layout)

        assert manifest_path.exists()
        loaded = m.read()
        assert loaded["l0_order"] == ["aaa", "bbb", "ccc"]
        assert loaded["levels"] == {}

    def test_write_with_levels(self, manifest_path: Path) -> None:
        m = Manifest(manifest_path)
        m.write({
            "l0_order": ["a"],
            "levels": {"1": "l1_id", "2": "l2_id"},
        })
        loaded = m.read()
        assert loaded["l0_order"] == ["a"]
        levels = loaded["levels"]
        assert levels == {"1": "l1_id", "2": "l2_id"}  # type: ignore[comparison-overlap]

    def test_write_overwrites(self, manifest_path: Path) -> None:
        m = Manifest(manifest_path)
        m.write({"l0_order": ["old"], "levels": {"1": "old_l1"}})
        m.write({"l0_order": ["new"], "levels": {"1": "new_l1"}})

        loaded = m.read()
        assert loaded["l0_order"] == ["new"]
        assert loaded["levels"] == {"1": "new_l1"}  # type: ignore[comparison-overlap]

    def test_read_corrupt_returns_empty(self, manifest_path: Path) -> None:
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text("NOT VALID JSON!!!", encoding="utf-8")

        m = Manifest(manifest_path)
        layout = m.read()
        assert layout["l0_order"] == []
        assert layout["levels"] == {}

    def test_write_is_atomic(self, manifest_path: Path) -> None:
        m = Manifest(manifest_path)
        m.write({"l0_order": ["first"], "levels": {}})
        m.write({"l0_order": ["second"], "levels": {"1": "x"}})

        loaded = m.read()
        assert loaded["l0_order"] == ["second"]

    def test_empty_order(self, manifest_path: Path) -> None:
        m = Manifest(manifest_path)
        m.write({"l0_order": [], "levels": {}})
        loaded = m.read()
        assert loaded["l0_order"] == []

    def test_preserves_order(self, manifest_path: Path) -> None:
        m = Manifest(manifest_path)
        order = ["zzz", "aaa", "mmm"]
        m.write({"l0_order": order, "levels": {}})
        assert m.read()["l0_order"] == order

    def test_backward_compat_old_l1_file(self, manifest_path: Path) -> None:
        """Old manifests with l1_file key migrate to levels dict."""
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            '{"l0_order": ["a"], "l1_file": "old_l1"}',
            encoding="utf-8",
        )
        m = Manifest(manifest_path)
        loaded = m.read()
        assert loaded["l0_order"] == ["a"]
        assert loaded["levels"] == {"1": "old_l1"}  # type: ignore[comparison-overlap]

    def test_backward_compat_list_format(self, manifest_path: Path) -> None:
        """Very old manifests were just a JSON list."""
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text('["a", "b"]', encoding="utf-8")
        m = Manifest(manifest_path)
        loaded = m.read()
        assert loaded["l0_order"] == ["a", "b"]
        assert loaded["levels"] == {}
