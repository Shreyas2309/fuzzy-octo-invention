"""SSTableManager — coordinates all on-disk read-side state.

Owns the block cache, SSTable registry, L0 file list, per-level single
files (L1+), and the persistent **manifest** that tracks SSTable
ordering across restarts.  Provides per-level AsyncRWLock for
compaction safety.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from app.cache.block import BlockCache
from app.common.rwlock import AsyncRWLock
from app.common.uuid7 import uuid7_hex
from app.memtable.immutable import ImmutableMemTable
from app.observability import get_logger
from app.sstable.meta import SSTableMeta
from app.sstable.reader import SSTableReader
from app.sstable.registry import SSTableRegistry
from app.sstable.writer import SSTableWriter
from app.types import FileID, Key, Level, SeqNum, Value

if TYPE_CHECKING:
    from app.compaction.task import CompactionTask
    from app.engine.config import LSMConfig

logger = get_logger(__name__)

DEFAULT_MAX_LEVEL: int = 3  # fallback when no config is available


# ---------------------------------------------------------------------------
# Manifest — persistent layout (L0 order + per-level files)
# ---------------------------------------------------------------------------


class Manifest:
    """Tracks SSTable ordering on disk via ``manifest.json``."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def read(self) -> dict[str, object]:
        """Load layout from disk. Returns default if file missing."""
        if not self._path.exists():
            return {"l0_order": [], "levels": {}}
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            # Backward-compat: old formats
            if isinstance(data, list):
                return {"l0_order": data, "levels": {}}
            if "l0_order" not in data:
                data["l0_order"] = []
            # Migrate old l1_file field to levels dict
            if "l1_file" in data and "levels" not in data:
                levels: dict[str, str] = {}
                if data["l1_file"]:
                    levels["1"] = data["l1_file"]
                data["levels"] = levels
                del data["l1_file"]
            data.setdefault("levels", {})
            logger.info(
                "Manifest loaded",
                l0_count=len(data["l0_order"]),
                levels=data["levels"],
            )
            result: dict[str, object] = dict(data)
            return result
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Manifest corrupt, rebuilding", error=str(exc))
            return {"l0_order": [], "levels": {}}

    def write(self, layout: dict[str, object]) -> None:
        """Atomically persist the manifest layout."""
        data = json.dumps(layout, indent=2) + "\n"
        try:
            tmp = tempfile.NamedTemporaryFile(  # noqa: SIM115
                dir=self._path.parent,
                delete=False,
                mode="w",
                encoding="utf-8",
                suffix=".tmp",
            )
            tmp_path = Path(tmp.name)
            try:
                tmp.write(data)
                tmp.flush()
                os.fsync(tmp.fileno())
                tmp.close()
                os.replace(tmp_path, self._path)
            except BaseException:
                tmp.close()
                tmp_path.unlink(missing_ok=True)
                raise
        except OSError as exc:
            logger.error("Manifest write failed", error=str(exc))
            raise

        logger.debug("Manifest saved", layout=layout)


# ---------------------------------------------------------------------------
# SSTableManager
# ---------------------------------------------------------------------------


class SSTableManager:
    """Manages all on-disk SSTable state (L0 + L1 + L2 + L3)."""

    def __init__(
        self,
        data_root: Path,
        cache: BlockCache,
        registry: SSTableRegistry,
        l0_order: list[FileID],
        l0_dirs: dict[FileID, Path],
        manifest: Manifest,
        config: LSMConfig | None = None,
    ) -> None:
        self._data_root = data_root
        self._cache = cache
        self._registry = registry
        self._l0_order = l0_order  # newest first
        self._l0_dirs = l0_dirs
        self._manifest = manifest
        self._config = config
        self._max_seq: SeqNum = 0
        self._state_lock = threading.Lock()
        # Per-level single file: level → (file_id, directory)
        self._level_files: dict[Level, tuple[FileID, Path]] = {}
        self._level_rwlocks: dict[Level, AsyncRWLock] = {}

    @property
    def max_level(self) -> int:
        """Maximum level depth (from config or default)."""
        if self._config is not None:
            return int(self._config.max_levels)
        return DEFAULT_MAX_LEVEL

    def _level_lock(self, level: Level) -> AsyncRWLock:
        """Return (or create) the per-level AsyncRWLock."""
        if level not in self._level_rwlocks:
            self._level_rwlocks[level] = AsyncRWLock(name=f"L{level}")
        return self._level_rwlocks[level]

    # ── manifest helpers ────────────────────────────────────────────────

    def _build_manifest_layout(self) -> dict[str, object]:
        """Build the current manifest layout dict (caller holds lock)."""
        levels: dict[str, str] = {
            str(lv): fid for lv, (fid, _) in self._level_files.items()
        }
        return {"l0_order": list(self._l0_order), "levels": levels}

    def _persist_manifest(self) -> None:
        """Write the current layout to disk."""
        self._manifest.write(self._build_manifest_layout())

    # ── factory ─────────────────────────────────────────────────────────

    @classmethod
    async def load(
        cls,
        data_root: Path,
        cache: BlockCache | None = None,
        config: LSMConfig | None = None,
    ) -> SSTableManager:
        """Load SSTables from disk using the manifest for ordering."""
        if cache is None:
            cache = BlockCache()

        registry = SSTableRegistry()
        l0_dirs: dict[FileID, Path] = {}

        l0_dir = data_root / "sstable" / "L0"
        manifest_path = data_root / "sstable" / "manifest.json"
        manifest = Manifest(manifest_path)

        # Discover all complete L0 SSTables on disk
        on_disk: dict[FileID, Path] = {}
        if l0_dir.exists():
            for child in l0_dir.iterdir():
                if not child.is_dir():
                    continue
                if not (child / "meta.json").exists():
                    logger.warning(
                        "Incomplete SSTable skipped", path=str(child),
                    )
                    continue
                on_disk[child.name] = child

        # Load manifest
        layout = manifest.read()
        manifest_l0: list[str] = layout.get("l0_order", [])  # type: ignore[assignment]
        manifest_levels: dict[str, str] = layout.get("levels", {})  # type: ignore[assignment]

        # Reconcile L0: manifest order + orphans
        ordered: list[FileID] = []
        seen: set[FileID] = set()
        for fid in manifest_l0:
            if fid in on_disk:
                ordered.append(fid)
                seen.add(fid)
            else:
                logger.warning(
                    "Manifest references missing SSTable", file_id=fid,
                )

        orphans = sorted(
            (fid for fid in on_disk if fid not in seen),
            reverse=True,
        )
        if orphans:
            logger.info("Orphan L0 SSTables found", count=len(orphans))
            ordered.extend(orphans)

        # Open L0 readers
        l0_order: list[FileID] = []
        for fid in ordered:
            sst_dir = on_disk[fid]
            try:
                reader = await SSTableReader.open(
                    sst_dir, fid, cache, level=0,
                )
            except Exception:
                logger.exception(
                    "Failed to open SSTable", path=str(sst_dir),
                )
                continue
            registry.register(fid, reader)
            l0_order.append(fid)
            l0_dirs[fid] = sst_dir

        mgr = cls(
            data_root, cache, registry, l0_order, l0_dirs, manifest, config,
        )

        # Compute max seq from L0
        for fid in l0_order:
            with registry.open_reader(fid) as reader:
                if reader.meta.seq_max > mgr._max_seq:
                    mgr._max_seq = reader.meta.seq_max

        # Load L1+ from manifest
        for level_str, fid in manifest_levels.items():
            level = int(level_str)
            await mgr._load_level_file(level, fid, cache, registry)

        # Check for orphan level files on disk (not in manifest)
        for level in range(1, mgr.max_level + 1):
            if level in mgr._level_files:
                continue  # already loaded from manifest
            level_root = data_root / "sstable" / f"L{level}"
            if not level_root.exists():
                continue
            for child in level_root.iterdir():
                if child.is_dir() and (child / "meta.json").exists():
                    logger.info(
                        "Orphan SSTable found",
                        level=level,
                        file_id=child.name,
                    )
                    await mgr._load_level_file(
                        level, child.name, cache, registry,
                    )
                    break

        # Persist reconciled layout
        mgr._persist_manifest()

        # Clean up stale level directories
        for level in range(1, mgr.max_level + 1):
            mgr._cleanup_stale_level_dirs(level)

        logger.info(
            "SSTableManager loaded",
            l0_count=len(l0_order),
            levels={
                lv: fid for lv, (fid, _) in mgr._level_files.items()
            },
            max_seq=mgr._max_seq,
        )
        return mgr

    async def _load_level_file(
        self,
        level: Level,
        fid: FileID,
        cache: BlockCache,
        registry: SSTableRegistry,
    ) -> None:
        """Load a single SSTable file for a given level."""
        level_dir = self._data_root / "sstable" / f"L{level}" / fid
        if not (level_dir / "meta.json").exists():
            logger.warning(
                "Level file missing on disk",
                level=level,
                file_id=fid,
            )
            return
        try:
            reader = await SSTableReader.open(
                level_dir, fid, cache, level=level,
            )
            registry.register(fid, reader)
            self._level_files[level] = (fid, level_dir)
            if reader.meta.seq_max > self._max_seq:
                self._max_seq = reader.meta.seq_max
            logger.debug(
                "Level file loaded",
                level=level,
                file_id=fid,
                records=reader.meta.record_count,
            )
        except Exception:
            logger.exception(
                "Failed to open level SSTable",
                level=level,
                file_id=fid,
            )

    def _cleanup_stale_level_dirs(self, level: Level) -> None:
        """Remove directories not matching the current file for this level."""
        level_root = self._data_root / "sstable" / f"L{level}"
        if not level_root.exists():
            return
        current_fid = (
            self._level_files[level][0]
            if level in self._level_files
            else None
        )
        for child in level_root.iterdir():
            if child.is_dir() and child.name != current_fid:
                logger.info(
                    "Removing stale dir",
                    level=level,
                    path=str(child),
                )
                shutil.rmtree(child, ignore_errors=True)

    # ── flush ───────────────────────────────────────────────────────────

    async def flush(
        self,
        snapshot: ImmutableMemTable,
        file_id: FileID,
    ) -> tuple[SSTableMeta, SSTableReader]:
        """Write *snapshot* to a new L0 SSTable, return (meta, reader)."""
        sst_dir = self._data_root / "sstable" / "L0" / file_id

        block_size = 0
        block_entries = 0
        if self._config is not None and self._config.is_dev:
            block_entries = max(1, len(snapshot) // 8)
        elif self._config is not None and self._config.is_prod:
            block_size = max(1, self._config.max_memtable_bytes // 8)
        else:
            block_size = int(self._config.block_size) if self._config else 4096

        writer = SSTableWriter(
            directory=sst_dir,
            file_id=file_id,
            snapshot_id=snapshot.snapshot_id,
            level=0,
            block_size=block_size,
            block_entries=block_entries,
        )

        for key, seq, ts, value in snapshot.items():
            writer.put(key, seq, ts, value)

        meta = await writer.finish()
        reader = await SSTableReader.open(
            sst_dir, file_id, self._cache, level=0,
        )
        return meta, reader

    def commit(
        self, file_id: FileID, reader: SSTableReader, sst_dir: Path,
    ) -> None:
        """Register a flushed L0 SSTable and persist manifest."""
        self._registry.register(file_id, reader)
        with self._state_lock:
            self._l0_order.insert(0, file_id)
            self._l0_dirs[file_id] = sst_dir
            if reader.meta.seq_max > self._max_seq:
                self._max_seq = reader.meta.seq_max

        self._persist_manifest()

        logger.info(
            "SSTable committed",
            file_id=file_id,
            l0_count=len(self._l0_order),
        )

    # ── compaction commit ───────────────────────────────────────────────

    async def commit_compaction_async(
        self,
        task: CompactionTask,
        new_meta: SSTableMeta,
        new_reader: SSTableReader,
    ) -> None:
        """Atomically commit a compaction result.

        Acquires write locks on src and dst levels in ascending order.

        Commit ordering (non-negotiable):
          1. Register new reader   (dst level becomes readable)
          2. Write manifest        (durable)
          3. Mark old files for deletion (deferred by ref-count)
          4. Update in-memory state
          5. Evict stale cache blocks
        """
        src_level = task.output_level - 1
        dst_level = task.output_level

        logger.info(
            "Compaction commit starting",
            input_files=len(task.input_file_ids),
            new_file=new_meta.file_id,
            src_level=src_level,
            dst_level=dst_level,
        )

        async with (  # noqa: SIM117
            self._level_lock(src_level).write_lock(),
            self._level_lock(dst_level).write_lock(),
        ):
                logger.debug("Write locks acquired")
                with self._state_lock:
                    # Step 1: Register new reader
                    self._registry.register(
                        new_meta.file_id, new_reader,
                    )
                    logger.debug("Step 1/5: Reader registered",
                                 file_id=new_meta.file_id)

                    # Step 2: Write manifest
                    input_set = set(task.input_file_ids)
                    old_dst = self._level_files.get(dst_level)
                    old_dst_fid = old_dst[0] if old_dst else None

                    if src_level == 0:
                        new_l0 = [
                            f for f in self._l0_order
                            if f not in input_set
                        ]
                        self._l0_order = new_l0
                    else:
                        # Src level file consumed → remove
                        self._level_files.pop(src_level, None)

                    self._level_files[dst_level] = (
                        new_meta.file_id,
                        Path(task.output_dir),
                    )
                    self._persist_manifest()
                    logger.debug("Step 2/5: Manifest written")

                    # Step 3: Mark old files for deletion
                    for fid in task.input_file_ids:
                        self._registry.mark_for_deletion(fid)
                    if old_dst_fid is not None:
                        self._registry.mark_for_deletion(old_dst_fid)
                    logger.debug("Step 3/5: Old files marked",
                                 count=len(task.input_file_ids),
                                 old_dst=old_dst_fid)

                    # Step 4: Update state
                    if new_meta.seq_max > self._max_seq:
                        self._max_seq = new_meta.seq_max
                    logger.debug("Step 4/5: State updated")

                    # Step 5: Evict cache
                    self._cache.invalidate_all(task.input_file_ids)
                    logger.debug("Step 5/5: Cache invalidated")

        logger.info(
            "Compaction commit done",
            src_level=src_level,
            dst_level=dst_level,
            l0_remaining=len(self._l0_order),
            dst_file=new_meta.file_id,
        )

        # Step 6 (outside locks): delete old directories from disk
        self._delete_compacted_dirs(task.input_file_ids, old_dst_fid)

    def _delete_compacted_dirs(
        self,
        file_ids: list[FileID],
        old_dst_fid: FileID | None,
    ) -> None:
        """Remove compacted SSTable directories from disk."""
        for fid in file_ids:
            sst_dir = self._l0_dirs.pop(fid, None)
            if sst_dir is None:
                # Check all level dirs
                for lv in range(1, self.max_level + 1):
                    candidate = (
                        self._data_root / "sstable" / f"L{lv}" / fid
                    )
                    if candidate.exists():
                        sst_dir = candidate
                        break
            if sst_dir is not None and sst_dir.exists():
                try:
                    shutil.rmtree(sst_dir)
                    logger.debug("Deleted SSTable dir", path=str(sst_dir))
                except OSError as exc:
                    logger.warning(
                        "Failed to delete SSTable dir",
                        path=str(sst_dir),
                        error=str(exc),
                    )

        if old_dst_fid is not None and old_dst_fid not in file_ids:
            for lv in range(1, self.max_level + 1):
                old_dir = (
                    self._data_root / "sstable" / f"L{lv}" / old_dst_fid
                )
                if old_dir.exists():
                    try:
                        shutil.rmtree(old_dir)
                        logger.debug("Deleted old dst dir",
                                     path=str(old_dir))
                    except OSError as exc:
                        logger.warning(
                            "Failed to delete old dst dir",
                            path=str(old_dir),
                            error=str(exc),
                        )
                    break

    # ── read path ───────────────────────────────────────────────────────

    async def get(self, key: Key) -> tuple[SeqNum, int, Value] | None:
        """Look up *key* across L0 then L1, L2, L3.

        Read locks prevent compaction commits from swapping level
        contents mid-scan.
        """
        with self._state_lock:
            l0_snapshot = list(self._l0_order)
            level_snapshot = dict(self._level_files)

        best: tuple[SeqNum, int, Value] | None = None

        # L0: hold read lock while scanning
        async with self._level_lock(0).read_lock():
            for file_id in l0_snapshot:
                try:
                    with self._registry.open_reader(file_id) as reader:
                        result = reader.get(key)
                except KeyError:
                    continue
                if result is not None and (
                    best is None or result[0] > best[0]
                ):
                    best = result

        # L1+: one file per level
        for level in range(1, self.max_level + 1):
            entry = level_snapshot.get(level)
            if entry is None:
                continue
            fid, _ = entry
            async with self._level_lock(level).read_lock():
                try:
                    with self._registry.open_reader(fid) as reader:
                        result = reader.get(key)
                except KeyError:
                    result = None
                if result is not None and (
                    best is None or result[0] > best[0]
                ):
                    best = result

        return best

    # ── helpers ─────────────────────────────────────────────────────────

    @property
    def cache(self) -> BlockCache:
        """Return the block cache instance."""
        return self._cache

    def compaction_snapshot(self) -> dict[str, object]:
        """Return a snapshot of state needed by CompactionManager."""
        with self._state_lock:
            level_snap: dict[str, tuple[str, str]] = {
                str(lv): (fid, str(d))
                for lv, (fid, d) in self._level_files.items()
            }
            return {
                "l0_order": list(self._l0_order),
                "l0_dirs": {
                    fid: str(d) for fid, d in self._l0_dirs.items()
                },
                "level_files": level_snap,
            }

    def level_seq_min(self, level: Level) -> SeqNum:
        """Return seq_min of the SSTable at *level*, or 0 if none."""
        entry = self._level_files.get(level)
        if entry is None:
            return 0
        fid, _ = entry
        try:
            with self._registry.open_reader(fid) as reader:
                return reader.meta.seq_min
        except KeyError:
            return 0

    def level_size_bytes(self, level: Level) -> int:
        """Return the size in bytes of the SSTable at *level*, or 0."""
        entry = self._level_files.get(level)
        if entry is None:
            return 0
        fid, _ = entry
        try:
            with self._registry.open_reader(fid) as reader:
                return reader.meta.size_bytes
        except KeyError:
            return 0

    def level_record_count(self, level: Level) -> int:
        """Return the record count of the SSTable at *level*, or 0."""
        entry = self._level_files.get(level)
        if entry is None:
            return 0
        fid, _ = entry
        try:
            with self._registry.open_reader(fid) as reader:
                return reader.meta.record_count
        except KeyError:
            return 0

    def max_seq_seen(self) -> SeqNum:
        """Return the highest seq across all SSTables."""
        return self._max_seq

    @property
    def l0_count(self) -> int:
        """Number of L0 SSTables."""
        return len(self._l0_order)

    def sst_dir_for(self, file_id: FileID) -> Path:
        """Return the directory for a given SSTable file ID."""
        return self._data_root / "sstable" / "L0" / file_id

    def new_file_id(self) -> FileID:
        """Generate a new time-ordered UUIDv7 file ID."""
        return uuid7_hex()

    # ── show_disk ────────────────────────────────────────────────────────

    def show_disk(
        self, file_id: FileID | None = None,
    ) -> dict[str, object]:
        """Inspect SSTable contents."""
        if file_id is not None:
            # Detail mode — check all levels
            all_dirs: dict[FileID, Path] = dict(self._l0_dirs)
            for _, (fid, d) in self._level_files.items():
                all_dirs[fid] = d

            if file_id not in all_dirs:
                return {"error": f"No SSTable found with id {file_id!r}"}

            try:
                with self._registry.open_reader(file_id) as reader:
                    meta = reader.meta
                    records = reader.scan_all()
            except KeyError:
                return {"error": f"No SSTable found with id {file_id!r}"}

            return {
                "file_id": meta.file_id,
                "level": meta.level,
                "snapshot_id": meta.snapshot_id,
                "record_count": meta.record_count,
                "block_count": meta.block_count,
                "size_bytes": meta.size_bytes,
                "min_key": meta.min_key,
                "max_key": meta.max_key,
                "seq_min": meta.seq_min,
                "seq_max": meta.seq_max,
                "created_at": meta.created_at,
                "entries": [
                    {"key": k, "seq": seq, "timestamp_ms": ts, "value": v}
                    for k, seq, ts, v in records
                ],
            }

        # Listing mode — by level
        def _meta_list(
            fids: list[FileID],
        ) -> list[dict[str, object]]:
            tables: list[dict[str, object]] = []
            for fid in fids:
                try:
                    with self._registry.open_reader(fid) as reader:
                        m = reader.meta
                        tables.append({
                            "file_id": m.file_id,
                            "record_count": m.record_count,
                            "block_count": m.block_count,
                            "size_bytes": m.size_bytes,
                            "min_key": m.min_key,
                            "max_key": m.max_key,
                            "seq_min": m.seq_min,
                            "seq_max": m.seq_max,
                            "created_at": m.created_at,
                        })
                except KeyError:
                    continue
            return tables

        result: dict[str, object] = {"L0": _meta_list(self._l0_order)}
        for level in range(1, self.max_level + 1):
            entry = self._level_files.get(level)
            fids = [entry[0]] if entry else []
            result[f"L{level}"] = _meta_list(fids)
        return result

    def close_all(self) -> None:
        """Close all open readers."""
        self._registry.close_all()
        logger.info("SSTableManager closed all readers")
