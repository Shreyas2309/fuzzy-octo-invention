"""SSTableManager — coordinates all on-disk read-side state.

Owns the block cache, SSTable registry, the L0 file list, and the
persistent **manifest** that tracks SSTable ordering across restarts.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from app.cache.block import BlockCache
from app.common.uuid7 import uuid7_hex
from app.memtable.immutable import ImmutableMemTable
from app.observability import get_logger
from app.sstable.meta import SSTableMeta
from app.sstable.reader import SSTableReader
from app.sstable.registry import SSTableRegistry
from app.sstable.writer import SSTableWriter
from app.types import FileID, Key, SeqNum, Value

if TYPE_CHECKING:
    from app.engine.config import LSMConfig

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Manifest — persistent L0 ordering
# ---------------------------------------------------------------------------


class Manifest:
    """Tracks SSTable ordering on disk via ``manifest.json``.

    The manifest is the authoritative source of L0 order.  It is
    written atomically (temp + ``os.replace``) on every commit.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def read(self) -> list[FileID]:
        """Load L0 order from disk.  Returns ``[]`` if file missing."""
        if not self._path.exists():
            return []
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            l0: list[str] = data.get("l0_order", [])
            logger.info("Manifest loaded", l0_count=len(l0))
            return l0
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Manifest corrupt, rebuilding", error=str(exc))
            return []

    def write(self, l0_order: list[FileID]) -> None:
        """Atomically persist the current L0 order."""
        data = json.dumps({"l0_order": l0_order}, indent=2) + "\n"
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

        logger.debug("Manifest saved", l0_count=len(l0_order))


# ---------------------------------------------------------------------------
# SSTableManager
# ---------------------------------------------------------------------------


class SSTableManager:
    """Manages all on-disk SSTable state."""

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
        self._state_lock = threading.Lock()  # guards _l0_order + _max_seq

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

        # Discover all complete SSTables on disk
        on_disk: dict[FileID, Path] = {}
        if l0_dir.exists():
            for child in l0_dir.iterdir():
                if not child.is_dir():
                    continue
                meta_path = child / "meta.json"
                if not meta_path.exists():
                    logger.warning(
                        "Incomplete SSTable skipped",
                        path=str(child),
                    )
                    continue
                on_disk[child.name] = child

        # Load manifest order — authoritative if present
        manifest_order = manifest.read()

        # Reconcile: use manifest order, but include any on-disk SSTables
        # not in the manifest (e.g., committed but manifest write failed).
        # Fallback: sort by UUIDv7 hex (time-ordered) for those not in manifest.
        ordered: list[FileID] = []
        seen: set[FileID] = set()

        for fid in manifest_order:
            if fid in on_disk:
                ordered.append(fid)
                seen.add(fid)
            else:
                logger.warning(
                    "Manifest references missing SSTable",
                    file_id=fid,
                )

        # Append orphans sorted by name (UUIDv7 hex = time order)
        orphans = sorted(
            (fid for fid in on_disk if fid not in seen),
            reverse=True,  # newest first (UUIDv7 hex sorts chronologically)
        )
        if orphans:
            logger.info("Orphan SSTables found", count=len(orphans))
            ordered.extend(orphans)

        # Open all readers
        l0_order: list[FileID] = []
        for fid in ordered:
            sst_dir = on_disk[fid]
            try:
                reader = await SSTableReader.open(
                    sst_dir, fid, cache, level=0,
                )
            except Exception:
                logger.exception(
                    "Failed to open SSTable",
                    path=str(sst_dir),
                )
                continue

            registry.register(fid, reader)
            l0_order.append(fid)
            l0_dirs[fid] = sst_dir

        mgr = cls(
            data_root, cache, registry, l0_order, l0_dirs, manifest, config,
        )

        # Compute max seq from all open readers
        for fid in l0_order:
            with registry.open_reader(fid) as reader:
                if reader.meta.seq_max > mgr._max_seq:
                    mgr._max_seq = reader.meta.seq_max

        # Persist reconciled order (in case orphans were added)
        if l0_order != manifest_order:
            manifest.write(l0_order)

        logger.info(
            "SSTableManager loaded",
            l0_count=len(l0_order),
            max_seq=mgr._max_seq,
        )
        return mgr

    # ── flush ───────────────────────────────────────────────────────────

    async def flush(
        self,
        snapshot: ImmutableMemTable,
        file_id: FileID,
    ) -> tuple[SSTableMeta, SSTableReader]:
        """Write *snapshot* to a new L0 SSTable, return (meta, reader)."""
        sst_dir = self._data_root / "sstable" / "L0" / file_id

        # Compute block sizing: 1/8th of SSTable → exactly 8 blocks
        block_size = 0
        block_entries = 0
        if self._config is not None and self._config.is_dev:
            # Dev: entry-count based blocks
            block_entries = max(1, len(snapshot) // 8)
        elif self._config is not None and self._config.is_prod:
            # Prod: data-size based blocks (1/8th of memtable size limit)
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

    def commit(self, file_id: FileID, reader: SSTableReader, sst_dir: Path) -> None:
        """Register a flushed SSTable, update L0 order, and persist manifest."""
        self._registry.register(file_id, reader)
        with self._state_lock:
            self._l0_order.insert(0, file_id)  # newest first
            self._l0_dirs[file_id] = sst_dir
            if reader.meta.seq_max > self._max_seq:
                self._max_seq = reader.meta.seq_max

        # Persist the new order atomically
        self._manifest.write(self._l0_order)

        logger.info(
            "SSTable committed",
            file_id=file_id,
            l0_count=len(self._l0_order),
        )

    # ── read path ───────────────────────────────────────────────────────

    async def get(self, key: Key) -> tuple[SeqNum, int, Value] | None:
        """Look up *key* across all L0 SSTables (newest first).

        BUG-12: Reads are serial (mmap is microseconds, asyncio.gather
        overhead would exceed the benefit). Lock-snapshot ensures safe
        iteration during concurrent commits.
        """
        # BUG-08: snapshot under lock to avoid concurrent modification
        with self._state_lock:
            order = list(self._l0_order)

        best: tuple[SeqNum, int, Value] | None = None
        for file_id in order:
            try:
                with self._registry.open_reader(file_id) as reader:
                    result = reader.get(key)
            except KeyError:
                continue
            if result is not None and (best is None or result[0] > best[0]):
                best = result

        return best

    # ── stats / lifecycle ───────────────────────────────────────────────

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
        """Inspect SSTable contents.

        Parameters
        ----------
        file_id:
            **Optional.**  Pass a file ID to see the full record list
            for that SSTable.  When ``None`` (the default), returns a
            summary listing all SSTables organised by level.

        Returns
        -------
        dict
            In listing mode (no *file_id*)::

                {
                    "L0": [
                        {"file_id": ..., "record_count": ..., ...},
                        ...
                    ]
                }

            In detail mode (with *file_id*)::

                {
                    "file_id": ...,
                    "level": ...,
                    "record_count": ...,
                    "entries": [
                        {"key": ..., "seq": ..., "ts": ..., "value": ...},
                        ...
                    ],
                }
        """
        # ── Detail mode ────────────────────────────────────────────────
        if file_id is not None:
            if file_id not in self._l0_dirs:
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
                    {
                        "key": k,
                        "seq": seq,
                        "timestamp_ms": ts,
                        "value": v,
                    }
                    for k, seq, ts, v in records
                ],
            }

        # ── Listing mode ───────────────────────────────────────────────
        l0_tables: list[dict[str, object]] = []
        for fid in self._l0_order:
            try:
                with self._registry.open_reader(fid) as reader:
                    m = reader.meta
                    l0_tables.append({
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

        return {"L0": l0_tables}

    def close_all(self) -> None:
        """Close all open readers."""
        self._registry.close_all()
        logger.info("SSTableManager closed all readers")
