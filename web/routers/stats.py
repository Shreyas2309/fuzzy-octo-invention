"""Stats and WAL endpoints."""

from __future__ import annotations

import json

from fastapi import APIRouter

import web.server as srv

router = APIRouter()


@router.get("/stats")
async def stats_snapshot() -> dict[str, object]:
    e = srv.get_engine()
    s = e.stats()
    cm = e._compaction
    return {
        "key_count": s.key_count,
        "seq": s.seq,
        "wal_entry_count": s.wal_entry_count,
        "active_table_id": s.active_table_id,
        "active_size_bytes": s.active_size_bytes,
        "immutable_queue_len": s.immutable_queue_len,
        "l0_sstable_count": s.l0_sstable_count,
        "compaction_active": len(cm.active_jobs) > 0 if cm else False,
    }


@router.get("/stats/history")
async def stats_history() -> dict[str, object]:
    return {"samples": list(srv.stats_history)}


@router.get("/stats/write-amp")
async def write_amplification() -> dict[str, object]:
    """Write amplification = total disk bytes written / user bytes written.

    Disk bytes written is computed as:
      - Sum of all current SSTable sizes on disk (created by flushes + latest compaction)
      - Plus sum of output_bytes from historical compaction commits in compaction.log
        (these represent compaction rewrites whose output was later replaced)
    This gives cumulative bytes the engine has written to disk over its lifetime.
    """
    e = srv.get_engine()
    user_bytes = srv.wa_user_bytes

    # Current SSTable sizes on disk
    disk_listing = e.show_disk()
    current_disk_bytes = 0
    for level_name in ("L0", "L1", "L2", "L3"):
        for table in (disk_listing.get(level_name) or []):
            current_disk_bytes += table.get("size_bytes", 0)  # type: ignore[union-attr]

    # Historical compaction output bytes from compaction.log
    # Each committed compaction wrote output_bytes to disk; if it was later
    # replaced by a newer compaction, those bytes are no longer in current_disk
    # but they WERE written.
    compaction_historical_bytes = 0
    current_file_ids: set[str] = set()
    for level_name in ("L0", "L1", "L2", "L3"):
        for table in (disk_listing.get(level_name) or []):
            current_file_ids.add(table.get("file_id", ""))  # type: ignore[union-attr]

    log_path = e.data_root / "compaction.log"
    if log_path.exists():
        try:
            for line in log_path.read_text(encoding="utf-8").strip().splitlines():
                if not line.strip():
                    continue
                entry = json.loads(line)
                if entry.get("event") == "committed":
                    out_bytes = entry.get("output_bytes", 0)
                    out_fid = entry.get("output", "")
                    # Only count if this output file is no longer on disk
                    # (it was replaced by a later compaction)
                    if out_fid not in current_file_ids:
                        compaction_historical_bytes += out_bytes
        except (json.JSONDecodeError, OSError):
            pass

    disk_bytes = current_disk_bytes + compaction_historical_bytes

    # WAL bytes (also written to disk, often overlooked)
    wal_path = e._wal._wal.path
    wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0

    total_disk_bytes = disk_bytes + wal_bytes

    if user_bytes == 0:
        ratio = 0.0
    else:
        ratio = round(total_disk_bytes / user_bytes, 2)

    return {
        "user_bytes_written": user_bytes,
        "disk_bytes_current_sstables": current_disk_bytes,
        "disk_bytes_compaction_historical": compaction_historical_bytes,
        "disk_bytes_wal": wal_bytes,
        "disk_bytes_total": total_disk_bytes,
        "write_amplification": ratio,
    }


@router.get("/wal")
async def wal_info() -> dict[str, object]:
    e = srv.get_engine()
    wal_path = e._wal._wal.path
    entries = e._wal.replay()
    size = wal_path.stat().st_size if wal_path.exists() else 0
    last_seq = entries[-1].seq if entries else 0
    return {
        "entry_count": len(entries),
        "size_bytes": size,
        "last_seq": last_seq,
        "path": str(wal_path),
    }
