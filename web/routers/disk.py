"""SSTable inspection endpoints."""

from __future__ import annotations

from fastapi import APIRouter

import web.server as srv

router = APIRouter()


@router.get("")
async def disk_list() -> dict[str, object]:
    e = srv.get_engine()
    return e.show_disk()


@router.get("/{file_id}")
async def disk_detail(file_id: str) -> dict[str, object]:
    e = srv.get_engine()
    return e.show_disk(file_id)


@router.get("/{file_id}/meta")
async def disk_meta(file_id: str) -> dict[str, object]:
    e = srv.get_engine()
    # Get the reader directly for meta fields
    try:
        with e._sst._registry.open_reader(file_id) as reader:
            m = reader.meta
            return {
                "file_id": m.file_id,
                "level": m.level,
                "snapshot_id": m.snapshot_id,
                "record_count": m.record_count,
                "block_count": m.block_count,
                "size_bytes": m.size_bytes,
                "min_key": m.min_key.decode(errors="replace"),
                "max_key": m.max_key.decode(errors="replace"),
                "seq_min": m.seq_min,
                "seq_max": m.seq_max,
                "bloom_fpr": m.bloom_fpr,
                "created_at": m.created_at,
                "data_file": m.data_file,
                "index_file": m.index_file,
                "filter_file": m.filter_file,
            }
    except KeyError:
        return {"error": f"SSTable {file_id} not found"}


@router.get("/{file_id}/bloom")
async def disk_bloom(file_id: str) -> dict[str, object]:
    e = srv.get_engine()
    try:
        with e._sst._registry.open_reader(file_id) as reader:
            reader._ensure_loaded()
            bloom = reader._bloom
            if bloom is None:
                return {"error": "Bloom filter not loaded"}
            raw = bloom.to_bytes()
            return {
                "file_id": file_id,
                "hash_count": bloom._num_hashes,
                "bit_count": bloom._bit_count,
                "approx_fpr": round(reader.meta.bloom_fpr, 6),
                "size_bytes": len(raw),
            }
    except KeyError:
        return {"error": f"SSTable {file_id} not found"}


@router.get("/{file_id}/index")
async def disk_index(file_id: str) -> dict[str, object]:
    e = srv.get_engine()
    try:
        with e._sst._registry.open_reader(file_id) as reader:
            reader._ensure_loaded()
            index = reader._index
            if index is None:
                return {"error": "Index not loaded"}
            entries = [
                {
                    "first_key": index._keys[i].decode(errors="replace"),
                    "block_offset": index._offsets[i],
                }
                for i in range(len(index._keys))
            ]
            return {
                "file_id": file_id,
                "entries": entries,
                "entry_count": len(entries),
            }
    except KeyError:
        return {"error": f"SSTable {file_id} not found"}
