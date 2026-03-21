"""SSTableMeta — frozen dataclass for per-SSTable metadata.

Serialized to ``meta.json`` inside each SSTable directory.
Its presence on disk is the completeness signal — if missing,
the SSTable is considered incomplete and ignored on recovery.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass

from app.types import FileID, Key, Level, SeqNum, SnapshotID


@dataclass(frozen=True)
class SSTableMeta:
    """Immutable metadata for one SSTable."""

    file_id: FileID
    snapshot_id: SnapshotID
    level: Level
    size_bytes: int
    record_count: int
    block_count: int
    min_key: Key
    max_key: Key
    seq_min: SeqNum
    seq_max: SeqNum
    bloom_fpr: float
    created_at: str
    data_file: str
    index_file: str
    filter_file: str

    def to_json(self) -> str:
        """Serialize to JSON with base64-encoded keys."""
        d = {
            "file_id": self.file_id,
            "snapshot_id": self.snapshot_id,
            "level": self.level,
            "size_bytes": self.size_bytes,
            "record_count": self.record_count,
            "block_count": self.block_count,
            "min_key": base64.b64encode(self.min_key).decode("ascii"),
            "max_key": base64.b64encode(self.max_key).decode("ascii"),
            "seq_min": self.seq_min,
            "seq_max": self.seq_max,
            "bloom_fpr": self.bloom_fpr,
            "created_at": self.created_at,
            "data_file": self.data_file,
            "index_file": self.index_file,
            "filter_file": self.filter_file,
        }
        return json.dumps(d, indent=2)

    @classmethod
    def from_json(cls, data: str) -> SSTableMeta:
        """Deserialize from JSON."""
        d = json.loads(data)
        return cls(
            file_id=d["file_id"],
            snapshot_id=d["snapshot_id"],
            level=d["level"],
            size_bytes=d["size_bytes"],
            record_count=d["record_count"],
            block_count=d["block_count"],
            min_key=base64.b64decode(d["min_key"]),
            max_key=base64.b64decode(d["max_key"]),
            seq_min=d["seq_min"],
            seq_max=d["seq_max"],
            bloom_fpr=d["bloom_fpr"],
            created_at=d["created_at"],
            data_file=d["data_file"],
            index_file=d["index_file"],
            filter_file=d["filter_file"],
        )
