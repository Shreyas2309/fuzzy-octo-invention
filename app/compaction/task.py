"""CompactionTask — serialisable description of one compaction job.

All fields are primitive types (str, int, list, dict) so the task
can be passed across the ProcessPoolExecutor subprocess boundary.
No live objects, no file handles, no locks.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.types import FileID, Level, SeqNum


@dataclass(frozen=True)
class CompactionTask:
    """Immutable description of one compaction job."""

    task_id: str
    input_file_ids: list[FileID]
    input_dirs: dict[FileID, str]
    output_file_id: FileID
    output_dir: str
    output_level: Level
    seq_cutoff: SeqNum
