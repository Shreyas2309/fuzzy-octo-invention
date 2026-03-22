"""Terminal endpoint — runs REPL commands and returns text output.

This does NOT execute shell commands. It processes lsm-kv REPL commands
(put, get, del, flush, mem, disk, stats, config, trace, help) against
the in-process LSMEngine and returns formatted text output.
"""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

import web.server as srv

router = APIRouter()

HELP_TEXT = """\
commands:
  put <key> <value>    write a key-value pair
  get <key>            read a value
  del <key>            tombstone a key
  flush                force-flush memtable to SSTable
  mem                  list all active & immutable memtables
  mem <id>             show entries of a specific memtable
  disk                 list all SSTables by level
  disk <id>            show entries of a specific SSTable
  stats                show engine stats
  config               show current config as JSON
  config set <k> <v>   update a config parameter
  trace <key>          trace a lookup step-by-step
  clear                clear the terminal
  help                 show this message"""


class CommandRequest(BaseModel):
    command: str


class CommandResponse(BaseModel):
    output: str
    error: bool = False


def _decode(val: object) -> str:
    """Decode bytes or return str."""
    if isinstance(val, bytes):
        return val.decode(errors="replace")
    return str(val)


async def _run_command(line: str) -> str:
    """Process a single REPL command and return text output."""
    parts = line.strip().split(maxsplit=2)
    if not parts:
        return ""

    cmd, *args = parts
    cmd = cmd.lower()
    e = srv.get_engine()

    if cmd in ("put", "set"):
        if len(args) < 2:
            return "usage: put <key> <value>"
        key_b, val_b = args[0].encode(), args[1].encode()
        srv.wa_user_bytes += len(key_b) + len(val_b)
        await e.put(key_b, val_b)
        s = e.stats()
        return f"OK (seq={s.seq})"

    elif cmd == "get":
        if not args:
            return "usage: get <key>"
        result = await e.get(args[0].encode())
        if result is None:
            return "(nil)"
        return result.decode(errors="replace")

    elif cmd == "del":
        if not args:
            return "usage: del <key>"
        key_b = args[0].encode()
        srv.wa_user_bytes += len(key_b)
        await e.delete(key_b)
        return f"OK (deleted {args[0]})"

    elif cmd == "flush":
        flushed = await e.flush()
        if flushed:
            return "OK (memtable flushed to SSTable)"
        return "OK (nothing to flush — memtable empty)"

    elif cmd == "mem":
        table_id = args[0] if args else None
        result = e.show_mem(table_id)

        if "error" in result:
            return f"error: {result['error']}"

        if table_id is not None:
            lines = [
                f"[{result['type']}] {result['table_id']}  "
                f"({result['entry_count']} entries, "
                f"{result.get('size_bytes', 0)} bytes)"
            ]
            entries = result.get("entries", [])
            if not entries:
                lines.append("  (empty)")
            for entry in entries:  # type: ignore[union-attr]
                k = _decode(entry["key"])  # type: ignore[index]
                v = _decode(entry["value"])  # type: ignore[index]
                seq = entry["seq"]  # type: ignore[index]
                lines.append(f"  seq={seq:<6}  {k} -> {v}")
            return "\n".join(lines)

        # Listing mode
        active = result["active"]
        lines = [
            f"active   {active['table_id']}  "  # type: ignore[index]
            f"entries={active['entry_count']}  "  # type: ignore[index]
            f"size={active['size_bytes']}B"  # type: ignore[index]
        ]
        for i, snap in enumerate(result.get("immutable", [])):  # type: ignore[union-attr]
            lines.append(
                f"imm[{i}]   {snap['snapshot_id']}  "  # type: ignore[index]
                f"entries={snap['entry_count']}  "  # type: ignore[index]
                f"size={snap['size_bytes']}B  "  # type: ignore[index]
                f"seq={snap['seq_min']}..{snap['seq_max']}"  # type: ignore[index]
            )
        if not result.get("immutable"):
            lines.append("(no immutable snapshots)")
        return "\n".join(lines)

    elif cmd == "disk":
        file_id = args[0] if args else None
        result = e.show_disk(file_id)

        if "error" in result:
            return f"error: {result['error']}"

        if file_id is not None:
            min_k = _decode(result.get("min_key", ""))
            max_k = _decode(result.get("max_key", ""))
            lines = [
                f"[L{result['level']}] {result['file_id']}  "
                f"({result['record_count']} records, "
                f"{result['block_count']} blocks, "
                f"{result['size_bytes']} bytes)",
                f"  keys: {min_k} .. {max_k}  "
                f"seq: {result['seq_min']}..{result['seq_max']}  "
                f"created: {result['created_at']}",
            ]
            entries = result.get("entries", [])
            if not entries:
                lines.append("  (empty)")
            for entry in entries:  # type: ignore[union-attr]
                k = _decode(entry["key"])  # type: ignore[index]
                v = _decode(entry["value"])  # type: ignore[index]
                seq = entry["seq"]  # type: ignore[index]
                lines.append(f"  seq={seq:<6}  {k} -> {v}")
            return "\n".join(lines)

        # Listing mode
        lines = []
        for level_name in ("L0", "L1", "L2", "L3"):
            tables = result.get(level_name, [])
            if tables is None:
                continue
            lines.append(f"{level_name}  ({len(tables)} SSTables)")  # type: ignore[arg-type]
            if not tables:
                lines.append("  (empty)")
            for i, t in enumerate(tables):  # type: ignore[union-attr]
                min_k = _decode(t.get("min_key", ""))  # type: ignore[union-attr]
                max_k = _decode(t.get("max_key", ""))  # type: ignore[union-attr]
                lines.append(
                    f"  [{i}] {t['file_id']}  "  # type: ignore[index]
                    f"records={t['record_count']}  "  # type: ignore[index]
                    f"blocks={t['block_count']}  "  # type: ignore[index]
                    f"size={t['size_bytes']}B  "  # type: ignore[index]
                    f"keys={min_k}..{max_k}  "
                    f"seq={t['seq_min']}..{t['seq_max']}"  # type: ignore[index]
                )
        return "\n".join(lines)

    elif cmd == "stats":
        s = e.stats()
        return (
            f"entries={s.key_count}  seq={s.seq}  "
            f"wal={s.wal_entry_count}  "
            f"active={s.active_table_id[:8]}..({s.active_size_bytes}B)  "
            f"immutable_q={s.immutable_queue_len}  "
            f"l0={s.l0_sstable_count}"
        )

    elif cmd == "config":
        if not args:
            return e.config.to_json()
        if len(args) >= 2 and args[0] == "set":
            # "config set <key> <value>" — args came from split(maxsplit=2)
            # so args = ["set", "<key> <value>"] — re-split
            kv_parts = args[1].split(maxsplit=1)
            if len(kv_parts) < 2:
                return "usage: config set <key> <value>"
            cfg_key, cfg_val_str = kv_parts
            try:
                parsed: int | float | str
                if cfg_val_str.isdigit() or (
                    cfg_val_str.startswith("-") and cfg_val_str[1:].isdigit()
                ):
                    parsed = int(cfg_val_str)
                elif "." in cfg_val_str:
                    try:
                        parsed = float(cfg_val_str)
                    except ValueError:
                        parsed = cfg_val_str
                else:
                    parsed = cfg_val_str
                old, new = e.update_config(cfg_key, parsed)
                return f"OK ({cfg_key}: {old} -> {new})"
            except Exception as err:
                return f"error: {err}"
        return "usage: config | config set <key> <value>"

    elif cmd == "trace":
        if not args:
            return "usage: trace <key>"
        from web.routers.kv import kv_trace
        trace_result = await kv_trace(args[0])
        lines = [f"trace: {args[0]}"]
        for step in trace_result.get("steps", []):
            comp = step["component"]
            res = step["result"]
            bloom = step.get("bloom_check", "")
            cache = step.get("block_cache_hit")
            offset = step.get("bisect_offset")
            seq = step.get("seq")

            detail_parts = [f"  [{step['step']}] {comp}: {res}"]
            if bloom:
                detail_parts.append(f"bloom={bloom}")
            if offset is not None:
                detail_parts.append(f"offset={offset}")
            if cache is not None:
                detail_parts.append(f"cache={'HIT' if cache else 'MISS'}")
            if seq is not None:
                detail_parts.append(f"seq={seq}")
            lines.append("  ".join(detail_parts))

        found = trace_result.get("found", False)
        if found:
            lines.append(
                f"-> FOUND: \"{trace_result['value']}\" "
                f"(source={trace_result['source']}, seq={trace_result['seq']})"
            )
        else:
            lines.append("-> NOT FOUND")
        return "\n".join(lines)

    elif cmd == "help":
        return HELP_TEXT

    elif cmd == "clear":
        return "__CLEAR__"

    else:
        return f"unknown command: {cmd!r} -- type 'help'"


@router.post("/run")
async def terminal_run(req: CommandRequest) -> CommandResponse:
    """Process an lsm-kv REPL command and return text output."""
    try:
        output = await _run_command(req.command)
        return CommandResponse(output=output)
    except Exception as err:
        return CommandResponse(output=f"error: {err}", error=True)
