# main.py — Interactive REPL for lsm-kv
from __future__ import annotations

import asyncio
import signal

from prompt_toolkit import PromptSession
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.styles import Style

from app.engine import LSMEngine
from app.types import TOMBSTONE

STYLE = Style.from_dict({"prompt": "ansigreen bold italic"})

HELP = """
commands:
  put|set <key> <value>  write a key-value pair
  get <key>              read a value
  del <key>              tombstone a key
  flush                  force-flush memtable to SSTable
  mem                    list all active & immutable memtables
  mem <id>               show entries of a specific memtable
  disk                   list all SSTables by level
  disk <id>              show entries of a specific SSTable
  stats                  show engine stats
  config                 show current config as JSON
  config set <k> <v>     update a config parameter
  trace <key>            trace a lookup step-by-step
  help                   show this message
  exit / quit            exit the REPL

live logs:
  python -m app.tools.logstream          # tail logs from another terminal
"""


async def handle(engine: LSMEngine, line: str) -> None:
    """Parse and execute a single REPL command."""
    parts = line.strip().split(maxsplit=2)
    if not parts:
        return

    cmd, *args = parts

    match cmd:
        case "put" | "set":
            if len(args) < 2:
                print("usage: put <key> <value>")
                return
            key, value = args[0].encode(), args[1].encode()
            await engine.put(key, value)
            print("OK")

        case "get":
            if not args:
                print("usage: get <key>")
                return
            key = args[0].encode()
            result = await engine.get(key)
            print(result.decode() if result else "(nil)")

        case "del":
            if not args:
                print("usage: del <key>")
                return
            key = args[0].encode()
            await engine.delete(key)
            print(f"OK (deleted {args[0]})")

        case "mem":
            table_id = args[0] if args else None
            result = engine.show_mem(table_id)

            if "error" in result:
                print(f"error: {result['error']}")
            elif table_id is not None:
                # Detail mode — show entries
                print(
                    f"[{result['type']}] {result['table_id']}  "
                    f"({result['entry_count']} entries, "
                    f"{result.get('size_bytes', 0)} bytes)"
                )
                entries = result.get("entries", [])
                if not entries:
                    print("  (empty)")
                for e in entries:  # type: ignore[union-attr]
                    key = e["key"]  # type: ignore[index]
                    val = e["value"]  # type: ignore[index]
                    seq = e["seq"]  # type: ignore[index]
                    k_str = key.decode(errors="replace") if isinstance(key, bytes) else str(key)
                    v_str = val.decode(errors="replace") if isinstance(val, bytes) else str(val)
                    print(f"  seq={seq:<6}  {k_str} → {v_str}")
            else:
                # Listing mode — summarise
                active = result["active"]
                print(
                    f"active   {active['table_id']}  "  # type: ignore[index]
                    f"entries={active['entry_count']}  "  # type: ignore[index]
                    f"size={active['size_bytes']}B"  # type: ignore[index]
                )
                immutables = result.get("immutable", [])
                if not immutables:
                    print("(no immutable snapshots)")
                for i, snap in enumerate(immutables):  # type: ignore[union-attr]
                    print(
                        f"imm[{i}]   {snap['snapshot_id']}  "  # type: ignore[index]
                        f"entries={snap['entry_count']}  "  # type: ignore[index]
                        f"size={snap['size_bytes']}B  "  # type: ignore[index]
                        f"seq={snap['seq_min']}..{snap['seq_max']}"  # type: ignore[index]
                    )

        case "disk":
            file_id = args[0] if args else None
            result = engine.show_disk(file_id)

            if "error" in result:
                print(f"error: {result['error']}")
            elif file_id is not None:
                # Detail mode — show SSTable entries
                min_k = result.get("min_key", b"")
                max_k = result.get("max_key", b"")
                min_s = min_k.decode(errors="replace") if isinstance(min_k, bytes) else str(min_k)
                max_s = max_k.decode(errors="replace") if isinstance(max_k, bytes) else str(max_k)
                print(
                    f"[L{result['level']}] {result['file_id']}  "
                    f"({result['record_count']} records, "
                    f"{result['block_count']} blocks, "
                    f"{result['size_bytes']} bytes)"
                )
                print(
                    f"  keys: {min_s} .. {max_s}  "
                    f"seq: {result['seq_min']}..{result['seq_max']}  "
                    f"created: {result['created_at']}"
                )
                entries = result.get("entries", [])
                if not entries:
                    print("  (empty)")
                for e in entries:  # type: ignore[union-attr]
                    key = e["key"]  # type: ignore[index]
                    val = e["value"]  # type: ignore[index]
                    seq = e["seq"]  # type: ignore[index]
                    k_str = key.decode(errors="replace") if isinstance(key, bytes) else str(key)
                    v_str = val.decode(errors="replace") if isinstance(val, bytes) else str(val)
                    print(f"  seq={seq:<6}  {k_str} → {v_str}")
            else:
                # Listing mode — by level
                for level_name in ("L0", "L1", "L2", "L3"):
                    tables = result.get(level_name, [])
                    if tables is None:
                        continue
                    print(f"{level_name}  ({len(tables)} SSTables)")  # type: ignore[arg-type]
                    if not tables:
                        print("  (empty)")
                    for i, t in enumerate(tables):  # type: ignore[union-attr]
                        min_k = t.get("min_key", b"")  # type: ignore[union-attr]
                        max_k = t.get("max_key", b"")  # type: ignore[union-attr]
                        min_s = min_k.decode(errors="replace") if isinstance(min_k, bytes) else str(min_k)
                        max_s = max_k.decode(errors="replace") if isinstance(max_k, bytes) else str(max_k)
                        print(
                            f"  [{i}] {t['file_id']}  "  # type: ignore[index]
                            f"records={t['record_count']}  "  # type: ignore[index]
                            f"blocks={t['block_count']}  "  # type: ignore[index]
                            f"size={t['size_bytes']}B  "  # type: ignore[index]
                            f"keys={min_s}..{max_s}  "
                            f"seq={t['seq_min']}..{t['seq_max']}"  # type: ignore[index]
                        )

        case "flush":
            flushed = await engine.flush()
            if flushed:
                print("OK (memtable flushed to SSTable)")
            else:
                print("OK (nothing to flush — memtable empty)")

        case "stats":
            s = engine.stats()
            print(
                f"entries={s.key_count}  seq={s.seq}  "
                f"wal={s.wal_entry_count}  "
                f"active={s.active_table_id[:8]}..({s.active_size_bytes}B)  "
                f"immutable_q={s.immutable_queue_len}"
            )

        case "config":
            if not args:
                print(engine.config.to_json())
            elif len(args) >= 3 and args[0] == "set":
                cfg_key = args[1]
                cfg_val = args[2]
                try:
                    parsed: int | float | str
                    if cfg_val.isdigit() or (
                        cfg_val.startswith("-") and cfg_val[1:].isdigit()
                    ):
                        parsed = int(cfg_val)
                    elif "." in cfg_val:
                        try:
                            parsed = float(cfg_val)
                        except ValueError:
                            parsed = cfg_val
                    else:
                        parsed = cfg_val
                    old, new = engine.update_config(cfg_key, parsed)
                    print(f"OK ({cfg_key}: {old} → {new})")
                except Exception as exc:
                    print(f"error: {exc}")
            else:
                print("usage: config | config set <key> <value>")

        case "trace":
            if not args:
                print("usage: trace <key>")
                return
            raw_key = args[0].encode()
            steps: list[dict[str, object]] = []
            found_value: bytes | None = None
            found_source: str | None = None
            found_seq: int | None = None
            step_num = 0

            # 1. Active memtable
            step_num += 1
            result = engine._mem._active.get(raw_key)
            if result is not None:
                seq, value = result
                is_tomb = value == TOMBSTONE
                tag = "TOMBSTONE" if is_tomb else "HIT"
                print(f"  [{step_num}] active_memtable: {tag}  seq={seq}")
                if not is_tomb:
                    found_value, found_source, found_seq = value, "active_memtable", seq
            else:
                print(f"  [{step_num}] active_memtable: MISS")

            # 2. Immutable snapshots
            if found_value is None:
                for i, table in enumerate(engine._mem._immutable_q):
                    step_num += 1
                    result = table.get(raw_key)
                    if result is not None:
                        seq, value = result
                        is_tomb = value == TOMBSTONE
                        tag = "TOMBSTONE" if is_tomb else "HIT"
                        print(f"  [{step_num}] immutable_{i}: {tag}  seq={seq}")
                        if not is_tomb:
                            found_value, found_source, found_seq = value, f"immutable_{i}", seq
                        break
                    else:
                        print(f"  [{step_num}] immutable_{i}: MISS")

            # 3. L0 SSTables
            if found_value is None:
                with engine._sst._state_lock:
                    l0_snap = list(engine._sst._l0_order)
                for fid in l0_snap:
                    step_num += 1
                    try:
                        with engine._sst._registry.open_reader(fid) as reader:
                            reader._ensure_loaded()
                            bloom_hit = reader._bloom.may_contain(raw_key)  # type: ignore[union-attr]
                            if not bloom_hit:
                                print(f"  [{step_num}] l0:{fid[:12]}: BLOOM_SKIP  bloom=negative")
                                continue
                            offset = reader._index.floor_offset(raw_key)  # type: ignore[union-attr]
                            cache_hit = (
                                reader._cache is not None
                                and offset is not None
                                and reader._cache.get(fid, offset) is not None
                            )
                            val = reader.get(raw_key)
                            if val is not None:
                                seq, _, value = val
                                is_tomb = value == TOMBSTONE
                                tag = "TOMBSTONE" if is_tomb else "FOUND"
                                print(
                                    f"  [{step_num}] l0:{fid[:12]}: {tag}  "
                                    f"bloom=positive  offset={offset}  "
                                    f"cache={'HIT' if cache_hit else 'MISS'}  seq={seq}"
                                )
                                if not is_tomb:
                                    found_value, found_source, found_seq = value, f"l0:{fid[:12]}", seq
                                break
                            else:
                                print(
                                    f"  [{step_num}] l0:{fid[:12]}: MISS (false positive)  "
                                    f"bloom=positive  offset={offset}  "
                                    f"cache={'HIT' if cache_hit else 'MISS'}"
                                )
                    except KeyError:
                        continue

            # 4. L1+ SSTables
            if found_value is None:
                with engine._sst._state_lock:
                    level_snap = dict(engine._sst._level_files)
                for level in range(1, engine._sst.max_level + 1):
                    entry = level_snap.get(level)
                    if entry is None:
                        continue
                    fid, _ = entry
                    step_num += 1
                    try:
                        with engine._sst._registry.open_reader(fid) as reader:
                            reader._ensure_loaded()
                            bloom_hit = reader._bloom.may_contain(raw_key)  # type: ignore[union-attr]
                            if not bloom_hit:
                                print(f"  [{step_num}] l{level}:{fid[:12]}: BLOOM_SKIP  bloom=negative")
                                continue
                            offset = reader._index.floor_offset(raw_key)  # type: ignore[union-attr]
                            cache_hit = (
                                reader._cache is not None
                                and offset is not None
                                and reader._cache.get(fid, offset) is not None
                            )
                            val = reader.get(raw_key)
                            if val is not None:
                                seq, _, value = val
                                is_tomb = value == TOMBSTONE
                                tag = "TOMBSTONE" if is_tomb else "FOUND"
                                print(
                                    f"  [{step_num}] l{level}:{fid[:12]}: {tag}  "
                                    f"bloom=positive  offset={offset}  "
                                    f"cache={'HIT' if cache_hit else 'MISS'}  seq={seq}"
                                )
                                if not is_tomb:
                                    found_value, found_source, found_seq = value, f"l{level}:{fid[:12]}", seq
                                break
                            else:
                                print(
                                    f"  [{step_num}] l{level}:{fid[:12]}: MISS (false positive)  "
                                    f"bloom=positive  offset={offset}  "
                                    f"cache={'HIT' if cache_hit else 'MISS'}"
                                )
                    except KeyError:
                        continue

            # Result
            if found_value is not None:
                print(
                    f"→ FOUND: \"{found_value.decode(errors='replace')}\" "
                    f"(source={found_source}, seq={found_seq})"
                )
            else:
                print("→ NOT FOUND")

        case "help":
            print(HELP)

        case "exit" | "quit":
            raise SystemExit(0)

        case _:
            print(f"unknown command: {cmd!r}  — type 'help'")


async def main() -> None:
    """Open the engine, run the REPL, close on exit."""
    engine = await LSMEngine.open()
    shutdown_event = asyncio.Event()

    # ── Signal handlers for graceful shutdown ─────────────────────────────

    loop = asyncio.get_running_loop()

    def _signal_handler() -> None:
        print("\nShutting down...")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # ── REPL ──────────────────────────────────────────────────────────────

    session: PromptSession[str] = PromptSession(
        history=InMemoryHistory(),
        auto_suggest=AutoSuggestFromHistory(),
        style=STYLE,
    )

    print(f"lsm-kv  —  data at {engine.data_root}")
    if engine.log_port:
        print(
            f"log stream on port {engine.log_port}  "
            f"(tail: python -m app.tools.logstream)"
        )
    print(f"log file at {engine.data_root / 'logs' / 'lsm-kv.log'}")
    print("type 'help' to list commands")

    try:
        while not shutdown_event.is_set():
            try:
                line = await asyncio.to_thread(
                    session.prompt,
                    [("class:prompt", "lsm-kv"), ("", "> ")],
                )
                await handle(engine, line)

            except KeyboardInterrupt:
                continue
            except EOFError:
                break
            except SystemExit:
                break
    finally:
        await engine.close()
        print("bye")


if __name__ == "__main__":
    asyncio.run(main())
