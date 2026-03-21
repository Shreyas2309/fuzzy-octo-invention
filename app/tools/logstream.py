"""Live log stream client — connects to the lsm-kv log broadcast server.

Usage:
    python -m app.tools.logstream              # default: 127.0.0.1:9009
    python -m app.tools.logstream 9009         # custom port
    python -m app.tools.logstream 9009 myhost  # custom host + port

Press Ctrl+C to disconnect.
"""

from __future__ import annotations

import socket
import sys


def main(host: str = "127.0.0.1", port: int = 9009) -> None:
    """Connect to the log broadcast server and print lines to stdout."""
    print(f"Connecting to log stream at {host}:{port} ...")

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
    except ConnectionRefusedError:
        print(
            f"Connection refused — is lsm-kv running with log server on port {port}?",
            file=sys.stderr,
        )
        sys.exit(1)

    print("Connected. Streaming logs (Ctrl+C to stop):\n")

    buf = b""
    try:
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                print("\n[server disconnected]")
                break
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                print(line.decode(errors="replace"))
    except KeyboardInterrupt:
        print("\n[disconnected]")
    finally:
        sock.close()


if __name__ == "__main__":
    args = sys.argv[1:]
    _port = int(args[0]) if len(args) >= 1 else 9009
    _host = args[1] if len(args) >= 2 else "127.0.0.1"
    main(host=_host, port=_port)
