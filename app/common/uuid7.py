"""UUIDv7 generator — time-ordered UUIDs (RFC 9562).

UUIDv7 embeds a Unix timestamp in the high 48 bits, making them
monotonically sortable by creation time when compared as strings.
This replaces UUIDv4 for SSTable file IDs so that lexicographic
directory sorting == chronological ordering.

Layout (128 bits):
    [48-bit ms timestamp] [4-bit version=0x7] [12-bit rand_a]
    [2-bit variant=0b10]  [62-bit rand_b]
"""

from __future__ import annotations

import os
import time


def uuid7_hex() -> str:
    """Return a UUIDv7 as a 32-character lowercase hex string."""
    # 48-bit Unix timestamp in milliseconds
    timestamp_ms = int(time.time() * 1000)

    # 10 bytes of randomness (80 bits)
    rand_bytes = os.urandom(10)

    # Build the 128-bit UUID
    # Bits 0-47:  timestamp_ms (48 bits)
    # Bits 48-51: version = 0b0111 (4 bits)
    # Bits 52-63: rand_a (12 bits, from rand_bytes[0:2])
    # Bits 64-65: variant = 0b10 (2 bits)
    # Bits 66-127: rand_b (62 bits, from rand_bytes[2:10])

    rand_a = int.from_bytes(rand_bytes[0:2], "big") & 0x0FFF  # 12 bits
    rand_b = int.from_bytes(rand_bytes[2:10], "big") & 0x3FFF_FFFF_FFFF_FFFF  # 62 bits

    high_64 = (timestamp_ms << 16) | (0x7 << 12) | rand_a
    low_64 = (0b10 << 62) | rand_b

    return f"{high_64:016x}{low_64:016x}"
