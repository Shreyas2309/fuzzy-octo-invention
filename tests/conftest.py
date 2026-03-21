"""Shared test fixtures for lsm-kv."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from app.observability.logging import reset_logging


@pytest.fixture(autouse=True)
def _disable_tcp_logging_in_tests(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Disable TCP log server in tests to avoid port conflicts."""
    monkeypatch.setenv("LSM_LOG_PORT", "0")
    yield
    reset_logging()
