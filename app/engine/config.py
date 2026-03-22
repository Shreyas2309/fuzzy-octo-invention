"""LSMConfig — runtime-mutable, disk-persisted engine configuration.

Loaded at startup from ``<data_root>/config.json``.  If the file does
not exist, defaults are written.  Updates via :meth:`set` are written
to disk synchronously (atomic write via temp + ``os.replace``).

All consumers hold a reference to the same :class:`LSMConfig` instance
and read fields directly — always getting the current value.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path

from app.common.errors import LSMError
from app.observability import get_logger

logger = get_logger(__name__)


class ConfigError(LSMError):
    """Configuration load or save failed."""


# ---------------------------------------------------------------------------
# Default values
# ---------------------------------------------------------------------------

_DEFAULTS: dict[str, int | float | str] = {
    "env": "dev",
    "max_memtable_size_mb": 64,
    "max_memtable_entries": 10,
    "immutable_queue_max_len": 4,
    "backpressure_timeout": 60.0,
    "l0_compaction_threshold": 10,
    "compaction_check_interval": 0.5,
    "block_size": 4096,
    "flush_max_workers": 2,
    "compaction_max_workers": 1,
    "max_levels": 3,
    "cache_data_entry_limit": 256,
    "cache_index_entry_limit": 64,
    "cache_bloom_entry_limit": 64,
}

_VALID_ENV_VALUES: set[str] = {"dev", "prod"}


# ---------------------------------------------------------------------------
# LSMConfig
# ---------------------------------------------------------------------------


class LSMConfig:
    """Thread-safe, disk-persisted engine configuration.

    Read any field as an attribute::

        config.max_memtable_size_mb   # → 64
        config.max_memtable_bytes     # → 67108864 (convenience)

    Update at runtime::

        config.set("max_memtable_entries", 500_000)
        # writes to disk synchronously, all further reads see new value
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.RLock()
        self._data: dict[str, int | float | str] = {}
        self._load()

    # ── load / save ───────────────────────────────────────────────────────

    def _load(self) -> None:
        """Load from disk, or write defaults if file doesn't exist."""
        if self._path.exists():
            try:
                raw = self._path.read_text(encoding="utf-8")
                stored = json.loads(raw)
                # Merge with defaults so new keys are added on upgrade
                self._data = {**_DEFAULTS, **stored}
                logger.info(
                    "Config loaded from disk",
                    path=str(self._path),
                    keys=len(self._data),
                )
            except (json.JSONDecodeError, OSError) as exc:
                logger.error(
                    "Config file corrupt, using defaults",
                    path=str(self._path),
                    error=str(exc),
                )
                
                self._data = dict(_DEFAULTS)
        else:
            self._data = dict(_DEFAULTS)
            logger.info("Config created with defaults")

        # Always persist (ensures new default keys are written)
        self._save()

    def _save(self) -> None:
        """Atomically write config to disk (temp + os.replace)."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            tmp_fd = tempfile.NamedTemporaryFile(  # noqa: SIM115
                dir=self._path.parent,
                delete=False,
                mode="w",
                encoding="utf-8",
                suffix=".tmp",
            )
            tmp_path = Path(tmp_fd.name)
            try:
                json.dump(self._data, tmp_fd, indent=2)
                tmp_fd.write("\n")
                tmp_fd.flush()
                os.fsync(tmp_fd.fileno())
                tmp_fd.close()
                os.replace(tmp_path, self._path)
            except BaseException:
                tmp_fd.close()
                tmp_path.unlink(missing_ok=True)
                raise
        except OSError as exc:
            logger.error(
                "Config save failed", path=str(self._path), error=str(exc),
            )
            raise ConfigError(
                f"Failed to save config to {self._path}: {exc}"
            ) from exc

        logger.debug("Config saved", path=str(self._path))

    # ── read ──────────────────────────────────────────────────────────────

    def __getattr__(self, name: str) -> int | float | str:
        """Read a config field by name. Thread-safe."""
        # Avoid recursion during __init__
        if name.startswith("_"):
            raise AttributeError(name)
        with self._lock:
            if name in self._data:
                return self._data[name]
        raise AttributeError(
            f"Unknown config key: {name!r}. "
            f"Valid keys: {', '.join(sorted(_DEFAULTS))}"
        )

    @property
    def is_dev(self) -> bool:
        """True when env is 'dev'."""
        with self._lock:
            return self._data["env"] == "dev"

    @property
    def is_prod(self) -> bool:
        """True when env is 'prod'."""
        with self._lock:
            return self._data["env"] == "prod"

    @property
    def max_memtable_bytes(self) -> int:
        """Convenience: max_memtable_size_mb converted to bytes."""
        with self._lock:
            return int(self._data["max_memtable_size_mb"]) * 1024 * 1024

    # ── update ────────────────────────────────────────────────────────────

    def set(
        self, key: str, value: int | float | str,
    ) -> tuple[int | float | str, int | float | str]:
        """Update a config field and persist to disk.

        Returns ``(old_value, new_value)``.
        Raises :class:`ConfigError` if the key is unknown or the value
        is invalid.
        """
        if key not in _DEFAULTS:
            raise ConfigError(
                f"Unknown config key: {key!r}. "
                f"Valid keys: {', '.join(sorted(_DEFAULTS))}"
            )

        expected_type = type(_DEFAULTS[key])

        # String fields: validate against allowed values
        if expected_type is str:
            str_value = str(value)
            if key == "env" and str_value not in _VALID_ENV_VALUES:
                raise ConfigError(
                    f"Invalid value for {key}: {str_value!r}. "
                    f"Must be one of {sorted(_VALID_ENV_VALUES)}"
                )
            cast_value: int | float | str = str_value
        else:
            try:
                cast_value = expected_type(value)
            except (ValueError, TypeError) as exc:
                raise ConfigError(
                    f"Invalid value for {key}: {value!r} "
                    f"(expected {expected_type.__name__})"
                ) from exc

            if cast_value <= 0:  # type: ignore[operator]
                raise ConfigError(
                    f"Value for {key} must be positive, got {cast_value}"
                )

        with self._lock:
            old = self._data[key]
            self._data[key] = cast_value
            self._save()

        logger.info(
            "Config updated", key=key, old=old, new=cast_value,
        )
        return (old, cast_value)

    # ── serialisation ─────────────────────────────────────────────────────

    def to_dict(self) -> dict[str, int | float | str]:
        """Return a snapshot of all config values."""
        with self._lock:
            return dict(self._data)

    def to_json(self, indent: int = 2) -> str:
        """Return config as a formatted JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    # ── factory ───────────────────────────────────────────────────────────

    @classmethod
    def load(
        cls,
        data_root: Path,
        config_path: Path | None = None,
    ) -> LSMConfig:
        """Load config from *config_path*, defaulting to ``<data_root>/config.json``."""
        path = config_path or (data_root / "config.json")
        return cls(path)
