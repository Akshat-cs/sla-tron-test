"""Mirror stdout/stderr to terminal and a timestamped log file."""

from __future__ import annotations

import atexit
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

_LOG_FILE = None
_ORIGINAL_STDOUT = None
_ORIGINAL_STDERR = None


class _Tee:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data: str) -> int:
        if not data:
            return 0
        for stream in self.streams:
            stream.write(data)
            stream.flush()
        return len(data)

    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()

    def isatty(self) -> bool:
        return getattr(self.streams[0], "isatty", lambda: False)()


def setup_run_logging(log_dir: str | None = None) -> Path:
    """
    Duplicate all print() / stdout to logs/sla_<timestamp>.log.
    Call once at start of sla_report.py (or test_nomics_sla.py).
    """
    global _LOG_FILE, _ORIGINAL_STDOUT, _ORIGINAL_STDERR

    if _LOG_FILE is not None:
        return Path(_LOG_FILE.name)

    log_root = Path(log_dir or os.getenv("LOG_DIR", "logs"))
    log_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_path = log_root / f"sla_{stamp}.log"

    _ORIGINAL_STDOUT = sys.stdout
    _ORIGINAL_STDERR = sys.stderr
    _LOG_FILE = open(log_path, "a", encoding="utf-8", buffering=1)

    header = (
        f"# SLA run log started {datetime.now(timezone.utc).isoformat()}\n"
        f"# log file: {log_path.resolve()}\n\n"
    )
    _LOG_FILE.write(header)
    _LOG_FILE.flush()

    sys.stdout = _Tee(_ORIGINAL_STDOUT, _LOG_FILE)
    sys.stderr = _Tee(_ORIGINAL_STDERR, _LOG_FILE)
    atexit.register(close_run_logging)
    return log_path


def close_run_logging() -> None:
    global _LOG_FILE, _ORIGINAL_STDOUT, _ORIGINAL_STDERR
    if _ORIGINAL_STDOUT is not None:
        sys.stdout = _ORIGINAL_STDOUT
    if _ORIGINAL_STDERR is not None:
        sys.stderr = _ORIGINAL_STDERR
    if _LOG_FILE is not None:
        _LOG_FILE.write(
            f"\n# log closed {datetime.now(timezone.utc).isoformat()}\n"
        )
        _LOG_FILE.close()
        _LOG_FILE = None
    _ORIGINAL_STDOUT = None
    _ORIGINAL_STDERR = None

