"""
Detailed log of every unsuccessful HTTP request (sibling to the main run log).

Each entry contains the address, status label, HTTP code, latency, and the
*full* raw error/message captured from Bitquery (if any).

Requests we cancelled client-side (timeout or response > SLA) are explicitly
labeled "WE STOPPED" so they are easy to distinguish from server-side failures.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import IO

_FILE: IO[str] | None = None
_PATH: Path | None = None


_STATUS_LABEL = {
    "response_time": "WE STOPPED (response > SLA)",
    "http_error": "SERVER ERROR (HTTP non-200)",
    "graphql_null": "SERVER ERROR (GraphQL null data — often rate-limit / overload)",
    "graphql_error": "SERVER ERROR (GraphQL errors)",
    "parse_error": "CLIENT ERROR (invalid JSON / schema)",
    "other": "CLIENT / NETWORK ERROR",
    "insufficient": "INSUFFICIENT ROWS (server returned fewer than expected)",
}


def open_unsuccessful_log(main_log_path: Path) -> Path:
    """Open the sibling unsuccessful log next to the main run log."""
    global _FILE, _PATH
    if _FILE is not None and _PATH is not None:
        return _PATH
    base = main_log_path.parent / f"{main_log_path.stem}_unsuccessful.log"
    _PATH = base
    _FILE = open(base, "a", encoding="utf-8", buffering=1)
    _FILE.write(
        f"# Unsuccessful requests for {main_log_path.name}\n"
        f"# Opened {datetime.now(timezone.utc).isoformat()}\n"
        "# Each entry: address, status (WE STOPPED / SERVER ERROR / etc.),\n"
        "# HTTP status, latency, and full raw error from Bitquery (if any).\n\n"
    )
    return base


def close_unsuccessful_log() -> None:
    global _FILE, _PATH
    if _FILE is not None:
        try:
            _FILE.write(
                f"\n# Closed {datetime.now(timezone.utc).isoformat()}\n"
            )
            _FILE.close()
        except Exception:
            pass
    _FILE = None
    _PATH = None


def get_path() -> Path | None:
    return _PATH


def _status_label(category: str) -> str:
    return _STATUS_LABEL.get(category, category.upper())


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"


def _indent_multiline(text: str, indent: str = "                 ") -> str:
    return text.replace("\n", "\n" + indent)


def log_unsuccessful(
    *,
    req_num: int,
    total: int,
    address: str,
    category: str,
    http_status: int,
    latency_ms: float,
    record_count: int,
    expected_count: int | None,
    raw_error: str | None,
    sla_target_sec: float,
) -> None:
    """Append one detailed entry. Safe no-op if the file was never opened."""
    if _FILE is None:
        return
    status = _status_label(category)
    lines = [
        "─" * 80,
        f"[req {req_num:>5}/{total}]  {_ts()}",
        f"  Address:       {address}",
        f"  Status:        {status}",
        f"  Category:      {category}",
        f"  HTTP status:   {http_status if http_status else 'n/a'}",
        f"  Latency:       {latency_ms:.0f} ms (SLA limit {sla_target_sec:g}s)",
        f"  Rows returned: {record_count:,}",
    ]
    if category == "insufficient":
        exp = f"{expected_count:,}" if expected_count is not None else "?"
        lines.append(f"  Expected rows: >= {exp}")
        lines.append(
            "  Note:          server returned 200 OK but with a truncated row set"
        )
    else:
        if raw_error:
            wrapped = _indent_multiline(raw_error)
            lines.append(f"  Bitquery msg:  {wrapped}")
        else:
            lines.append("  Bitquery msg:  (no message captured)")
    lines.append("")
    _FILE.write("\n".join(lines) + "\n")
