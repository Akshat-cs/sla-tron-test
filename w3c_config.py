"""W3C multi-chain SLA settings — defaults, env, and CLI overrides.

Kept fully separate from sla_config.py (Nomics/Tron) so the two scenarios
cannot accidentally clobber each other's globals.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# W3C agreement: provision 72 RPS sustained, ~42 concurrent in flight.
DEFAULT_TARGET_QPS = 72
DEFAULT_CONCURRENCY = 42
DEFAULT_DURATION_SEC = 60
DEFAULT_RESPONSE_TIME_SEC = 10.0
DEFAULT_ERROR_RATE_THRESHOLD = 1.0   # percent; reporting threshold (not a hard pass/fail)


@dataclass(frozen=True)
class W3cSettings:
    target_qps: int
    concurrency: int
    duration_sec: float
    max_response_sec: float
    error_rate_threshold_pct: float

    @property
    def request_timeout_sec(self) -> float:
        """Per-request HTTP timeout always matches the response-time SLA."""
        return self.max_response_sec


_settings: W3cSettings | None = None


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def defaults_from_env() -> W3cSettings:
    return W3cSettings(
        target_qps=_env_int("W3C_TARGET_QPS", DEFAULT_TARGET_QPS),
        concurrency=_env_int("W3C_CONCURRENCY", DEFAULT_CONCURRENCY),
        duration_sec=_env_float("W3C_DURATION_SEC", DEFAULT_DURATION_SEC),
        max_response_sec=_env_float("W3C_RESPONSE_SEC", DEFAULT_RESPONSE_TIME_SEC),
        error_rate_threshold_pct=_env_float(
            "W3C_ERROR_RATE_PCT", DEFAULT_ERROR_RATE_THRESHOLD
        ),
    )


def configure(
    *,
    target_qps: int | None = None,
    concurrency: int | None = None,
    duration_sec: float | None = None,
    max_response_sec: float | None = None,
    error_rate_threshold_pct: float | None = None,
) -> W3cSettings:
    """Apply W3C SLA settings (env defaults, then explicit overrides)."""
    global _settings
    base = _settings if _settings is not None else defaults_from_env()
    _settings = W3cSettings(
        target_qps=target_qps if target_qps is not None else base.target_qps,
        concurrency=concurrency if concurrency is not None else base.concurrency,
        duration_sec=duration_sec if duration_sec is not None else base.duration_sec,
        max_response_sec=(
            max_response_sec if max_response_sec is not None else base.max_response_sec
        ),
        error_rate_threshold_pct=(
            error_rate_threshold_pct
            if error_rate_threshold_pct is not None
            else base.error_rate_threshold_pct
        ),
    )
    return _settings


def get_settings() -> W3cSettings:
    global _settings
    if _settings is None:
        _settings = defaults_from_env()
    return _settings


def reset_settings() -> None:
    global _settings
    _settings = None
