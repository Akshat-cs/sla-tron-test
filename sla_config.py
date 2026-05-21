"""Nomics SLA thresholds — defaults, env, and CLI overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass

# Default constants (Nomics agreement)
PEAK_QPS = 4
MAX_RECORDS_PER_QUERY = 20_000
MAX_QUERY_RESPONSE_SEC = 5
MAX_DATA_FRESHNESS_SEC = 60


@dataclass(frozen=True)
class SlaSettings:
    peak_qps: int
    max_query_response_sec: float
    max_data_freshness_sec: float

    @property
    def request_timeout_sec(self) -> float:
        """HTTP timeout always matches the response-time SLA."""
        return self.max_query_response_sec


_settings: SlaSettings | None = None


def defaults_from_env() -> SlaSettings:
    return SlaSettings(
        peak_qps=int(os.getenv("SLA_PEAK_QPS", str(PEAK_QPS))),
        max_query_response_sec=float(
            os.getenv("SLA_RESPONSE_SEC", os.getenv("SLA_RESPONSE_TIME_SEC", str(MAX_QUERY_RESPONSE_SEC)))
        ),
        max_data_freshness_sec=float(
            os.getenv("SLA_FRESHNESS_SEC", str(MAX_DATA_FRESHNESS_SEC))
        ),
    )


def configure(
    *,
    peak_qps: int | None = None,
    max_query_response_sec: float | None = None,
    max_data_freshness_sec: float | None = None,
) -> SlaSettings:
    """Apply SLA settings (CLI/env defaults, then explicit overrides)."""
    global _settings
    base = _settings if _settings is not None else defaults_from_env()
    _settings = SlaSettings(
        peak_qps=peak_qps if peak_qps is not None else base.peak_qps,
        max_query_response_sec=(
            max_query_response_sec
            if max_query_response_sec is not None
            else base.max_query_response_sec
        ),
        max_data_freshness_sec=(
            max_data_freshness_sec
            if max_data_freshness_sec is not None
            else base.max_data_freshness_sec
        ),
    )
    return _settings


def get_settings() -> SlaSettings:
    global _settings
    if _settings is None:
        _settings = defaults_from_env()
    return _settings


def reset_settings() -> None:
    """Reset to env/defaults (useful in tests)."""
    global _settings
    _settings = None
