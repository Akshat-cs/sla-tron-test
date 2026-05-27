"""W3C SLA metrics + report rendering.

Separate from sla_metrics.py (Nomics/Tron) — the two scenarios measure different
things. The W3C test cares about: sustained throughput, response-time percentiles,
and per-chain success rate. There is NO row-count check (the W3C query is an
aggregation that returns 0-2 result rows by design).
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from w3c_client import W3cQueryOutcome
from w3c_config import get_settings

_HTTP_STATUS_RE = re.compile(r"^http\s+(\d{3})", re.IGNORECASE)


def _extract_http_status(err: str | None) -> int | None:
    if not err:
        return None
    m = _HTTP_STATUS_RE.match(err.strip())
    return int(m.group(1)) if m else None


def _normalize_sample(err: str | None, max_len: int = 110) -> str:
    if not err:
        return "(no message)"
    s = re.sub(r"\s+", " ", err.strip().replace("\n", " "))
    return s if len(s) <= max_len else s[:max_len] + "…"


# Disjoint failure categories (rendered in this order).
_FAIL_CATEGORIES: list[tuple[str, str]] = [
    ("response_time", "Response time > {sla:g}s (timeout or slow response)"),
    ("http_error", "HTTP error (non-200)"),
    ("graphql_null", "GraphQL null data (often rate-limit / overload)"),
    ("graphql_error", "GraphQL errors"),
    ("parse_error", "Parse error (invalid JSON / schema)"),
    ("other", "Other (connection / unknown)"),
]


def categorize_failure(error_str: str | None) -> str:
    if not error_str:
        return "other"
    s = error_str.lower()
    if "request timeout" in s or ("exceeded" in s and "sla" in s):
        return "response_time"
    if s.startswith("http "):
        return "http_error"
    if "invalid json" in s or "could not parse" in s:
        return "parse_error"
    if "is null" in s:
        return "graphql_null"
    if s.startswith("[{") or '"message"' in s:
        return "graphql_error"
    return "other"


def is_w3c_success(outcome: W3cQueryOutcome, sla_sec: float) -> bool:
    """Success = no error and response under SLA."""
    return outcome.error is None and outcome.latency_ms <= sla_sec * 1000


def _percentile(sorted_values: list[float], pct: float) -> float:
    """Plain rank-based percentile (no interpolation); returns 0.0 on empty."""
    if not sorted_values:
        return 0.0
    if pct <= 0:
        return sorted_values[0]
    if pct >= 100:
        return sorted_values[-1]
    rank = (pct / 100.0) * (len(sorted_values) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = rank - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


@dataclass
class LatencyDistribution:
    count: int
    avg_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float

    @classmethod
    def from_values(cls, values: list[float]) -> LatencyDistribution:
        if not values:
            return cls(0, 0.0, 0.0, 0.0, 0.0, 0.0)
        sorted_vals = sorted(values)
        return cls(
            count=len(values),
            avg_ms=sum(values) / len(values),
            p50_ms=_percentile(sorted_vals, 50),
            p95_ms=_percentile(sorted_vals, 95),
            p99_ms=_percentile(sorted_vals, 99),
            max_ms=sorted_vals[-1],
        )


@dataclass
class ChainBreakdown:
    chain: str
    total: int
    success: int
    failure_by_category: dict[str, int] = field(default_factory=dict)
    latency: LatencyDistribution = field(
        default_factory=lambda: LatencyDistribution(0, 0.0, 0.0, 0.0, 0.0, 0.0)
    )
    latency_successful: LatencyDistribution = field(
        default_factory=lambda: LatencyDistribution(0, 0.0, 0.0, 0.0, 0.0, 0.0)
    )

    @property
    def success_pct(self) -> float:
        return (100.0 * self.success / self.total) if self.total else 0.0


@dataclass
class W3cReport:
    target_qps: int
    concurrency_cap: int
    duration_sec: float
    sla_response_sec: float
    error_rate_threshold_pct: float

    # Filled in by classify(...)
    total: int = 0
    success: int = 0
    failure: int = 0
    failure_by_category: dict[str, int] = field(default_factory=dict)
    http_status_counts: Counter[int] = field(default_factory=Counter)
    category_samples: dict[str, Counter[str]] = field(default_factory=dict)

    # Throughput context
    scheduled_count: int = 0
    scheduling_wall_sec: float = 0.0
    completion_wall_sec: float = 0.0
    peak_in_flight: int = 0

    latency_overall: LatencyDistribution = field(
        default_factory=lambda: LatencyDistribution(0, 0.0, 0.0, 0.0, 0.0, 0.0)
    )
    latency_successful: LatencyDistribution = field(
        default_factory=lambda: LatencyDistribution(0, 0.0, 0.0, 0.0, 0.0, 0.0)
    )

    per_chain: list[ChainBreakdown] = field(default_factory=list)
    unsuccessful_log_path: Path | None = None
    notes: list[str] = field(default_factory=list)

    @property
    def achieved_qps_sustained(self) -> float:
        """
        Achieved sustained QPS = how fast we actually launched requests.
        Uses scheduling wall time (open-loop launch rate), not response time.
        """
        if self.scheduling_wall_sec <= 0:
            return 0.0
        return self.scheduled_count / self.scheduling_wall_sec

    @property
    def success_pct(self) -> float:
        return (100.0 * self.success / self.total) if self.total else 0.0

    @property
    def failure_pct(self) -> float:
        return (100.0 * self.failure / self.total) if self.total else 0.0


def classify_outcomes(
    outcomes: list[W3cQueryOutcome],
    *,
    sla_sec: float,
) -> tuple[int, int, dict[str, int], Counter[int], dict[str, Counter[str]]]:
    success = failure = 0
    by_cat: dict[str, int] = {k: 0 for k, _ in _FAIL_CATEGORIES}
    http_codes: Counter[int] = Counter()
    samples: dict[str, Counter[str]] = {k: Counter() for k, _ in _FAIL_CATEGORIES}
    for o in outcomes:
        if is_w3c_success(o, sla_sec):
            success += 1
            continue
        failure += 1
        cat = categorize_failure(o.error)
        by_cat[cat] += 1
        if cat == "http_error":
            code = _extract_http_status(o.error)
            if code is not None:
                http_codes[code] += 1
        samples[cat][_normalize_sample(o.error)] += 1
    return success, failure, by_cat, http_codes, samples


def build_per_chain_breakdowns(
    outcomes: list[W3cQueryOutcome], *, sla_sec: float
) -> list[ChainBreakdown]:
    by_chain: dict[str, list[W3cQueryOutcome]] = {}
    for o in outcomes:
        by_chain.setdefault(o.chain, []).append(o)
    out: list[ChainBreakdown] = []
    for chain in sorted(by_chain):
        chain_outcomes = by_chain[chain]
        succ, fail, by_cat, _http, _samples = classify_outcomes(
            chain_outcomes, sla_sec=sla_sec
        )
        succ_latencies = [
            o.latency_ms for o in chain_outcomes if is_w3c_success(o, sla_sec)
        ]
        out.append(
            ChainBreakdown(
                chain=chain,
                total=len(chain_outcomes),
                success=succ,
                failure_by_category={k: v for k, v in by_cat.items() if v > 0},
                latency=LatencyDistribution.from_values(
                    [o.latency_ms for o in chain_outcomes]
                ),
                latency_successful=LatencyDistribution.from_values(succ_latencies),
            )
        )
    return out


def build_report(
    outcomes: list[W3cQueryOutcome],
    *,
    scheduled_count: int,
    scheduling_wall_sec: float,
    completion_wall_sec: float,
    peak_in_flight: int,
    unsuccessful_log_path: Path | None,
) -> W3cReport:
    settings = get_settings()
    sla_sec = settings.max_response_sec
    report = W3cReport(
        target_qps=settings.target_qps,
        concurrency_cap=settings.concurrency,
        duration_sec=settings.duration_sec,
        sla_response_sec=sla_sec,
        error_rate_threshold_pct=settings.error_rate_threshold_pct,
        scheduled_count=scheduled_count,
        scheduling_wall_sec=scheduling_wall_sec,
        completion_wall_sec=completion_wall_sec,
        peak_in_flight=peak_in_flight,
        unsuccessful_log_path=unsuccessful_log_path,
    )
    report.total = len(outcomes)
    report.success, report.failure, report.failure_by_category, \
        report.http_status_counts, report.category_samples = classify_outcomes(
            outcomes, sla_sec=sla_sec
        )

    all_latencies = [o.latency_ms for o in outcomes]
    succ_latencies = [
        o.latency_ms for o in outcomes if is_w3c_success(o, sla_sec)
    ]
    report.latency_overall = LatencyDistribution.from_values(all_latencies)
    report.latency_successful = LatencyDistribution.from_values(succ_latencies)
    report.per_chain = build_per_chain_breakdowns(outcomes, sla_sec=sla_sec)
    return report


def _fmt_ms(ms: float) -> str:
    if ms <= 0:
        return "    0 ms"
    if ms >= 1000:
        return f"{ms / 1000:>5.2f}s "
    return f"{ms:>5.0f} ms"


def print_report(r: W3cReport) -> None:
    print("\n" + "=" * 72)
    print("W3C MULTI-CHAIN SLA REPORT")
    print("=" * 72)

    print("\nLoad configuration (how the test was run — not an SLA %):")
    print(
        f"  Target QPS:        {r.target_qps} sustained\n"
        f"  Concurrency cap:   {r.concurrency_cap} in-flight (semaphore)\n"
        f"  Duration target:   {r.duration_sec:g} s\n"
        f"  Response SLA:      <= {r.sla_response_sec:g} s per query (also HTTP timeout)\n"
        f"  Requests scheduled: {r.scheduled_count}  (in {r.scheduling_wall_sec:.1f}s "
        f"scheduling wall time)\n"
        f"  Completion wall:   {r.completion_wall_sec:.1f}s (last response received)\n"
        f"  Peak in-flight:    {r.peak_in_flight} / {r.concurrency_cap} cap"
    )
    print(
        f"  Achieved sustained QPS: {r.achieved_qps_sustained:.2f} "
        f"(target {r.target_qps}) — based on launch rate, not response rate"
    )

    print(f"\n{'Metric':<32} {'SLA target':<26} {'Fulfilled':>10} {'%':>8}")
    print("-" * 72)
    print(
        f"{'Query response time':<32} {'<= ' + f'{r.sla_response_sec:g}s per query':<26} "
        f"{r.success}/{r.total:<9} {r.success_pct:>7.2f}%"
    )

    print("\nResponse time distribution (all requests, including failed):")
    print(
        f"  count={r.latency_overall.count:,}  "
        f"avg={r.latency_overall.avg_ms:,.0f} ms  "
        f"p50={r.latency_overall.p50_ms:,.0f}  "
        f"p95={r.latency_overall.p95_ms:,.0f}  "
        f"p99={r.latency_overall.p99_ms:,.0f}  "
        f"max={r.latency_overall.max_ms:,.0f} ms"
    )
    print("Response time distribution (successful only):")
    print(
        f"  count={r.latency_successful.count:,}  "
        f"avg={r.latency_successful.avg_ms:,.0f} ms  "
        f"p50={r.latency_successful.p50_ms:,.0f}  "
        f"p95={r.latency_successful.p95_ms:,.0f}  "
        f"p99={r.latency_successful.p99_ms:,.0f}  "
        f"max={r.latency_successful.max_ms:,.0f} ms"
    )

    print("\nUnsuccessful breakdown (disjoint — no overlap):")
    label_total = f"FAILED (response > {r.sla_response_sec:g}s, HTTP/GraphQL/etc.)"
    print(
        f"  {label_total:<60} {r.failure:>5} / {r.total}  "
        f"({r.failure_pct:.2f}%)"
    )
    for key, fmt in _FAIL_CATEGORIES:
        n = r.failure_by_category.get(key, 0)
        if n == 0:
            continue
        sub_label = "    · " + fmt.format(sla=r.sla_response_sec)
        pct = (100.0 * n / r.total) if r.total else 0.0
        print(f"  {sub_label:<60} {n:>5} / {r.total}  ({pct:.2f}%)")
        _print_sub_detail(key, r)

    label_succ = "SUCCESS (response time within SLA, no error)"
    print(
        f"  {label_succ:<60} {r.success:>5} / {r.total}  "
        f"({r.success_pct:.2f}%)"
    )

    print("\nPer-chain breakdown (all requests, ms):")
    header = (
        f"  {'chain':<10} {'reqs':>6}  {'success':>8}  {'success%':>9}  "
        f"{'avg':>7}  {'p50':>7}  {'p95':>7}  {'p99':>7}  {'max':>7}"
    )
    print(header)
    print("  " + "-" * (len(header) - 2))
    for cb in r.per_chain:
        print(
            f"  {cb.chain:<10} {cb.total:>6,}  {cb.success:>8,}  "
            f"{cb.success_pct:>8.2f}%  "
            f"{cb.latency.avg_ms:>6,.0f}  {cb.latency.p50_ms:>6,.0f}  "
            f"{cb.latency.p95_ms:>6,.0f}  {cb.latency.p99_ms:>6,.0f}  "
            f"{cb.latency.max_ms:>6,.0f}"
        )

    print("\nPer-chain response time (successful only, ms):")
    header2 = (
        f"  {'chain':<10} {'success':>8}  "
        f"{'avg':>7}  {'p50':>7}  {'p95':>7}  {'p99':>7}  {'max':>7}"
    )
    print(header2)
    print("  " + "-" * (len(header2) - 2))
    for cb in r.per_chain:
        if cb.success == 0:
            print(f"  {cb.chain:<10} {cb.success:>8,}  (no successful responses)")
            continue
        print(
            f"  {cb.chain:<10} {cb.success:>8,}  "
            f"{cb.latency_successful.avg_ms:>6,.0f}  "
            f"{cb.latency_successful.p50_ms:>6,.0f}  "
            f"{cb.latency_successful.p95_ms:>6,.0f}  "
            f"{cb.latency_successful.p99_ms:>6,.0f}  "
            f"{cb.latency_successful.max_ms:>6,.0f}"
        )

    if r.failure_pct > r.error_rate_threshold_pct:
        print(
            f"\n  Note: failure rate ({r.failure_pct:.2f}%) is above the "
            f"reporting threshold ({r.error_rate_threshold_pct:.2f}%) — "
            "investigate Bitquery capacity or rate-limit headroom."
        )

    if r.unsuccessful_log_path is not None:
        print(
            "\n  Full per-request error details (chain + addresses + raw Bitquery message):"
            f"\n    {r.unsuccessful_log_path}"
        )

    if r.notes:
        print("\nNotes:")
        for note in r.notes:
            print(f"  - {note}")

    print("=" * 72)


def _print_sub_detail(key: str, r: W3cReport) -> None:
    if key == "response_time":
        return
    if key == "http_error" and r.http_status_counts:
        top = sorted(r.http_status_counts.items(), key=lambda x: (-x[1], x[0]))
        parts = [f"HTTP {status}: {n}" for status, n in top]
        print(f"        codes: {'  |  '.join(parts)}")
        return
    samples = r.category_samples.get(key)
    if not samples:
        return
    for msg, n in samples.most_common(3):
        suffix = f"  (× {n})" if n > 1 else ""
        print(f'        e.g. "{msg}"{suffix}')
