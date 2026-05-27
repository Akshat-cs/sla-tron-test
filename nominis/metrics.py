"""Compute Nomics SLA fulfillment as percentages (no pass/fail tolerances)."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from nominis.config import get_settings


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


def sla_limit_ms() -> float:
    return get_settings().max_query_response_sec * 1000


def is_sla_success(*, latency_ms: float, error: str | None) -> bool:
    """Successful = no error and response completed within SLA seconds."""
    return error is None and latency_ms <= sla_limit_ms()


@dataclass
class SlaMetric:
    name: str
    sla_target: str
    fulfilled: int
    total: int
    avg_label: str | None = None  # sub-row label (scoped to this metric only)
    avg_ms: float | None = None

    @property
    def pct(self) -> float:
        if self.total == 0:
            return 0.0
        return round(100.0 * self.fulfilled / self.total, 2)


# Display labels for each SLA-fail sub-category (rendered in the order listed).
_SLA_FAIL_CATEGORIES: list[tuple[str, str]] = [
    ("response_time", "Response time > {sla:g}s (timeout or slow response)"),
    ("http_error", "HTTP error (non-200)"),
    ("graphql_null", "GraphQL null data (often rate-limit / overload)"),
    ("graphql_error", "GraphQL errors"),
    ("parse_error", "Parse error (invalid JSON / schema)"),
    ("other", "Other (connection / unknown)"),
]


def categorize_sla_failure(error_str: str | None) -> str:
    """Bucket a raw error string from execute_transfer_query into one category."""
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


@dataclass
class Classification:
    """Disjoint breakdown of HTTP pool requests."""

    total: int
    sla_fail: int  # timeout, error, or response > SLA seconds
    insufficient: int  # SLA-pass BUT returned fewer rows than pool's expected count
    success: int  # SLA-pass AND record_count >= expected (or expected unknown)
    sla_target_sec: float
    query_limit: int
    sla_fail_by_category: dict[str, int] = field(default_factory=dict)
    http_status_counts: Counter[int] = field(default_factory=Counter)
    category_samples: dict[str, Counter[str]] = field(default_factory=dict)
    unsuccessful_log_path: Path | None = None

    @property
    def total_unsuccessful(self) -> int:
        return self.sla_fail + self.insufficient

    def _pct(self, n: int) -> float:
        return (100.0 * n / self.total) if self.total else 0.0

    def print_section(self) -> None:
        print(
            f"\nRow count check "
            f"(expected per address = min(pool.estimated_txs, query limit {self.query_limit:,})):"
        )
        print(
            f"  Returned fewer rows than expected: "
            f"{self.insufficient} / {self.total}  ({self._pct(self.insufficient):.2f}%)"
        )

        print("\nUnsuccessful breakdown (disjoint — no overlap):")
        label_a = f"SLA-fail (response time > {self.sla_target_sec:g}s, HTTP/GraphQL/etc.)"
        label_b = "Returned fewer rows than expected"
        label_t = "TOTAL unsuccessful"
        label_s = "TOTAL successful"

        print(
            f"  {label_a:<60} {self.sla_fail:>5} / {self.total}  "
            f"({self._pct(self.sla_fail):.2f}%)"
        )
        for key, fmt in _SLA_FAIL_CATEGORIES:
            n = self.sla_fail_by_category.get(key, 0)
            if n == 0:
                continue
            sub_label = "    · " + fmt.format(sla=self.sla_target_sec)
            print(
                f"  {sub_label:<60} {n:>5} / {self.total}  "
                f"({self._pct(n):.2f}%)"
            )
            self._print_sub_detail(key)
        print(
            f"  {label_b:<60} {self.insufficient:>5} / {self.total}  "
            f"({self._pct(self.insufficient):.2f}%)"
        )
        print(f"  {'-' * 72}")
        print(
            f"  {label_t:<60} {self.total_unsuccessful:>5} / {self.total}  "
            f"({self._pct(self.total_unsuccessful):.2f}%)"
        )
        print(
            f"  {label_s:<60} {self.success:>5} / {self.total}  "
            f"({self._pct(self.success):.2f}%)"
        )
        if self.unsuccessful_log_path is not None:
            print(
                f"\n  Full per-address error details (address + raw Bitquery message):\n"
                f"    {self.unsuccessful_log_path}"
            )

    def _print_sub_detail(self, key: str) -> None:
        """Indented detail line per sub-category: HTTP status codes or sample messages."""
        if key == "response_time":
            # Category label already says it — message is always the SLA-stop string.
            return
        if key == "http_error" and self.http_status_counts:
            top = sorted(
                self.http_status_counts.items(), key=lambda x: (-x[1], x[0])
            )
            parts = [f"HTTP {status}: {n}" for status, n in top]
            print(f"        codes: {'  |  '.join(parts)}")
            return
        samples = self.category_samples.get(key)
        if not samples:
            return
        for msg, n in samples.most_common(3):
            suffix = f"  (× {n})" if n > 1 else ""
            print(f'        e.g. "{msg}"{suffix}')


@dataclass
class SlaReport:
    metrics: list[SlaMetric] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    total_runtime_sec: float | None = None
    phase_runtime_sec: dict[str, float] = field(default_factory=dict)
    http_context: dict[str, float | int] = field(default_factory=dict)
    classification: Classification | None = None

    def add(self, metric: SlaMetric) -> None:
        self.metrics.append(metric)

    def print_table(self) -> None:
        settings = get_settings()
        print("\n" + "=" * 72)
        print("NOMICS SLA FULFILLMENT REPORT")
        print("=" * 72)
        if self.http_context:
            qps = int(self.http_context.get("qps", settings.peak_qps))
            addrs = int(self.http_context.get("addresses", 0))
            sched = float(self.http_context.get("schedule_sec", 0))
            wall = float(self.http_context.get("wall_sec", 0))
            print("\nHTTP load (how the test was run — not an SLA %):")
            print(
                f"  {qps} queries/sec for {sched:.0f}s to schedule {addrs} addresses "
                f"(1 query per address, open-loop); all responses finished in {wall:.0f}s wall time"
            )

        print(f"\n{'Metric':<32} {'SLA target':<26} {'Fulfilled':>10} {'%':>8}")
        print("-" * 72)
        for m in self.metrics:
            print(
                f"{m.name:<32} {m.sla_target:<26} "
                f"{m.fulfilled}/{m.total:<9} {m.pct:>7.2f}%"
            )
            if m.avg_ms is not None and m.avg_label:
                print(
                    f"{m.avg_label:<32} {'':<26} "
                    f"{m.avg_ms:>10,.0f} ms"
                )
        print("=" * 72)

        if self.classification:
            self.classification.print_section()
            print("=" * 72)

        if self.total_runtime_sec is not None:
            print(f"\nTotal test runtime: {self.total_runtime_sec:.1f}s")
        if self.phase_runtime_sec:
            print("Phase runtime:")
            for phase, secs in self.phase_runtime_sec.items():
                print(f"  - {phase}: {secs:.1f}s")
        if self.notes:
            print("\nNotes:")
            for note in self.notes:
                print(f"  - {note}")


def expected_row_count(
    estimated_txs: int | None, query_limit: int
) -> int | None:
    """How many rows we expect back from one query (capped at query limit)."""
    if not estimated_txs or estimated_txs <= 0 or query_limit <= 0:
        return None
    return min(int(estimated_txs), int(query_limit))


def classify_requests(
    *,
    latencies: list[float],
    errors: list[str | None],
    counts: list[int],
    expected_counts: list[int | None],
    query_limit: int,
) -> Classification:
    """
    Bucket every request into exactly one of:
      - sla_fail        (error/timeout/>SLA seconds)
      - insufficient    (SLA pass but record_count < expected)
      - success         (SLA pass and enough rows, or expected unknown)
    """
    n = len(latencies)
    assert len(errors) == n == len(counts) == len(expected_counts), (
        "classify_requests inputs must be the same length"
    )
    target_sec = get_settings().max_query_response_sec
    sla_fail = insufficient = success = 0
    sla_fail_by_category: dict[str, int] = {key: 0 for key, _ in _SLA_FAIL_CATEGORIES}
    http_status_counts: Counter[int] = Counter()
    category_samples: dict[str, Counter[str]] = {
        key: Counter() for key, _ in _SLA_FAIL_CATEGORIES
    }
    for ms, err, cnt, exp in zip(
        latencies, errors, counts, expected_counts, strict=True
    ):
        if not is_sla_success(latency_ms=ms, error=err):
            sla_fail += 1
            category = categorize_sla_failure(err)
            sla_fail_by_category[category] = sla_fail_by_category.get(category, 0) + 1
            if category == "http_error":
                status = _extract_http_status(err)
                if status is not None:
                    http_status_counts[status] += 1
            category_samples[category][_normalize_sample(err)] += 1
        elif exp is not None and cnt < exp:
            insufficient += 1
        else:
            success += 1
    return Classification(
        total=n,
        sla_fail=sla_fail,
        insufficient=insufficient,
        success=success,
        sla_target_sec=target_sec,
        query_limit=query_limit,
        sla_fail_by_category=sla_fail_by_category,
        http_status_counts=http_status_counts,
        category_samples=category_samples,
    )


def metric_query_response_time(
    latency_ms: list[float], errors: list[str | None]
) -> SlaMetric:
    """% of all HTTP requests that completed successfully within SLA seconds."""
    limit = get_settings().max_query_response_sec
    total = len(latency_ms)
    fulfilled = sum(
        1
        for ms, err in zip(latency_ms, errors, strict=True)
        if is_sla_success(latency_ms=ms, error=err)
    )
    return SlaMetric(
        name="Query response time",
        sla_target=f"<= {limit:g}s per query",
        fulfilled=fulfilled,
        total=total,
    )


def metric_freshness(lags_sec: list[float]) -> SlaMetric:
    limit = get_settings().max_data_freshness_sec
    total = len(lags_sec)
    fulfilled = sum(1 for lag in lags_sec if lag < limit)
    avg_ms = (sum(lags_sec) / total * 1000) if total else None
    return SlaMetric(
        name="Data freshness (subscription)",
        sla_target=f"< {limit:g}s ingest lag per event",
        fulfilled=fulfilled,
        total=total,
        avg_label="  avg subscription ingest lag",
        avg_ms=avg_ms,
    )
