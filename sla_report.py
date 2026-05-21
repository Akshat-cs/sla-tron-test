"""
Run Nomics SLA measurement and print fulfillment % per metric.

HTTP tests: one query per address in the pool (stops after full pool traversal).
Freshness: WebSocket subscription (default 10 minutes).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv

from address_pool import AddressPool, set_random_seed
from graphql_client import execute_transfer_query
from sla_config import (
    MAX_RECORDS_PER_QUERY,
    SlaSettings,
    configure,
    defaults_from_env,
    get_settings,
    reset_settings,
)
from sla_metrics import (
    SlaReport,
    categorize_sla_failure,
    classify_requests,
    expected_row_count,
    is_sla_success,
    metric_freshness,
    metric_query_response_time,
)
from run_logging import close_run_logging, setup_run_logging
from subscription_client import collect_freshness_lags
from unsuccessful_log import (
    close_unsuccessful_log,
    get_path as get_unsuccessful_log_path,
    log_unsuccessful,
    open_unsuccessful_log,
)

load_dotenv()
set_random_seed()

QUERY_LIMIT = int(os.getenv("QUERY_LIMIT", str(MAX_RECORDS_PER_QUERY)))
FRESHNESS_SUBSCRIPTION_SEC = int(os.getenv("FRESHNESS_SUBSCRIPTION_SEC", "600"))
LOG_EVERY_N = max(1, int(os.getenv("POOL_LOG_EVERY_N", "1")))
MAX_IN_FLIGHT = max(1, int(os.getenv("MAX_IN_FLIGHT", "16")))
PROGRESS_EVERY_N = max(1, int(os.getenv("PROGRESS_EVERY_N", "100")))


def build_arg_parser() -> argparse.ArgumentParser:
    env = defaults_from_env()
    parser = argparse.ArgumentParser(
        description="Nomics SLA report — Tron Transfers HTTP load + freshness subscription",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--qps",
        type=int,
        default=env.peak_qps,
        metavar="N",
        help="Peak query rate (queries started per second)",
    )
    parser.add_argument(
        "--response-time",
        "--response-time-sec",
        "--sla-response",
        dest="response_sec",
        type=float,
        default=env.max_query_response_sec,
        metavar="SEC",
        help="Max query response time SLA (also used as per-request HTTP timeout)",
    )
    parser.add_argument(
        "--freshness",
        "--freshness-sec",
        "--sla-freshness",
        dest="freshness_sec",
        type=float,
        default=env.max_data_freshness_sec,
        metavar="SEC",
        help="Max ingest lag for subscription freshness SLA",
    )
    return parser


def init_sla(argv: list[str] | None = None) -> SlaSettings:
    """Parse CLI flags (or env defaults when argv is empty)."""
    reset_settings()
    parser = build_arg_parser()
    args = parser.parse_args(argv if argv is not None else [])
    if args.qps <= 0:
        parser.error("--qps must be positive")
    if args.response_sec <= 0:
        parser.error("--response-time must be positive")
    if args.freshness_sec <= 0:
        parser.error("--freshness must be positive")
    return configure(
        peak_qps=args.qps,
        max_query_response_sec=args.response_sec,
        max_data_freshness_sec=args.freshness_sec,
    )


def _print_section(title: str) -> None:
    line = "─" * 64
    print(f"\n{line}\n  {title}\n{line}", flush=True)


def _ts() -> str:
    """UTC timestamp with milliseconds for request logs."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"


def _log_request_sent(req_num: int, total: int, address: str, tag: str) -> None:
    if req_num % LOG_EVERY_N != 0 and req_num != total:
        return
    w = len(str(total))
    print(
        f"  {_ts()}  [{req_num:>{w}}/{total}] → sent     "
        f"{address}  ({tag})",
        flush=True,
    )


def _format_failure(error_detail: str | None, http_status: int) -> str:
    detail = (error_detail or "unknown error").replace("\n", " ")
    lower = detail.lower()
    sla = get_settings().max_query_response_sec
    if "insufficient rows" in lower:
        return detail
    if "request timeout" in lower:
        return f"WE STOPPED · TIMEOUT (>{sla:g}s) — {detail}"
    if "exceeded" in lower and "sla" in lower:
        return f"WE STOPPED · SLOW (>{sla:g}s SLA) — {detail}"
    if lower.startswith("http "):
        # detail already begins with "HTTP <status>:"; don't double-prefix
        return f"SERVER ERROR · {detail}"
    if "is null" in lower:
        return f"SERVER ERROR · GraphQL null — {detail}"
    if "invalid json" in lower or "could not parse" in lower:
        return f"CLIENT ERROR · parse — {detail}"
    if http_status not in (0, 200):
        return f"SERVER ERROR · HTTP {http_status} — {detail}"
    return f"CLIENT/NETWORK ERROR — {detail}"


# Per-request status: ✓ ok, ⚠ SLA-pass but insufficient rows, ✗ SLA fail.
_STATE_MARK = {"ok": "✓", "insufficient": "⚠", "fail": "✗"}


def _log_request_done(
    req_num: int,
    total: int,
    address: str,
    *,
    state: str,
    latency_ms: float,
    record_count: int,
    error_detail: str | None = None,
    http_status: int = 0,
    expected_count: int | None = None,
) -> None:
    if req_num % LOG_EVERY_N != 0 and req_num != total:
        return
    w = len(str(total))
    mark = _STATE_MARK.get(state, "?")

    if state == "ok":
        status = f"{latency_ms:.0f} ms · {record_count:,} rows"
    elif state == "insufficient":
        exp_str = f"{expected_count:,}" if expected_count is not None else "?"
        status = (
            f"{latency_ms:.0f} ms · {record_count:,} rows · "
            f"INSUFFICIENT (expected >= {exp_str})"
        )
    else:  # "fail"
        status = _format_failure(error_detail, http_status)
        if latency_ms > 0:
            status = f"{latency_ms:.0f} ms · {status}"

    print(
        f"  {_ts()}  [{req_num:>{w}}/{total}] {mark} done    "
        f"{address}  {status}",
        flush=True,
    )


async def run_pool_traversal(
    *,
    pool: AddressPool,
    qps: int,
    limit: int,
    timeout_sec: float,
) -> tuple[list[float], list[int], list[str | None], float]:
    """
    Open-loop parallel load: exactly one query per pool address (index 0..N-1),
    starting a new query every 1/qps seconds until the whole pool is covered.
    """
    pool_size = len(pool.receivers)
    interval = 1.0 / qps
    tasks: list[asyncio.Task] = []
    phase_started = time.perf_counter()

    _print_section(
        f"Pool traversal · {pool_size} requests · limit {limit:,} · {qps} QPS"
    )
    if LOG_EVERY_N > 1:
        print(f"  (logging every {LOG_EVERY_N} requests — set POOL_LOG_EVERY_N=1 for all)\n", flush=True)
    schedule_est = pool_size / qps
    print(
        f"  Max in-flight: {MAX_IN_FLIGHT} | Per-request timeout: {timeout_sec:g}s\n"
        f"  Scheduling at {qps} QPS will take ~{schedule_est / 60:.1f} min; "
        f"backlog drains after the last queue at up to {MAX_IN_FLIGHT}/avg_latency req/s.\n",
        flush=True,
    )

    semaphore = asyncio.Semaphore(MAX_IN_FLIGHT)
    connector = aiohttp.TCPConnector(limit=MAX_IN_FLIGHT + 4)
    async with aiohttp.ClientSession(connector=connector) as session:

        async def _run_one(
            req_num: int,
            receiver: str,
            tag: str,
            estimated_txs: int | None,
        ) -> tuple[int, float, int, str | None]:
            async with semaphore:
                r = await execute_transfer_query(
                    session,
                    receiver=receiver,
                    limit=limit,
                    timeout_sec=timeout_sec,
                )
                sla_ok = is_sla_success(latency_ms=r.latency_ms, error=r.error)
                expected = expected_row_count(estimated_txs, limit)
                insufficient = (
                    sla_ok
                    and expected is not None
                    and r.record_count < expected
                )
                if not sla_ok:
                    state = "fail"
                elif insufficient:
                    state = "insufficient"
                else:
                    state = "ok"
                _log_request_done(
                    req_num,
                    pool_size,
                    receiver,
                    state=state,
                    latency_ms=r.latency_ms,
                    record_count=r.record_count,
                    error_detail=r.error,
                    http_status=r.status,
                    expected_count=expected,
                )
                if state != "ok":
                    category = (
                        "insufficient"
                        if state == "insufficient"
                        else categorize_sla_failure(r.error)
                    )
                    log_unsuccessful(
                        req_num=req_num,
                        total=pool_size,
                        address=receiver,
                        category=category,
                        http_status=r.status,
                        latency_ms=r.latency_ms,
                        record_count=r.record_count,
                        expected_count=expected,
                        raw_error=r.error,
                        sla_target_sec=timeout_sec,
                    )
                return req_num, r.latency_ms, r.record_count, r.error

        next_tick = time.monotonic()

        for req_index in range(pool_size):
            now = time.monotonic()
            if now < next_tick:
                await asyncio.sleep(next_tick - now)
            next_tick += interval

            profile = pool.pick_at(req_index)
            req_num = req_index + 1
            tag = profile.source or profile.tier
            _log_request_sent(req_num, pool_size, profile.address, tag)

            tasks.append(
                asyncio.create_task(
                    _run_one(
                        req_num,
                        profile.address,
                        tag,
                        profile.estimated_txs,
                    ),
                    name=f"sla-req-{req_num}",
                )
            )

        schedule_sec = time.perf_counter() - phase_started
        print(
            f"\n  All {pool_size} requests scheduled in {schedule_sec:.1f}s "
            f"— waiting for in-flight responses (session stays open)…\n",
            flush=True,
        )

        results_by_num: dict[int, tuple[float, int, str | None]] = {}
        completed = 0
        for finished in asyncio.as_completed(tasks):
            req_num, ms, count, err_msg = await finished
            results_by_num[req_num] = (ms, count, err_msg)
            completed += 1
            if completed % PROGRESS_EVERY_N == 0 or completed == pool_size:
                print(
                    f"  … {completed}/{pool_size} responses received "
                    f"({time.perf_counter() - phase_started:.0f}s elapsed)",
                    flush=True,
                )

        latencies: list[float] = []
        counts: list[int] = []
        errors: list[str | None] = []

        for req_num in range(1, pool_size + 1):
            ms, count, err_msg = results_by_num.get(
                req_num, (0.0, 0, "missing task result")
            )
            latencies.append(ms)
            counts.append(count)
            errors.append(err_msg)

        done_sec = time.perf_counter() - phase_started
        sla_ok = sum(
            1
            for ms, err in zip(latencies, errors, strict=True)
            if is_sla_success(latency_ms=ms, error=err)
        )
        sla_fail = pool_size - sla_ok
        print(
            f"\n  Finished {pool_size} requests in {done_sec:.1f}s wall time "
            f"({sla_ok} within {timeout_sec:g}s response SLA, "
            f"{sla_fail} SLA-fail). "
            f"Insufficient-row count is computed separately.\n",
            flush=True,
        )

        return latencies, counts, errors, schedule_sec


async def run_report() -> SlaReport:
    if not os.getenv("BITQUERY_TOKEN"):
        raise SystemExit("Set BITQUERY_TOKEN in .env")

    settings = get_settings()
    timeout_sec = settings.request_timeout_sec

    run_started = time.perf_counter()
    pool = AddressPool.resolve()
    pool_size = len(pool.receivers)
    report = SlaReport()
    report.notes.append(
        f"Address pool: {pool.summary()} — 1 query per address, then stop"
    )
    report.notes.append(
        f"HTTP phase: {pool_size} queries at {settings.peak_qps} QPS, "
        f"{timeout_sec:g}s timeout per request (= response-time SLA)"
    )

    expected_counts: list[int | None] = [
        expected_row_count(p.estimated_txs, QUERY_LIMIT) for p in pool.receivers
    ]

    phase_start = time.perf_counter()
    latencies, counts, errors, schedule_sec = await run_pool_traversal(
        pool=pool,
        qps=settings.peak_qps,
        limit=QUERY_LIMIT,
        timeout_sec=timeout_sec,
    )
    phase_elapsed = time.perf_counter() - phase_start

    report.http_context = {
        "qps": settings.peak_qps,
        "addresses": pool_size,
        "schedule_sec": schedule_sec,
        "wall_sec": phase_elapsed,
    }
    report.phase_runtime_sec[
        f"HTTP pool traversal ({pool_size} addrs @ {settings.peak_qps} QPS)"
    ] = phase_elapsed

    classification = classify_requests(
        latencies=latencies,
        errors=errors,
        counts=counts,
        expected_counts=expected_counts,
        query_limit=QUERY_LIMIT,
    )
    classification.unsuccessful_log_path = get_unsuccessful_log_path()
    report.classification = classification
    report.notes.append(
        f"HTTP: {classification.success}/{pool_size} fully successful; "
        f"{classification.sla_fail} SLA-fail (>{settings.max_query_response_sec:g}s/timeout/error), "
        f"{classification.insufficient} returned fewer rows than pool's expected count "
        f"(disjoint; total unsuccessful = {classification.total_unsuccessful})"
    )

    report.add(metric_query_response_time(latencies, errors))

    _print_section(
        f"Freshness subscription · {FRESHNESS_SUBSCRIPTION_SEC}s "
        f"({FRESHNESS_SUBSCRIPTION_SEC / 60:.0f} min)"
    )
    print("  Listening for live Tron transfers (ingest lag = now − Block.Time)…\n", flush=True)
    phase_start = time.perf_counter()
    try:
        lags, msg_count = await collect_freshness_lags(
            duration_sec=FRESHNESS_SUBSCRIPTION_SEC
        )
        fresh = metric_freshness(lags)
        report.add(fresh)
        report.phase_runtime_sec[
            f"Freshness subscription ({FRESHNESS_SUBSCRIPTION_SEC}s)"
        ] = time.perf_counter() - phase_start
        report.notes.append(
            f"Freshness: {msg_count} subscription payloads, {fresh.total} transfer "
            f"events sampled; {fresh.fulfilled} under {settings.max_data_freshness_sec:g}s lag"
        )
        if not lags:
            report.notes.append(
                "No subscription events — check token, wss endpoint, or Tron activity"
            )
    except Exception as exc:
        report.notes.append(f"Freshness subscription failed: {exc}")
        report.add(metric_freshness([]))
        report.phase_runtime_sec["Freshness subscription"] = (
            time.perf_counter() - phase_start
        )

    report.total_runtime_sec = time.perf_counter() - run_started
    return report


def main(argv: list[str] | None = None) -> None:
    if argv is None:
        argv = sys.argv[1:]
    settings = init_sla(argv)
    log_path = setup_run_logging()
    unsuccessful_path = open_unsuccessful_log(log_path)
    print(f"Logging to terminal and file:\n  {log_path.resolve()}", flush=True)
    print(f"Unsuccessful requests log:\n  {unsuccessful_path.resolve()}\n", flush=True)
    print(
        f"SLA: {settings.peak_qps} QPS · {settings.max_query_response_sec:g}s response "
        f"(timeout {settings.request_timeout_sec:g}s) · "
        f"{settings.max_data_freshness_sec:g}s freshness\n",
        flush=True,
    )
    try:
        report = asyncio.run(run_report())
        report.print_table()
    finally:
        close_unsuccessful_log()
        close_run_logging()


if __name__ == "__main__":
    main()
