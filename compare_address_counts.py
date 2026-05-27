"""
Compare transfer counts for one Tron address using the two methods used in SLA testing.

1. Discovery-style: Bitquery aggregate `txs: count` (Success, inbound, Amount > 0.1, last N days)
2. SLA-style:       Row list from tron_transfers.graphql → len(Transfers) (no date filter, up to limit)

Run:
  python compare_address_counts.py TM4qYGqL7cKdZotQ53U2qDKkevaFkkge36
  python compare_address_counts.py TM4qYGqL7cKdZotQ53U2qDKkevaFkkge36 --days 90 --limit 20000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

from address_pool import DEFAULT_POOL_PATH, AddressPool
from graphql_client import execute_graphql, execute_transfer_query
from sla_config import MAX_RECORDS_PER_QUERY
from sla_metrics import expected_row_count

load_dotenv()

AGGREGATE_QUERY_PATH = Path(__file__).parent / "queries" / "count_receiver_aggregate.graphql"
DEFAULT_DAYS = int(os.getenv("DISCOVER_DAYS_AGO", "60"))
COMPARE_TIMEOUT_SEC = 120


def _extract_aggregate_count(payload: dict) -> int | None:
    try:
        rows = payload["data"]["Tron"]["Transfers"]
        if not rows:
            return 0
        return int(rows[0].get("txs") or 0)
    except (KeyError, TypeError, IndexError, ValueError):
        return None


def _pool_estimate(address: str) -> int | None:
    if not DEFAULT_POOL_PATH.is_file():
        return None
    pool = AddressPool.load(DEFAULT_POOL_PATH)
    for p in pool.receivers:
        if p.address == address:
            return p.estimated_txs
    return None


async def run_compare(
    *,
    address: str,
    days_ago: int,
    limit: int,
) -> int:
    if not os.getenv("BITQUERY_TOKEN"):
        print("Set BITQUERY_TOKEN in .env", file=sys.stderr)
        return 1

    aggregate_query = AGGREGATE_QUERY_PATH.read_text(encoding="utf-8")
    timeout = aiohttp.ClientTimeout(total=COMPARE_TIMEOUT_SEC)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        print(f"Address: {address}")
        print(f"Aggregate window: last {days_ago} days (discovery-style)")
        print(f"SLA row query limit: {limit:,} (tron_transfers.graphql, no date filter)\n")

        print("1) Aggregate count (discovery-style)…")
        agg_payload = await execute_graphql(
            session,
            query=aggregate_query,
            variables={"receiver": address, "days_ago": days_ago},
        )
        if agg_payload.get("errors"):
            print(f"   ERROR: {json.dumps(agg_payload['errors'], ensure_ascii=False)}")
            return 1
        aggregate_count = _extract_aggregate_count(agg_payload)
        if aggregate_count is None:
            print("   ERROR: could not parse txs: count from aggregate response")
            return 1
        print(f"   aggregate txs: count = {aggregate_count:,}")

        print("\n2) SLA row count (tron_transfers.graphql)…")
        result = await execute_transfer_query(
            session,
            receiver=address,
            limit=limit,
            timeout_sec=None,
        )
        if result.error:
            print(f"   ERROR: {result.error}")
            return 1
        sla_row_count = result.record_count
        print(f"   len(Transfers[]) = {sla_row_count:,}  (HTTP {result.status}, {result.latency_ms:.0f} ms)")

    expected_for_test = expected_row_count(aggregate_count, limit)
    pool_est = _pool_estimate(address)

    print("\n" + "=" * 60)
    print("COMPARISON")
    print("=" * 60)
    print(f"  Discovery aggregate ({days_ago}d):  {aggregate_count:,}")
    print(f"  SLA row list (no date, limit {limit:,}): {sla_row_count:,}")
    if pool_est is not None:
        print(f"  Pool JSON estimated_txs:            {pool_est:,}")
    if expected_for_test is not None:
        print(f"  Test expects (min(aggregate, limit)): >= {expected_for_test:,}")

    diff_agg_sla = sla_row_count - aggregate_count
    print()
    if aggregate_count == sla_row_count:
        print("  Match: aggregate count == SLA row count")
    else:
        sign = "+" if diff_agg_sla > 0 else ""
        print(
            f"  Mismatch: SLA rows differ from aggregate by {sign}{diff_agg_sla:,} "
            f"({sla_row_count:,} vs {aggregate_count:,})"
        )
        if sla_row_count < aggregate_count:
            print(
                "  → SLA returned fewer rows than the time-window aggregate "
                "(indexer drift, or aggregate vs enumerate difference)."
            )
        else:
            print(
                "  → SLA returned more rows than the N-day aggregate "
                "(SLA query has no date filter — includes older transfers)."
            )

    if expected_for_test is not None:
        if sla_row_count >= expected_for_test:
            print(f"  SLA test verdict: OK (rows {sla_row_count:,} >= expected {expected_for_test:,})")
        else:
            print(
                f"  SLA test verdict: INSUFFICIENT "
                f"(rows {sla_row_count:,} < expected {expected_for_test:,}, "
                f"short by {expected_for_test - sla_row_count:,})"
            )

    if pool_est is not None and pool_est != aggregate_count:
        print(
            f"\n  Note: pool estimated_txs ({pool_est:,}) != fresh aggregate ({aggregate_count:,}). "
            "Pool was built at a different time or with a different days_ago window."
        )

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare discovery aggregate count vs SLA row count for one address",
    )
    parser.add_argument("address", help="Tron receiver address (base58, starts with T)")
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Days for aggregate window (default {DEFAULT_DAYS}, matches discover_*.graphql)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("QUERY_LIMIT", str(MAX_RECORDS_PER_QUERY))),
        help="SLA query row limit (default QUERY_LIMIT / 20000)",
    )
    args = parser.parse_args()
    if args.days <= 0 or args.limit <= 0:
        parser.error("--days and --limit must be positive")
    raise SystemExit(asyncio.run(run_compare(address=args.address, days_ago=args.days, limit=args.limit)))


if __name__ == "__main__":
    main()
