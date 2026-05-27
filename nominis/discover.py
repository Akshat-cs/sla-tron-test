"""
Discover Tron receiver addresses for SLA testing.

Fetches 2000 receivers from descending tx count + 2000 from ascending (90-day window),
merges into one pool (up to 4000 unique addresses). No per-address probing.

Run before SLA tests:
  python -m nominis.discover
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

from nominis.address_pool import DEFAULT_POOL_PATH, ReceiverProfile
from nominis.client import execute_graphql

load_dotenv()

DISCOVER_TOP = Path(__file__).parent.parent / "queries" / "nominis" / "discover_top_receivers.graphql"
DISCOVER_LIGHT = Path(__file__).parent.parent / "queries" / "nominis" / "discover_light_receivers.graphql"
DEFAULT_DISCOVER_TIMEOUT_SEC = int(os.getenv("DISCOVER_TIMEOUT_SEC", "60"))

DEFAULT_TOP_N = int(os.getenv("DISCOVER_TOP_N", "2000"))
DEFAULT_LIGHT_N = int(os.getenv("DISCOVER_LIGHT_N", "2000"))


@dataclass
class Candidate:
    address: str
    aggregate_txs: int


def _extract_discovery_rows(payload: dict) -> list[Candidate]:
    rows = []
    try:
        transfers = payload["data"]["Tron"]["Transfers"] or []
    except (KeyError, TypeError):
        return rows
    for row in transfers:
        receiver = (row.get("Transfer") or {}).get("Receiver")
        txs = row.get("txs") or 0
        if receiver:
            rows.append(Candidate(address=receiver, aggregate_txs=int(txs)))
    return rows


async def _fetch_ranked(
    session: aiohttp.ClientSession,
    *,
    query_path: Path,
    pool_size: int,
    label: str,
) -> list[Candidate]:
    query = query_path.read_text(encoding="utf-8")
    payload = await execute_graphql(
        session,
        query=query,
        variables={"pool_size": pool_size},
    )
    if payload.get("errors"):
        raise RuntimeError(f"Discovery {label} failed: {payload['errors']}")
    return _extract_discovery_rows(payload)


def build_combined_pool(
    desc_candidates: list[Candidate],
    asc_candidates: list[Candidate],
) -> list[ReceiverProfile]:
    """
    2000 desc (heavy) + 2000 asc (light), dedupe on address (asc skips duplicates).
    """
    profiles: list[ReceiverProfile] = []
    seen: set[str] = set()

    for c in desc_candidates:
        if c.address in seen:
            continue
        seen.add(c.address)
        profiles.append(
            ReceiverProfile(
                address=c.address,
                tier="heavy",
                estimated_txs=c.aggregate_txs,
                source="desc",
            )
        )

    for c in asc_candidates:
        if c.address in seen:
            continue
        seen.add(c.address)
        profiles.append(
            ReceiverProfile(
                address=c.address,
                tier="light",
                estimated_txs=c.aggregate_txs,
                source="asc",
            )
        )

    return profiles


async def run_discovery(
    *,
    top_n: int,
    light_n: int,
    output: Path,
    timeout_sec: int = DEFAULT_DISCOVER_TIMEOUT_SEC,
) -> list[ReceiverProfile]:
    if not os.getenv("BITQUERY_TOKEN"):
        raise SystemExit("Set BITQUERY_TOKEN in .env")

    discover_timeout = aiohttp.ClientTimeout(total=timeout_sec)
    async with aiohttp.ClientSession(timeout=discover_timeout) as session:
        print(
            f"Fetching {top_n} desc + {light_n} asc receivers "
            f"(timeout {timeout_sec}s each)…"
        )
        desc = await _fetch_ranked(
            session, query_path=DISCOVER_TOP, pool_size=top_n, label="desc"
        )
        asc = await _fetch_ranked(
            session, query_path=DISCOVER_LIGHT, pool_size=light_n, label="asc"
        )
        print(f"  desc rows: {len(desc)} | asc rows: {len(asc)}")

    profiles = build_combined_pool(desc, asc)
    print(f"Combined pool: {len(profiles)} unique addresses")

    output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "pool_size_target": {"desc": top_n, "asc": light_n, "combined_max": top_n + light_n},
        "filters": {
            "Block.Time": "since_relative days_ago: 90",
            "TransactionStatus": "Success",
            "Transfer.Amount": "> 0.1",
            "dataset": "combined",
        },
        "discover_timeout_sec": timeout_sec,
        "receivers": [asdict(p) for p in profiles],
    }
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"\nWrote {len(profiles)} receivers to {output}")
    return profiles


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build receiver pool (2000 desc + 2000 asc, up to 4000 unique)"
    )
    parser.add_argument(
        "--top",
        type=int,
        default=DEFAULT_TOP_N,
        help="Receivers from descending tx count (default 2000)",
    )
    parser.add_argument(
        "--light",
        type=int,
        default=DEFAULT_LIGHT_N,
        help="Receivers from ascending tx count (default 2000)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_POOL_PATH,
        help="Output JSON path",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_DISCOVER_TIMEOUT_SEC,
        help=(
            "HTTP timeout per discovery query in seconds "
            "(env DISCOVER_TIMEOUT_SEC; default 60)"
        ),
    )
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be a positive integer (seconds)")
    asyncio.run(
        run_discovery(
            top_n=args.top,
            light_n=args.light,
            output=args.output,
            timeout_sec=args.timeout,
        )
    )


if __name__ == "__main__":
    main()
