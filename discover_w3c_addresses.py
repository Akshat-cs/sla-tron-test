"""
Build the W3C multi-chain address pool — one discovery query per chain.

For each chain in CHAINS:
  * EVM chains (ethereum/bsc/matic/arbitrum) → V2 EAP top-receivers query on
    BITQUERY_GRAPHQL_URL.
  * Solana → V1 top-receivers query on BITQUERY_V1_URL.

Writes data/w3c_addresses.json in this shape (consumed by w3c_workload.AddressBook):

  {
    "generated_at": "2026-05-27T10:30:00Z",
    "discovery_days": 365,
    "per_chain_target": 1000,
    "chains": {
      "ethereum": {"discovered": 1000, "addresses": [{"address": "...", "estimated_txs": 12345}, ...]},
      ...
    }
  }

Usage:
  python discover_w3c_addresses.py                          # 1000 per chain, 365 days
  python discover_w3c_addresses.py --per-chain 500
  python discover_w3c_addresses.py --chains ethereum,solana
  python discover_w3c_addresses.py --days 180 --timeout 300
  python discover_w3c_addresses.py --days 365 --solana-days 30   # shorter window for Solana V1
  python discover_w3c_addresses.py --max-txs 50000 --overfetch 5  # realism cap (default)
  python discover_w3c_addresses.py --max-txs 10000 --min-txs 100  # tighter realism band
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

from w3c_workload import (
    CHAINS,
    CHAINS_BY_NAME,
    Chain,
    DEFAULT_W3C_POOL_PATH,
    build_discovery_query_for_chain,
)

load_dotenv()

DEFAULT_PER_CHAIN = int(os.getenv("W3C_DISCOVER_PER_CHAIN", "1000"))
DEFAULT_DAYS = int(os.getenv("W3C_DISCOVER_DAYS", "365"))
# Solana V1 aggregation is dramatically heavier per day than EVM, so allow a
# shorter window just for Solana. None means "fall back to --days".
_solana_days_env = os.getenv("W3C_DISCOVER_SOLANA_DAYS")
DEFAULT_SOLANA_DAYS: int | None = int(_solana_days_env) if _solana_days_env else None
DEFAULT_TIMEOUT_SEC = int(os.getenv("W3C_DISCOVER_TIMEOUT_SEC", "300"))

# Realism cap: the very top of the "by descending tx count" list is dominated
# by DEX routers / CEX hot wallets with 15M+ transfers. Aggregating those over
# the SLA window will always blow latency — they are not what a W3C end-user
# looks like.
#
# Per-chain strategy:
#   * EVM    → orderBy desc + `count(selectWhere: {ge:$min, lt:$max})` filters
#              server-side, so we pick the busiest addresses still under the cap.
#   * Solana → orderBy `asc: "count"` naturally yields low-count addresses
#              already under the cap. Client-side filter stays as a safety net
#              and to enforce --min-txs.
DEFAULT_MAX_TXS = int(os.getenv("W3C_DISCOVER_MAX_TXS", "50000"))
DEFAULT_MIN_TXS = int(os.getenv("W3C_DISCOVER_MIN_TXS", "0"))
# Safety multiplier for client-side filtering paths (Solana). Default 1 because
# asc-ordering already keeps Solana under the cap.
DEFAULT_OVERFETCH = max(1, int(os.getenv("W3C_DISCOVER_OVERFETCH", "1")))


def _days_for_chain(chain: Chain, days: int, solana_days: int | None) -> int:
    if chain.family == "solana_v1" and solana_days is not None:
        return solana_days
    return days


def _filter_by_tx_count(
    rows: list[dict],
    *,
    min_txs: int,
    max_txs: int,
) -> tuple[list[dict], int, int]:
    """Drop rows whose estimated_txs falls outside [min_txs, max_txs].

    Returns (kept, dropped_too_busy, dropped_too_quiet).
    Rows whose estimated_txs is None are dropped (counted as too_quiet) because
    we cannot verify they meet the realism cap.
    """
    kept: list[dict] = []
    too_busy = too_quiet = 0
    for r in rows:
        txs = r.get("estimated_txs")
        if txs is None:
            too_quiet += 1
            continue
        if txs > max_txs:
            too_busy += 1
            continue
        if txs < min_txs:
            too_quiet += 1
            continue
        kept.append(r)
    return kept, too_busy, too_quiet


def _date_window(chain: Chain, days: int) -> tuple[str, str]:
    """Return (from, to) in the date format the chain's schema requires.

    * EVM V2 (Block.Date)         → "YYYY-MM-DD"           (Date scalar)
    * Solana V1 (transfers.date)  → "YYYY-MM-DDTHH:MM:SSZ" (ISO8601DateTime scalar)
    """
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)
    if chain.family == "solana_v1":
        return (
            start.strftime("%Y-%m-%dT00:00:00Z"),
            now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
    return start.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d")


def _extract_evm_receivers(payload: dict) -> list[dict]:
    out: list[dict] = []
    try:
        rows = payload["data"]["EVM"]["Transfers"] or []
    except (KeyError, TypeError):
        return out
    for row in rows:
        addr = ((row or {}).get("Transfer") or {}).get("Receiver")
        txs = (row or {}).get("txs")
        if isinstance(addr, str) and addr:
            out.append({"address": addr, "estimated_txs": int(txs) if txs is not None else None})
    return out


def _extract_solana_receivers(payload: dict) -> list[dict]:
    out: list[dict] = []
    try:
        rows = payload["data"]["solana"]["transfers"] or []
    except (KeyError, TypeError):
        return out
    for row in rows:
        addr = ((row or {}).get("receiver") or {}).get("address")
        cnt = (row or {}).get("count")
        if isinstance(addr, str) and addr:
            out.append({"address": addr, "estimated_txs": int(cnt) if cnt is not None else None})
    return out


def _dedupe_keep_order(items: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for it in items:
        addr = it.get("address")
        if not isinstance(addr, str) or addr in seen:
            continue
        seen.add(addr)
        out.append(it)
    return out


async def _post(
    session: aiohttp.ClientSession,
    *,
    url: str,
    token: str,
    query: str,
    variables: dict,
    timeout_sec: int,
) -> dict:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    body = {"query": query, "variables": variables}
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    async with session.post(url, json=body, headers=headers, timeout=timeout) as resp:
        text = await resp.text()
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} from {url}: {text}")
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON from {url}: {exc}; body={text}") from exc


async def discover_chain(
    session: aiohttp.ClientSession,
    *,
    chain: Chain,
    per_chain: int,
    date_from: str,
    date_to: str,
    timeout_sec: int,
    token: str,
    max_txs: int,
    min_txs: int,
    overfetch: int,
) -> tuple[list[dict], int, int, int]:
    """Run the discovery query, filter by tx-count realism cap, truncate to target.

    Returns (kept_rows, raw_returned, dropped_too_busy, dropped_too_quiet).
    """
    query = build_discovery_query_for_chain(chain)
    url = chain.endpoint_url()
    # EVM filters server-side via `count(selectWhere:...)` so we ask for exactly
    # `per_chain` rows. Solana V1 filters client-side, so we oversample.
    fetch_limit = per_chain if chain.family == "evm" else per_chain * overfetch
    variables: dict = {"limit": fetch_limit, "from": date_from, "to": date_to}
    if chain.family == "evm":
        variables["network"] = chain.evm_network
        # Bitquery V2 EAP `selectWhere` expects the bound values as strings.
        variables["min_txs"] = str(min_txs)
        variables["max_txs"] = str(max_txs)
    payload = await _post(
        session,
        url=url,
        token=token,
        query=query,
        variables=variables,
        timeout_sec=timeout_sec,
    )
    if payload.get("errors"):
        raise RuntimeError(
            f"Discovery for {chain.name} returned GraphQL errors: "
            f"{json.dumps(payload['errors'], ensure_ascii=False)}"
        )
    if chain.family == "evm":
        rows = _extract_evm_receivers(payload)
    elif chain.family == "solana_v1":
        rows = _extract_solana_receivers(payload)
    else:
        raise RuntimeError(f"Unsupported chain family: {chain.family!r}")
    rows = _dedupe_keep_order(rows)
    raw_returned = len(rows)
    # Defensive client-side filter even for EVM — covers the rare case where the
    # server returned a row outside the bounds (shouldn't happen with selectWhere
    # but cheap insurance).
    kept, too_busy, too_quiet = _filter_by_tx_count(
        rows, min_txs=min_txs, max_txs=max_txs
    )
    return kept[:per_chain], raw_returned, too_busy, too_quiet


async def run_discovery(
    *,
    chains_to_run: list[Chain],
    per_chain: int,
    days: int,
    solana_days: int | None,
    timeout_sec: int,
    max_txs: int,
    min_txs: int,
    overfetch: int,
    output_path: Path,
) -> None:
    token = os.getenv("BITQUERY_TOKEN")
    if not token:
        raise SystemExit("Set BITQUERY_TOKEN in .env")

    solana_msg = (
        f" (solana: last {solana_days} days)"
        if solana_days is not None and solana_days != days
        else ""
    )
    floor_msg = f" floor {min_txs:,}," if min_txs > 0 else ""
    print(
        f"Discovering {per_chain} addresses per chain over the last {days} days"
        f"{solana_msg} (per-query timeout {timeout_sec}s)\n"
        f"Realism filter:{floor_msg} cap {max_txs:,} txs/address  "
        f"(EVM: desc + server-side selectWhere · Solana: asc + client-side, "
        f"overfetch {overfetch}×)\n"
    )

    results: dict[str, dict] = {}
    overall_start = time.perf_counter()
    async with aiohttp.ClientSession() as session:
        for chain in chains_to_run:
            chain_days = _days_for_chain(chain, days, solana_days)
            date_from, date_to = _date_window(chain, chain_days)
            label = (
                f"{chain.name} (V2 EAP, network={chain.evm_network})"
                if chain.family == "evm"
                else f"{chain.name} (V1, ISO8601 dates)"
            )
            print(
                f"  → {label} … {chain_days}d window: {date_from} → {date_to}",
                flush=True,
            )
            chain_start = time.perf_counter()
            try:
                addrs, raw_returned, too_busy, too_quiet = await discover_chain(
                    session,
                    chain=chain,
                    per_chain=per_chain,
                    date_from=date_from,
                    date_to=date_to,
                    timeout_sec=timeout_sec,
                    token=token,
                    max_txs=max_txs,
                    min_txs=min_txs,
                    overfetch=overfetch,
                )
            except Exception as exc:  # noqa: BLE001
                elapsed = time.perf_counter() - chain_start
                print(
                    f"    FAILED in {elapsed:.1f}s — {exc}\n"
                    f"    (continuing with other chains)\n",
                    file=sys.stderr,
                )
                results[chain.name] = {
                    "discovered": 0,
                    "addresses": [],
                    "window_days": chain_days,
                    "date_window": {"from": date_from, "to": date_to},
                    "error": str(exc),
                }
                continue
            elapsed = time.perf_counter() - chain_start
            results[chain.name] = {
                "discovered": len(addrs),
                "addresses": addrs,
                "window_days": chain_days,
                "date_window": {"from": date_from, "to": date_to},
                "raw_returned": raw_returned,
                "dropped_too_busy": too_busy,
                "dropped_too_quiet": too_quiet,
                "tx_count_filter": {"min": min_txs, "max": max_txs},
            }
            shortfall_note = ""
            if len(addrs) < per_chain:
                shortfall_note = (
                    f"  [WARN: only {len(addrs)}/{per_chain} qualified — "
                    f"consider raising --overfetch or --max-txs]"
                )
            print(
                f"    {len(addrs)} kept in {elapsed:.1f}s "
                f"(raw {raw_returned}, dropped {too_busy} too busy, "
                f"{too_quiet} too quiet){shortfall_note}",
                flush=True,
            )

    total_elapsed = time.perf_counter() - overall_start
    output: dict = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "discovery_days": days,
        "solana_discovery_days": solana_days if solana_days is not None else days,
        "per_chain_target": per_chain,
        "tx_count_filter": {"min": min_txs, "max": max_txs},
        "overfetch_multiplier": overfetch,
        "elapsed_sec": round(total_elapsed, 1),
        "chains": results,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2), encoding="utf-8")

    print(f"\nWrote pool to {output_path}")
    print("Summary:")
    for name, body in results.items():
        n = body.get("discovered", 0)
        marker = " (FAILED)" if body.get("error") else ""
        print(f"  {name:<10} {n} addresses{marker}")
    print(f"  total elapsed: {total_elapsed:.1f}s")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Discover W3C address pool — top receivers per chain over the last N days",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--per-chain", type=int, default=DEFAULT_PER_CHAIN, metavar="N",
        help="Top N receivers to fetch per chain",
    )
    parser.add_argument(
        "--days", type=int, default=DEFAULT_DAYS, metavar="D",
        help="Discovery date window (days, trailing) — applies to all chains "
             "unless --solana-days overrides for Solana",
    )
    parser.add_argument(
        "--solana-days", type=int, default=DEFAULT_SOLANA_DAYS, metavar="D",
        help="Override --days just for Solana (V1 aggregation is heavier per day; "
             "default: same as --days, or W3C_DISCOVER_SOLANA_DAYS env var)",
    )
    parser.add_argument(
        "--timeout", type=int, default=DEFAULT_TIMEOUT_SEC, metavar="SEC",
        help="HTTP timeout per discovery query (heavy aggregations can take minutes)",
    )
    parser.add_argument(
        "--max-txs", type=int, default=DEFAULT_MAX_TXS, metavar="N",
        help="Drop any address with more than N transfers in the discovery window "
             "(filters out DEX routers / CEX hot wallets with 15M+ txs that are "
             "not representative of W3C end-users)",
    )
    parser.add_argument(
        "--min-txs", type=int, default=DEFAULT_MIN_TXS, metavar="N",
        help="Drop any address with fewer than N transfers (0 = no floor)",
    )
    parser.add_argument(
        "--overfetch", type=int, default=DEFAULT_OVERFETCH, metavar="X",
        help="Fetch X × per-chain raw rows from Bitquery, then filter, then "
             "truncate to per-chain. Raise if --max-txs leaves you short.",
    )
    parser.add_argument(
        "--chains", type=str, default=None, metavar="LIST",
        help="Comma-separated subset of chains to discover (default: all CHAINS)",
    )
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_W3C_POOL_PATH,
        help="Where to write the pool JSON",
    )
    args = parser.parse_args()

    if args.per_chain <= 0:
        parser.error("--per-chain must be positive")
    if args.days <= 0:
        parser.error("--days must be positive")
    if args.solana_days is not None and args.solana_days <= 0:
        parser.error("--solana-days must be positive")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.max_txs <= 0:
        parser.error("--max-txs must be positive")
    if args.min_txs < 0:
        parser.error("--min-txs cannot be negative")
    if args.min_txs >= args.max_txs:
        parser.error("--min-txs must be less than --max-txs")
    if args.overfetch <= 0:
        parser.error("--overfetch must be positive")

    if args.chains:
        names = [s.strip() for s in args.chains.split(",") if s.strip()]
        bad = [n for n in names if n not in CHAINS_BY_NAME]
        if bad:
            parser.error(
                f"Unknown chains {bad}; valid: {sorted(CHAINS_BY_NAME)}"
            )
        chains_to_run = [CHAINS_BY_NAME[n] for n in names]
    else:
        chains_to_run = list(CHAINS)

    asyncio.run(
        run_discovery(
            chains_to_run=chains_to_run,
            per_chain=args.per_chain,
            days=args.days,
            solana_days=args.solana_days,
            timeout_sec=args.timeout,
            max_txs=args.max_txs,
            min_txs=args.min_txs,
            overfetch=args.overfetch,
            output_path=args.output,
        )
    )


if __name__ == "__main__":
    main()
