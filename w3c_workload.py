"""
W3C multi-chain SLA workload definitions.

Chain registry (per-chain endpoint URL, schema family, query templates) +
address pool loader + per-request generator that simulates one user's batched
wallet call (1-3 addresses, trailing 12-month window).

Schema/endpoint choice per chain:
  * ethereum / arbitrum / bsc / matic  →  V2 EAP at BITQUERY_GRAPHQL_URL
  * solana                              →  V1 at BITQUERY_V1_URL (V2 Solana is
                                           only ~last 8 hours of real-time data)

Extending to a new chain: append to CHAINS, drop addresses into the pool JSON.
"""

from __future__ import annotations

import json
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).parent
_QUERIES_DIR = _REPO_ROOT / "queries"
DEFAULT_W3C_POOL_PATH = _REPO_ROOT / "data" / "w3c_addresses.json"

# Workload knobs (W3C requirement: up to 3 addresses batched per query, up to 5 chains).
MIN_ADDRESSES_PER_QUERY = max(1, int(os.getenv("W3C_MIN_ADDRESSES", "1")))
MAX_ADDRESSES_PER_QUERY = max(
    MIN_ADDRESSES_PER_QUERY, int(os.getenv("W3C_MAX_ADDRESSES", "3"))
)
DATE_WINDOW_DAYS = max(1, int(os.getenv("W3C_DATE_WINDOW_DAYS", "365")))

# Env var names that hold each endpoint URL.
ENV_V2_URL = "BITQUERY_GRAPHQL_URL"   # streaming.bitquery.io/graphql (EVM)
ENV_V1_URL = "BITQUERY_V1_URL"        # graphql.bitquery.io (Solana V1)
DEFAULT_V1_URL = "https://graphql.bitquery.io"


@dataclass(frozen=True)
class Chain:
    """One chain in the W3C workload: schema family + endpoint + query template."""

    name: str                 # short label for logs / reports (e.g. "ethereum")
    family: str               # "evm" | "solana_v1"
    query_file: str           # filename under queries/ (main SLA query)
    discovery_file: str       # filename under queries/ (top-receivers discovery)
    endpoint_env: str         # env var that holds the endpoint URL for this chain
    # EVM only: $network arg value passed to EVM(network: …).
    evm_network: str | None = None

    def query_path(self) -> Path:
        return _QUERIES_DIR / self.query_file

    def discovery_path(self) -> Path:
        return _QUERIES_DIR / self.discovery_file

    def endpoint_url(self) -> str:
        """Resolve the endpoint URL via the env var. V1 has a default fallback."""
        url = os.getenv(self.endpoint_env)
        if url:
            return url
        if self.endpoint_env == ENV_V1_URL:
            return DEFAULT_V1_URL
        raise RuntimeError(
            f"Endpoint env var {self.endpoint_env} is not set "
            f"(needed for chain {self.name!r})"
        )


# All chains exercised by the W3C SLA test. Order is stable for round-robin generators.
CHAINS: tuple[Chain, ...] = (
    Chain(
        name="ethereum",
        family="evm",
        query_file="w3c_evm_transfers.graphql",
        discovery_file="w3c_discover_evm_receivers.graphql",
        endpoint_env=ENV_V2_URL,
        evm_network="eth",
    ),
    Chain(
        name="bsc",
        family="evm",
        query_file="w3c_evm_transfers.graphql",
        discovery_file="w3c_discover_evm_receivers.graphql",
        endpoint_env=ENV_V2_URL,
        evm_network="bsc",
    ),
    Chain(
        name="matic",
        family="evm",
        query_file="w3c_evm_transfers.graphql",
        discovery_file="w3c_discover_evm_receivers.graphql",
        endpoint_env=ENV_V2_URL,
        evm_network="matic",
    ),
    Chain(
        name="arbitrum",
        family="evm",
        query_file="w3c_evm_transfers.graphql",
        discovery_file="w3c_discover_evm_receivers.graphql",
        endpoint_env=ENV_V2_URL,
        evm_network="arbitrum",
    ),
    Chain(
        name="solana",
        family="solana_v1",
        query_file="w3c_solana_transfers.graphql",
        discovery_file="w3c_discover_solana_receivers.graphql",
        endpoint_env=ENV_V1_URL,
    ),
)
CHAINS_BY_NAME: dict[str, Chain] = {c.name: c for c in CHAINS}


@dataclass(frozen=True)
class W3cRequest:
    """One scheduled query: which chain, which addresses, which date window."""

    chain: Chain
    addresses: tuple[str, ...]
    date_from: str
    date_to: str


class AddressBook:
    """Per-chain pool of sample addresses, loaded from JSON.

    Accepts two pool entry formats per chain:
      * list of strings:  ["0xabc...", "0xdef..."]
      * list of dicts:    [{"address": "0x...", "estimated_txs": 12345}, ...]
        (discovery script writes this richer shape; only `address` is used here)
    """

    def __init__(self, by_chain: dict[str, list[str]]) -> None:
        if not by_chain:
            raise ValueError("AddressBook is empty")
        for name in by_chain:
            if name not in CHAINS_BY_NAME:
                raise ValueError(
                    f"Address pool references unknown chain {name!r}; "
                    f"allowed: {sorted(CHAINS_BY_NAME)}"
                )
            if not by_chain[name]:
                raise ValueError(f"Chain {name!r} has zero addresses in the pool")
        self._by_chain = {name: list(addrs) for name, addrs in by_chain.items()}

    @staticmethod
    def _extract_addresses(items: list) -> list[str]:
        cleaned: list[str] = []
        for item in items:
            if isinstance(item, str):
                addr = item.strip()
            elif isinstance(item, dict):
                addr = (item.get("address") or "").strip()
            else:
                continue
            if addr:
                cleaned.append(addr)
        return cleaned

    @classmethod
    def load(cls, path: Path | None = None) -> AddressBook:
        target = Path(
            path
            or os.getenv("W3C_ADDRESSES_PATH", DEFAULT_W3C_POOL_PATH)
        )
        if not target.is_file():
            raise FileNotFoundError(
                f"W3C address pool not found: {target}\n"
                "Run:  python discover_w3c_addresses.py\n"
                "(builds top-receivers per chain; or edit data/w3c_addresses.json by hand.)"
            )
        raw = json.loads(target.read_text(encoding="utf-8"))
        chains_obj = raw.get("chains", {})
        by_chain: dict[str, list[str]] = {}
        for name, body in chains_obj.items():
            if not isinstance(body, dict):
                continue
            addrs = cls._extract_addresses(body.get("addresses") or [])
            if addrs:
                by_chain[name] = addrs
        return cls(by_chain)

    def chains_present(self) -> list[str]:
        return list(self._by_chain)

    def sample_addresses(self, chain_name: str, count: int) -> tuple[str, ...]:
        pool = self._by_chain[chain_name]
        if count >= len(pool):
            return tuple(pool)
        return tuple(random.sample(pool, count))

    def summary(self) -> str:
        parts = [f"{name}={len(addrs)}" for name, addrs in self._by_chain.items()]
        return ", ".join(parts)


def trailing_window(
    chain: Chain | None = None,
    now: datetime | None = None,
    days: int = DATE_WINDOW_DAYS,
) -> tuple[str, str]:
    """Return (from, to) date strings in the format the chain's schema requires.

    * EVM V2 (Block.Date)         → "YYYY-MM-DD"           (Date scalar)
    * Solana V1 (transfers.date)  → "YYYY-MM-DDTHH:MM:SSZ" (ISO8601DateTime scalar)

    If `chain` is None, defaults to plain "YYYY-MM-DD".
    """
    now = now or datetime.now(timezone.utc)
    start = now - timedelta(days=days)
    if chain is not None and chain.family == "solana_v1":
        # V1 schema declares the variable as ISO8601DateTime — pass full timestamp.
        return (
            start.strftime("%Y-%m-%dT00:00:00Z"),
            now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
    return start.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d")


def build_variables(req: W3cRequest) -> dict[str, object]:
    """Variables dict for the GraphQL POST body.

    EVM queries also need `network` (the evm_network enum value: eth/bsc/...).
    Solana V1 omits it (network is a literal in the query body).
    """
    base: dict[str, object] = {
        "addresses": list(req.addresses),
        "from": req.date_from,
        "to": req.date_to,
    }
    if req.chain.family == "evm":
        base["network"] = req.chain.evm_network
    return base


def build_query_for_chain(chain: Chain) -> str:
    """Load the main SLA query template for a chain.

    NOTE: Bitquery V2 EAP's GraphQL parser breaks when variable declarations
    span multiple lines. Keep `query X(...vars...) {` on a single line in the
    `.graphql` files — do NOT reformat the variable list onto multiple lines.
    """
    return chain.query_path().read_text(encoding="utf-8")


def build_discovery_query_for_chain(chain: Chain) -> str:
    """Load the top-receivers discovery template for a chain.

    Same single-line variable-declaration constraint as `build_query_for_chain`.
    """
    return chain.discovery_path().read_text(encoding="utf-8")


class RequestGenerator:
    """Yields W3cRequest objects round-robin across active chains."""

    def __init__(
        self,
        book: AddressBook,
        *,
        chains: list[Chain] | None = None,
        min_addresses: int = MIN_ADDRESSES_PER_QUERY,
        max_addresses: int = MAX_ADDRESSES_PER_QUERY,
    ) -> None:
        active = [c for c in (chains or CHAINS) if c.name in book.chains_present()]
        if not active:
            raise ValueError(
                "No active chains — address pool has no entries for any registered chain."
            )
        self._chains = active
        self._book = book
        self._min = max(1, min_addresses)
        self._max = max(self._min, max_addresses)
        self._idx = 0

    def active_chains(self) -> list[Chain]:
        return list(self._chains)

    def next_request(self) -> W3cRequest:
        chain = self._chains[self._idx % len(self._chains)]
        self._idx += 1
        n = random.randint(self._min, self._max)
        addrs = self._book.sample_addresses(chain.name, n)
        date_from, date_to = trailing_window(chain=chain)
        return W3cRequest(chain=chain, addresses=addrs, date_from=date_from, date_to=date_to)


def load_query_text(chain: Chain) -> str:
    """Back-compat alias — prefer `build_query_for_chain` for SLA traffic."""
    return build_query_for_chain(chain)
