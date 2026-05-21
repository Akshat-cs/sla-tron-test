"""Load and rotate Tron receiver addresses for SLA tests (500-address pool)."""

from __future__ import annotations

import json
import os
import random
from dataclasses import dataclass
from pathlib import Path

DEFAULT_POOL_PATH = Path(__file__).parent / "data" / "receivers_pool.json"
MIN_POOL_SIZE = int(os.getenv("MIN_POOL_SIZE", "1"))


@dataclass(frozen=True)
class ReceiverProfile:
    address: str
    tier: str  # heavy (desc) | light (asc) — metadata only
    estimated_txs: int | None = None
    source: str | None = None  # desc | asc

    @classmethod
    def from_dict(cls, row: dict) -> ReceiverProfile:
        return cls(
            address=row["address"],
            tier=row.get("tier", "medium"),
            estimated_txs=row.get("estimated_txs"),
            source=row.get("source"),
        )


class AddressPool:
    """Combined pool; every request uses pick_at(index) round-robin across all addresses."""

    def __init__(self, receivers: list[ReceiverProfile], *, shuffle: bool = True) -> None:
        if not receivers:
            raise ValueError("Address pool is empty")
        self.receivers = list(receivers)
        if shuffle:
            random.shuffle(self.receivers)
        self._by_tier = {
            "heavy": [r for r in self.receivers if r.tier == "heavy"],
            "light": [r for r in self.receivers if r.tier == "light"],
        }

    @classmethod
    def load(cls, path: Path | None = None) -> AddressPool:
        path = path or Path(os.getenv("RECEIVERS_POOL_PATH", DEFAULT_POOL_PATH))
        if not path.exists():
            raise FileNotFoundError(
                f"Receiver pool not found: {path}\n"
                "Run: python discover_addresses.py\n"
                "(builds 2000 desc + 2000 asc addresses — no single RECEIVER fallback)"
            )
        data = json.loads(path.read_text(encoding="utf-8"))
        rows = data.get("receivers") or data
        profiles = [ReceiverProfile.from_dict(r) for r in rows]
        pool = cls(profiles, shuffle=True)
        if len(pool.receivers) < MIN_POOL_SIZE:
            raise ValueError(f"Pool has only {len(pool.receivers)} addresses")
        return pool

    @classmethod
    def resolve(cls) -> AddressPool:
        return cls.load()

    def pick_at(self, index: int) -> ReceiverProfile:
        """Round-robin: one address per request index across the full combined pool."""
        return self.receivers[index % len(self.receivers)]

    def all_addresses(self) -> list[str]:
        return [r.address for r in self.receivers]

    def summary(self) -> str:
        desc = sum(1 for r in self.receivers if r.source == "desc" or r.tier == "heavy")
        asc = sum(1 for r in self.receivers if r.source == "asc" or r.tier == "light")
        return f"total={len(self.receivers)} (desc/heavy≈{desc}, asc/light≈{asc})"


def set_random_seed() -> None:
    seed = os.getenv("RANDOM_SEED")
    if seed is not None:
        random.seed(int(seed))
