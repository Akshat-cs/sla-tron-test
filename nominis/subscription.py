"""WebSocket subscription for Tron transfer freshness (ingest lag)."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

from nominis.client import _parse_block_time

SUBSCRIPTION_PATH = (
    Path(__file__).parent.parent / "queries" / "nominis" / "tron_freshness_subscription.graphql"
)
LOG_SUBSCRIPTION_BLOCKS = os.getenv("LOG_SUBSCRIPTION_BLOCKS", "true").lower() in (
    "1",
    "true",
    "yes",
)


def _graphql_ws_url() -> str:
    http_url = os.environ.get(
        "BITQUERY_GRAPHQL_URL", "https://streaming.bitquery.io/graphql"
    )
    token = os.environ["BITQUERY_TOKEN"]
    base = http_url.replace("https://", "wss://").replace("http://", "ws://")
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}token={token}"


def _format_utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )[:-3] + "Z"


def _format_block_number(block_num: int | str | None) -> str:
    if block_num is None:
        return "n/a"
    if isinstance(block_num, int):
        return f"{block_num:,}"
    return str(block_num)


def _process_subscription_payload(
    payload: dict,
    received_at: float,
    *,
    seen_blocks: set[str],
    event_index: int,
) -> tuple[list[float], int]:
    """Extract lags and print one line per new block."""
    lags: list[float] = []
    lines_logged = 0
    try:
        rows = payload["data"]["Tron"]["Transfers"] or []
    except (KeyError, TypeError):
        return lags, lines_logged

    for row in rows:
        block = row.get("Block") or {}
        block_time = block.get("Time")
        block_num = block.get("Number")
        ts = _parse_block_time(block_time) if block_time else None
        if ts is None:
            continue

        lag_sec = max(0.0, received_at - ts)
        lags.append(lag_sec)
        lag_ms = lag_sec * 1000

        block_key = f"{block_num or ''}:{block_time}"
        is_new_block = block_key not in seen_blocks
        if is_new_block:
            seen_blocks.add(block_key)

        if LOG_SUBSCRIPTION_BLOCKS and is_new_block:
            recv_str = _format_utc(received_at)
            block_label = _format_block_number(block_num)
            print(
                f"  SUB #{event_index:>5}  "
                f"block={block_label}  block_time={block_time}  |  "
                f"received={recv_str}  |  lag_ms={lag_ms:,.0f}",
                flush=True,
            )
            lines_logged += 1

    return lags, lines_logged


async def collect_freshness_lags(
    *,
    duration_sec: int,
    subscription_query: str | None = None,
) -> tuple[list[float], int]:
    """
    Subscribe to live Tron transfers; measure ingest lag per event.

    lag = wall_clock_now - Block.Time  (how long ago the block was mined when we received it)
    """
    query = subscription_query or SUBSCRIPTION_PATH.read_text(encoding="utf-8")
    url = _graphql_ws_url()
    lags: list[float] = []
    messages = 0
    blocks_logged = 0
    seen_blocks: set[str] = set()
    deadline = time.monotonic() + duration_sec

    print(
        "  SUB #  | block | block_time | received_at (UTC) | lag_ms",
        flush=True,
    )
    print("  " + "-" * 72, flush=True)

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(
            url,
            protocols=["graphql-ws"],
            heartbeat=30,
        ) as ws:
            await ws.send_json({"type": "connection_init"})
            started = False

            async for msg in ws:
                if time.monotonic() >= deadline:
                    break
                if msg.type != aiohttp.WSMsgType.TEXT:
                    if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
                    continue

                data = json.loads(msg.data)
                msg_type = data.get("type")

                if msg_type == "connection_ack" and not started:
                    await ws.send_json(
                        {
                            "type": "start",
                            "id": "freshness",
                            "payload": {"query": query},
                        }
                    )
                    started = True
                    print("  WebSocket connected, subscription started.", flush=True)
                    continue

                if msg_type == "data":
                    received_at = time.time()
                    payload = data.get("payload") or {}
                    batch, n_lines = _process_subscription_payload(
                        payload,
                        received_at,
                        seen_blocks=seen_blocks,
                        event_index=messages + 1,
                    )
                    if batch:
                        lags.extend(batch)
                        messages += 1
                        blocks_logged += n_lines
                    continue

                if msg_type == "error":
                    errors = (data.get("payload") or {}).get("errors", data)
                    raise RuntimeError(f"Subscription error: {errors}")

            if started:
                try:
                    await ws.send_json({"type": "complete", "id": "freshness"})
                except Exception:
                    pass

    print(
        f"\n  Subscription summary: {messages} payloads with transfers, "
        f"{blocks_logged} new blocks logged, {len(lags)} transfer lag samples",
        flush=True,
    )
    return lags, messages
