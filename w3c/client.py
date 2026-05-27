"""W3C HTTP/GraphQL client.

Standalone (does not share code with graphql_client.py, which stays Tron-specific)
so Nomics regressions are impossible. Same structural patterns as the Tron client:

  * SLA-bounded request (asyncio.wait_for + aiohttp ClientTimeout)
  * Per-chain endpoint URL (V2 EAP for EVM, V1 for Solana)
  * Distinct error categories: HTTP-non-200, JSON-parse, GraphQL-errors, GraphQL-null
  * Returns a normalized W3cQueryOutcome with the full raw response body retained on error
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass

import aiohttp

from w3c.workload import W3cRequest, build_query_for_chain, build_variables


@dataclass
class W3cQueryOutcome:
    chain: str
    addresses: tuple[str, ...]
    latency_ms: float
    status: int                  # HTTP status (0 on timeout / connection error)
    error: str | None            # None on full success
    raw_response_body: str | None = None
    sending_sum_usd: float | None = None
    receiving_sum_usd: float | None = None


def _first_amount(rows: object, *, key: str) -> float | None:
    """Return the aggregated USD amount from the first row of a transfers list."""
    if not isinstance(rows, list) or not rows:
        return 0.0
    first = rows[0]
    if not isinstance(first, dict):
        return None
    val = first.get(key)
    if val is None:
        return 0.0
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _validate_payload(payload: dict, family: str) -> tuple[str | None, float | None, float | None]:
    """
    Return (error_msg, sending_sum, receiving_sum).

    error_msg is None when the payload looks structurally OK; otherwise it carries
    the full Bitquery message (no truncation) so the per-request log captures it.
    """
    errors = payload.get("errors")
    if errors:
        return json.dumps(errors, ensure_ascii=False), None, None

    data = payload.get("data")
    if data is None:
        return "GraphQL data is null (check errors/extensions in raw response)", None, None

    # V2 EVM: data.EVM.{Sending,Receiving}[0].Transfer_AmountInUSD
    # V1 Solana: data.solana.{sending,receiving}[0].amount
    if family == "evm":
        namespace_key = "EVM"
        sending_key, receiving_key = "Sending", "Receiving"
        amount_field = "Transfer_AmountInUSD"
    elif family == "solana_v1":
        namespace_key = "solana"
        sending_key, receiving_key = "sending", "receiving"
        amount_field = "amount"
    else:
        return f"Unknown chain family: {family!r}", None, None

    ns_payload = data.get(namespace_key)
    if ns_payload is None:
        return (
            f"GraphQL data.{namespace_key} is null — often rate-limit / overload",
            None,
            None,
        )

    sending = _first_amount(ns_payload.get(sending_key), key=amount_field)
    receiving = _first_amount(ns_payload.get(receiving_key), key=amount_field)
    if sending is None or receiving is None:
        return (
            f"GraphQL data.{namespace_key} payload missing {sending_key}/{receiving_key} sums",
            sending,
            receiving,
        )
    return None, sending, receiving


def _enforce_sla_latency(outcome: W3cQueryOutcome, sla_sec: float) -> W3cQueryOutcome:
    """Mark a response that arrived but exceeded SLA as failed."""
    if outcome.error is not None:
        return outcome
    if outcome.latency_ms <= sla_sec * 1000:
        return outcome
    return W3cQueryOutcome(
        chain=outcome.chain,
        addresses=outcome.addresses,
        latency_ms=outcome.latency_ms,
        status=outcome.status,
        error=(
            f"Exceeded {sla_sec:g}s SLA ({outcome.latency_ms:.0f}ms) — request stopped"
        ),
        raw_response_body=outcome.raw_response_body,
        sending_sum_usd=outcome.sending_sum_usd,
        receiving_sum_usd=outcome.receiving_sum_usd,
    )


async def execute_w3c_query(
    session: aiohttp.ClientSession,
    *,
    request: W3cRequest,
    timeout_sec: float,
    url: str | None = None,
    token: str | None = None,
) -> W3cQueryOutcome:
    """One HTTP POST for one W3C query (Send + Receive aggregation).

    Routes to the chain's endpoint URL (V2 EAP for EVM, V1 for Solana) unless
    `url` is supplied explicitly.
    """

    endpoint = url or request.chain.endpoint_url()
    token = token or os.environ["BITQUERY_TOKEN"]
    query_text = build_query_for_chain(request.chain)
    body = {"query": query_text, "variables": build_variables(request)}
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    started = time.perf_counter()

    async def _do_post() -> W3cQueryOutcome:
        post_timeout = aiohttp.ClientTimeout(
            total=timeout_sec,
            connect=min(10.0, timeout_sec),
            sock_read=timeout_sec,
            sock_connect=min(10.0, timeout_sec),
        )
        async with session.post(endpoint, json=body, headers=headers, timeout=post_timeout) as resp:
            text = await resp.text()
            latency_ms = (time.perf_counter() - started) * 1000

            if resp.status != 200:
                return W3cQueryOutcome(
                    chain=request.chain.name,
                    addresses=request.addresses,
                    latency_ms=latency_ms,
                    status=resp.status,
                    error=f"HTTP {resp.status}: {text}",
                    raw_response_body=text,
                )

            try:
                payload = json.loads(text)
            except json.JSONDecodeError as exc:
                return W3cQueryOutcome(
                    chain=request.chain.name,
                    addresses=request.addresses,
                    latency_ms=latency_ms,
                    status=resp.status,
                    error=f"Invalid JSON: {exc}; body={text}",
                    raw_response_body=text,
                )

            err_msg, sending, receiving = _validate_payload(payload, request.chain.family)
            return W3cQueryOutcome(
                chain=request.chain.name,
                addresses=request.addresses,
                latency_ms=latency_ms,
                status=resp.status,
                error=err_msg,
                raw_response_body=(text if err_msg else None),
                sending_sum_usd=sending,
                receiving_sum_usd=receiving,
            )

    try:
        outcome = await asyncio.wait_for(_do_post(), timeout=timeout_sec)
        return _enforce_sla_latency(outcome, timeout_sec)
    except (asyncio.TimeoutError, TimeoutError):
        elapsed = (time.perf_counter() - started) * 1000
        return W3cQueryOutcome(
            chain=request.chain.name,
            addresses=request.addresses,
            latency_ms=max(elapsed, timeout_sec * 1000),
            status=0,
            error=f"Request timeout after {timeout_sec:g}s (SLA limit) — request stopped",
        )
    except aiohttp.ClientError as exc:
        elapsed = (time.perf_counter() - started) * 1000
        return W3cQueryOutcome(
            chain=request.chain.name,
            addresses=request.addresses,
            latency_ms=elapsed,
            status=0,
            error=str(exc),
        )
    except Exception as exc:  # noqa: BLE001
        elapsed = (time.perf_counter() - started) * 1000
        return W3cQueryOutcome(
            chain=request.chain.name,
            addresses=request.addresses,
            latency_ms=elapsed,
            status=0,
            error=str(exc),
        )
