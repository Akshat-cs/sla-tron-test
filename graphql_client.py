import asyncio
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path

import aiohttp

QUERY_PATH = Path(__file__).parent / "queries" / "tron_transfers.graphql"


@dataclass
class QueryResult:
    latency_ms: float
    status: int
    record_count: int
    error: str | None
    raw: dict | None


def load_query() -> str:
    return QUERY_PATH.read_text(encoding="utf-8")


def _parse_block_time(value: str) -> float | None:
    from datetime import datetime, timezone

    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _extract_transfers(payload: dict) -> list[dict] | None:
    try:
        tron = payload["data"]["Tron"]
        if tron is None:
            return None
        transfers = tron.get("Transfers")
        if transfers is None:
            return None
        return transfers or []
    except (KeyError, TypeError):
        return None


def _graphql_error_message(payload: dict, *, http_status: int, body_text: str) -> str | None:
    if http_status != 200:
        return f"HTTP {http_status}: {body_text[:400]}"

    errors = payload.get("errors")
    if errors:
        return json.dumps(errors, ensure_ascii=False)[:2000]

    data = payload.get("data")
    if data is None:
        return "GraphQL data is null (check errors/extensions in raw response)"

    tron = data.get("Tron")
    if tron is None:
        return (
            "GraphQL data.Tron is null — often rate-limit or overload under parallel load"
        )

    if tron.get("Transfers") is None:
        return "GraphQL data.Tron.Transfers is null"

    return None


def _enforce_sla_latency(
    result: QueryResult, *, timeout_sec: float | None
) -> QueryResult:
    """Any response slower than SLA limit is marked unsuccessful."""
    if timeout_sec is None or result.error is not None:
        return result
    limit_ms = timeout_sec * 1000
    if result.latency_ms > limit_ms:
        return QueryResult(
            latency_ms=result.latency_ms,
            status=result.status,
            record_count=result.record_count,
            error=(
                f"Exceeded {timeout_sec}s SLA ({result.latency_ms:.0f}ms) — "
                "request stopped"
            ),
            raw=result.raw,
        )
    return result


async def execute_graphql(
    session: aiohttp.ClientSession,
    *,
    query: str,
    variables: dict | None = None,
    url: str | None = None,
    token: str | None = None,
) -> dict:
    url = url or os.environ["BITQUERY_GRAPHQL_URL"]
    token = token or os.environ["BITQUERY_TOKEN"]
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    body = {"query": query, "variables": variables or {}}
    async with session.post(url, json=body, headers=headers) as resp:
        text = await resp.text()
        if resp.status != 200:
            return {"errors": [{"message": text[:500]}], "status": resp.status}
        return json.loads(text)


async def execute_transfer_query(
    session: aiohttp.ClientSession,
    *,
    receiver: str,
    limit: int,
    url: str | None = None,
    token: str | None = None,
    timeout_sec: float | None = None,
) -> QueryResult:
    url = url or os.environ["BITQUERY_GRAPHQL_URL"]
    token = token or os.environ["BITQUERY_TOKEN"]
    query = load_query()
    body = {
        "query": query,
        "variables": {"receiver": receiver, "limit": limit},
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    started = time.perf_counter()

    async def _do_post() -> QueryResult:
        post_timeout = None
        if timeout_sec is not None:
            post_timeout = aiohttp.ClientTimeout(
                total=timeout_sec,
                connect=min(10.0, timeout_sec),
                sock_read=timeout_sec,
                sock_connect=min(10.0, timeout_sec),
            )
        async with session.post(
            url,
            json=body,
            headers=headers,
            timeout=post_timeout,
        ) as resp:
            text = await resp.text()
            latency_ms = (time.perf_counter() - started) * 1000
            if resp.status != 200:
                return QueryResult(
                    latency_ms=latency_ms,
                    status=resp.status,
                    record_count=0,
                    error=f"HTTP {resp.status}: {text[:2000]}",
                    raw=None,
                )
            try:
                payload = json.loads(text)
            except json.JSONDecodeError as exc:
                return QueryResult(
                    latency_ms=latency_ms,
                    status=resp.status,
                    record_count=0,
                    error=f"Invalid JSON: {exc}; body={text[:300]}",
                    raw=None,
                )

            err_msg = _graphql_error_message(
                payload, http_status=resp.status, body_text=text
            )
            if err_msg:
                return QueryResult(
                    latency_ms=latency_ms,
                    status=resp.status,
                    record_count=0,
                    error=err_msg,
                    raw=payload,
                )

            transfers = _extract_transfers(payload)
            if transfers is None:
                return QueryResult(
                    latency_ms=latency_ms,
                    status=resp.status,
                    record_count=0,
                    error="Could not parse Transfers from response",
                    raw=payload,
                )

            return QueryResult(
                latency_ms=latency_ms,
                status=resp.status,
                record_count=len(transfers),
                error=None,
                raw=payload,
            )

    try:
        if timeout_sec is not None:
            result = await asyncio.wait_for(_do_post(), timeout=timeout_sec)
        else:
            result = await _do_post()
        return _enforce_sla_latency(result, timeout_sec=timeout_sec)
    except (asyncio.TimeoutError, TimeoutError):
        elapsed = (time.perf_counter() - started) * 1000
        limit_ms = (timeout_sec or 0) * 1000
        return QueryResult(
            latency_ms=max(elapsed, limit_ms),
            status=0,
            record_count=0,
            error=f"Request timeout after {timeout_sec}s (SLA limit) — request stopped",
            raw=None,
        )
    except aiohttp.ClientError as exc:
        elapsed = (time.perf_counter() - started) * 1000
        return QueryResult(
            latency_ms=elapsed,
            status=0,
            record_count=0,
            error=str(exc),
            raw=None,
        )
    except Exception as exc:  # noqa: BLE001
        elapsed = (time.perf_counter() - started) * 1000
        return QueryResult(
            latency_ms=elapsed,
            status=0,
            record_count=0,
            error=str(exc),
            raw=None,
        )
