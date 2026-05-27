# Bitquery customer SLA test suite

Two customer scenarios live side by side in this repo. They share the HTTP/log/error-categorization plumbing but each has its own entry point, config, and metrics so they cannot clobber each other.

| Scenario | Entry point | What it tests |
|----------|-------------|---------------|
| **Nomics — Tron** | `python sla_report.py` | 4 QPS Tron `Transfers` row-list queries + WebSocket freshness subscription |
| **W3C — multi-chain** | `python w3c_report.py` | 72 QPS sustained Transfers-aggregation across Ethereum / Arbitrum / BNB / Polygon / Solana, ~42 concurrent |

---

## Scenario 1 — Nomics Tron SLA

Measures Bitquery performance against the **Nomics SLA** and reports **fulfillment %** per metric (no pass/fail thresholds).

| SLA | How we measure |
|-----|----------------|
| Peak query throughput (4 QPS) | **Context only** in report: e.g. `4 queries/sec for 1000s to schedule N addresses` — not a fulfillment % |
| Query response time | % of **all** HTTP requests that finish within ≤ 5s (unsuccessful = timeout, error, or &gt;5s) |
| Transfers count correctness | % of requests that returned ≥ `min(pool.estimated_txs, query limit)` rows |
| Data freshness | **WebSocket subscription**: % of events where ingest lag &lt; 60s |

## Setup

```bash
cd "/Users/akshatmeena/Desktop/Bitquery/Nominis SLA Tron Test"
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Set BITQUERY_TOKEN from account.bitquery.io
```

## Discover address pool (required)

Builds up to **4000 addresses**: 2000 descending + 2000 ascending (deduped).

```bash
python discover_addresses.py
```

SLA tests **require** `data/receivers_pool.json`. HTTP phase runs **one query per address** at 4 QPS, then stops when the whole pool is covered. Freshness subscription runs **10 minutes** by default.

## Run SLA report

```bash
python sla_report.py
```

Override SLA targets on the command line (request timeout always matches `--response-time`):

```bash
python sla_report.py --qps 4 --response-time 5 --freshness 60
```

Same via env (used when flags are omitted): `SLA_PEAK_QPS`, `SLA_RESPONSE_SEC`, `SLA_FRESHNESS_SEC`.

Logs go to **terminal and** `logs/sla_<timestamp>.log` (see `LOG_DIR` in `.env`).

Example output:

```text
HTTP load (how the test was run — not an SLA %):
  4 queries/sec for 1000s to schedule 3999 addresses (1 query per address, open-loop);
  all responses finished in 1120s wall time

Metric                           SLA target                  Fulfilled        %
Query response time              <= 5s per query             3292/3999     82.32%
Transfers count correctness      rows >= pool count          3150/3999     78.77%
Data freshness (subscription)    < 60s ingest lag per event 75983/75983   100.00%

Transfers count correctness (expected per address = min(pool.estimated_txs, query limit 20,000)):
  Returned fewer rows than expected: 142 / 3999  (3.55%)

Unsuccessful breakdown (disjoint — no overlap):
  Response time > 5s / timeout / error              707 / 3999  (17.68%)
  Returned fewer rows than expected                 142 / 3999  ( 3.55%)
  ------------------------------------------------------------
  TOTAL unsuccessful                                849 / 3999  (21.23%)
  TOTAL successful                                 3150 / 3999  (78.77%)
```

## Queries

| File | Use |
|------|-----|
| `queries/tron_transfers.graphql` | HTTP SLA query (`limit: 20000`) |
| `queries/discover_*.graphql` | Find heavy/light receivers (90-day window) |
| `queries/tron_freshness_subscription.graphql` | WebSocket freshness |

## Configuration

| Variable | Default |
|----------|---------|
| `RECEIVERS_POOL_PATH` | `data/receivers_pool.json` (required) |
| `DISCOVER_TOP_N` / `DISCOVER_LIGHT_N` | `2000` / `2000` |
| `QUERY_LIMIT` | `20000` |
| `SLA_PEAK_QPS` / `--qps` | `4` (stops after full pool, not fixed duration) |
| `SLA_RESPONSE_SEC` / `--response-time` | `5` (also per-request HTTP timeout) |
| `SLA_FRESHNESS_SEC` / `--freshness` | `60` |
| `FRESHNESS_SUBSCRIPTION_SEC` | `600` (10 min) |

## Design notes

- **Pool traversal**: Exactly `len(pool)` queries (one per address), 4 starts/sec, parallel until all finish. ~4000 addrs ≈ ~17 min to schedule + time for queries to complete.
- **Freshness**: Uses live **subscription** (new blocks on the wire), not average age of historical rows in a query.
- **Success vs unsuccessful**: A request counts toward fulfillment only if it completes with no error **and** latency ≤ 5s. Slower responses are marked unsuccessful even if the server eventually returned data.
- **Correctness vs pool count**: `estimated_txs` is the 90-day count from discovery. The main query has no time filter, so it should normally return ≥ that count (capped at the query limit). Counts below the expected value indicate a partial/truncated response and are reported as INSUFFICIENT.
- **Disjoint unsuccessful buckets**: `sla_fail` and `insufficient` never overlap — a timeout/error never produces row counts, and a slow-but-ok response is already SLA-fail before the rows are checked.
- **Report only %**: No built-in 95%/99% tolerances — share the table with Nomics/your boss.

---

## Scenario 2 — W3C multi-chain SLA

W3C is provisioning **72 RPS sustained** with **~42 concurrent** in-flight queries across 5 chains (Ethereum, Arbitrum, BNB Chain, Polygon, Solana). Each user query is the same shape — a `Transfers` aggregation summing USD sent/received for 1-3 batched addresses over the trailing 12 months.

| SLA dimension | How we measure |
|---------------|----------------|
| Sustained QPS | Achieved launch rate vs target (open-loop scheduler at `1/qps` interval for `duration_sec`) |
| Response time | % of all requests under `--response-time` (default 10s); p50/p95/p99/max distribution reported |
| Failure rate | Disjoint breakdown: timeout, HTTP non-200 (with status codes), GraphQL errors, GraphQL null, parse, other |
| Per-chain | Success % and latency percentiles per chain so chain-specific degradation surfaces immediately |
| Concurrency | Peak in-flight tracked vs the configured cap |

### Two endpoints in play (important)

| Chain family | Endpoint | Env var | Why |
|---|---|---|---|
| EVM (ethereum / bsc / matic / arbitrum) | `https://streaming.bitquery.io/graphql` | `BITQUERY_GRAPHQL_URL` | V2 EAP archive — full history |
| Solana | `https://graphql.bitquery.io` | `BITQUERY_V1_URL` | V1 archive — full history. V2 Solana is only ~last 8 hours, so it can't serve a trailing-12-month window. See [Bitquery Solana Token Holders docs](https://docs.bitquery.io/docs/blockchain/Solana/solana-token-holders/) for the V1 vs V2 split. |

Both endpoints accept the same `ory_at_…` OAuth token via `Authorization: Bearer`.

### Step 1 — Discover the per-chain address pool (one-time)

Same pattern as the Tron Nomics flow: real addresses are pulled from Bitquery (top receivers per chain over the last 12 months) and saved to `data/w3c_addresses.json`. The test then samples 1-3 of those per request.

```bash
python discover_w3c_addresses.py
# Optional:
python discover_w3c_addresses.py --per-chain 1000 --days 365 --timeout 300
python discover_w3c_addresses.py --chains ethereum,solana      # subset
```

This runs one discovery query per chain (4 × V2 EAP for EVM + 1 × V1 for Solana) and writes:

```jsonc
{
  "generated_at": "2026-05-27T…Z",
  "discovery_days": 365,
  "per_chain_target": 1000,
  "chains": {
    "ethereum": {
      "discovered": 1000,
      "addresses": [ {"address": "0x…", "estimated_txs": 7_111_479}, … ]
    },
    "solana": {
      "discovered": 1000,
      "addresses": [ {"address": "base58…", "estimated_txs": 123_456}, … ]
    }
  }
}
```

`AddressBook` accepts both shapes (plain `["addr"…]` or `[{"address": "addr", "estimated_txs": N}…]`) so you can also hand-edit the file.

### Step 2 — Run the SLA test

```bash
python w3c_report.py
# Equivalent explicit form:
python w3c_report.py --qps 72 --concurrency 42 --duration 60 --response-time 10
```

Quick smoke test (1 chain, low rate):

```bash
python w3c_report.py --chains ethereum --qps 4 --concurrency 4 --duration 10
```

### Queries

| File | Endpoint | Use |
|------|----------|-----|
| `queries/w3c_evm_transfers.graphql` | V2 EAP | Single template for all 4 EVM chains (`$network` variable selects eth/bsc/matic/arbitrum) |
| `queries/w3c_solana_transfers.graphql` | V1 | Solana V1 schema — lowercase `solana`/`transfers`, `senderAddress`/`receiverAddress` filters, `amount(in: USD, calculate: sum)` aggregation |
| `queries/w3c_discover_evm_receivers.graphql` | V2 EAP | Discovery: top-N receivers per EVM chain over `$from..$to` |
| `queries/w3c_discover_solana_receivers.graphql` | V1 | Discovery: top-N Solana receivers grouped by `receiver.address`, `desc: count` |

### CLI flags

| Flag | Default | Env override |
|------|---------|--------------|
| `--qps` | 72 | `W3C_TARGET_QPS` |
| `--concurrency` | 42 | `W3C_CONCURRENCY` |
| `--duration` (sec) | 60 | `W3C_DURATION_SEC` |
| `--response-time` (sec) | 10 | `W3C_RESPONSE_SEC` |
| `--error-rate-threshold` (%) | 1.0 | `W3C_ERROR_RATE_PCT` |
| `--addresses-path` | `data/w3c_addresses.json` | `W3C_ADDRESSES_PATH` |
| `--chains` | all in pool | — |

### Adding a new chain

1. Append a `Chain(...)` entry to `CHAINS` in `w3c_workload.py` (specify `family="evm"` + `evm_network=...`, or `family="solana"` for a Solana-shaped schema).
2. Add a query file under `queries/` if the new chain's family doesn't already have one.
3. Add the chain's address pool to `data/w3c_addresses.json`.

No changes to the runner, metrics, or report needed.

### Design notes

- **Open-loop launches**: requests start every `1/qps` seconds regardless of response latency. If average latency exceeds `concurrency / qps`, the semaphore queue back-pressures and the *achieved* QPS drops below target — that's the warning sign for under-provisioning.
- **Per-request timeout == response-time SLA**: a slow Bitquery cannot quietly hide as a long-tail latency; it is marked SLA-fail with `WE STOPPED` in the unsuccessful log.
- **No row-count check**: the W3C query is an aggregation that returns at most 2 rows; success means HTTP 200 + valid GraphQL payload + latency under SLA. This is intentionally different from the Tron scenario.
- **Tron code is untouched**: the W3C scenario uses its own `w3c_client.py`, `w3c_config.py`, `w3c_metrics.py`, `w3c_workload.py`, and `w3c_report.py`. `sla_report.py` and friends are not modified.
