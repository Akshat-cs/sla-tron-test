# Bitquery SLA performance tests

Two independent SLA load tests against the Bitquery GraphQL API.

| Test        | Customer | What it measures                                                        |
| ----------- | -------- | ----------------------------------------------------------------------- |
| **Nominis** | Nominis  | Tron transfer queries — peak QPS, response time, freshness (ingest lag) |
| **W3C**     | W3C      | Multi-chain wallet queries — sustained 72 RPS across ~5 chains          |

Each test is a self-contained Python package: separate entry points, separate
GraphQL templates, separate address pool, separate metrics. Shared utilities
live in `utils/`.

---

## Repository layout

```
.
├── README.md
├── .env.example                 ← all env vars used by both tests
├── requirements.txt
├── pytest.ini
│
├── nominis/                     ← Nominis (Tron) test
│   ├── report.py                  entry point — run the SLA test
│   ├── discover.py                entry point — build the Tron address pool
│   ├── compare.py                 entry point — per-address debug helper
│   ├── address_pool.py            pool loader + round-robin picker
│   ├── client.py                  HTTP + GraphQL transfer query
│   ├── config.py                  SLA thresholds (env + CLI)
│   ├── metrics.py                 fulfillment %, response-time table
│   └── subscription.py            WebSocket freshness lag
│
├── w3c/                         ← W3C multi-chain test
│   ├── report.py                  entry point — run the SLA test
│   ├── discover.py                entry point — build the per-chain pool
│   ├── workload.py                chain registry + address book + generator
│   ├── client.py                  per-chain HTTP query executor
│   ├── config.py                  SLA thresholds (env + CLI)
│   └── metrics.py                 per-chain + overall report
│
├── utils/                       ← shared by both tests
│   ├── logging_setup.py           tee stdout/stderr to logs/sla_<ts>.log
│   └── unsuccessful_log.py        dedicated log of failed requests
│
├── queries/
│   ├── nominis/                   Tron .graphql templates
│   └── w3c/                       EVM + Solana V1 .graphql templates
│
├── data/                        ← generated address pools (json)
├── tests/                       ← pytest smoke test (Nominis)
├── runs/                        ← archived EC2 run outputs (reference only)
└── logs/                        ← local run logs (gitignored)
```

Every entry point is run from the repo root with
`python -m <package>.<entry>`, e.g. `python -m nominis.report`. This works
because `nominis/`, `w3c/` and `utils/` are proper Python packages.

---

## Setup (once)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# edit .env and set BITQUERY_TOKEN=<your ory_at_... token>
```

`BITQUERY_TOKEN` is the only required var. Defaults for everything else are in
`.env.example`.

---

## Test 1 — Nominis (Tron)

Measures three SLA metrics for a Tron transfer query:

1. **Peak QPS** — sustained queries-per-second the API can serve
2. **Response time** — % of requests under the configured limit (default 5 s)
3. **Freshness** — ingest lag from the WebSocket subscription (default 60 s)

### 1a. Build the address pool (run once, or whenever it goes stale)

Discovery picks a large, realistic set of Tron receiver addresses up front so
the SLA test can rotate through real, varied workloads instead of hammering a
single hardcoded address.

```bash
python -m nominis.discover
```

Writes `data/receivers_pool.json` (default 2000 desc + 2000 asc = up to 4000
unique Tron receivers from the last 90 days). Adjust with `--top`, `--light`,
`--timeout`.

### 1b. Run the SLA test

```bash
python -m nominis.report --qps 4 --response-time 5 --freshness 60
```

Prints the fulfillment report at the end, e.g.:

```
NOMICS SLA FULFILLMENT REPORT
  peak_qps           : 100.0% (4/4 sustained)
  response_time_5s   :  98.4% (1968/2000 under 5s)
  freshness_60s      :  99.7% ...
```

Logs are mirrored to `logs/sla_<timestamp>.log` and failed requests to
`logs/sla_<timestamp>_unsuccessful.log`.

Useful flags:

| Flag                | Purpose                                             |
| ------------------- | --------------------------------------------------- |
| `--qps N`           | Target QPS for the peak-QPS metric                  |
| `--response-time S` | SLA limit in seconds for the response-time metric   |
| `--freshness S`     | SLA limit in seconds for the freshness (lag) metric |
| `--pool-path PATH`  | Override `data/receivers_pool.json`                 |

### 1c. (Optional) Compare aggregate count vs SLA-row count for one address

```bash
python -m nominis.compare <tron_address> --days 90
```

Debugging helper — explains why a given address passes/fails the SLA test.

### 1d. (Optional) Run via pytest

```bash
pytest tests/test_nominis.py -v -s
```

Same as `python -m nominis.report` but wrapped in a pytest assertion (always
passes — inspect the report output).

---

## Test 2 — W3C (multi-chain)

Open-loop load test: **72 RPS** sustained for 60 s across up to **5 chains**
(Ethereum, BSC, Polygon, Arbitrum, Solana). Each request batches 1–3 random
wallet addresses, trailing 12-month window.

Two endpoints are used:

- EVM chains → `BITQUERY_GRAPHQL_URL` (V2 EAP, `streaming.bitquery.io`)
- Solana → `BITQUERY_V1_URL` (V1, `graphql.bitquery.io`) — V2 Solana only
  exposes the last ~8 h of data, which isn't enough for a 12-month window.

### 2a. Build the multi-chain address pool

Discovery fetches a realistic per-chain set of wallet addresses (filtered to
exclude DEX routers / CEX hot wallets) so the SLA test queries reflect typical
W3C end-user traffic rather than a few hand-picked addresses.

```bash
python -m w3c.discover \
    --per-chain 1000 \
    --days 30 \
    --solana-days 1 \
    --max-txs 500 \
    --timeout 60
```

Writes `data/w3c_addresses.json` with up to 1000 sample addresses per chain.

Key flags:

| Flag              | Purpose                                                             |
| ----------------- | ------------------------------------------------------------------- |
| `--per-chain N`   | Addresses to fetch per chain                                        |
| `--days D`        | Discovery window in days (EVM)                                      |
| `--solana-days D` | Override window just for Solana (V1 aggregation is heavier per day) |
| `--max-txs N`     | Realism cap — skip DEX routers / CEX hot wallets with > N transfers |
| `--min-txs N`     | Optional lower bound (default 0)                                    |
| `--chains LIST`   | Comma-separated subset, e.g. `ethereum,solana`                      |
| `--timeout SEC`   | Per-discovery-query HTTP timeout                                    |

### 2b. Run the SLA test

```bash
python -m w3c.report \
    --qps 72 \
    --concurrency 42 \
    --duration 60 \
    --response-time 10
```

Prints:

- **Overall fulfillment** — sustained QPS, success rate, error-rate threshold
- **Response-time distribution** (all requests) — avg, p50, p95, p99, max
- **Per-chain breakdown** — success rate + latency per chain
- **Per-chain response time (successful only)** — distribution per chain
  including only HTTP 200 successes

Key flags:

| Flag                    | Purpose                                                    |
| ----------------------- | ---------------------------------------------------------- |
| `--qps N`               | Open-loop launch rate (req/s); default `W3C_TARGET_QPS=72` |
| `--concurrency N`       | Max in-flight requests (client semaphore); default `42`    |
| `--duration SEC`        | Test duration; default `60`                                |
| `--response-time S`     | SLA cutoff per request, in seconds; default `10`           |
| `--chains LIST`         | Subset of chains, e.g. `ethereum,bsc,solana`               |
| `--addresses-path PATH` | Override `data/w3c_addresses.json`                         |

### How `--qps` and `--concurrency` interact

- `--qps 72` schedules a new request every ~13.9 ms (open-loop).
- `--concurrency 42` is a client-side semaphore that caps simultaneous
  in-flight requests. When 42 are already in flight, new requests **queue
  locally** (latency goes up) — they are NOT dropped.
- Server-side, Bitquery enforces its own concurrent-request cap; exceeding it
  produces `HTTP 429` (visible in the unsuccessful log).

### Per-request timeout

Each request gets a hard `aiohttp` timeout equal to `--response-time`. A
request slower than the cutoff is cancelled and counted as failed (the
`unsuccessful.log` reason is `"Request timeout after Xs (SLA limit)"`).

---

## Where things end up

| Artifact                                  | Location                                    |
| ----------------------------------------- | ------------------------------------------- |
| Nominis address pool                      | `data/receivers_pool.json`                  |
| W3C address pool                          | `data/w3c_addresses.json`                   |
| Full run log (stdout + stderr, mirrored)  | `logs/sla_<UTC-timestamp>.log`              |
| Just the failed requests                  | `logs/sla_<UTC-timestamp>_unsuccessful.log` |
| Archived EC2 run outputs (reference only) | `runs/run-{1,2,3}/`                         |

All `logs/` files are gitignored.

---

## Quick reference — all entry points

```bash
# Nominis (Tron)
python -m nominis.discover                  # build pool
python -m nominis.report                    # run SLA test
python -m nominis.compare <tron_address>    # debug one address

# W3C (multi-chain)
python -m w3c.discover                      # build pool
python -m w3c.report                        # run SLA test

# Pytest wrapper (Nominis)
pytest tests/test_nominis.py -v -s
```

Add `--help` to any of them to see all flags.

---

## Adding a new chain to the W3C test

1. Append a `Chain(...)` entry to `CHAINS` in `w3c/workload.py`.
2. If it's an EVM chain, reuse `evm_transfers.graphql` /
   `discover_evm_receivers.graphql` and just set `evm_network=<bitquery slug>`.
3. If it's a new schema family, add its `.graphql` templates under
   `queries/w3c/` and handle the family in `w3c/client.py`'s result parser.
4. Re-run `python -m w3c.discover` to populate the pool for the new chain.
