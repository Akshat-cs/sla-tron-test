# Nomics SLA — Tron Transfers Performance Test

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
