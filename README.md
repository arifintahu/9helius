<div align="center">

# 9helius

### One Helius RPC endpoint. All your free-tier keys. Zero rate limits.

A transparent **Helius RPC load balancer** in Rust that pools multiple free-tier
api-keys behind a single endpoint — round-robin routing, automatic rate-limit
failover, and per-key credit accounting.

[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-36%20passing-brightgreen.svg)](#testing)
[![Status](https://img.shields.io/badge/status-v0.1-yellow.svg)](#roadmap)

</div>

---

`9helius` makes _N_ Helius free-tier keys look like one big paid plan. Each free
key gives **1,000,000 credits/month** and **~10 RPS**; point your client at the
gateway and it transparently spreads traffic across all of them — skipping keys
that are exhausted or rate-limited, and tracking exactly how many credits each
one has burned.

```
                        ┌─────────────────────────────────────────┐
                        │                9helius                   │
   ┌────────┐  api-key  │                                          │   ┌──────────────┐
   │ client │──────────▶│  auth ▶ cost-estimate ▶ select key ▶ ━━━━━━━▶│  Helius key 1 │
   └────────┘  (one     │           │                  ▲         │   ├──────────────┤
                gateway │           │   round-robin,   │ on 429  │   │  Helius key 2 │
                key)    │           │   skip over-quota│ cooldown│   ├──────────────┤
                        │           │   / cooling /    │ + retry │   │      ...       │
                        │           ▼   RPS-starved    │  next   │   ├──────────────┤
                        │      credit metering ────────┘         │   │  Helius key N │
                        └─────────────────────────────────────────┘   └──────────────┘
```

## Table of contents

- [Features](#features)
- [Quick start](#quick-start)
- [Using the gateway](#using-the-gateway)
- [Endpoints](#endpoints)
- [Configuration](#configuration)
- [How it works](#how-it-works)
- [Metrics](#metrics)
- [Testing](#testing)
- [Roadmap](#roadmap)
- [License](#license)

## Features

- 🔀 **Drop-in replacement** — speaks the Helius wire protocol exactly. Only the
  `api-key` is rewritten; method, path, query, body, and response pass through
  byte-for-byte.
- ⚖️ **Round-robin pooling** — lock-free atomic selection spreads load evenly and
  multiplies your free quota and RPS by the number of keys.
- 💳 **Credit accounting** — estimates each request's cost from its JSON-RPC
  method (single **and** batch) using the real [Helius credit table](https://www.helius.dev/docs/billing/credits) and tracks per-key monthly usage.
- 🛡️ **Rate-limit handling** — proactive per-key/per-class token buckets, plus
  reactive failover: on HTTP `429` or JSON-RPC `-32005`, the key is put on an
  exponential-backoff cooldown and the request retries the next available key.
- ♻️ **Survives restarts** — per-key usage is snapshotted atomically and restored
  on boot; counters reset automatically at each UTC month boundary.
- 📊 **Observable** — Prometheus `/metrics`, a JSON `/stats` view, and a
  capacity-aware `/health` probe.
- 🔒 **Secret-safe** — real keys live only in a gitignored config and are redacted
  in logs; clients only ever see the gateway key.

## Quick start

```bash
# 1. Configure
cp config.example.toml config.toml
#    edit config.toml: set gateway.api_key and add one [[upstreams]] per Helius key

# 2. Run
cargo run --release

# 3. Use it
curl -X POST "http://127.0.0.1:8080/?api-key=$GATEWAY_KEY" \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}'
# → {"jsonrpc":"2.0","result":"ok","id":1}
```

> `config.toml` is gitignored — your real keys never get committed.
> The config path defaults to `config.toml`; override with `NINEHELIUS_CONFIG`.

## Using the gateway

Call it exactly like Helius, presenting your **gateway** key. Three auth styles
are accepted:

```bash
# query param (most Helius-compatible)
curl "http://host:8080/?api-key=$GATEWAY_KEY"   -d '{...}'
# header
curl -H "x-api-key: $GATEWAY_KEY"               ...
curl -H "Authorization: Bearer $GATEWAY_KEY"    ...
```

Any Helius SDK works — set the RPC URL to your gateway and use the gateway key:

```ts
import { Connection } from "@solana/web3.js";
const rpc = new Connection("http://host:8080/?api-key=YOUR_GATEWAY_KEY");
```

## Endpoints

| Path        | Method | Description                                                  |
|-------------|:------:|--------------------------------------------------------------|
| `/health`   | GET    | `200` if any key has quota left, `503` if all are exhausted  |
| `/metrics`  | GET    | Prometheus exposition                                        |
| `/stats`    | GET    | JSON: per-key credits, remaining, in-flight, cooldown        |
| *anything else* | any | Proxied transparently to the upstream Helius host          |

`/health`, `/metrics`, and `/stats` are the only reserved paths; everything else
is forwarded.

## Configuration

Full annotated schema in [`config.example.toml`](config.example.toml).

```toml
[gateway]
bind = "0.0.0.0:8080"
api_key = "your-secret-gateway-key"   # clients present this
upstream_base = "https://mainnet.helius-rpc.com"
max_retries = 6                        # distinct keys to try per request

[persistence]
path = "state/credits.snapshot.json"
interval_secs = 10
on_snapshot_error = "zero"             # "zero" | "cap"

[costs.overrides]                      # defaults are built in; override as needed
getProgramAccounts = 10
getValidityProofs = 100

[rps]                                  # free-tier per-key limits
standard_rpc = 10
send_transaction = 1
get_program_accounts = 5
das = 2
zk = 2

[[upstreams]]
name = "helius-1"
api_key = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
credit_cap = 1000000
enabled = true
# ... one block per key
```

**Environment overrides:** any field can be set via an env var prefixed
`NINEHELIUS_` with `__` for nesting — handy for secrets in containers:

```bash
NINEHELIUS_GATEWAY__API_KEY=…
NINEHELIUS_UPSTREAMS__0__API_KEY=…
```

## How it works

| Concern        | Behaviour                                                                                                   |
|----------------|------------------------------------------------------------------------------------------------------------|
| **Cost**       | Estimated from the JSON-RPC method. Standard = 1, `getProgramAccounts`/DAS/ZK = 10, `getValidityProofs` = 100. Batches sum their calls. Charged only after a serviced (non-rate-limited) response. |
| **Selection**  | Round-robin, skipping keys that are disabled, over their monthly cap, on cooldown, or out of RPS tokens for the request's class. |
| **Rate limits**| Proactive token buckets per key per class. On `429`/`-32005`, the key cools down (1s→30s exponential, ±25% jitter) and the request retries the next key. All keys unavailable → `429` + `Retry-After`. |
| **Persistence**| Usage snapshotted to disk (atomic temp-file + rename), restored on boot only if the snapshot is from the current UTC month. |
| **Reset**      | Per-key counters reset automatically at each UTC month boundary.                                            |

## Metrics

All Prometheus metrics are prefixed `ninehelius_`:

| Metric | Type | Labels |
|--------|------|--------|
| `requests_total` | counter | `upstream`, `outcome` |
| `credits_consumed_total` | counter | `upstream` |
| `credits_remaining` | gauge | `upstream` |
| `rate_limit_hits_total` | counter | `upstream` |
| `upstream_errors_total` | counter | `upstream`, `kind` |
| `inflight` | gauge | `upstream` |
| `rpc_method_total` | counter | `method` |
| `all_exhausted_total` | counter | — |
| `request_duration_seconds` | histogram | — |

## Testing

```bash
cargo test                  # unit + integration (wiremock) + e2e (spawns the binary)
cargo clippy --all-targets
```

- **Unit tests** cover method classification, cost/quota math, selection, backoff, and snapshot restore.
- **Integration tests** (`tests/proxy.rs`) build the router in-process and verify forwarding, auth, and rate-limit failover against a mock Helius.
- **End-to-end tests** (`tests/e2e.rs`) spawn the real compiled binary against a mock upstream and verify round-robin, 429 failover, credit tracking, capacity-aware health, and persistence across a restart.

**Project layout** (`src/`): `proxy` (transparent handler + retry loop) ·
`upstream` (per-key state + pool selection) · `credits` (classification + cost) ·
`ratelimit` (token buckets + backoff) · `persistence` (snapshot) · `config` ·
`state` · `metrics` · `error`.

## Roadmap

- [x] Transparent HTTP JSON-RPC proxy with gateway auth
- [x] Round-robin pooling with quota + rate-limit awareness
- [x] Credit accounting, persistence, monthly reset
- [x] Prometheus metrics + stats/health endpoints
- [ ] WebSocket subscription proxying (sticky per-key sessions)
- [ ] Enhanced Transactions (`api-mainnet/v0`) and Wallet (`api.helius.xyz/v1`) host routing
- [ ] Weighted / least-credits selection strategies

## License

[MIT](LICENSE) © 2026 Miftahul Arifin

> Not affiliated with or endorsed by Helius. "Helius" is a trademark of its respective owner.
