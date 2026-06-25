# 9helius

A transparent **Helius RPC load balancer** written in Rust. It combines several
Helius free-tier api-keys behind a **single gateway URL and api-key**, forwarding
every request to `https://mainnet.helius-rpc.com` in round-robin while:

- **skipping keys** that are over their monthly credit cap or currently rate-limited,
- **estimating credit cost** per request (per the [Helius credit table](https://www.helius.dev/docs/billing/credits)) and tracking it per key,
- **respecting rate limits** ([Helius rate limits](https://www.helius.dev/docs/billing/rate-limits)) proactively (per-key token buckets) and reactively (on HTTP 429 / JSON-RPC `-32005` it cools the key down and retries the next one),
- **exposing metrics** for routing, credits, and rate-limit activity.

Each free-tier key gives 1,000,000 credits/month and ~10 RPS; pooling *N* keys
multiplies the effective free quota and throughput. It is a drop-in replacement —
point any Helius client at the gateway and it just works.

## How clients use it

Call the gateway exactly like Helius, using your **gateway** api-key:

```bash
curl -X POST "http://<host>:8080/?api-key=<GATEWAY_KEY>" \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}'
```

The gateway validates `<GATEWAY_KEY>`, swaps in one of the real upstream keys, and
relays the upstream status/headers/body unchanged. The gateway key may also be
sent as `x-api-key: <KEY>` or `Authorization: Bearer <KEY>`.

## Quick start

1. **Configure.** Copy the example and fill in your keys:

   ```bash
   cp config.example.toml config.toml
   ```

   Edit `config.toml`: set `gateway.api_key` to a secret of your choosing
   (clients present this), and add one `[[upstreams]]` block per Helius key.
   `config.toml` is gitignored — real keys never get committed.

2. **Run.**

   ```bash
   cargo run --release
   ```

   The config path defaults to `config.toml`; override with `NINEHELIUS_CONFIG`.

3. **Verify.**

   ```bash
   curl http://127.0.0.1:8080/health
   curl http://127.0.0.1:8080/stats
   curl http://127.0.0.1:8080/metrics
   ```

## Endpoints

| Path        | Method | Purpose                                                        |
|-------------|--------|----------------------------------------------------------------|
| `/health`   | GET    | `200` if any key has quota left, `503` if all are exhausted    |
| `/metrics`  | GET    | Prometheus exposition                                          |
| `/stats`    | GET    | JSON: per-key credits used/remaining, in-flight, cooldown      |
| *everything else* | any | Proxied transparently to the upstream Helius host        |

`/health`, `/metrics`, and `/stats` are the only reserved paths; all other
requests are forwarded.

## Configuration

See [`config.example.toml`](config.example.toml) for the full annotated schema.
Highlights:

- `gateway.bind` — listen address (default `0.0.0.0:8080`).
- `gateway.api_key` — the single key clients must present.
- `gateway.max_retries` — max distinct keys to try per request (default 6).
- `[costs.overrides]` — per-method credit costs (defaults are built in).
- `[rps]` — per-class requests-per-second limits (free-tier defaults).
- `[[upstreams]]` — one block per Helius key (`name`, `api_key`, `credit_cap`, `enabled`).
- `[persistence]` — credit-usage snapshot path / interval / corruption policy.

Any field can be overridden by an environment variable prefixed `NINEHELIUS_`
with `__` as the nesting separator, e.g.
`NINEHELIUS_GATEWAY__API_KEY=…` or `NINEHELIUS_UPSTREAMS__0__API_KEY=…`.

## How it works

- **Credit costs** are estimated from the JSON-RPC `method` (single and batch
  requests supported). Standard calls = 1 credit, `getProgramAccounts`/DAS/ZK =
  10, `getValidityProofs` = 100. Credits are charged after a serviced response;
  rate-limited and transient attempts are not charged.
- **Monthly reset.** Per-key usage resets at each UTC month boundary and is
  snapshotted to `state/credits.snapshot.json` (atomic temp-file + rename) so it
  survives restarts. On boot, usage is restored only if the snapshot is from the
  current month.
- **Rate limiting.** Each key holds a token bucket per method class sized from the
  configured RPS. If a key's bucket is empty the selector moves to another key.
  On a `429`/`-32005`, the key is put on an exponential-backoff cooldown
  (1s → 30s, ±25% jitter) and the request retries the next available key. When
  every key is unavailable, the gateway returns `429` with a `Retry-After` header.

## Metrics

Prometheus metrics (all prefixed `ninehelius_`): `requests_total{upstream,outcome}`,
`credits_consumed_total{upstream}`, `credits_remaining{upstream}`,
`rate_limit_hits_total{upstream}`, `upstream_errors_total{upstream,kind}`,
`inflight{upstream}`, `rpc_method_total{method}`, `all_exhausted_total`,
`request_duration_seconds`.

## Development

```bash
cargo test            # unit + wiremock integration tests
cargo clippy --all-targets
cargo run             # uses config.toml
```

Project layout (`src/`): `proxy` (transparent handler + retry loop), `upstream`
(per-key state + pool selection), `credits` (method classification + cost),
`ratelimit` (token buckets + backoff), `persistence` (snapshot), `config`,
`state`, `metrics`, `error`.

## Scope

v1 covers HTTP JSON-RPC / REST against `mainnet.helius-rpc.com`. WebSocket
subscription proxying (sticky per-key connections) and the separate
Enhanced/Wallet REST hosts are planned for a later phase.

## Security

- Real api-keys live only in the gitignored `config.toml` and are redacted in logs.
- Clients must present the gateway api-key; upstream keys are never exposed to clients.
