# CLAUDE.md

Guidance for working in this repository.

## Project

**9helius** — a transparent Helius RPC load balancer in Rust. It pools several
Helius free-tier api-keys behind one gateway URL + api-key, forwarding requests
to `mainnet.helius-rpc.com` in round-robin while tracking credits, respecting
rate limits, and persisting all stats across restarts.

- The cargo **package/binary is `ninehelius`** (cargo names can't start with a
  digit); the repo/dir is `9helius`.
- Edition 2021, async on tokio, `axum` server + `reqwest` upstream client.

## Commands

```bash
cargo run --release            # run (reads ./config.toml; override via NINEHELIUS_CONFIG)
cargo build
cargo test                     # unit + integration (wiremock) + e2e (spawns the binary)
cargo test --test e2e          # one integration-test binary
cargo test <name>              # a single test by name
cargo clippy --all-targets     # keep clean — CI-equivalent gate
```

## Architecture (`src/`)

Single `Arc<AppState>` shared into every handler; per-key mutable state is
atomics + lock-free `governor` limiters (no global mutex on the hot path).

- **proxy.rs** — axum fallback handler: gateway auth → cost estimate → select key
  → forward → retry-on-rate-limit loop → relay response. The request hot path.
- **upstream.rs** — `Upstream` (per-key atomic state: monthly + lifetime counters,
  cooldown, RPS limiters, day baseline) and `Pool` (round-robin `select`).
- **credits.rs** — JSON-RPC method classification (single + batch) → `CostTable`.
- **ratelimit.rs** — per-class token buckets + backoff/jitter + `now_ms`.
- **stats.rs** — durable global stats: month/day rollover, per-method tallies,
  monthly + daily history, and `replay_to_prometheus` (re-seeds metrics on boot).
- **persistence.rs** — atomic JSON snapshot (temp-file + rename), load + restore.
- **state.rs / config.rs / metrics.rs / error.rs / main.rs** — wiring, config
  (figment: toml + `NINEHELIUS_` env), endpoints, errors, bootstrap + tickers.

Thin **lib + bin** split: logic lives in the `ninehelius` lib so `tests/` can
build the router; `main.rs` is a small wrapper.

## Key invariants — preserve these

- **Transparent forwarding**: only the `api-key` query param is rewritten. Never
  alter method/path/body/other-query; relay upstream status+headers+body verbatim
  (strip hop-by-hop). Reserved paths: `/health`, `/metrics`, `/stats`,
  `/stats/history` — everything else is proxied.
- **Credit commit policy**: charge credits *after* a serviced (non-rate-limited)
  response. Never charge a 429/`-32005` or a transient failure.
- **Selection order** (`Pool::select`): skip disabled → over-quota → cooling-down
  → RPS-starved; the RPS `try_acquire` consumes a token only for the chosen key.
- **Rate-limit detection** = HTTP 429 **or** JSON-RPC `error.code == -32005`.
  On hit: cooldown the key, push its index to the per-request `skip` set, retry
  the next key. All keys unavailable → 429 + `Retry-After`.
- **Counters**: `credits_used` is monthly (resets at UTC month boundary);
  `credits_total` and the request/rate-limit counters are lifetime (never reset).
  `daily_used = credits_total - day_start_total`.
- **Durability**: everything except transient state (`in_flight`, `cooldown`,
  latency histogram) is in the snapshot and restored on boot, then replayed into
  Prometheus so `/metrics` resumes rather than restarting at zero. If you add a
  counter, add it to the snapshot + `replay_to_prometheus` too.
- **Secrets**: real api-keys live only in gitignored `config.toml`; `SecretString`
  redacts them in logs. Never log or persist raw keys (snapshots store only
  names + counts). Keep `config.example.toml` placeholders.

## Testing layers

- **Unit** (`#[cfg(test)]` in modules) — classification, cost/quota math,
  selection, backoff, month/day rollover, snapshot restore.
- **Integration** (`tests/proxy.rs`) — build the router in-process, drive via
  `tower`/reqwest against a `wiremock` Helius.
- **E2E** (`tests/e2e.rs`) — spawn the real binary (`CARGO_BIN_EXE_ninehelius`)
  against wiremock; covers failover, persistence/restart, history. A `ChildGuard`
  reaps the process on panic.

Add tests at the layer that matches the change; keep `cargo clippy` clean.

## Workflow conventions

- **Commit on every meaningful increment** (each feature/passing test batch), not
  just at the end.
- **Do not** add the `Co-Authored-By: Claude` trailer to commits or PRs.

## Gotchas

- Port **8080 is taken by IIS** on this machine; local `config.toml` uses `18080`.
- Git warns about LF→CRLF on Windows — harmless.
- `config.toml` is gitignored (real keys); only `config.example.toml` is committed.
- Snapshot schema is versioned (`SCHEMA_VERSION`); add new fields with
  `#[serde(default)]` so older snapshots still load.

## Scope

v1 is HTTP JSON-RPC against `mainnet.helius-rpc.com`. WebSocket subscriptions
(sticky per-key sessions) and the Enhanced/Wallet REST hosts are deferred — see
the Roadmap in `README.md`.
