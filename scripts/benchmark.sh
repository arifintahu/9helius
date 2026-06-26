#!/usr/bin/env bash
#
# Latency benchmark: 9helius (your local gateway, backed by Helius) vs a public
# Solana RPC. Outputs a Markdown table (paste-ready for the README).
#
# Each method is measured N times over a single reused connection (the first
# DROP samples are discarded as warmup), reporting min / p50 / avg in ms.
#
# Usage:
#   ./scripts/benchmark.sh
#   GATEWAY="http://127.0.0.1:18080/?api-key=KEY" PUBLIC_RPC="https://..." ./scripts/benchmark.sh
#
# Env overrides:
#   CONFIG   config.toml to read gateway bind + api-key from   [./config.toml]
#   GATEWAY  full gateway URL incl ?api-key=...                [derived from CONFIG]
#   PUBLIC_RPC   public RPC URL to compare against                 [solana-rpc.publicnode.com]
#   N        samples per method                                [25]
#   DROP     warmup samples to discard                         [3]
set -euo pipefail

CONFIG="${CONFIG:-config.toml}"
PUBLIC_RPC="${PUBLIC_RPC:-https://solana-rpc.publicnode.com}"
N="${N:-25}"
DROP="${DROP:-3}"

if [ -z "${GATEWAY:-}" ]; then
  [ -f "$CONFIG" ] || { echo "config not found: $CONFIG (or set GATEWAY=...)" >&2; exit 1; }
  bind=$(grep -m1 -E '^[[:space:]]*bind[[:space:]]*=' "$CONFIG" | sed -E 's/.*"([^"]+)".*/\1/')
  key=$(grep -m1 -E '^[[:space:]]*api_key[[:space:]]*=' "$CONFIG" | sed -E 's/.*"([^"]+)".*/\1/')
  GATEWAY="http://${bind}/?api-key=${key}"
fi

echo "# gateway: ${GATEWAY%%\?*}?api-key=***   public: $PUBLIC_RPC   (N=$N, drop=$DROP)" >&2

# A finalized slot a little in the past, for getBlock.
slot=$(curl -s -X POST "$PUBLIC_RPC" -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment":"finalized"}]}' \
  | grep -oE '"result":[0-9]+' | grep -oE '[0-9]+')
target=$((slot - 150))

bench() { # url payload  ->  N lines of time_total (connection reused)
  local url=$1 payload=$2 args=(-s)
  for ((i = 0; i < N; i++)); do
    [ $i -gt 0 ] && args+=(--next)
    args+=(-o /dev/null -w "%{time_total}\n" -X POST "$url" -H "content-type: application/json" -d "$payload")
  done
  curl "${args[@]}" 2>/dev/null
}

stats() { # stdin: times in seconds -> "min|p50|avg" in ms
  tail -n +$((DROP + 1)) | sort -n | awk '{a[NR]=$1; s+=$1} END{n=NR; printf "%.0f|%.0f|%.0f", a[1]*1000, a[int((n+1)/2)]*1000, (s/n)*1000}'
}

methods=(
  "getSlot|{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getSlot\"}"
  "getBalance|{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getBalance\",\"params\":[\"11111111111111111111111111111111\"]}"
  "getBlock|{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getBlock\",\"params\":[$target,{\"encoding\":\"json\",\"maxSupportedTransactionVersion\":0,\"transactionDetails\":\"none\",\"rewards\":false}]}"
  "getBlockHeight|{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getBlockHeight\"}"
  "getLatestBlockhash|{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getLatestBlockhash\"}"
  "getEpochInfo|{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getEpochInfo\"}"
)

echo "| method | 9helius min / p50 / avg (ms) | public min / p50 / avg (ms) |"
echo "|--------|------------------------------|-----------------------------|"
for m in "${methods[@]}"; do
  name="${m%%|*}"; payload="${m#*|}"
  l=$(bench "$GATEWAY" "$payload" | stats)
  p=$(bench "$PUBLIC_RPC" "$payload" | stats)
  IFS='|' read -r lmin lp50 lavg <<<"$l"
  IFS='|' read -r pmin pp50 pavg <<<"$p"
  printf "| %s | %s / %s / %s | %s / %s / %s |\n" "$name" "$lmin" "$lp50" "$lavg" "$pmin" "$pp50" "$pavg"
done
