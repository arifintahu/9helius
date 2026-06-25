//! Credit estimation: classify JSON-RPC methods, map them to credit costs, and
//! parse request bodies (single + batch) to compute a per-request estimate.
//!
//! Costs follow the Helius credit table: standard RPC = 1, getProgramAccounts =
//! 10, DAS = 10, ZK = 10, getValidityProofs = 100. Per-method overrides come
//! from config.

use std::collections::HashMap;

use serde::Deserialize;

use crate::config::CostsConfig;

/// Coarse method category, used both for cost defaults and (later) RPS gating.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum MethodClass {
    StandardRpc,
    SendTransaction,
    GetProgramAccounts,
    Das,
    Zk,
}

impl MethodClass {
    /// Default credit cost for the class when no per-method override applies.
    fn default_cost(self) -> u32 {
        match self {
            MethodClass::StandardRpc | MethodClass::SendTransaction => 1,
            MethodClass::GetProgramAccounts | MethodClass::Das | MethodClass::Zk => 10,
        }
    }

    /// Restrictiveness rank — lower = stricter RPS limit. Used to pick the most
    /// restrictive class in a batch.
    fn restrictiveness(self) -> u8 {
        match self {
            MethodClass::SendTransaction => 0, // 1 RPS
            MethodClass::Das | MethodClass::Zk => 1, // 2 RPS
            MethodClass::GetProgramAccounts => 2, // 5 RPS
            MethodClass::StandardRpc => 3,     // 10 RPS
        }
    }
}

/// Classify a JSON-RPC method name into a [`MethodClass`].
pub fn classify(method: &str) -> MethodClass {
    match method {
        "sendTransaction" => MethodClass::SendTransaction,
        "getProgramAccounts" => MethodClass::GetProgramAccounts,
        m if is_das(m) => MethodClass::Das,
        m if is_zk(m) => MethodClass::Zk,
        _ => MethodClass::StandardRpc,
    }
}

fn is_das(m: &str) -> bool {
    matches!(
        m,
        "getAsset"
            | "getAssetBatch"
            | "getAssetProof"
            | "getAssetProofBatch"
            | "getAssetsByOwner"
            | "getAssetsByAuthority"
            | "getAssetsByCreator"
            | "getAssetsByGroup"
            | "searchAssets"
            | "getSignaturesForAsset"
            | "getTokenAccounts"
            | "getNftEditions"
    )
}

fn is_zk(m: &str) -> bool {
    matches!(
        m,
        "getCompressedAccount"
            | "getCompressedAccountProof"
            | "getCompressedAccountsByOwner"
            | "getCompressedBalance"
            | "getCompressedBalanceByOwner"
            | "getCompressedTokenAccountsByOwner"
            | "getCompressedTokenAccountBalance"
            | "getValidityProof"
            | "getValidityProofs"
            | "getMultipleCompressedAccounts"
    )
}

/// Resolved cost table: built-in per-method overrides merged with config
/// overrides, plus a fallback cost for non-JSON-RPC REST paths.
#[derive(Debug, Clone)]
pub struct CostTable {
    overrides: HashMap<String, u32>,
    default_rest_cost: u32,
}

impl CostTable {
    pub fn from_config(c: &CostsConfig) -> Self {
        // Built-in exceptions that aren't captured by class defaults.
        let mut overrides: HashMap<String, u32> = HashMap::new();
        overrides.insert("getValidityProof".into(), 100);
        overrides.insert("getValidityProofs".into(), 100);
        // User overrides win.
        for (k, v) in &c.overrides {
            overrides.insert(k.clone(), *v);
        }
        CostTable {
            overrides,
            default_rest_cost: c.default_rest_cost,
        }
    }

    /// Credit cost of a single method.
    pub fn cost_of(&self, method: &str) -> u32 {
        if let Some(c) = self.overrides.get(method) {
            return *c;
        }
        classify(method).default_cost()
    }
}

/// A parsed request body, reduced to what we need for cost + class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Parsed {
    Single { method: String },
    Batch { methods: Vec<String> },
    /// GET, REST path, empty, or unparseable body.
    NonJsonRpc,
}

#[derive(Deserialize)]
struct RpcCall {
    method: Option<String>,
}

/// Parse a request to determine its JSON-RPC method(s). The body bytes are only
/// read, never mutated — the original is forwarded verbatim.
pub fn parse_body(path: &str, body: &[u8]) -> Parsed {
    // REST endpoints (enhanced/wallet) are not JSON-RPC.
    if path.starts_with("/v0/") || path.starts_with("/v1/") {
        return Parsed::NonJsonRpc;
    }
    match first_non_ws(body) {
        Some(b'[') => match serde_json::from_slice::<Vec<RpcCall>>(body) {
            Ok(calls) => {
                let methods: Vec<String> = calls.into_iter().filter_map(|c| c.method).collect();
                if methods.is_empty() {
                    Parsed::NonJsonRpc
                } else {
                    Parsed::Batch { methods }
                }
            }
            Err(_) => Parsed::NonJsonRpc,
        },
        Some(b'{') => match serde_json::from_slice::<RpcCall>(body) {
            Ok(RpcCall {
                method: Some(method),
            }) => Parsed::Single { method },
            _ => Parsed::NonJsonRpc,
        },
        _ => Parsed::NonJsonRpc,
    }
}

fn first_non_ws(body: &[u8]) -> Option<u8> {
    body.iter().copied().find(|b| !b.is_ascii_whitespace())
}

/// Estimated credit cost of a parsed request.
pub fn request_cost(p: &Parsed, t: &CostTable) -> u64 {
    match p {
        Parsed::Single { method } => t.cost_of(method) as u64,
        Parsed::Batch { methods } => methods.iter().map(|m| t.cost_of(m) as u64).sum(),
        Parsed::NonJsonRpc => t.default_rest_cost as u64,
    }
}

/// The most restrictive class present (drives RPS gating in M4).
pub fn primary_class(p: &Parsed) -> MethodClass {
    match p {
        Parsed::Single { method } => classify(method),
        Parsed::Batch { methods } => methods
            .iter()
            .map(|m| classify(m))
            .min_by_key(|c| c.restrictiveness())
            .unwrap_or(MethodClass::StandardRpc),
        Parsed::NonJsonRpc => MethodClass::StandardRpc,
    }
}

/// Method names for per-method metric labelling (batch expands to all).
pub fn methods(p: &Parsed) -> Vec<&str> {
    match p {
        Parsed::Single { method } => vec![method.as_str()],
        Parsed::Batch { methods } => methods.iter().map(String::as_str).collect(),
        Parsed::NonJsonRpc => vec!["other"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> CostTable {
        CostTable::from_config(&CostsConfig::default())
    }

    #[test]
    fn classifies_methods() {
        assert_eq!(classify("getBalance"), MethodClass::StandardRpc);
        assert_eq!(classify("sendTransaction"), MethodClass::SendTransaction);
        assert_eq!(classify("getProgramAccounts"), MethodClass::GetProgramAccounts);
        assert_eq!(classify("getAssetsByOwner"), MethodClass::Das);
        assert_eq!(classify("getValidityProofs"), MethodClass::Zk);
    }

    #[test]
    fn costs_follow_table() {
        let t = table();
        assert_eq!(t.cost_of("getBalance"), 1);
        assert_eq!(t.cost_of("getProgramAccounts"), 10);
        assert_eq!(t.cost_of("getAssetsByOwner"), 10);
        assert_eq!(t.cost_of("getValidityProofs"), 100); // built-in override
    }

    #[test]
    fn config_override_wins() {
        let mut c = CostsConfig::default();
        c.overrides.insert("getBalance".into(), 7);
        let t = CostTable::from_config(&c);
        assert_eq!(t.cost_of("getBalance"), 7);
    }

    #[test]
    fn parses_single() {
        let p = parse_body("/", br#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#);
        assert_eq!(
            p,
            Parsed::Single {
                method: "getSlot".into()
            }
        );
    }

    #[test]
    fn parses_batch_and_sums_cost() {
        let body = br#"[{"method":"getBalance"},{"method":"getProgramAccounts"}]"#;
        let p = parse_body("/", body);
        assert_eq!(request_cost(&p, &table()), 11);
        assert_eq!(primary_class(&p), MethodClass::GetProgramAccounts);
    }

    #[test]
    fn batch_picks_most_restrictive_class() {
        let body = br#"[{"method":"getBalance"},{"method":"sendTransaction"}]"#;
        let p = parse_body("/", body);
        assert_eq!(primary_class(&p), MethodClass::SendTransaction);
    }

    #[test]
    fn rest_path_is_non_jsonrpc() {
        let p = parse_body("/v0/transactions", b"{}");
        assert_eq!(p, Parsed::NonJsonRpc);
        assert_eq!(request_cost(&p, &table()), 100);
    }

    #[test]
    fn leading_whitespace_ok() {
        let p = parse_body("/", b"  \n {\"method\":\"getSlot\"}");
        assert_eq!(
            p,
            Parsed::Single {
                method: "getSlot".into()
            }
        );
    }
}
