use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ShadowReceipt {
    pub signature: String,
    pub observed_at_ms: u128,
    pub wallet: String,
    pub mint: String,
    pub classification: String,
    pub sol_spent: f64,
    pub tokens_received: f64,
    pub gate_whitelist: bool,
    pub gate_state_change: bool,
    pub gate_size: bool,
    pub gate_double_buy: bool,
    pub gate_circuit_breaker: bool,
    pub decision: String,
    pub decision_latency_ms: f64,
    pub entry_price_simulated: f64,
    pub estimated_priority_fee_sol: f64,
    pub reason: String,
}

pub struct ShadowLogger;

impl ShadowLogger {
    pub async fn append_receipt(receipt: ShadowReceipt) {
        let json_line = match serde_json::to_string(&receipt) {
            Ok(json) => format!("{json}\n"),
            Err(error) => {
                eprintln!("Failed to serialize shadow receipt: {error}");
                return;
            }
        };

        let mut file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open("shadow_ledger.jsonl")
            .await
        {
            Ok(file) => file,
            Err(error) => {
                eprintln!("Failed to open shadow ledger: {error}");
                return;
            }
        };

        if let Err(error) = file.write_all(json_line.as_bytes()).await {
            eprintln!("Failed to append shadow receipt: {error}");
        }
    }

    pub fn now_ms() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    }
}
