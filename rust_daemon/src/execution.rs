use reqwest::Client;
use serde_json::json;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{signature::Keypair, signer::Signer, transaction::VersionedTransaction};
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use crate::state::BotState;

pub async fn execute_pump_buy(
    target_mint: String,
    rpc_client: Arc<RpcClient>,
    http_client: Arc<Client>,
    bot_keypair: Arc<Keypair>,
    bot_state: Arc<BotState>,
    rpc_url: String,
) {
    let wallet_address = bot_keypair.pubkey().to_string();

    let payload = json!({
        "publicKey": wallet_address,
        "action": "buy",
        "mint": target_mint,
        "amount": 0.1, // Fixed 0.1 SOL buy size (adjust as needed)
        "denominatedInSol": "true",
        "slippage": 15, // 15% Slippage
        "priorityFee": 0.0005,
        "pool": "pump"
    });

    println!("⚡ Requesting Local TX for {}...", target_mint);

    // Call PumpPortal's Local Trade API
    let res = match http_client.post("https://pumpportal.fun/api/trade-local")
        .json(&payload)
        .send().await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Failed to fetch tx from PumpPortal: {}", e);
                return;
            }
        };

    let tx_bytes = match res.bytes().await {
        Ok(b) => b,
        Err(_) => return,
    };

    // Deserialize into Solana VersionedTransaction
    let mut tx: VersionedTransaction = match bincode::deserialize(&tx_bytes) {
        Ok(t) => t,
        Err(_) => {
            eprintln!("Failed to deserialize PumpPortal transaction bytes.");
            return;
        }
    };

    // Sign locally
    let recent_blockhash = match rpc_client.get_latest_blockhash().await {
        Ok(bh) => bh,
        Err(_) => return,
    };
    tx.message.set_recent_blockhash(recent_blockhash);
    // Sign the VersionedTransaction by serializing the message and signing the bytes.
    // VersionedTransaction does not have a .sign() method — only legacy Transaction does.
    // After mutating the blockhash, we must re-derive and assign the signature manually.
    let message_bytes = tx.message.serialize();
    tx.signatures[0] = bot_keypair.sign_message(&message_bytes);

    println!("🚀 Firing execution for {}", target_mint);

    // The Anti-Drop Broadcast Loop
    for _ in 0..4 {
        let tx_clone = tx.clone();
        let rpc_clone = rpc_client.clone();
        
        tokio::spawn(async move {
            let _ = rpc_clone.send_transaction_with_config(
                &tx_clone,
                solana_client::rpc_config::RpcSendTransactionConfig {
                    skip_preflight: true, // CRITICAL: Do not pre-simulate
                    preflight_commitment: Some(solana_sdk::commitment_config::CommitmentLevel::Processed),
                    ..Default::default()
                },
            ).await;
        });
        sleep(Duration::from_millis(300)).await;
    }

    // Spawn the exit watcher. Passes rpc_url so monitor_and_sell can query
    // Helius for dynamic priority fees on the eventual sell transaction.
    let rpc_clone = rpc_client.clone();
    let http_clone = http_client.clone();
    let keypair_clone = bot_keypair.clone();
    let target_mint_clone = target_mint.clone();
    let state_clone = bot_state.clone();
    let rpc_url_clone = rpc_url.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(3)).await;
        crate::exits::monitor_and_sell(
            target_mint_clone,
            rpc_clone,
            http_clone,
            keypair_clone,
            state_clone,
            rpc_url_clone,
        ).await;
    });
}
