mod config;
mod error;
mod execution;
mod exits;
mod redis_client;
mod state;
mod types;
mod websocket;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Initialize Alpha Nexus Daemon...");

    // 1. Load configuration
    let config = config::AppConfig::load_from_env()?;
    
    // 2. Initialize shared HTTP/RPC clients
    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        config.rpc_url.clone(), 
        CommitmentConfig::processed()
    ));
    let http_client = Arc::new(reqwest::Client::new());
    let bot_keypair = Arc::new(config.bot_keypair);

    // 3. Fetch whitelist
    let elite_wallets = match redis_client::fetch_elite_wallets().await {
        Ok(w) => w,
        Err(e) => {
            eprintln!("Failed to load wallets from Redis: {}", e);
            return Ok(());
        }
    };

    if elite_wallets.is_empty() {
        println!("Whitelist is empty. Wait for Python sync.");
        return Ok(());
    }

    let bot_state = state::BotState::new();

    // 4. Start listener loop
    if let Err(e) = websocket::run_listener(
        elite_wallets,
        config.pumpportal_api_key,
        rpc_client,
        http_client,
        bot_keypair,
        bot_state,
        config.rpc_url.clone(), // Helius URL for dynamic priority fee estimation
    ).await {
        eprintln!("Fatal Listener Error: {}", e);
    }

    Ok(())
}
