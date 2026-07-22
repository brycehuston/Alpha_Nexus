mod config;
mod db;
mod error;
mod execution;
mod exits;
mod redis_client;
mod state;
mod telegram;
mod types;
mod utils;
mod websocket;

/// Waits for either SIGINT (Ctrl+C) or SIGTERM (systemctl stop).
/// On non-Unix platforms only SIGINT is caught.
async fn wait_for_shutdown_signal() {
    let ctrl_c = async { tokio::signal::ctrl_c().await.ok() };

    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())
            .expect("🚨 Failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c          => {},
            _ = sigterm.recv()  => {},
        }
    }

    #[cfg(not(unix))]
    ctrl_c.await;
}

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signer::Signer;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Initialize Alpha Nexus Daemon...");

    // Load .env for local development (both debug and release builds).
    // Tries cwd first, then ../ then ../../ so the bot works whether you
    // run from the project root OR rust_daemon/. In production (systemd),
    // env vars are injected by the unit file — these are no-ops if no
    // .env is found, so always safe to run.
    if dotenvy::dotenv().is_err() {
        if dotenvy::from_path("../.env").is_err() {
            let _ = dotenvy::from_path("../../.env");
        }
    }

    // 1. Load configuration
    let config = config::AppConfig::load_from_env()?;
    
    // 2. Initialize shared HTTP/RPC clients
    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        config.rpc_url.clone(), 
        CommitmentConfig::processed()
    ));
    let http_client = Arc::new(reqwest::Client::new());
    let bot_keypair = Arc::new(config.bot_keypair);

    // 2.5 Initialize Telemetry DB
    db::init_db();

    // 2.6 Send Startup Alert
    if let (Some(token), Some(chat_id)) = (&config.telegram_bot_token, &config.telegram_chat_id) {
        let pubkey_str = bot_keypair.pubkey().to_string();
        telegram::send_startup_alert(&http_client, token, chat_id, &pubkey_str).await;
    }

    // 3. Fetch whitelist
    let elite_wallets = match redis_client::fetch_elite_wallets().await {
        Ok(w) => w,
        Err(e) => {
            if config.dry_run {
                println!("⚠️  Failed to load wallets from Redis: {}. (Dry Run mode active, using dummy wallet to bypass)", e);
                vec!["11111111111111111111111111111111".to_string()]
            } else {
                eprintln!("Failed to load wallets from Redis: {}", e);
                return Ok(());
            }
        }
    };

    if elite_wallets.is_empty() {
        println!("Whitelist is empty. Wait for Python sync.");
        return Ok(());
    }

    let bot_state = state::BotState::new();

    // 3.5 Recover Open Positions from Database
    let open_positions = db::get_all_open_positions();
    for mint in open_positions {
        if let Some(permit) = bot_state.try_acquire_position() {
            bot_state.try_lock_mint(&mint).await;
            
            let rpc_clone = rpc_client.clone();
            let http_clone = http_client.clone();
            let keypair_clone = bot_keypair.clone();
            let state_clone = bot_state.clone();
            let rpc_url_clone = config.rpc_url.clone();
            let tg_token = config.telegram_bot_token.clone();
            let tg_chat = config.telegram_chat_id.clone();
            let dry_run = config.dry_run;
            
            println!("🔄 RECOVERING OPEN POSITION: {} - Respawning exit watcher.", mint);
            
            tokio::spawn(async move {
                let _permit = permit;
                crate::exits::monitor_and_sell(
                    mint,
                    rpc_clone,
                    http_clone,
                    keypair_clone,
                    state_clone,
                    rpc_url_clone,
                    tg_token,
                    tg_chat,
                    dry_run,
                ).await;
            });
        } else {
            eprintln!("⚠️  Could not recover position {} (Max positions reached).", mint);
        }
    }

    // 4. Start listener loop with graceful shutdown.
    //
    // tokio::select! races two futures:
    //   Branch A: the WebSocket listener (runs indefinitely in normal operation)
    //   Branch B: an OS signal (SIGINT or SIGTERM)
    //
    // If the listener errors out, it is logged and the process exits cleanly.
    // If a shutdown signal arrives, we log a loud warning about open positions
    // (any running monitor_and_sell tasks are orphaned -- they hold no position
    // permits after this point but may still be in a sell attempt), then exit.
    //
    // NOTE: For a zero-downtime deploy, drain open positions before stopping.
    //   Check `journalctl -u alphanexus-daemon | grep 'POSITION CLOSED'`
    //   to confirm all watchers have exited before issuing systemctl stop.
    let open_at_shutdown = bot_state.open_position_count();
    tokio::select! {
        result = websocket::run_listener(
            elite_wallets,
            config.pumpportal_api_key,
            rpc_client,
            http_client,
            bot_keypair,
            bot_state,
            config.rpc_url.clone(),
            config.telegram_bot_token.clone(),
            config.telegram_chat_id.clone(),
            config.dry_run,
            config.trade_size_sol,
        ) => {
            if let Err(e) = result {
                eprintln!("🚨 Fatal Listener Error: {}", e);
            }
        }
        _ = wait_for_shutdown_signal() => {
            eprintln!("🛑 Shutdown signal received. Alpha Nexus stopping.");
            if open_at_shutdown > 0 {
                eprintln!(
                    "⚠️  WARNING: {} open position(s) were active at shutdown. \
                     MANUAL REVIEW REQUIRED — sell any stranded bags manually.",
                    open_at_shutdown
                );
            } else {
                eprintln!("✅ No open positions at shutdown. Clean exit.");
            }
        }
    }

    Ok(())
}
