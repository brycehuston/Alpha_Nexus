use reqwest::Client;
use serde_json::json;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{signature::{Keypair, Signature}, signer::Signer, transaction::VersionedTransaction};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::oneshot,
    time::{sleep, timeout},
};
use crate::state::BotState;
use crate::utils::get_dynamic_priority_fee;

/// How long to wait for on-chain confirmation of any broadcast before
/// giving up and aborting the watcher spawn.
/// Set to the Solana blockhash lifetime (~60s) plus a small buffer.
const BUY_CONFIRM_TIMEOUT: Duration = Duration::from_secs(75);

/// WSOL mint address — used as account key for Helius priority fee estimation.
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

pub async fn execute_pump_buy(
    target_mint: String,
    rpc_client: Arc<RpcClient>,
    http_client: Arc<Client>,
    bot_keypair: Arc<Keypair>,
    bot_state: Arc<BotState>,
    rpc_url: String,
    // The position semaphore permit, held for the FULL trade lifetime.
    // It is moved into the monitor_and_sell task and released only when
    // the watcher exits — ensuring the slot stays occupied for the entire
    // duration of the open position, not just the buy confirmation window.
    position_permit: tokio::sync::OwnedSemaphorePermit,
    telegram_bot_token: Option<String>,
    telegram_chat_id: Option<String>,
    dry_run: bool,
    trade_size_sol: f64,
) {
    let wallet_address = bot_keypair.pubkey().to_string();

    // FIX #1 — DYNAMIC BUY PRIORITY FEE
    //
    // WHY: The entry transaction is the most time-critical in the entire trade
    // lifecycle. A hardcoded 0.0005 SOL fee gets deprioritized or dropped entirely
    // during a memecoin pump — exactly when speed matters most. The exit path
    // already used Helius dynamic fees; the buy side now uses the same estimator.
    //
    // CONVERSION: PumpPortal trade-local API expects priorityFee in SOL.
    // get_dynamic_priority_fee returns lamports. Divide by 1e9.
    let fee_account_keys = vec![target_mint.clone(), WSOL_MINT.to_string()];
    let fee_lamports = get_dynamic_priority_fee(&http_client, &rpc_url, &fee_account_keys).await;
    let priority_fee_sol = fee_lamports as f64 / 1_000_000_000.0;
    println!(
        "💡 Buy priority fee for {}: {} lamports ({:.6} SOL)",
        target_mint, fee_lamports, priority_fee_sol
    );

    if dry_run {
        println!("🚀 [DRY RUN] Simulating buy execution for {}", target_mint);
        // Simulate immediate confirmation and spawn exit watcher
        let rpc_clone     = rpc_client.clone();
        let http_clone    = http_client.clone();
        let keypair_clone = bot_keypair.clone();
        let state_clone   = bot_state.clone();
        let rpc_url_clone = rpc_url.clone();
        let mint_clone    = target_mint.clone();
        let tg_token      = telegram_bot_token.clone();
        let tg_chat       = telegram_chat_id.clone();
        
        tokio::spawn(async move {
            let _position_permit = position_permit;
            crate::exits::monitor_and_sell(
                mint_clone, rpc_clone, http_clone, keypair_clone, state_clone, rpc_url_clone, tg_token, tg_chat, dry_run
            ).await;
        });
        return;
    }

    // ========================================================================
    // LIVE EXECUTION PATH
    // ========================================================================
    let payload = json!({
        "publicKey": wallet_address,
        "action": "buy",
        "mint": target_mint,
        "amount": trade_size_sol, // Loaded dynamically from .env
        "denominatedInSol": "true",
        "slippage": 15, // 15% Slippage
        "priorityFee": priority_fee_sol, // Dynamic — market-responsive via Helius
        "pool": "pump"
    });

    println!("⚡ Requesting Local TX for {} (Size: {} SOL)...", target_mint, trade_size_sol);

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

    // -------------------------------------------------------------------------
    // BUY CONFIRMATION GATE
    //
    // PROBLEM (pre-fix):
    //   The exit watcher was spawned unconditionally immediately after firing the
    //   broadcast loop. If all 30 broadcasts were dropped (congestion, expired
    //   blockhash, validator rejection), the watcher would start, find zero token
    //   balance after 30 seconds, and exit — silently. No capital was at risk, but
    //   the pattern hides buy failures and wastes a watcher task and its RPC calls.
    //
    //   More critically, with the 60-second broadcast loop, the watcher STARTED
    //   while broadcasts were still in-flight. If the tx eventually landed at t=55s,
    //   the watcher had already been polling for 55 seconds and would have aborted
    //   at t=30s (no balance found). Buy lands, but no watcher is running. Bag
    //   stranded with no exit logic.
    //
    // FIX APPROACH:
    //   1. Open a oneshot channel (tx_confirmed_sig) between the broadcast tasks
    //      and this function.
    //   2. The FIRST broadcast task that gets an Ok(sig) back from the RPC sends
    //      the signature through the channel and terminates. All subsequent tasks
    //      that also get Ok(sig) will find the channel already closed and silently
    //      drop their result — this is correct behavior.
    //   3. This function awaits the receiver with a BUY_CONFIRM_TIMEOUT.
    //      - If a sig arrives: the buy landed. Spawn the exit watcher.
    //      - If timeout fires: all 30 broadcasts expired or failed. Abort cleanly.
    //        The circuit breaker in BotState is NOT incremented because this is an
    //        infrastructure failure, not a stop-loss event.
    // -------------------------------------------------------------------------
    const BROADCAST_ATTEMPTS: u32 = 30;
    const BROADCAST_INTERVAL: Duration = Duration::from_secs(2);

    // Channel capacity = 1. Only the first confirmed sig matters.
    let (confirm_tx, confirm_rx) = oneshot::channel::<Signature>();
    // Wrap in Arc<Mutex> so multiple spawned tasks can race to send the first sig.
    let confirm_tx = Arc::new(tokio::sync::Mutex::new(Some(confirm_tx)));

    println!(
        "📡 Broadcasting tx for {} ({} attempts over {}s)...",
        target_mint,
        BROADCAST_ATTEMPTS,
        BROADCAST_ATTEMPTS * BROADCAST_INTERVAL.as_secs() as u32
    );

    for attempt in 1..=BROADCAST_ATTEMPTS {
        let tx_clone     = tx.clone();
        let rpc_clone    = rpc_client.clone();
        let mint_label   = target_mint.clone();
        let confirm_slot = confirm_tx.clone();

        tokio::spawn(async move {
            match rpc_clone.send_transaction_with_config(
                &tx_clone,
                solana_client::rpc_config::RpcSendTransactionConfig {
                    skip_preflight: true, // CRITICAL: Do not pre-simulate
                    preflight_commitment: Some(solana_sdk::commitment_config::CommitmentLevel::Processed),
                    ..Default::default()
                },
            ).await {
                Ok(sig) => {
                    println!("✅ Broadcast #{} accepted by RPC for {}: {}", attempt, mint_label, sig);
                    // Race to be the first to confirm. Only one sender wins.
                    let mut slot = confirm_slot.lock().await;
                    if let Some(sender) = slot.take() {
                        // Channel send can only fail if the receiver was dropped
                        // (i.e. BUY_CONFIRM_TIMEOUT already fired). Safe to ignore.
                        let _ = sender.send(sig);
                    }
                }
                Err(e) => eprintln!("⚠️  Broadcast #{} failed for {}: {}", attempt, mint_label, e),
            }
        });

        sleep(BROADCAST_INTERVAL).await;
    }

    // Await confirmation with a hard timeout equal to the blockhash lifetime + buffer.
    let confirmed_sig = match timeout(BUY_CONFIRM_TIMEOUT, confirm_rx).await {
        Ok(Ok(sig)) => {
            println!(
                "🎯 Buy CONFIRMED on-chain for {} — sig: {}. Spawning exit watcher.",
                target_mint, sig
            );
            sig
        }
        Ok(Err(_)) => {
            // Sender was dropped (all broadcast tasks finished with Err).
            eprintln!(
                "❌ Buy FAILED for {} — all {} broadcast attempts rejected by RPC. \
                 No watcher spawned. Check RPC health and blockhash freshness.",
                target_mint, BROADCAST_ATTEMPTS
            );
            return;
        }
        Err(_timeout) => {
            eprintln!(
                "❌ Buy TIMEOUT for {} — no confirmation received within {}s. \
                 Transaction likely expired. No watcher spawned.",
                target_mint, BUY_CONFIRM_TIMEOUT.as_secs()
            );
            return;
        }
    };

    // Log the confirmed signature for auditability (useful for Paper Trading).
    println!("📋 Confirmed buy sig for {}: {}", target_mint, confirmed_sig);
    
    if !dry_run {
        crate::db::insert_open_position(&target_mint);
    }

    // Spawn the exit watcher ONLY after buy is confirmed on-chain.
    // The position_permit is moved HERE — into the watcher task — so the
    // semaphore slot is held for the true lifetime of the position (up to
    // WATCHER_MAX_LIFETIME = 2 hours), not just the ~75s buy confirm window.
    // When monitor_and_sell returns (sell executed or timeout), the permit
    // drops automatically and the slot becomes available for a new position.
    let rpc_clone     = rpc_client.clone();
    let http_clone    = http_client.clone();
    let keypair_clone = bot_keypair.clone();
    let state_clone   = bot_state.clone();
    let rpc_url_clone = rpc_url.clone();
    let mint_clone    = target_mint.clone();
    let tg_token      = telegram_bot_token.clone();
    let tg_chat       = telegram_chat_id.clone();
    tokio::spawn(async move {
        // Permit lives here. Drop happens when this async block exits.
        let _position_permit = position_permit;
        crate::exits::monitor_and_sell(
            mint_clone,
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
}
