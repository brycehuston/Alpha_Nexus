use base64::{Engine as _, engine::general_purpose};
use reqwest::Client;
use serde_json::{json, Value};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_request::TokenAccountsFilter;
use solana_sdk::{
    pubkey::Pubkey, signature::Keypair, signer::Signer, transaction::VersionedTransaction,
};
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio::time::{sleep, timeout};
use crate::state::BotState;

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const TP_MULTIPLIER: f64 = 1.50; // 50% Profit target
const SL_MULTIPLIER: f64 = 0.80; // 20% Stop loss

/// FIX #3: Hard deadline for each watcher task.
/// A watcher that fails to exit cleanly (e.g. TP/SL logic bug, Jupiter API
/// outage) will be forcibly dropped after this duration, preventing indefinite
/// zombie task accumulation and the file-descriptor exhaustion that follows.
const WATCHER_MAX_LIFETIME: Duration = Duration::from_secs(2 * 60 * 60); // 2 hours

/// Maximum consecutive Jupiter Price API failures before the watcher aborts.
/// Prevents the poll loop from running forever during a sustained API outage.
/// At 3-second poll intervals, 10 failures = 30 seconds of continuous failure.
const MAX_CONSECUTIVE_API_FAILURES: u32 = 10;

/// Maximum priority fee cap to prevent fee runaway during extreme congestion spikes.
/// Set to 0.005 SOL (5,000,000 lamports). Adjust based on acceptable cost-per-trade.
const MAX_PRIORITY_FEE_LAMPORTS: u64 = 5_000_000;

/// Fallback priority fee if the dynamic estimator API call fails.
/// 0.001 SOL (1,000,000 lamports) — aggressive enough to land most of the time.
const FALLBACK_PRIORITY_FEE_LAMPORTS: u64 = 1_000_000;

// ============================================================================
// FIX #1: Dynamic Token Account Resolution (Replaces hard-coded ATA derivation)
// ============================================================================
//
// OLD APPROACH (REMOVED):
//   fn get_ata(wallet, mint) — hardcoded SPL Token Program ID. Would silently
//   derive a non-existent account for any Token-2022 mint, causing the watcher
//   to immediately abort and leaving the bag stranded.
//
// NEW APPROACH:
//   Ask the RPC which token account the wallet *actually* holds for this mint.
//   This is program-agnostic: it works for SPL Token, Token-2022, and any future
//   token program. Returns `None` if no account exists yet (expected during the
//   confirmation wait loop).
//
async fn resolve_token_account(
    rpc_client: &RpcClient,
    wallet: &Pubkey,
    mint: &Pubkey,
) -> Option<Pubkey> {
    // Fetch all token accounts owned by wallet, filtered by this specific mint.
    // The RPC searches across all token programs automatically.
    let accounts = rpc_client
        .get_token_accounts_by_owner(wallet, TokenAccountsFilter::Mint(*mint))
        .await
        .ok()?;

    // Take the first account (a wallet should only ever have one ATA per mint).
    let pubkey_str = accounts.first()?.pubkey.as_str();
    Pubkey::from_str(pubkey_str).ok()
}

// ============================================================================
// FIX #2: Dynamic Priority Fee Estimator
// ============================================================================
//
// OLD APPROACH (REMOVED):
//   Hardcoded `"prioritizationFeeLamports": 500_000` in the swap payload.
//   During network congestion spikes (exactly when stop-losses fire), this fee
//   is consistently outbid, causing the sell transaction to be dropped.
//
// NEW APPROACH:
//   Query the Helius `getPriorityFeeEstimate` RPC method before constructing
//   the swap payload. We request the `veryHigh` percentile estimate, which
//   targets validators accepting the top ~5% of fees — sufficient for time-
//   sensitive exit transactions. A hard cap of MAX_PRIORITY_FEE_LAMPORTS
//   prevents fee runaway. Falls back to FALLBACK_PRIORITY_FEE_LAMPORTS if
//   the API call fails for any reason.
//
async fn get_dynamic_priority_fee(
    http_client: &Client,
    rpc_url: &str,
    account_keys: &[String],
) -> u64 {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getPriorityFeeEstimate",
        "params": [{
            "accountKeys": account_keys,
            "options": {
                // "veryHigh" targets the ~95th percentile of recent fees for
                // these specific accounts — appropriate for exit transactions
                // where confirmation latency has direct financial consequences.
                "priorityLevel": "veryHigh"
            }
        }]
    });

    let res = match http_client.post(rpc_url).json(&payload).send().await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("⚠️  Priority fee API call failed (network error): {}. Using fallback.", e);
            return FALLBACK_PRIORITY_FEE_LAMPORTS;
        }
    };

    let body: Value = match res.json().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("⚠️  Priority fee API response malformed: {}. Using fallback.", e);
            return FALLBACK_PRIORITY_FEE_LAMPORTS;
        }
    };

    let estimated = body
        .get("result")
        .and_then(|r| r.get("priorityFeeEstimate"))
        .and_then(|f| f.as_f64())
        .map(|f| f as u64)
        .unwrap_or_else(|| {
            eprintln!("⚠️  Priority fee estimate missing from API response. Using fallback.");
            FALLBACK_PRIORITY_FEE_LAMPORTS
        });

    // Apply hard cap to prevent runaway fees on a single exit transaction.
    let capped = estimated.min(MAX_PRIORITY_FEE_LAMPORTS);
    println!("💡 Dynamic priority fee: {} lamports (raw estimate: {}, cap: {})",
        capped, estimated, MAX_PRIORITY_FEE_LAMPORTS);
    capped
}

pub async fn monitor_and_sell(
    target_mint: String,
    rpc_client: Arc<RpcClient>,
    http_client: Arc<Client>,
    bot_keypair: Arc<Keypair>,
    _bot_state: Arc<BotState>, // Reserved for future circuit-breaker integration on sell events
    rpc_url: String,
) {
    // FIX #3: Wrap the entire watcher body in a hard 2-hour timeout.
    // If the position is still open after WATCHER_MAX_LIFETIME, the task is
    // dropped. This is the guaranteed backstop against all zombie scenarios:
    // TP/SL logic never triggering, sell always failing, etc.
    // In production, consider logging this as an alert requiring manual review.
    match timeout(WATCHER_MAX_LIFETIME, monitor_and_sell_inner(
        target_mint.clone(), rpc_client, http_client, bot_keypair, rpc_url
    )).await {
        Ok(()) => {}
        Err(_elapsed) => {
            eprintln!(
                "🚨 WATCHER TIMEOUT: monitor_and_sell for {} exceeded {}h hard limit. \
                 Task terminated. MANUAL REVIEW REQUIRED — position may still be open.",
                target_mint,
                WATCHER_MAX_LIFETIME.as_secs() / 3600
            );
        }
    }
}

/// Inner implementation. Called exclusively by monitor_and_sell's timeout wrapper.
async fn monitor_and_sell_inner(
    target_mint: String,
    rpc_client: Arc<RpcClient>,
    http_client: Arc<Client>,
    bot_keypair: Arc<Keypair>,
    rpc_url: String,
) {
    let wallet_pubkey = bot_keypair.pubkey();

    // FIX #8: Replace `unwrap_or_default()` with explicit error handling.
    //
    // OLD: `Pubkey::from_str(&target_mint).unwrap_or_default()`
    //   If target_mint is an invalid base58 string, this silently falls back
    //   to the System Program address (1111...1111). The watcher then queries
    //   the balance of the *System Program's* ATA, fails silently, and the
    //   real token bag is permanently stranded with no error logged.
    //
    // NEW: Parse explicitly. Log the error and abort the watcher task.
    //   A malformed mint at this stage indicates a bug upstream in the
    //   WebSocket parser. Crashing this task loudly is the correct behavior.
    let mint_pubkey = match Pubkey::from_str(&target_mint) {
        Ok(pk) => pk,
        Err(e) => {
            eprintln!(
                "🚨 CRITICAL: monitor_and_sell received an invalid mint address '{}': {}. \
                 Aborting watcher to prevent System Program fallback.",
                target_mint, e
            );
            return;
        }
    };

    println!("👀 Watcher started for {}. Waiting for buy to settle...", target_mint);

    let mut raw_balance = String::new();
    let mut ui_balance: f64 = 0.0;

    // 1. Wait for the buy transaction to finalize.
    //    FIX #1 in action: instead of querying a statically derived (possibly
    //    wrong-program) ATA, we ask the RPC to find the *actual* token account
    //    the wallet holds for this mint. Works for Token, Token-2022, etc.
    for attempt in 0..15 {
        sleep(Duration::from_secs(2)).await;

        let token_account = match resolve_token_account(&rpc_client, &wallet_pubkey, &mint_pubkey).await {
            Some(acct) => acct,
            None => {
                println!("  [{}/15] Token account not yet visible on-chain for {}...", attempt + 1, target_mint);
                continue;
            }
        };

        if let Ok(balance) = rpc_client.get_token_account_balance(&token_account).await {
            if let Some(ui) = balance.ui_amount {
                if ui > 0.0 {
                    ui_balance = ui;
                    raw_balance = balance.amount;
                    println!("  [{}/15] ✅ Balance confirmed: {} tokens in account {}",
                        attempt + 1, ui_balance, token_account);
                    break;
                }
            }
        }
    }

    if ui_balance == 0.0 {
        println!("❌ Buy transaction for {} likely dropped (no token balance found after 30s). Terminating watcher.", target_mint);
        return;
    }

    println!("✅ Buy confirmed! Tracking {} tokens. Monitoring price...", ui_balance);
    let price_url = format!("https://api.jup.ag/price/v3?ids={},{}", target_mint, WSOL_MINT);

    // 2. Poll Price API and evaluate targets.
    // FIX #3: Track consecutive Jupiter API failures. After MAX_CONSECUTIVE_API_FAILURES
    // in a row, abort the watcher. This prevents an infinite loop during a prolonged
    // Jupiter outage from becoming a permanent zombie task.
    let mut consecutive_api_failures: u32 = 0;

    loop {
        sleep(Duration::from_secs(3)).await;

        let res = match http_client.get(&price_url).send().await {
            Ok(r) => match r.json::<Value>().await {
                Ok(v) => {
                    consecutive_api_failures = 0; // reset on any successful response
                    v
                }
                Err(_) => {
                    consecutive_api_failures += 1;
                    eprintln!("⚠️  Jupiter API parse error ({}/{}). Retrying...",
                        consecutive_api_failures, MAX_CONSECUTIVE_API_FAILURES);
                    if consecutive_api_failures >= MAX_CONSECUTIVE_API_FAILURES {
                        eprintln!("🚨 Jupiter API: {} consecutive failures for {}. Aborting watcher.",
                            consecutive_api_failures, target_mint);
                        return;
                    }
                    continue;
                }
            },
            Err(_) => {
                consecutive_api_failures += 1;
                eprintln!("⚠️  Jupiter API network error ({}/{}). Retrying...",
                    consecutive_api_failures, MAX_CONSECUTIVE_API_FAILURES);
                if consecutive_api_failures >= MAX_CONSECUTIVE_API_FAILURES {
                    eprintln!("🚨 Jupiter API: {} consecutive failures for {}. Aborting watcher.",
                        consecutive_api_failures, target_mint);
                    return;
                }
                continue;
            }
        };

        // Extract both token and SOL prices in USD safely
        let data = match res.get("data") {
            Some(d) => d,
            None => continue,
        };

        let token_price = data.get(&target_mint)
            .and_then(|t| t.get("usdPrice").or_else(|| t.get("price")))
            .and_then(|p| p.as_f64());
            
        let sol_price = data.get(WSOL_MINT)
            .and_then(|t| t.get("usdPrice").or_else(|| t.get("price")))
            .and_then(|p| p.as_f64());

        if let (Some(tp_usd), Some(sp_usd)) = (token_price, sol_price) {
            let current_value_usd = ui_balance * tp_usd;
            let entry_value_usd = 0.1 * sp_usd; // We bought exactly 0.1 SOL worth

            if current_value_usd >= entry_value_usd * TP_MULTIPLIER {
                println!("🎯 TAKE PROFIT HIT! Value: ${:.2} (Entry: ${:.2})", current_value_usd, entry_value_usd);
                if execute_jupiter_sell(&target_mint, &raw_balance, rpc_client.clone(), http_client.clone(), bot_keypair.clone(), &rpc_url).await { break; }
            } else if current_value_usd <= entry_value_usd * SL_MULTIPLIER {
                println!("🛑 STOP LOSS HIT! Value: ${:.2} (Entry: ${:.2})", current_value_usd, entry_value_usd);
                if execute_jupiter_sell(&target_mint, &raw_balance, rpc_client.clone(), http_client.clone(), bot_keypair.clone(), &rpc_url).await { break; }
            }
        }
    }
}

async fn execute_jupiter_sell(
    target_mint: &str,
    raw_amount: &str,
    rpc_client: Arc<RpcClient>,
    http_client: Arc<Client>,
    bot_keypair: Arc<Keypair>,
    rpc_url: &str,
) -> bool {
    let wallet_address = bot_keypair.pubkey().to_string();

    let quote_url = format!(
        "https://quote-api.jup.ag/v6/quote?inputMint={}&outputMint={}&amount={}&slippageBps=2000",
        target_mint, WSOL_MINT, raw_amount
    );

    let quote_res = match http_client.get(&quote_url).send().await {
        Ok(res) => res.json::<Value>().await.unwrap_or(json!({})),
        Err(_) => return false,
    };

    if quote_res.get("error").is_some() {
        println!("⏳ Jupiter hasn't indexed {} yet. Retrying sell...", target_mint);
        return false;
    }

    // FIX #2 in action: fetch a dynamic priority fee before building the swap
    // payload. We pass the input/output mint accounts as context so Helius can
    // estimate fees based on actual contention for those specific accounts.
    let account_keys_for_fee = vec![target_mint.to_string(), WSOL_MINT.to_string()];
    let priority_fee = get_dynamic_priority_fee(&http_client, rpc_url, &account_keys_for_fee).await;

    let swap_payload = json!({
        "quoteResponse": quote_res,
        "userPublicKey": wallet_address,
        "wrapAndUnwrapSol": true,
        "dynamicComputeUnitLimit": true,
        // FIX #2: Was hardcoded `500_000`. Now dynamically estimated per-transaction.
        "prioritizationFeeLamports": priority_fee
    });

    let swap_res = match http_client.post("https://quote-api.jup.ag/v6/swap").json(&swap_payload).send().await {
        Ok(res) => res.json::<Value>().await.unwrap_or(json!({})),
        Err(_) => return false,
    };

    let swap_tx_base64 = match swap_res.get("swapTransaction").and_then(|t| t.as_str()) {
        Some(tx) => tx,
        None => return false,
    };

    // base64 v0.21+ requires the Engine API; the top-level base64::decode() is removed.
    let raw_tx_bytes = match general_purpose::STANDARD.decode(swap_tx_base64) {
        Ok(b) => b,
        Err(_) => return false,
    };
    
    let mut tx: VersionedTransaction = match bincode::deserialize(&raw_tx_bytes) {
        Ok(t) => t,
        Err(_) => return false,
    };
    
    let recent_blockhash = match rpc_client.get_latest_blockhash().await {
        Ok(bh) => bh,
        Err(_) => return false,
    };
    
    tx.message.set_recent_blockhash(recent_blockhash);
    // Sign the VersionedTransaction by serializing the message and signing the bytes.
    // VersionedTransaction does not have a .sign() method — only legacy Transaction does.
    let message_bytes = tx.message.serialize();
    tx.signatures[0] = bot_keypair.sign_message(&message_bytes);

    println!("💸 Firing JUPITER SELL for {} (priority fee: {} lamports)", target_mint, priority_fee);

    for _ in 0..4 {
        let tx_clone = tx.clone();
        let rpc_clone = rpc_client.clone();
        tokio::spawn(async move {
            let _ = rpc_clone.send_transaction_with_config(
                &tx_clone,
                solana_client::rpc_config::RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: Some(solana_sdk::commitment_config::CommitmentLevel::Processed),
                    ..Default::default()
                },
            ).await;
        });
        sleep(Duration::from_millis(300)).await;
    }
    
    true
}
