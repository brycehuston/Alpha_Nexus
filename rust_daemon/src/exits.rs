use base64::{Engine as _, engine::general_purpose};
use reqwest::Client;
use serde_json::{json, Value};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_request::TokenAccountsFilter;
use solana_sdk::{
    pubkey::Pubkey, signature::{Keypair, Signature}, signer::Signer, transaction::VersionedTransaction,
};
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio::time::{sleep, timeout};
use crate::state::BotState;
use crate::utils::{get_dynamic_priority_fee, MAX_PRIORITY_FEE_LAMPORTS};

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

// ============================================================================
// FIX #3: Hard deadline for each watcher task.
// ============================================================================
const WATCHER_MAX_LIFETIME: Duration = Duration::from_secs(2 * 60 * 60); // 2 hours

/// Maximum consecutive Jupiter Price API failures before the watcher aborts.
const MAX_CONSECUTIVE_API_FAILURES: u32 = 10;

// Priority fee constants and get_dynamic_priority_fee() have been extracted
// to crate::utils so both the buy (execution.rs) and sell paths share them.

// ============================================================================
// VELOCITY-BASED ADAPTIVE TRAILING STOP (VBATS) — Configuration
// ============================================================================
//
// MATHEMATICAL MODEL SUMMARY:
//
//   Each 3-second poll yields a log-return velocity:
//       v_t = ln(P_t / P_{t-1})
//
//   We smooth with an EMA (alpha = 2/(N+1), N=5 ticks ~= 15 second half-life):
//       ema_v_t = alpha * v_t + (1 - alpha) * ema_v_{t-1}
//
//   The adaptive trail distance is:
//       trail_pct = clamp(TRAIL_BASE - ema_v * VELOCITY_SENSITIVITY,
//                         TRAIL_MIN, TRAIL_MAX)
//
//   The stop price is path-dependent (high-water mark based):
//       stop_price = max_price_ever * (1 - trail_pct)
//
//   EXIT CONDITIONS:
//       current_price < stop_price           (adaptive trail hit)
//       OR v_t < PANIC_VELOCITY_THRESHOLD    (single-tick flash crash)
//
//   PROFIT LOCK (once price >= PROFIT_LOCK_THRESHOLD * entry):
//       stop_price is floored at entry_price * PROFIT_LOCK_FLOOR_MULT
//       -- we can NEVER give back more than (1 - PROFIT_LOCK_FLOOR_MULT) of entry
//          once we have confirmed a profitable position.

/// EMA smoothing factor alpha = 2 / (N + 1) for N=5 ticks.
/// Gives recent ticks ~2x weight of older ones. Half-life ~= 2 ticks (6 seconds).
const EMA_ALPHA: f64 = 2.0 / (5.0 + 1.0); // ~= 0.3333

/// Default trail distance when velocity is near-zero (neutral market).
/// 8% buffer absorbs normal memecoin micro-volatility without premature exits.
const TRAIL_BASE: f64 = 0.08;

/// Tightest trail allowed, applied during explosive upward velocity.
/// 3% -- tight enough to capture the spike peak, loose enough for continuation.
const TRAIL_MIN: f64 = 0.03;

/// Widest trail allowed. Also serves as the absolute hard-floor stop-loss
/// when price has not yet moved (EMA not converged, first few ticks).
const TRAIL_MAX: f64 = 0.20;

/// Sensitivity of trail tightening/widening to EMA velocity.
/// trail_pct = TRAIL_BASE - ema_v * VELOCITY_SENSITIVITY
/// At ema_v = +0.05 (strong up): trail = 0.08 - 0.075 = 0.005 -> floored at 3%
/// At ema_v = -0.05 (strong dn): trail = 0.08 + 0.075 = 0.155 -> capped at 20%
const VELOCITY_SENSITIVITY: f64 = 1.5;

/// Single-tick velocity threshold for an emergency "panic dump".
/// ln(0.88) ~= -0.128 -- price dropped >12% in one 3-second tick.
/// This is not normal volatility noise; it is a rug or flash crash. Exit NOW.
const PANIC_VELOCITY_THRESHOLD: f64 = -0.128;

/// Once the position gains this multiple of entry value, activate profit lock.
/// 1.30 = +30% unrealized gain -- meaningful enough to protect.
const PROFIT_LOCK_THRESHOLD: f64 = 1.30;

/// The trail stop floor when profit lock is active, expressed as a multiple
/// of entry value. 1.10 = we lock in at least +10% profit once +30% is hit.
const PROFIT_LOCK_FLOOR_MULT: f64 = 1.10;

// ============================================================================
// FIX #1: Dynamic Token Account Resolution
// ============================================================================
async fn resolve_token_account(
    rpc_client: &RpcClient,
    wallet: &Pubkey,
    mint: &Pubkey,
) -> Option<Pubkey> {
    let accounts = rpc_client
        .get_token_accounts_by_owner(wallet, TokenAccountsFilter::Mint(*mint))
        .await
        .ok()?;
    let pubkey_str = accounts.first()?.pubkey.as_str();
    Pubkey::from_str(pubkey_str).ok()
}

// get_dynamic_priority_fee() is now in crate::utils (imported above).

// ============================================================================
// PUBLIC ENTRYPOINT -- FIX #3: 2-hour hard timeout wrapper
// ============================================================================
pub async fn monitor_and_sell(
    target_mint: String,
    rpc_client: Arc<RpcClient>,
    http_client: Arc<Client>,
    bot_keypair: Arc<Keypair>,
    bot_state: Arc<BotState>,
    rpc_url: String,
) {
    match timeout(WATCHER_MAX_LIFETIME, monitor_and_sell_inner(
        target_mint.clone(), rpc_client, http_client, bot_keypair, bot_state, rpc_url
    )).await {
        Ok(()) => {}
        Err(_elapsed) => {
            eprintln!(
                "🚨 WATCHER TIMEOUT: monitor_and_sell for {} exceeded {}h hard limit. \
                 Task terminated. MANUAL REVIEW REQUIRED -- position may still be open.",
                target_mint,
                WATCHER_MAX_LIFETIME.as_secs() / 3600
            );
        }
    }
}

// ============================================================================
// INNER WATCHER -- Velocity-Based Adaptive Trailing Stop implementation
// ============================================================================
async fn monitor_and_sell_inner(
    target_mint: String,
    rpc_client: Arc<RpcClient>,
    http_client: Arc<Client>,
    bot_keypair: Arc<Keypair>,
    bot_state: Arc<BotState>,
    rpc_url: String,
) {
    let wallet_pubkey = bot_keypair.pubkey();

    // FIX #8: Explicit mint parse with loud failure -- no silent System Program fallback.
    let mint_pubkey = match Pubkey::from_str(&target_mint) {
        Ok(pk) => pk,
        Err(e) => {
            eprintln!(
                "🚨 CRITICAL: monitor_and_sell received an invalid mint address '{}': {}. \
                 Aborting watcher.",
                target_mint, e
            );
            return;
        }
    };

    println!("👀 Watcher started for {}. Waiting for buy to settle...", target_mint);

    // -----------------------------------------------------------------------
    // PHASE 1: Wait for the buy transaction to finalize on-chain.
    // FIX #1: Dynamic token account resolution (works for Token-2022, etc.)
    // -----------------------------------------------------------------------
    let mut raw_balance = String::new();
    let mut ui_balance: f64 = 0.0;

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
        println!("❌ Buy for {} likely dropped (no balance after 30s). Terminating watcher.", target_mint);
        return;
    }

    println!("✅ Buy confirmed! Tracking {} tokens. Starting VBATS...", ui_balance);
    let price_url = format!("https://api.jup.ag/price/v3?ids={},{}", target_mint, WSOL_MINT);

    // -----------------------------------------------------------------------
    // PHASE 2: VBATS -- Velocity-Based Adaptive Trailing Stop
    //
    //  State machine variables:
    //   entry_value_usd    -- portfolio value at the moment the buy was confirmed
    //   high_water_mark    -- highest portfolio value seen (denominated in USD)
    //   ema_velocity       -- exponential moving average of log-return velocity
    //   prev_value_usd     -- last tick's portfolio value (for computing velocity)
    //   profit_lock_active -- true once the position hits PROFIT_LOCK_THRESHOLD
    //
    //  On each 3-second poll tick t:
    //    1. Compute log-return:  v_t = ln(current_val / prev_val)
    //    2. Update EMA:          ema_v = alpha*v_t + (1-alpha)*ema_v
    //    3. Compute trail pct:   trail = clamp(BASE - ema_v*SENSITIVITY, MIN, MAX)
    //    4. Update HWM:          hwm = max(hwm, current_val)
    //    5. Compute stop price:  stop = hwm * (1 - trail)
    //    6. Apply profit lock:   stop = max(stop, entry * LOCK_FLOOR) if lock active
    //    7. Check exits:         sell if current < stop  OR  v_t < PANIC_THRESHOLD
    // -----------------------------------------------------------------------
    let mut consecutive_api_failures: u32 = 0;

    // Prime the pump: fetch the first price to set entry_value_usd.
    println!("⏳ Fetching entry price reference...");
    let entry_value_usd: f64;
    let sol_price_usd: f64;

    // NOTE: consecutive_api_failures is reused here from the outer scope.
    // It was reset to 0 just above. If the primer loop hits MAX_CONSECUTIVE_API_FAILURES
    // it aborts with the same loud error as the main loop.
    loop {
        sleep(Duration::from_secs(3)).await;
        let res = match http_client.get(&price_url).send().await {
            Ok(r) => match r.json::<Value>().await {
                Ok(v) => {
                    consecutive_api_failures = 0;
                    v
                }
                Err(e) => {
                    consecutive_api_failures += 1;
                    eprintln!("⚠️  Entry price primer: parse error ({}/{}): {}",
                        consecutive_api_failures, MAX_CONSECUTIVE_API_FAILURES, e);
                    if consecutive_api_failures >= MAX_CONSECUTIVE_API_FAILURES {
                        eprintln!("🚨 Cannot get entry price for {} after {} attempts. \
                                   Token may not be Jupiter-indexed. Aborting watcher.",
                            target_mint, MAX_CONSECUTIVE_API_FAILURES);
                        return;
                    }
                    continue;
                }
            },
            Err(e) => {
                consecutive_api_failures += 1;
                eprintln!("⚠️  Entry price primer: network error ({}/{}): {}",
                    consecutive_api_failures, MAX_CONSECUTIVE_API_FAILURES, e);
                if consecutive_api_failures >= MAX_CONSECUTIVE_API_FAILURES {
                    eprintln!("🚨 Cannot get entry price for {} after {} attempts. \
                               Aborting watcher.",
                        target_mint, MAX_CONSECUTIVE_API_FAILURES);
                    return;
                }
                continue;
            }
        };

        let data = match res.get("data") { Some(d) => d, None => continue };
        let tp_usd = data.get(&target_mint)
            .and_then(|t| t.get("usdPrice").or_else(|| t.get("price")))
            .and_then(|p| p.as_f64());
        let sp_usd = data.get(WSOL_MINT)
            .and_then(|t| t.get("usdPrice").or_else(|| t.get("price")))
            .and_then(|p| p.as_f64());

        if let (Some(tp), Some(sp)) = (tp_usd, sp_usd) {
            // GUARD: Jupiter returns 0.0 for unindexed / illiquid tokens.
            // A zero entry price makes profit_ratio = NaN/Inf on every tick,
            // silently disabling ALL exit conditions (stop, panic dump, profit lock).
            // Treat zero as "not yet liquid" and retry.
            if tp <= 0.0 || sp <= 0.0 {
                consecutive_api_failures += 1;
                eprintln!("⚠️  Entry price primer: zero price received (tp={}, sp={}) ({}/{}) — \
                           token not yet liquid on Jupiter. Retrying...",
                    tp, sp, consecutive_api_failures, MAX_CONSECUTIVE_API_FAILURES);
                if consecutive_api_failures >= MAX_CONSECUTIVE_API_FAILURES {
                    eprintln!("🚨 Token {} returned zero price {} consecutive times. \
                               Not Jupiter-tradeable at this time. Aborting watcher.",
                        target_mint, MAX_CONSECUTIVE_API_FAILURES);
                    return;
                }
                continue;
            }
            entry_value_usd = ui_balance * tp;
            sol_price_usd = sp;
            println!(
                "📊 VBATS initialized | Entry value: ${:.4} | Token price: ${:.8} | SOL: ${:.2}",
                entry_value_usd, tp, sp
            );
            break;
        }
    }


    // Calculate what the 0.1 SOL entry cost us in USD (for reference logging).
    let entry_cost_usd = 0.1 * sol_price_usd;

    // Initialize VBATS state
    let mut high_water_mark = entry_value_usd;
    let mut prev_value_usd = entry_value_usd;
    // Seed EMA at zero -- no directional bias before we have velocity data.
    let mut ema_velocity: f64 = 0.0;
    let mut profit_lock_active = false;
    let mut tick: u64 = 0;
    // Tracks the portfolio USD value at the moment of sell execution.
    // Set at each break site so the post-loop P&L logic can update the
    // circuit breaker counter. Stays 0.0 if the watcher exits via error
    // return (no sell executed), which correctly skips counter updates.
    // The 0.0 initializer IS intentionally "unused" by the compiler's view —
    // it is a sentinel, not a data value. Suppress the false-positive lint.
    #[allow(unused_assignments)]
    let mut exit_value_usd: f64 = 0.0;

    consecutive_api_failures = 0;

    // Main VBATS poll loop
    loop {
        sleep(Duration::from_secs(3)).await;
        tick += 1;

        // ---- 1. Fetch current price ----------------------------------------
        let res = match http_client.get(&price_url).send().await {
            Ok(r) => match r.json::<Value>().await {
                Ok(v) => {
                    consecutive_api_failures = 0;
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

        let data = match res.get("data") { Some(d) => d, None => continue };
        let token_price_usd = match data.get(&target_mint)
            .and_then(|t| t.get("usdPrice").or_else(|| t.get("price")))
            .and_then(|p| p.as_f64()) {
            Some(p) => p,
            None => continue,
        };

        let current_value_usd = ui_balance * token_price_usd;

        // ---- 2. Compute log-return velocity v_t ----------------------------
        // Guard against zero/negative prev_value (should not happen but be safe).
        // ALSO guard against current_value_usd being zero or negative:
        //   ln(0.0)     = -Infinity  →  EMA becomes -Inf permanently
        //   ln(negative)= NaN        →  EMA becomes NaN permanently, disabling all exits
        // Apply a final is_finite() check as a backstop against any IEEE 754 edge case.
        let log_velocity = if prev_value_usd > 1e-12 && current_value_usd > 1e-12 {
            let ratio = current_value_usd / prev_value_usd;
            let v = ratio.ln();
            // If ratio produced a non-finite result despite passing the guards
            // (e.g. API returned subnormal float garbage), treat as zero velocity.
            if v.is_finite() { v } else { 0.0 }
        } else {
            0.0
        };

        // ---- 3. Update EMA velocity ----------------------------------------
        // On the first tick (EMA seeded at 0), fully adopt the first reading
        // to avoid a biased initial state.
        if tick == 1 {
            ema_velocity = log_velocity;
        } else {
            ema_velocity = EMA_ALPHA * log_velocity + (1.0 - EMA_ALPHA) * ema_velocity;
        }

        // ---- 4. Update high-water mark -------------------------------------
        if current_value_usd > high_water_mark {
            high_water_mark = current_value_usd;
        }

        // ---- 5. Compute adaptive trail distance ----------------------------
        // trail_pct = clamp(TRAIL_BASE - ema_velocity * VELOCITY_SENSITIVITY,
        //                   TRAIL_MIN, TRAIL_MAX)
        let raw_trail = TRAIL_BASE - ema_velocity * VELOCITY_SENSITIVITY;
        let trail_pct = raw_trail.clamp(TRAIL_MIN, TRAIL_MAX);

        // ---- 6. Compute the trailing stop value ----------------------------
        let mut trail_stop_value = high_water_mark * (1.0 - trail_pct);

        // ---- 7. Activate and apply profit lock if threshold crossed --------
        let profit_ratio = current_value_usd / entry_value_usd;
        if profit_ratio >= PROFIT_LOCK_THRESHOLD && !profit_lock_active {
            profit_lock_active = true;
            println!(
                "🔒 PROFIT LOCK ACTIVATED at {:.1}% gain! \
                 Stop floor raised to ${:.4} (+{:.0}% of entry).",
                (profit_ratio - 1.0) * 100.0,
                entry_value_usd * PROFIT_LOCK_FLOOR_MULT,
                (PROFIT_LOCK_FLOOR_MULT - 1.0) * 100.0
            );
        }
        if profit_lock_active {
            let lock_floor = entry_value_usd * PROFIT_LOCK_FLOOR_MULT;
            if trail_stop_value < lock_floor {
                trail_stop_value = lock_floor;
            }
        }

        // ---- 8. Status log (every tick) ------------------------------------
        let pnl_pct = (current_value_usd / entry_cost_usd - 1.0) * 100.0;
        println!(
            "📈 [t={:>4}] Val: ${:.4}  HWM: ${:.4}  Stop: ${:.4}  Trail: {:.1}%  \
             v_t: {:+.4}  EMA_v: {:+.4}  PnL: {:+.1}%{}",
            tick,
            current_value_usd,
            high_water_mark,
            trail_stop_value,
            trail_pct * 100.0,
            log_velocity,
            ema_velocity,
            pnl_pct,
            if profit_lock_active { "  🔒" } else { "" }
        );

        // ---- 9. EXIT CONDITION A: Panic velocity dump ----------------------
        // A single-tick drop of >12.8% (PANIC_VELOCITY_THRESHOLD) is a flash
        // crash or rug. Bypass the trail and market-sell immediately.
        // The guard `profit_ratio > 0.85` prevents panic-dumping during the
        // normal initial buy-settle dip (entry is never perfectly flat).
        if log_velocity < PANIC_VELOCITY_THRESHOLD && profit_ratio > 0.85 {
            println!(
                "🚨 PANIC DUMP TRIGGERED! Single-tick velocity: {:.4} (threshold: {:.4}). \
                 Current: ${:.4}  Entry: ${:.4}  Executing emergency sell NOW.",
                log_velocity, PANIC_VELOCITY_THRESHOLD, current_value_usd, entry_value_usd
            );
            if execute_jupiter_sell(
                &target_mint, &raw_balance, rpc_client.clone(),
                http_client.clone(), bot_keypair.clone(), &rpc_url
            ).await {
                exit_value_usd = current_value_usd;
                break;
            }
        }

        // ---- 10. EXIT CONDITION B: Adaptive trailing stop ------------------
        if current_value_usd < trail_stop_value {
            let exit_reason = if profit_lock_active {
                "PROFIT LOCK FLOOR"
            } else if trail_pct >= TRAIL_MAX - 0.001 {
                "HARD FLOOR (20%)"
            } else {
                "ADAPTIVE TRAIL"
            };
            println!(
                "🛑 {} HIT! Val: ${:.4} < Stop: ${:.4} \
                 (Trail: {:.1}%  EMA_v: {:+.4}  HWM: ${:.4}). Selling...",
                exit_reason, current_value_usd, trail_stop_value,
                trail_pct * 100.0, ema_velocity, high_water_mark
            );
            if execute_jupiter_sell(
                &target_mint, &raw_balance, rpc_client.clone(),
                http_client.clone(), bot_keypair.clone(), &rpc_url
            ).await {
                exit_value_usd = current_value_usd;
                break;
            }
        }

        // Advance state for next tick
        prev_value_usd = current_value_usd;
    }

    // -------------------------------------------------------------------------
    // POST-SELL: Wire circuit breaker counter.
    //
    // exit_value_usd > 0.0 means the VBATS loop exited via a confirmed sell.
    // If it's still 0.0, the watcher aborted via a `return` (API failure, bad
    // mint, etc.) — no trade occurred, so we don't penalize the loss counter.
    //
    // Win/loss is measured against entry_cost_usd (the SOL we actually spent
    // in USD terms), not entry_value_usd (the token's Jupiter-quoted value at
    // entry, which already includes spread). This correctly answers the question
    // "did we make or lose money relative to what we paid?"
    // -------------------------------------------------------------------------
    if exit_value_usd > 0.0 {
        use std::sync::atomic::Ordering;
        let pnl_usd = exit_value_usd - entry_cost_usd;
        let pnl_pct_final = (exit_value_usd / entry_cost_usd - 1.0) * 100.0;

        if pnl_usd >= 0.0 {
            // Profitable exit — reset the streak. Any win breaks the losing run.
            bot_state.consecutive_losses.store(0, Ordering::SeqCst);
            println!(
                "✅ POSITION CLOSED [{target_mint}] | P&L: +${pnl_usd:.4} ({pnl_pct_final:+.1}%) \
                 | Loss streak RESET to 0.",
                target_mint = target_mint,
                pnl_usd = pnl_usd,
                pnl_pct_final = pnl_pct_final,
            );
        } else {
            // Loss — increment the streak. Circuit breaker will trip at threshold.
            let streak = bot_state.consecutive_losses.fetch_add(1, Ordering::SeqCst) + 1;
            eprintln!(
                "📉 POSITION CLOSED [{target_mint}] | P&L: ${pnl_usd:.4} ({pnl_pct_final:+.1}%) \
                 | Consecutive losses: {streak}.",
                target_mint = target_mint,
                pnl_usd = pnl_usd,
                pnl_pct_final = pnl_pct_final,
                streak = streak,
            );
        }
    }
}

// ============================================================================
// ON-CHAIN SELL CONFIRMATION VERIFIER
// ============================================================================
//
// WHY THIS EXISTS:
//   `send_transaction_with_config` with `skip_preflight: true` returning Ok(sig)
//   means the RPC *queued* the transaction. It does NOT mean it landed.
//   Validators can still drop it due to:
//     - Blockhash expiry between RPC acceptance and validator processing
//     - Fee too low to be included in the leader's block during congestion
//     - Duplicate transaction detection
//
//   Without this check, the old code declared "sell complete" on RPC acceptance,
//   terminated the watcher, and left the position open with no exit logic running.
//
// FIX:
//   After the primary broadcast is accepted, enter a 30-second polling loop
//   using get_signature_status. Return true only on confirmed on-chain execution.
//   On timeout or on-chain failure: return false so the outer retry loop can
//   re-run the full quote -> swap -> sign -> broadcast pipeline with a fresh
//   blockhash and escalated priority fee.

/// Polls for on-chain confirmation of a sell transaction for up to 30 seconds.
///
/// Returns `true` if the transaction is confirmed with no execution error.
/// Returns `false` on:
///   - Timeout (transaction dropped by validators — blockhash expired, fee too low)
///   - On-chain execution failure (e.g. slippage exceeded, invalid account state)
///   - Persistent RPC errors preventing status reads
async fn verify_transaction_landed(rpc_client: &RpcClient, sig: Signature) -> bool {
    /// How often to poll the RPC for signature status.
    const POLL_INTERVAL: Duration = Duration::from_secs(2);
    /// Maximum number of polls before declaring the transaction dropped.
    /// 15 polls × 2s = 30-second confirmation window.
    const MAX_POLLS: u32 = 15;

    for poll in 1..=MAX_POLLS {
        sleep(POLL_INTERVAL).await;

        match rpc_client.get_signature_status(&sig).await {
            // ✅ Confirmed on-chain with no execution error.
            Ok(Some(Ok(()))) => {
                println!("✅ On-chain confirmation verified (poll {}/{}): {}.", poll, MAX_POLLS, sig);
                return true;
            }
            // ❌ Transaction executed on-chain but the program returned an error
            //    (e.g. slippage tolerance exceeded, token account closed).
            //    No point retrying with the same params — return false so the outer
            //    loop can fetch a fresh quote with updated slippage.
            Ok(Some(Err(tx_err))) => {
                eprintln!(
                    "❌ Sell TX {} landed but failed on-chain: {:?}. Will retry pipeline.",
                    sig, tx_err
                );
                return false;
            }
            // ⏳ Transaction not yet visible — in-flight or still propagating.
            Ok(None) => {
                println!("  [{}/{}] Awaiting on-chain confirmation for {}...", poll, MAX_POLLS, sig);
            }
            // ⚠️  RPC error reading status. Don't abort — keep polling.
            Err(e) => {
                eprintln!(
                    "⚠️  Signature status RPC error (poll {}/{}): {}. Continuing poll...",
                    poll, MAX_POLLS, e
                );
            }
        }
    }

    eprintln!(
        "⚠️  TX {} not confirmed within {}s — likely dropped by validators \
         (expired blockhash or fee too low). Will retry sell pipeline.",
        sig,
        POLL_INTERVAL.as_secs() * MAX_POLLS as u64
    );
    false
}

// ============================================================================
// JUPITER SELL EXECUTION — With Full Pipeline Retry + Fee Escalation
// ============================================================================
//
// PROBLEM (pre-fix):
//   execute_jupiter_sell returned `false` on any failure at any pipeline stage.
//   The VBATS loop handled `false` by simply continuing and waiting the full
//   3-second poll tick before retrying. During a panic dump, that extra wait
//   costs real money.
//
// FIX: Wrap the entire quote->swap->sign->broadcast pipeline in an inner retry
//   loop that fires immediately on failure. Each retry:
//     1. Fetches a FRESH Jupiter quote  (quotes expire ~30s)
//     2. Fetches a FRESH blockhash      (critical: old ones expire fast)
//     3. Escalates priority fee by SELL_FEE_ESCALATION_FACTOR per attempt
//   Returns true on first RPC-accepted broadcast. Returns false only after all
//   MAX_SELL_ATTEMPTS are exhausted.

/// Maximum number of full quote->swap->broadcast pipeline retries for a sell.
const MAX_SELL_ATTEMPTS: u32 = 5;

/// Priority fee multiplier per retry attempt (geometric escalation).
/// Attempt 1: base x1.0 | Attempt 2: x1.5 | Attempt 3: x2.25 | etc.
const SELL_FEE_ESCALATION_FACTOR: f64 = 1.5;

/// Delay between sell pipeline retry attempts.
const SELL_RETRY_DELAY: Duration = Duration::from_secs(2);

async fn execute_jupiter_sell(
    target_mint: &str,
    raw_amount: &str,
    rpc_client: Arc<RpcClient>,
    http_client: Arc<Client>,
    bot_keypair: Arc<Keypair>,
    rpc_url: &str,
) -> bool {
    let wallet_address = bot_keypair.pubkey().to_string();

    // Fetch the base dynamic priority fee once upfront. Escalated per retry.
    let account_keys_for_fee = vec![target_mint.to_string(), WSOL_MINT.to_string()];
    let base_priority_fee = get_dynamic_priority_fee(&http_client, rpc_url, &account_keys_for_fee).await;

    for attempt in 1..=MAX_SELL_ATTEMPTS {
        // Escalate priority fee geometrically on each retry.
        let escalation = SELL_FEE_ESCALATION_FACTOR.powi((attempt - 1) as i32);
        // Saturating cast: if the product overflows f64 or produces +Inf
        // (e.g. due to corrupted base_priority_fee from the API), cap at MAX.
        let scaled_fee = base_priority_fee as f64 * escalation;
        let priority_fee = if scaled_fee.is_finite() && scaled_fee >= 0.0 && scaled_fee < u64::MAX as f64 {
            scaled_fee as u64
        } else {
            MAX_PRIORITY_FEE_LAMPORTS
        }.min(MAX_PRIORITY_FEE_LAMPORTS);

        if attempt > 1 {
            println!(
                "🔁 Sell retry #{}/{} for {} (fee: {} lamports, x{:.2} escalation)...",
                attempt, MAX_SELL_ATTEMPTS, target_mint, priority_fee, escalation
            );
            sleep(SELL_RETRY_DELAY).await;
        }

        // ---- 1. Fresh Jupiter quote ----------------------------------------
        let quote_url = format!(
            "https://quote-api.jup.ag/v6/quote?inputMint={}&outputMint={}&amount={}&slippageBps=2000",
            target_mint, WSOL_MINT, raw_amount
        );
        let quote_res = match http_client.get(&quote_url).send().await {
            Ok(res) => match res.json::<Value>().await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("⚠️  Sell #{}: quote parse error: {}", attempt, e);
                    continue;
                }
            },
            Err(e) => {
                eprintln!("⚠️  Sell #{}: quote network error: {}", attempt, e);
                continue;
            }
        };

        if quote_res.get("error").is_some() {
            println!("⏳ Sell #{}: Jupiter quote error for {} (not indexed yet?). Retrying...",
                attempt, target_mint);
            continue;
        }

        // ---- 2. Build swap payload with escalated fee ----------------------
        let swap_payload = json!({
            "quoteResponse": quote_res,
            "userPublicKey": wallet_address,
            "wrapAndUnwrapSol": true,
            "dynamicComputeUnitLimit": true,
            "prioritizationFeeLamports": priority_fee
        });

        let swap_res = match http_client
            .post("https://quote-api.jup.ag/v6/swap")
            .json(&swap_payload)
            .send()
            .await
        {
            Ok(res) => match res.json::<Value>().await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("⚠️  Sell #{}: swap parse error: {}", attempt, e);
                    continue;
                }
            },
            Err(e) => {
                eprintln!("⚠️  Sell #{}: swap network error: {}", attempt, e);
                continue;
            }
        };

        let swap_tx_base64 = match swap_res.get("swapTransaction").and_then(|t| t.as_str()) {
            Some(tx) => tx,
            None => {
                eprintln!("⚠️  Sell #{}: no swapTransaction in response.", attempt);
                continue;
            }
        };

        // ---- 3. Decode + sign with a FRESH blockhash per retry -------------
        let raw_tx_bytes = match general_purpose::STANDARD.decode(swap_tx_base64) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("⚠️  Sell #{}: base64 decode error: {}", attempt, e);
                continue;
            }
        };

        let mut tx: VersionedTransaction = match bincode::deserialize(&raw_tx_bytes) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("⚠️  Sell #{}: tx deserialization error: {}", attempt, e);
                continue;
            }
        };

        // CRITICAL: fresh blockhash on every retry — the embedded one from
        // Jupiter's response is already stale, and ages faster under congestion.
        let recent_blockhash = match rpc_client.get_latest_blockhash().await {
            Ok(bh) => bh,
            Err(e) => {
                eprintln!("⚠️  Sell #{}: RPC blockhash error: {}", attempt, e);
                continue;
            }
        };
        tx.message.set_recent_blockhash(recent_blockhash);
        let message_bytes = tx.message.serialize();
        tx.signatures[0] = bot_keypair.sign_message(&message_bytes);

        println!(
            "💸 Sell #{}/{} — firing for {} (priority fee: {} lamports)",
            attempt, MAX_SELL_ATTEMPTS, target_mint, priority_fee
        );

        // ---- 4. Broadcast: 1 synchronous + 2 fire-and-forget + on-chain verify ----
        //
        // The primary broadcast is awaited so we capture the signature for
        // on-chain confirmation polling. The two redundant spawns improve landing
        // probability under congestion but are not polled — the primary sig is the
        // source of truth.
        //
        // CRITICAL DISTINCTION:
        //   Ok(sig) from send_transaction_with_config = RPC *queued* the TX.
        //   It does NOT mean the TX landed. We verify with verify_transaction_landed.
        let mut primary_sig: Option<Signature> = None;

        match rpc_client.send_transaction_with_config(
            &tx,
            solana_client::rpc_config::RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(solana_sdk::commitment_config::CommitmentLevel::Processed),
                ..Default::default()
            },
        ).await {
            Ok(sig) => {
                println!(
                    "✅ Sell RPC-accepted for {} | sig: {}. Verifying on-chain...",
                    target_mint, sig
                );
                primary_sig = Some(sig);
            }
            Err(e) => eprintln!("⚠️  Sell broadcast #1 rejected for {}: {}", target_mint, e),
        }

        for bcast in 2..=3_u32 {
            let tx_clone  = tx.clone();
            let rpc_clone = rpc_client.clone();
            let label     = target_mint.to_string();
            tokio::spawn(async move {
                match rpc_clone.send_transaction_with_config(
                    &tx_clone,
                    solana_client::rpc_config::RpcSendTransactionConfig {
                        skip_preflight: true,
                        preflight_commitment: Some(solana_sdk::commitment_config::CommitmentLevel::Processed),
                        ..Default::default()
                    },
                ).await {
                    Ok(sig) => println!("✅ Sell broadcast #{} accepted for {}: {}", bcast, label, sig),
                    Err(e)  => eprintln!("⚠️  Sell broadcast #{} rejected for {}: {}", bcast, label, e),
                }
            });
            sleep(Duration::from_millis(400)).await;
        }

        // On-chain confirmation gate: only return true if the TX actually landed.
        // If it didn't land, fall through to retry the full pipeline with a fresh
        // blockhash and an escalated priority fee.
        if let Some(sig) = primary_sig {
            if verify_transaction_landed(rpc_client.as_ref(), sig).await {
                return true;
            }
            eprintln!(
                "⚠️  Sell #{}/{}: TX queued by RPC but did not land on-chain. \
                 Escalating fee and retrying full pipeline.",
                attempt, MAX_SELL_ATTEMPTS
            );
        }
        // Primary broadcast rejected or TX dropped — retry with escalated fee.
    }

    eprintln!(
        "🚨 SELL FAILED: all {} attempts exhausted for {}. \
         Position may be stranded. MANUAL REVIEW REQUIRED.",
        MAX_SELL_ATTEMPTS, target_mint
    );
    false
}
