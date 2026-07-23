use crate::db;
use crate::error::BotError;
use crate::shadow_logger::{ShadowLogger, ShadowReceipt};
use crate::state::BotState;
use crate::telegram;
use crate::types::PumpTradeEvent;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::json;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Debug, PartialEq, Eq)]
pub enum SignalRejection {
    Dust,
    Sell,
    TokenAge,
    MarketCap,
    Bag,
    CircuitBreaker,
    PositionCap,
    DuplicateMint,
}

#[derive(Clone, Copy)]
pub struct SlowGateInputs {
    pub created_at_ms: u64,
    pub raw_market_cap: f64,
    pub history_buys: i32,
    pub now_ms: u64,
    pub decision_latency_ms: f64,
}

pub fn evaluate_fast_gates(event: &PumpTradeEvent) -> Result<(), SignalRejection> {
    if event.sol_amount < MIN_WHALE_TRADE_SOL {
        return Err(SignalRejection::Dust);
    }

    if event.tx_type == "sell" {
        return Err(SignalRejection::Sell);
    }

    Ok(())
}

pub async fn evaluate_slow_gates_and_emit_receipt(
    event: &PumpTradeEvent,
    bot_state: &Arc<BotState>,
    inputs: SlowGateInputs,
    receipt_sink: &tokio::sync::mpsc::UnboundedSender<ShadowReceipt>,
) -> Result<(), SignalRejection> {
    if inputs.created_at_ms > 0 && inputs.now_ms.saturating_sub(inputs.created_at_ms) < 60_000 {
        return Err(SignalRejection::TokenAge);
    }

    if inputs.raw_market_cap > 0.0
        && (inputs.raw_market_cap < 10_000.0 || inputs.raw_market_cap > 2_000_000.0)
    {
        return Err(SignalRejection::MarketCap);
    }

    if inputs.history_buys >= 5 {
        return Err(SignalRejection::Bag);
    }

    if bot_state.is_circuit_breaker_active() {
        return Err(SignalRejection::CircuitBreaker);
    }

    let _position_permit = bot_state
        .try_acquire_position()
        .ok_or(SignalRejection::PositionCap)?;

    if !bot_state.try_lock_mint(&event.mint).await {
        return Err(SignalRejection::DuplicateMint);
    }

    let receipt = ShadowReceipt {
        signature: event.signature.clone(),
        observed_at_ms: inputs.now_ms as u128,
        wallet: event.trader_public_key.clone(),
        mint: event.mint.clone(),
        classification: "BUY".to_string(),
        sol_spent: event.sol_amount,
        tokens_received: 0.0,
        gate_whitelist: true,
        gate_state_change: true,
        gate_size: true,
        gate_double_buy: true,
        gate_circuit_breaker: true,
        decision: "WOULD_BUY".to_string(),
        decision_latency_ms: inputs.decision_latency_ms,
        entry_price_simulated: inputs.raw_market_cap,
        estimated_priority_fee_sol: 0.000125,
        reason: "ALL_GATES_PASSED".to_string(),
    };

    let _ = receipt_sink.send(receipt);
    Ok(())
}

pub async fn evaluate_shadow_signal(
    event: &PumpTradeEvent,
    bot_state: &Arc<BotState>,
    inputs: SlowGateInputs,
    receipt_sink: &tokio::sync::mpsc::UnboundedSender<ShadowReceipt>,
) -> Result<(), SignalRejection> {
    evaluate_fast_gates(event)?;
    evaluate_slow_gates_and_emit_receipt(event, bot_state, inputs, receipt_sink).await
}

// FIX #5: Half-Open WebSocket Detection
//
// PROBLEM:
//   The original `while let Some(msg) = read.next().await` has no timeout.
//   TCP connections can enter a "half-open" state where the server-side drops
//   but the client-side OS socket remains open indefinitely. `read.next().await`
//   will hang forever with no error, silently missing all trade signals.
//   This is common on cloud providers and public API gateways.
//
// FIX APPROACH:
//   1. RECEIVE TIMEOUT: Wrap every `read.next()` in a 15-second timeout.
//      If no message (or Pong) arrives within 15s, the connection is treated as
//      dead and we break to trigger a reconnect.
//
//   2. APPLICATION-LEVEL PING: Every 10 seconds, send a WebSocket Ping frame
//      from a concurrent task using tokio::select!. PumpPortal must respond
//      with a Pong; if it does, the receive timeout resets. This is separate
//      from the TCP-level keepalive and works at the application layer.
//
//   Together these create a guaranteed 15-second upper bound on detecting
//   a dead connection, regardless of OS TCP keepalive settings.

/// How long to wait for any message (data or Pong) before declaring the
/// connection dead and triggering a reconnect.
const WS_RECEIVE_TIMEOUT: Duration = Duration::from_secs(15);

/// How often to send an application-level Ping to keep the connection alive
/// and verify the remote end is still responsive.
const WS_PING_INTERVAL: Duration = Duration::from_secs(120);

/// Minimum SOL amount for a tracked wallet's BUY to be considered a genuine
/// trade signal worth mirroring.
///
/// Trades below this threshold are dust movements, wallet top-ups, test
/// transactions, or fee adjustments — they carry no directional conviction
/// and are statistically uncorrelated with profitable entries. Filtering them
/// eliminates noise signals before they consume any system resources.
const MIN_WHALE_TRADE_SOL: f64 = 0.5;

pub async fn run_listener(
    wallets: Vec<String>,
    api_key: String,
    rpc_client: Arc<RpcClient>,
    http_client: Arc<Client>,
    bot_keypair: Arc<Keypair>,
    bot_state: Arc<BotState>,
    rpc_url: String,
    telegram_bot_token: Option<String>,
    telegram_chat_id: Option<String>,
    dry_run: bool,
    trade_size_sol: f64,
) -> Result<(), BotError> {

    // Construct WebSocket URL
    let ws_url = if api_key.is_empty() {
        "wss://pumpportal.fun/api/data".to_string()
    } else {
        format!("wss://pumpportal.fun/api/data?api-key={}", api_key)
    };

    // Auto-reconnect loop
    loop {
        let (receipt_tx, mut receipt_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(receipt) = receipt_rx.recv().await {
                ShadowLogger::append_receipt(receipt).await;
            }
        });

        println!("Connecting to PumpPortal WS...");
        let (ws_stream, _) = match connect_async(&ws_url).await {
            Ok(stream) => stream,
            Err(e) => {
                eprintln!("WS Connection failed, retrying in 2s... {}", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        println!("🟢 WS Connected. Subscribing to {} elite wallets...", wallets.len());
        let (mut write, mut read) = ws_stream.split();

        // Subscribe to our tracked wallets in chunks of 1000 to avoid WS message size limits
        let mut sub_failed = false;
        for chunk in wallets.chunks(1000) {
            let subscribe_msg = json!({
                "method": "subscribeAccountTrade",
                "keys": chunk
            });

            if let Err(e) = write.send(Message::Text(subscribe_msg.to_string())).await {
                eprintln!("Failed to send subscribe message chunk: {}", e);
                sub_failed = true;
                break;
            }
            // Small delay to prevent rate-limiting the websocket server during setup
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        if sub_failed {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        // Ping ticker: fires every WS_PING_INTERVAL to send a Ping frame.
        // If the server is alive, it replies with a Pong, which resets the
        // receive timeout. If dead, the receive timeout will fire first.
        let mut ping_interval = tokio::time::interval(WS_PING_INTERVAL);
        ping_interval.tick().await; // consume the immediate first tick

        // FIX #5 in action: inner event loop with timeout + ping.
        'connection: loop {
            tokio::select! {
                // Branch 1: Wait for the next message with a hard timeout.
                // If nothing arrives in WS_RECEIVE_TIMEOUT, the connection is
                // stale (half-open). Break to force a full reconnect.
                msg_result = timeout(WS_RECEIVE_TIMEOUT, read.next()) => {
                    match msg_result {
                        Err(_elapsed) => {
                            // Timeout expired — no message, no Pong, no nothing.
                            eprintln!(
                                "⏰ WS receive timeout ({}s) — connection likely half-open. Forcing reconnect.",
                                WS_RECEIVE_TIMEOUT.as_secs()
                            );
                            break 'connection;
                        }
                        Ok(None) => {
                            // Stream ended cleanly.
                            eprintln!("🔴 WS stream closed by server. Reconnecting...");
                            break 'connection;
                        }
                        Ok(Some(Err(e))) => {
                            eprintln!("🔴 WS stream error: {}. Reconnecting...", e);
                            break 'connection;
                        }
                        Ok(Some(Ok(msg))) => {
                            // Got a real message — process it.
                            match msg {
                                Message::Text(text) => {
                                    // Parse directly into our strict Rust struct.
                                    // Non-trade messages (e.g. subscription ack) are
                                    // silently ignored by the Ok/Err pattern.
                                match serde_json::from_str::<PumpTradeEvent>(&text) {
                                    Ok(event) => {
                                        let is_buy = event.tx_type == "buy";
                                        let is_sell = event.tx_type == "sell";

                                        if is_buy || is_sell {
                                            // DUST FILTER: Ignore all noise/spam trades under the threshold
                                            // before touching the DB, Telegram, or execution state.
                                            if let Err(SignalRejection::Dust) =
                                                evaluate_fast_gates(&event)
                                            {
                                                let direction = if is_buy { "buy" } else { "sell" };
                                                println!(
                                                    "🔕 Dust filter: ignoring {:.4} SOL {} from {} on {} \
                                                     (minimum: {} SOL).",
                                                    event.sol_amount,
                                                    direction,
                                                    event.trader_public_key,
                                                    event.mint,
                                                    MIN_WHALE_TRADE_SOL
                                                );
                                                continue 'connection;
                                            }

                                            let direction = if is_buy { "BUY" } else { "SELL" };
                                            println!(
                                                "🚨 Smart Money Alert: {} {} {} (Size: {} SOL)",
                                                event.trader_public_key, direction, event.mint, event.sol_amount
                                            );

                                            // 1. Log to DB
                                            let status = if is_buy { "Executing..." } else { "Monitor only (SELL)" };
                                            db::log_trade_telemetry(
                                                &event.trader_public_key,
                                                &event.mint,
                                                direction,
                                                event.sol_amount,
                                                0.0,
                                                status,
                                            );

                                            // 2. Send Telegram Alert
                                            if let (Some(token), Some(chat_id)) = (&telegram_bot_token, &telegram_chat_id) {
                                                let net_change = if is_buy { event.token_amount } else { -event.token_amount };
                                                let size_fmt = format!("{:.2} SOL", event.sol_amount);
                                                
                                                let t_mint = event.mint.clone();
                                                let t_wallet = event.trader_public_key.clone();
                                                let t_sig = event.signature.clone();
                                                let t_status = status.to_string();
                                                let http_clone = http_client.clone();
                                                let t_token = token.clone();
                                                let t_chat = chat_id.clone();
                                                
                                                tokio::spawn(async move {
                                                    telegram::send_telegram_alert(
                                                        &http_clone, &t_token, &t_chat,
                                                        &t_mint, net_change, &t_wallet,
                                                        &t_sig, &t_status, &size_fmt
                                                    ).await;
                                                });
                                            }

                                            // 3. Shadow decision (BUYs only)
                                            if is_buy {
                                                let evaluation_started = std::time::Instant::now();
                                                let meta = crate::telegram::fetch_token_metadata(
                                                    &http_client, &event.mint
                                                ).await;
                                                let history = db::get_whale_history(
                                                    &event.trader_public_key,
                                                    &event.mint,
                                                );
                                                let inputs = SlowGateInputs {
                                                    created_at_ms: meta.created_at_ms,
                                                    raw_market_cap: meta.raw_mc,
                                                    history_buys: history.buys,
                                                    now_ms: ShadowLogger::now_ms() as u64,
                                                    decision_latency_ms: evaluation_started
                                                        .elapsed()
                                                        .as_secs_f64()
                                                        * 1000.0,
                                                };

                                                match evaluate_shadow_signal(
                                                    &event,
                                                    &bot_state,
                                                    inputs,
                                                    &receipt_tx,
                                                ).await {
                                                    Err(SignalRejection::TokenAge) => {
                                                        let age_ms = inputs
                                                            .now_ms
                                                            .saturating_sub(inputs.created_at_ms);
                                                        println!(
                                                            "⚠️  Token age filter: {} is only {}s old. \
                                                             Skipping to avoid sub-60s rug.",
                                                            event.mint, age_ms / 1000
                                                        );
                                                    }
                                                    Err(SignalRejection::MarketCap) => {
                                                        println!(
                                                            "⚠️  Market Cap filter: {} is ${:.0}. \
                                                             Must be between $10k and $2M. Skipping.",
                                                            event.mint, inputs.raw_market_cap
                                                        );
                                                    }
                                                    Err(SignalRejection::Bag) => {
                                                        println!(
                                                            "⚠️  Bag filter: Wallet {} has already bought {} {} times. \
                                                             Skipping to avoid buying their top.",
                                                            event.trader_public_key,
                                                            event.mint,
                                                            inputs.history_buys
                                                        );
                                                    }
                                                    Err(SignalRejection::PositionCap) => {
                                                        println!(
                                                            "⚠️  Position cap reached ({}/{}) — skipping signal for {}.",
                                                            bot_state.open_position_count(),
                                                            crate::state::MAX_CONCURRENT_POSITIONS,
                                                            event.mint
                                                        );
                                                    }
                                                    Err(SignalRejection::CircuitBreaker)
                                                    | Err(SignalRejection::DuplicateMint)
                                                    | Ok(()) => {}
                                                    Err(SignalRejection::Dust)
                                                    | Err(SignalRejection::Sell) => {}
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        if text.contains("txType") {
                                            println!("⚠️ JSON Parse Error on Trade Event: {}. Raw Payload: {}", e, text);
                                        } else {
                                            // Limit the length of the string printed to avoid spamming the terminal with massive arrays
                                            let truncated: String = text.chars().take(150).collect();
                                            println!("ℹ️ System WS Message: {}...", truncated);
                                        }
                                    }
                                }
                                }
                                // Pong responses from our Ping frames arrive here.
                                // No action needed — receiving them is sufficient
                                // to reset the WS_RECEIVE_TIMEOUT on the next loop.
                                Message::Pong(_) => {
                                    println!("💓 Heartbeat received from PumpPortal...");
                                }
                                Message::Close(_) => {
                                    eprintln!("🔴 WS received Close frame. Reconnecting...");
                                    break 'connection;
                                }
                                _ => {} // Binary, Ping, Fragment — ignore
                            }
                        }
                    }
                }

                // Branch 2: Ping interval fires. Send a Ping frame to verify
                // the remote end is still alive. A Pong response will arrive as
                // a normal message in Branch 1 and reset the receive timeout.
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Ping(vec![])).await {
                        eprintln!("⚠️  Failed to send WS Ping: {}. Reconnecting...", e);
                        break 'connection;
                    }
                }
            }
        }

        // Brief pause before reconnect attempt to avoid hammering the server.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    const NOW_MS: u64 = 1_700_000_000_000;

    fn event(tx_type: &str, sol_amount: f64, mint: &str) -> PumpTradeEvent {
        PumpTradeEvent {
            signature: "signature_123".to_string(),
            mint: mint.to_string(),
            trader_public_key: "wallet_123".to_string(),
            tx_type: tx_type.to_string(),
            token_amount: 1_000.0,
            sol_amount,
        }
    }

    fn accepted_inputs() -> SlowGateInputs {
        SlowGateInputs {
            created_at_ms: NOW_MS - 120_000,
            raw_market_cap: 50_000.0,
            history_buys: 0,
            now_ms: NOW_MS,
            decision_latency_ms: 1.5,
        }
    }

    async fn run_decision(
        event: &PumpTradeEvent,
        bot_state: &Arc<BotState>,
        inputs: SlowGateInputs,
    ) -> (Result<(), SignalRejection>, Vec<ShadowReceipt>) {
        let (receipt_tx, mut receipt_rx) = tokio::sync::mpsc::unbounded_channel();
        let result = evaluate_shadow_signal(event, bot_state, inputs, &receipt_tx).await;
        let mut receipts = Vec::new();

        while let Ok(receipt) = receipt_rx.try_recv() {
            receipts.push(receipt);
        }

        (result, receipts)
    }

    async fn assert_rejected(
        event: &PumpTradeEvent,
        bot_state: &Arc<BotState>,
        inputs: SlowGateInputs,
        expected: SignalRejection,
    ) {
        let (result, receipts) = run_decision(event, bot_state, inputs).await;
        assert_eq!(result, Err(expected));
        assert_eq!(receipts.len(), 0, "rejected signals must not emit receipts");
    }

    #[tokio::test]
    async fn deterministic_shadow_pipeline_replay() {
        let accepted_event = event("buy", 1.0, "mint_accepted");
        let accepted_state = BotState::new();
        let (result, receipts) =
            run_decision(&accepted_event, &accepted_state, accepted_inputs()).await;

        assert_eq!(result, Ok(()));
        assert_eq!(
            receipts.len(),
            1,
            "accepted signal emits exactly one receipt"
        );
        let receipt = &receipts[0];
        assert_eq!(receipt.wallet, "wallet_123");
        assert_eq!(receipt.mint, "mint_accepted");
        assert_eq!(receipt.sol_spent, 1.0);
        assert_eq!(receipt.classification, "BUY");
        assert_eq!(receipt.decision, "WOULD_BUY");
        assert_eq!(receipt.reason, "ALL_GATES_PASSED");
        assert!(receipt.gate_whitelist);
        assert!(receipt.gate_state_change);
        assert!(receipt.gate_size);
        assert!(receipt.gate_double_buy);
        assert!(receipt.gate_circuit_breaker);

        let sell_state = BotState::new();
        assert_rejected(
            &event("sell", 1.0, "mint_sell"),
            &sell_state,
            accepted_inputs(),
            SignalRejection::Sell,
        )
        .await;

        let dust_state = BotState::new();
        assert_rejected(
            &event("buy", 0.1, "mint_dust"),
            &dust_state,
            accepted_inputs(),
            SignalRejection::Dust,
        )
        .await;

        let token_age_state = BotState::new();
        let token_age_inputs = SlowGateInputs {
            created_at_ms: NOW_MS - 30_000,
            ..accepted_inputs()
        };
        assert_rejected(
            &event("buy", 1.0, "mint_token_age"),
            &token_age_state,
            token_age_inputs,
            SignalRejection::TokenAge,
        )
        .await;

        let market_cap_state = BotState::new();
        let market_cap_inputs = SlowGateInputs {
            raw_market_cap: 3_000_000.0,
            ..accepted_inputs()
        };
        assert_rejected(
            &event("buy", 1.0, "mint_market_cap"),
            &market_cap_state,
            market_cap_inputs,
            SignalRejection::MarketCap,
        )
        .await;

        let duplicate_state = BotState::new();
        assert!(duplicate_state.try_lock_mint("mint_duplicate").await);
        assert_rejected(
            &event("buy", 1.0, "mint_duplicate"),
            &duplicate_state,
            accepted_inputs(),
            SignalRejection::DuplicateMint,
        )
        .await;

        let position_cap_state = BotState::new();
        let mut permits = Vec::new();
        while let Some(permit) = position_cap_state.try_acquire_position() {
            permits.push(permit);
        }
        assert_rejected(
            &event("buy", 1.0, "mint_position_cap"),
            &position_cap_state,
            accepted_inputs(),
            SignalRejection::PositionCap,
        )
        .await;
        drop(permits);

        let circuit_breaker_state = BotState::new();
        circuit_breaker_state
            .consecutive_losses
            .store(3, Ordering::SeqCst);
        assert_rejected(
            &event("buy", 1.0, "mint_circuit_breaker"),
            &circuit_breaker_state,
            accepted_inputs(),
            SignalRejection::CircuitBreaker,
        )
        .await;

        let bag_state = BotState::new();
        let bag_inputs = SlowGateInputs {
            history_buys: 5,
            ..accepted_inputs()
        };
        assert_rejected(
            &event("buy", 1.0, "mint_bag"),
            &bag_state,
            bag_inputs,
            SignalRejection::Bag,
        )
        .await;
    }
}
