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
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Debug, PartialEq, Eq)]
pub enum SignalRejection {
    UnsupportedStateChange,
    Sell,
    Dust,
    NotWhitelisted,
    EnrichmentFailed(String),
    TokenAge,
    MarketCap,
    Bag,
    CircuitBreaker,
    PositionCap,
    DuplicateMint,
}

#[derive(Clone, Copy)]
pub struct ShadowDecisionTiming {
    pub now_ms: u64,
}

#[derive(Clone, Copy)]
pub struct ShadowEnrichment {
    pub created_at_ms: u64,
    pub raw_market_cap: f64,
    pub history_buys: i32,
}

pub async fn process_shadow_event<Enrich, EnrichFuture>(
    event: &PumpTradeEvent,
    is_whitelisted: bool,
    bot_state: &Arc<BotState>,
    timing: ShadowDecisionTiming,
    enrich: Enrich,
    receipt_sink: &tokio::sync::mpsc::UnboundedSender<ShadowReceipt>,
) -> Result<(), SignalRejection>
where
    Enrich: FnOnce() -> EnrichFuture,
    EnrichFuture: Future<Output = Result<ShadowEnrichment, String>>,
{
    let evaluation_started = std::time::Instant::now();
    bot_state
        .prune_expired_shadow_positions(timing.now_ms)
        .await;

    if event.tx_type != "buy" && event.tx_type != "sell" {
        return Err(SignalRejection::UnsupportedStateChange);
    }

    if event.tx_type == "sell" {
        return Err(SignalRejection::Sell);
    }

    if event.sol_amount < MIN_WHALE_TRADE_SOL {
        return Err(SignalRejection::Dust);
    }

    if !is_whitelisted {
        return Err(SignalRejection::NotWhitelisted);
    }

    let enrichment = enrich().await.map_err(SignalRejection::EnrichmentFailed)?;

    if timing.now_ms.saturating_sub(enrichment.created_at_ms) < 60_000 {
        return Err(SignalRejection::TokenAge);
    }

    if enrichment.raw_market_cap < 10_000.0 || enrichment.raw_market_cap > 2_000_000.0 {
        return Err(SignalRejection::MarketCap);
    }

    if enrichment.history_buys >= 5 {
        return Err(SignalRejection::Bag);
    }

    if bot_state.is_circuit_breaker_active() {
        return Err(SignalRejection::CircuitBreaker);
    }

    let position_permit = bot_state
        .try_acquire_position()
        .ok_or(SignalRejection::PositionCap)?;

    if !bot_state.try_lock_mint(&event.mint).await {
        return Err(SignalRejection::DuplicateMint);
    }

    if !bot_state
        .retain_shadow_position(&event.mint, timing.now_ms, position_permit)
        .await
    {
        return Err(SignalRejection::DuplicateMint);
    }

    let receipt = ShadowReceipt {
        signature: event.signature.clone(),
        observed_at_ms: timing.now_ms as u128,
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
        decision_latency_ms: evaluation_started.elapsed().as_secs_f64() * 1000.0,
        entry_price_simulated: enrichment.raw_market_cap,
        estimated_priority_fee_sol: 0.000125,
        reason: "ALL_GATES_PASSED".to_string(),
    };

    let _ = receipt_sink.send(receipt);
    Ok(())
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
    let wallet_set: HashSet<String> = wallets.iter().cloned().collect();

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

        println!(
            "🟢 WS Connected. Subscribing to {} elite wallets...",
            wallets.len()
        );
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
                                        let is_whitelisted =
                                            wallet_set.contains(&event.trader_public_key);
                                        let enrichment_http = http_client.clone();
                                        let enrichment_mint = event.mint.clone();
                                        let enrichment_wallet = event.trader_public_key.clone();
                                        let decision = process_shadow_event(
                                            &event,
                                            is_whitelisted,
                                            &bot_state,
                                            ShadowDecisionTiming {
                                                now_ms: ShadowLogger::now_ms() as u64,
                                            },
                                            move || async move {
                                                let metadata =
                                                    telegram::fetch_token_metadata_strict(
                                                        &enrichment_http,
                                                        &enrichment_mint,
                                                    )
                                                    .await?;
                                                let history = db::try_get_whale_history(
                                                    &enrichment_wallet,
                                                    &enrichment_mint,
                                                )?;
                                                Ok(ShadowEnrichment {
                                                    created_at_ms: metadata.created_at_ms,
                                                    raw_market_cap: metadata.raw_mc,
                                                    history_buys: history.buys,
                                                })
                                            },
                                            &receipt_tx,
                                        )
                                        .await;

                                        let should_report_trade = matches!(
                                            &decision,
                                            Ok(())
                                                | Err(SignalRejection::Sell)
                                                | Err(SignalRejection::TokenAge)
                                                | Err(SignalRejection::MarketCap)
                                                | Err(SignalRejection::Bag)
                                                | Err(SignalRejection::CircuitBreaker)
                                                | Err(SignalRejection::PositionCap)
                                                | Err(SignalRejection::DuplicateMint)
                                        );

                                        if should_report_trade && (is_buy || is_sell) {
                                            let direction = if is_buy { "BUY" } else { "SELL" };
                                            println!(
                                                "🚨 Smart Money Alert: {} {} {} (Size: {} SOL)",
                                                event.trader_public_key,
                                                direction,
                                                event.mint,
                                                event.sol_amount
                                            );

                                            let status = if is_buy {
                                                "Shadow decision"
                                            } else {
                                                "Monitor only (SELL)"
                                            };
                                            db::log_trade_telemetry(
                                                &event.trader_public_key,
                                                &event.mint,
                                                direction,
                                                event.sol_amount,
                                                0.0,
                                                status,
                                            );

                                            if is_buy {
                                                if let (Some(token), Some(chat_id)) =
                                                    (&telegram_bot_token, &telegram_chat_id)
                                                {
                                                let net_change = event.token_amount;
                                                let size_fmt =
                                                    format!("{:.2} SOL", event.sol_amount);
                                                let t_mint = event.mint.clone();
                                                let t_wallet = event.trader_public_key.clone();
                                                let t_sig = event.signature.clone();
                                                let t_status = status.to_string();
                                                let http_clone = http_client.clone();
                                                let t_token = token.clone();
                                                let t_chat = chat_id.clone();

                                                tokio::spawn(async move {
                                                    telegram::send_telegram_alert(
                                                        &http_clone,
                                                        &t_token,
                                                        &t_chat,
                                                        &t_mint,
                                                        net_change,
                                                        &t_wallet,
                                                        &t_sig,
                                                        &t_status,
                                                        &size_fmt,
                                                    )
                                                    .await;
                                                });
                                                }
                                            }
                                        }

                                        match &decision {
                                            Err(SignalRejection::TokenAge) => {
                                                println!(
                                                    "⚠️  Token age filter rejected {}.",
                                                    event.mint
                                                );
                                            }
                                            Err(SignalRejection::MarketCap) => {
                                                println!(
                                                    "⚠️  Market-cap filter rejected {}.",
                                                    event.mint
                                                );
                                            }
                                            Err(SignalRejection::Bag) => {
                                                println!(
                                                    "⚠️  Bag-history filter rejected {} for {}.",
                                                    event.mint, event.trader_public_key
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
                                            Err(SignalRejection::EnrichmentFailed(error)) => {
                                                eprintln!(
                                                    "⚠️  Shadow enrichment failed closed for {}: {}",
                                                    event.mint, error
                                                );
                                            }
                                            _ => {}
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
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    fn accepted_enrichment() -> ShadowEnrichment {
        ShadowEnrichment {
            created_at_ms: NOW_MS - 120_000,
            raw_market_cap: 50_000.0,
            history_buys: 0,
        }
    }

    async fn run_decision(
        event: &PumpTradeEvent,
        bot_state: &Arc<BotState>,
        is_whitelisted: bool,
        now_ms: u64,
        enrichment: Result<ShadowEnrichment, String>,
        enrichment_calls: Arc<AtomicUsize>,
    ) -> (Result<(), SignalRejection>, Vec<ShadowReceipt>) {
        let (receipt_tx, mut receipt_rx) = tokio::sync::mpsc::unbounded_channel();
        let result = process_shadow_event(
            event,
            is_whitelisted,
            bot_state,
            ShadowDecisionTiming { now_ms },
            move || {
                enrichment_calls.fetch_add(1, Ordering::SeqCst);
                async move { enrichment }
            },
            &receipt_tx,
        )
        .await;
        let mut receipts = Vec::new();

        while let Ok(receipt) = receipt_rx.try_recv() {
            receipts.push(receipt);
        }

        (result, receipts)
    }

    async fn assert_rejected(
        event: &PumpTradeEvent,
        bot_state: &Arc<BotState>,
        is_whitelisted: bool,
        now_ms: u64,
        enrichment: Result<ShadowEnrichment, String>,
        expected: SignalRejection,
        expected_enrichment_calls: usize,
    ) {
        let open_positions_before = bot_state.open_position_count();
        let shadow_positions_before = bot_state.shadow_position_count().await;
        let enrichment_calls = Arc::new(AtomicUsize::new(0));
        let (result, receipts) = run_decision(
            event,
            bot_state,
            is_whitelisted,
            now_ms,
            enrichment,
            enrichment_calls.clone(),
        )
        .await;

        assert_eq!(result, Err(expected));
        assert_eq!(receipts.len(), 0, "rejected signals must not emit receipts");
        assert_eq!(
            enrichment_calls.load(Ordering::SeqCst),
            expected_enrichment_calls,
            "enrichment call count"
        );
        assert_eq!(
            bot_state.open_position_count(),
            open_positions_before,
            "a rejected signal must not retain a position permit"
        );
        assert_eq!(
            bot_state.shadow_position_count().await,
            shadow_positions_before,
            "a rejected signal must not add a shadow position"
        );
    }

    #[tokio::test]
    async fn deterministic_shadow_pipeline_replay() {
        let accepted_event = event("buy", 1.0, "mint_accepted");
        let accepted_state = BotState::new();
        let accepted_enrichment_calls = Arc::new(AtomicUsize::new(0));
        let (result, receipts) = run_decision(
            &accepted_event,
            &accepted_state,
            true,
            NOW_MS,
            Ok(accepted_enrichment()),
            accepted_enrichment_calls.clone(),
        )
        .await;

        assert_eq!(result, Ok(()));
        assert_eq!(
            accepted_enrichment_calls.load(Ordering::SeqCst),
            1,
            "accepted signals enrich exactly once"
        );
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
        assert_eq!(accepted_state.open_position_count(), 1);
        assert_eq!(accepted_state.shadow_position_count().await, 1);
        assert!(accepted_state.has_shadow_position("mint_accepted").await);

        // Cheap deterministic gates must return before enrichment or state mutation.
        let unsupported_state = BotState::new();
        assert_rejected(
            &event("unknown", 1.0, "mint_unsupported"),
            &unsupported_state,
            true,
            NOW_MS,
            Ok(accepted_enrichment()),
            SignalRejection::UnsupportedStateChange,
            0,
        )
        .await;

        let sell_state = BotState::new();
        assert_rejected(
            &event("sell", 0.1, "mint_sell"),
            &sell_state,
            true,
            NOW_MS,
            Ok(accepted_enrichment()),
            SignalRejection::Sell,
            0,
        )
        .await;

        let dust_state = BotState::new();
        assert_rejected(
            &event("buy", 0.1, "mint_dust"),
            &dust_state,
            true,
            NOW_MS,
            Ok(accepted_enrichment()),
            SignalRejection::Dust,
            0,
        )
        .await;

        let unlisted_state = BotState::new();
        assert_rejected(
            &event("buy", 1.0, "mint_unlisted"),
            &unlisted_state,
            false,
            NOW_MS,
            Ok(accepted_enrichment()),
            SignalRejection::NotWhitelisted,
            0,
        )
        .await;

        let token_age_state = BotState::new();
        let token_age_enrichment = ShadowEnrichment {
            created_at_ms: NOW_MS - 30_000,
            ..accepted_enrichment()
        };
        assert_rejected(
            &event("buy", 1.0, "mint_token_age"),
            &token_age_state,
            true,
            NOW_MS,
            Ok(token_age_enrichment),
            SignalRejection::TokenAge,
            1,
        )
        .await;

        let market_cap_state = BotState::new();
        let market_cap_enrichment = ShadowEnrichment {
            raw_market_cap: 3_000_000.0,
            ..accepted_enrichment()
        };
        assert_rejected(
            &event("buy", 1.0, "mint_market_cap"),
            &market_cap_state,
            true,
            NOW_MS,
            Ok(market_cap_enrichment),
            SignalRejection::MarketCap,
            1,
        )
        .await;

        let bag_state = BotState::new();
        let bag_enrichment = ShadowEnrichment {
            history_buys: 5,
            ..accepted_enrichment()
        };
        assert_rejected(
            &event("buy", 1.0, "mint_bag"),
            &bag_state,
            true,
            NOW_MS,
            Ok(bag_enrichment),
            SignalRejection::Bag,
            1,
        )
        .await;

        let enrichment_failure_state = BotState::new();
        assert_rejected(
            &event("buy", 1.0, "mint_enrichment_failure"),
            &enrichment_failure_state,
            true,
            NOW_MS,
            Err("metadata unavailable".to_string()),
            SignalRejection::EnrichmentFailed("metadata unavailable".to_string()),
            1,
        )
        .await;
        assert!(
            !enrichment_failure_state
                .has_traded_mint("mint_enrichment_failure")
                .await,
            "failed enrichment must not mutate the duplicate-mint guard"
        );

        // Build duplicate state only through the production decision entry point.
        let duplicate_state = BotState::new();
        let duplicate_accept_calls = Arc::new(AtomicUsize::new(0));
        let duplicate_event = event("buy", 1.0, "mint_duplicate");
        let (duplicate_accept_result, duplicate_accept_receipts) = run_decision(
            &duplicate_event,
            &duplicate_state,
            true,
            NOW_MS,
            Ok(accepted_enrichment()),
            duplicate_accept_calls,
        )
        .await;
        assert_eq!(duplicate_accept_result, Ok(()));
        assert_eq!(duplicate_accept_receipts.len(), 1);
        assert_rejected(
            &duplicate_event,
            &duplicate_state,
            true,
            NOW_MS,
            Ok(accepted_enrichment()),
            SignalRejection::DuplicateMint,
            1,
        )
        .await;

        let circuit_breaker_state = BotState::new();
        circuit_breaker_state
            .consecutive_losses
            .store(3, Ordering::SeqCst);
        assert_rejected(
            &event("buy", 1.0, "mint_circuit_breaker"),
            &circuit_breaker_state,
            true,
            NOW_MS,
            Ok(accepted_enrichment()),
            SignalRejection::CircuitBreaker,
            1,
        )
        .await;

        // Fill capacity only through ordinary accepted production decisions.
        let position_cap_state = BotState::new();
        for index in 0..crate::state::MAX_CONCURRENT_POSITIONS {
            let cap_event = event("buy", 1.0, &format!("mint_cap_{index}"));
            let enrichment_calls = Arc::new(AtomicUsize::new(0));
            let (cap_result, cap_receipts) = run_decision(
                &cap_event,
                &position_cap_state,
                true,
                NOW_MS,
                Ok(accepted_enrichment()),
                enrichment_calls.clone(),
            )
            .await;
            assert_eq!(cap_result, Ok(()));
            assert_eq!(cap_receipts.len(), 1);
            assert_eq!(enrichment_calls.load(Ordering::SeqCst), 1);
        }
        assert_eq!(
            position_cap_state.open_position_count(),
            crate::state::MAX_CONCURRENT_POSITIONS
        );
        assert_eq!(
            position_cap_state.shadow_position_count().await,
            crate::state::MAX_CONCURRENT_POSITIONS
        );

        assert_rejected(
            &event("buy", 1.0, "mint_over_cap"),
            &position_cap_state,
            true,
            NOW_MS,
            Ok(accepted_enrichment()),
            SignalRejection::PositionCap,
            1,
        )
        .await;

        // Explicit release restores one slot without clearing duplicate history.
        assert!(
            position_cap_state
                .release_shadow_position("mint_cap_0")
                .await
        );
        assert_eq!(
            position_cap_state.open_position_count(),
            crate::state::MAX_CONCURRENT_POSITIONS - 1
        );
        assert_eq!(
            position_cap_state.shadow_position_count().await,
            crate::state::MAX_CONCURRENT_POSITIONS - 1
        );

        let after_release_event = event("buy", 1.0, "mint_after_release");
        let after_release_calls = Arc::new(AtomicUsize::new(0));
        let (after_release_result, after_release_receipts) = run_decision(
            &after_release_event,
            &position_cap_state,
            true,
            NOW_MS,
            Ok(accepted_enrichment()),
            after_release_calls,
        )
        .await;
        assert_eq!(after_release_result, Ok(()));
        assert_eq!(after_release_receipts.len(), 1);
        assert_eq!(
            position_cap_state.open_position_count(),
            crate::state::MAX_CONCURRENT_POSITIONS
        );

        // Event-time expiry releases all old positions without sleeping.
        let after_expiry_event = event("buy", 1.0, "mint_after_expiry");
        let after_expiry_calls = Arc::new(AtomicUsize::new(0));
        let (after_expiry_result, after_expiry_receipts) = run_decision(
            &after_expiry_event,
            &position_cap_state,
            true,
            NOW_MS + crate::state::SHADOW_POSITION_TTL_MS,
            Ok(accepted_enrichment()),
            after_expiry_calls,
        )
        .await;
        assert_eq!(after_expiry_result, Ok(()));
        assert_eq!(after_expiry_receipts.len(), 1);
        assert_eq!(position_cap_state.open_position_count(), 1);
        assert_eq!(position_cap_state.shadow_position_count().await, 1);
        assert!(
            position_cap_state
                .has_shadow_position("mint_after_expiry")
                .await
        );
        assert!(!position_cap_state.has_shadow_position("mint_cap_1").await);
    }
}
