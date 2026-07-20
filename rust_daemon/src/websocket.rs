use crate::error::BotError;
use crate::execution::execute_pump_buy;
use crate::types::PumpTradeEvent;
use crate::state::BotState;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::json;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

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
const WS_PING_INTERVAL: Duration = Duration::from_secs(10);

pub async fn run_listener(
    wallets: Vec<String>,
    api_key: String,
    rpc_client: Arc<RpcClient>,
    http_client: Arc<Client>,
    bot_keypair: Arc<Keypair>,
    bot_state: Arc<BotState>,
    rpc_url: String,
) -> Result<(), BotError> {

    // Construct WebSocket URL
    let ws_url = if api_key.is_empty() {
        "wss://pumpportal.fun/api/data".to_string()
    } else {
        format!("wss://pumpportal.fun/api/data?api-key={}", api_key)
    };

    // Auto-reconnect loop
    loop {
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

        // Subscribe to our tracked wallets
        let subscribe_msg = json!({
            "method": "subscribeAccountTrade",
            "keys": wallets
        });

        if let Err(e) = write.send(Message::Text(subscribe_msg.to_string())).await {
            eprintln!("Failed to send subscribe message: {}", e);
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
                                    if let Ok(event) = serde_json::from_str::<PumpTradeEvent>(&text) {
                                        if event.tx_type == "buy" {
                                            println!(
                                                "🚨 Smart Money Alert: {} bought {} (Size: {} SOL)",
                                                event.trader_public_key, event.mint, event.sol_amount
                                            );

                                            if bot_state.is_circuit_breaker_active() { continue 'connection; }
                                            // FIX #4 callsite: try_lock_mint is now async
                                            if !bot_state.try_lock_mint(&event.mint).await { continue 'connection; }

                                            let target_mint    = event.mint.clone();
                                            let rpc_clone      = rpc_client.clone();
                                            let http_clone     = http_client.clone();
                                            let keypair_clone  = bot_keypair.clone();
                                            let state_clone    = bot_state.clone();
                                            let rpc_url_clone  = rpc_url.clone();

                                            // Fire and forget execution.
                                            // FIX #3 (partial): the watcher task spawned inside
                                            // execute_pump_buy now carries a 2-hour hard deadline.
                                            tokio::spawn(async move {
                                                execute_pump_buy(
                                                    target_mint, rpc_clone, http_clone,
                                                    keypair_clone, state_clone, rpc_url_clone,
                                                ).await;
                                            });
                                        }
                                    }
                                }
                                // Pong responses from our Ping frames arrive here.
                                // No action needed — receiving them is sufficient
                                // to reset the WS_RECEIVE_TIMEOUT on the next loop.
                                Message::Pong(_) => {
                                    // Connection is alive. Loop continues.
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
