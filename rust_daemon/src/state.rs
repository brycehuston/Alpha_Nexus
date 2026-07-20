use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
// FIX #4: Replace std::sync::RwLock with tokio::sync::RwLock.
//
// WHY THIS MATTERS:
//   std::sync::RwLock is a blocking OS primitive. When held (even briefly), it
//   blocks the underlying OS thread backing the Tokio executor. Under sustained
//   WebSocket load, write contention across spawned tasks will stall the entire
//   async runtime, delaying order execution and causing missed entries.
//
//   tokio::sync::RwLock is async-aware: it yields the task (not the thread)
//   while waiting, keeping the executor free to process other work.
//
// CALLSITE CHANGE:
//   try_lock_mint() is now async. All callers must .await it.
use tokio::sync::RwLock;

pub struct BotState {
    // Tracks tokens we are currently trading or have already traded.
    pub traded_mints: RwLock<HashSet<String>>,
    // Tracks consecutive stop-losses to trigger the circuit breaker.
    pub consecutive_losses: AtomicUsize,
}

impl BotState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            traded_mints: RwLock::new(HashSet::new()),
            consecutive_losses: AtomicUsize::new(0),
        })
    }

    /// Atomically checks if we can trade this token, and locks it if we can.
    /// Returns true if the mint was freshly inserted (trade is allowed).
    /// Returns false if it was already present (duplicate — skip).
    pub async fn try_lock_mint(&self, mint: &str) -> bool {
        let mut mints = self.traded_mints.write().await; // yields task, not thread
        mints.insert(mint.to_string())
    }

    /// Checks if the circuit breaker is active (e.g., 3 consecutive losses).
    pub fn is_circuit_breaker_active(&self) -> bool {
        self.consecutive_losses.load(Ordering::SeqCst) >= 3
    }
}
