# Objective

Preserve and make Generation 1 operable as a verified, dry-run shadow signal collector while retaining its execution implementation as evidence and fallback; do not build Generation 2 in this repository.

# Definition of Done

- Repository state, runtime mode, and material architecture are verified against code and configuration.
- One concise operating guide and plan are the source of project workflow state.
- Normal development has one concrete, non-capital next task and no undocumented execution-safety claim.

# Current Verified State

- Last verified: 2026-07-22 15:32 PDT (UTC-07:00).
- Branch: `chore/alpha-nexus-operating-baseline`, created from the checked-out `v0-shadow-mode` worktree.
- Commit: `6798762e616203a2d56c699b0b03f2e8689a7160` (`feat: add Helius backtester, fix dry-run .env loading, professional README`).
- Worktree: dirty before this baseline. Pre-existing modified paths are `command.txt`, `data_pipeline/backtest_wallets.py`, `data_pipeline/candidates.txt`, `rust_daemon/src/main.rs`, and `rust_daemon/src/websocket.rs`; pre-existing untracked work includes the shadow logger, alternate pipeline data/scripts, and local artifacts. Preserve them unless separately scoped.
- Toolchain: Cargo 1.97.1 and rustc 1.97.1; Python 3.13.5. Cargo is installed locally but was not on this shell's `PATH`.
- Build: `cargo check --locked` passes. It reports nine warnings because the shadow worktree leaves the compiled live-buy path and related listener inputs unused; Solana client 1.18.26 also has a future-incompatibility notice.
- Tests: `cargo test --locked` passes with zero tests. Python syntax compilation passes for the primary and alternate backtest scripts.
- Formatting: `cargo fmt --check` fails across existing Rust formatting. `git diff --check` finds one trailing whitespace issue in the pre-existing `websocket.rs` change. Neither is fixed by this baseline.
- Runtime mode: `.env` verifies `DRY_RUN=true`; secret-bearing variables are set but were not read or displayed. The active WebSocket gate path records shadow receipts instead of invoking buy execution.
- Deployment: `deploy.sh` and `alphanexus-daemon.service` exist; no local runtime or VPS evidence establishes a deployed daemon.
- Data/providers: Redis is hard-coded to local `redis://127.0.0.1/`; the wallet pipeline uses Helius/Dune where configured. PumpPortal WebSocket and external price/alert services are part of the daemon path.
- Primary blocker: the active shadow signal pipeline has no deterministic, non-network behavioural test or replay fixture.

# Non-Negotiable Constraints

- Never enable live execution implicitly or expose secrets.
- Do not bypass circuit, duplicate-mint, position, or transaction-confirmation protection.
- Preserve the active dry-run/shadow mode until explicitly changed and validated.
- Do not modify bot source, runtime defaults, dependencies, CI, or deployment during this baseline task.

# Material Decisions

- Generation 1 remains intact as the evidence base, collector, and fallback. Valuable Generation 1 work may be stabilized in scoped changes.
- Any Generation 2 system must be independently built and shadow-tested before promotion. Its eventual relationship to this PumpPortal-focused daemon remains undecided.
- The checked-out shadow-mode worktree is current implementation evidence. Its dirty source/data changes are not included in this baseline documentation change.

# Architecture Snapshot

1. Python data-pipeline scripts select wallets and can seed Redis set `smart_herd_wallets`.
2. The Tokio daemon loads configuration, initializes SQLite telemetry, gets the Redis wallet whitelist, and subscribes to PumpPortal `subscribeAccountTrade`.
3. `websocket.rs` applies direction, whale-size, token-age, market-cap, history, circuit-breaker, position-cap, and duplicate-mint gates; it also logs telemetry and may send Telegram alerts.
4. The active dirty worktree writes an accepted-buy `ShadowReceipt` to `shadow_ledger.jsonl` rather than calling `execution::execute_pump_buy`.
5. Retained but inactive execution code can request PumpPortal buy transactions; retained exit code uses Jupiter sell transactions, RPC confirmation, SQLite open-position recovery, and `BotState` circuit/position controls.
6. The live execution path is compiled but unreferenced in the active worktree. Its runtime safety is therefore unverified by this baseline.

# Execution Plan

- [x] Inspect repository, Git history/status, configuration mode, architecture, tests, deployment files, and provider usage.
- [x] Establish this operating baseline without changing bot behaviour.
- [~] Add a deterministic, non-network replay test for the active shadow signal pipeline: verify one eligible buy receipt and representative sell, dust, token-age/market-cap, duplicate-mint, position-cap, and circuit-breaker rejections.
- [ ] Reassess live execution only after the replay test supplies repeatable gate evidence.
- [-] Generation 2 redesign or migration: explicitly deferred pending an independent design and shadow-testing decision.

# Current Action

Implement the fixture-driven, non-network shadow signal replay test and keep live execution disabled.

# Validation Evidence

- `cargo check --locked`: passed; nine dormant-live-path warnings and one future-incompatibility notice.
- `cargo test --locked`: passed; 0 passed, 0 failed because no tests are defined.
- `cargo fmt --check`: failed due to pre-existing repository-wide formatting drift.
- `python -m py_compile data_pipeline\\backtest_wallets.py data_pipeline\\update_whitelist.py data_pipeline\\backtest_wallets2.py`: passed.
- Daemon startup/replay: not run. It would require external PumpPortal, provider, alert, and local-write isolation not supplied by the repository.

# Blockers

- No deterministic fixture or replay boundary exercises the active shadow pipeline; compilation cannot prove signal-gate or runtime behaviour.
