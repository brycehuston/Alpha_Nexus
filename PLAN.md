# Objective

Preserve and make Generation 1 operable as a verified, dry-run shadow signal collector while retaining its execution implementation as evidence and fallback; do not build Generation 2 in this repository.

# Definition of Done

- Repository state, runtime mode, and material architecture are verified against code and configuration.
- One concise operating guide and plan are the source of project workflow state.
- Normal development has one concrete, non-capital next task and no undocumented execution-safety claim.

# Current Verified State

- Last verified: 2026-07-23.
- Branch: `test/shadow-pipeline-replay-clean`, created from baseline commit `4703fc3`.
- Commit state: HEAD remains `d041ef7`; the approved corrected production/replay entry path and shadow-position lifecycle are ready to commit. Canonical Generation 1 baseline approval is complete after this commit.
- Worktree: modified only in `PLAN.md`, `README.md`, `rust_daemon/src/db.rs`, `rust_daemon/src/state.rs`, `rust_daemon/src/telegram.rs`, and `rust_daemon/src/websocket.rs`. The separate original dirty worktree remains untouched.
- Toolchain: Cargo 1.97.1 and rustc 1.97.1; Python 3.13.5. Cargo is installed locally but was not on this shell's `PATH`.
- Build: `cargo check --locked` passes. Dormant execution and exit code produces expected unused-code warnings; Solana client 1.18.26 also has a future-incompatibility notice.
- Tests: `cargo test --locked` passes with five deterministic tests: four startup-policy cases and one expanded production-path shadow replay.
- Formatting: targeted `rustfmt --edition 2021 --check` passes for all four changed Rust files. Repository-wide formatting drift outside this scope remains pre-existing.
- Runtime mode: startup requires explicit `DRY_RUN=true` (or `1`) before any clients, database initialization, recovery, or listener startup. Missing, invalid, or false values terminate startup. The resulting policy forbids position recovery and capital execution.
- Deployment: `deploy.sh` and `alphanexus-daemon.service` exist; no local runtime or VPS evidence establishes a deployed daemon.
- Data/providers: Redis is hard-coded to local `redis://127.0.0.1/`; the wallet pipeline uses Helius/Dune where configured. PumpPortal WebSocket and external price/alert services are part of the daemon path.
- Primary blocker: none. Independent review passed with no blocking findings.

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
3. Production and replay both call `process_shadow_event`. It applies cheap deterministic gates before invoking injected enrichment, fails closed on enrichment errors, and then applies token-age, market-cap, history, circuit-breaker, position-cap, and duplicate-mint gates.
4. An accepted BUY retains its semaphore permit in mint-keyed shadow-position state for four hours or until explicit release, then emits one `ShadowReceipt` to the active listener's `shadow_ledger.jsonl` sink.
5. Retained but inactive execution and exit code remains compiled as Generation 1 evidence and fallback.
6. Startup fails closed unless shadow mode is explicitly enabled; position recovery, `monitor_and_sell`, buy execution, and sell execution have no active call path.

# Execution Plan

- [x] Inspect repository, Git history/status, configuration mode, architecture, tests, deployment files, and provider usage.
- [x] Establish this operating baseline without changing bot behaviour.
- [x] Add a deterministic, non-network replay test for the active shadow signal pipeline: verify one eligible buy receipt and representative sell, dust, token-age/market-cap, duplicate-mint, position-cap, circuit-breaker, and bag rejections through the production decision path.
- [x] Enforce fail-closed shadow startup before position recovery, monitor startup, or capital execution.
- [x] Independently review the fail-closed shadow startup and replay-test commit; rejected because the production/replay entry paths differ and accepted permits are dropped immediately.
- [x] Correct the production/replay entry path, fail-closed lazy enrichment, and retained four-hour shadow-position lifecycle; validate cap fill, explicit release, and deterministic expiry through ordinary accepted events.
- [x] Independently review the corrected production/replay entry path and shadow position lifecycle; passed with no blocking findings.
- [~] Push the approved baseline branch and open a pull request.
- [-] Generation 2 redesign or migration: explicitly deferred pending an independent design and shadow-testing decision.

# Current Action

Push the approved baseline branch and open a pull request.

# Validation Evidence

- `rustfmt --edition 2021 --check src/state.rs src/websocket.rs src/telegram.rs src/db.rs`: passed.
- Independent review: passed; blocking findings: none.
- `cargo check --locked`: passed; dormant execution/exit warnings and one future-incompatibility notice remain.
- `cargo test --locked`: passed; 5 passed, 0 failed. The replay exercises the production entry function with deterministic in-memory enrichment and no external services, credentials, database, or filesystem state.
- Unified production/replay decision path: verified.
- Four-hour retained shadow-position lifecycle: verified.
- Explicit and TTL capacity release: verified.
- Live execution, recovery, and `monitor_and_sell`: unreachable.
- `python -m py_compile data_pipeline\\backtest_wallets.py data_pipeline\\update_whitelist.py data_pipeline\\backtest_wallets2.py`: passed.
- Daemon startup/replay: not run. It would require external PumpPortal, provider, alert, and local-write isolation not supplied by the repository.

# Blockers

- No known implementation blocker remains in the corrected working tree. The canonical Generation 1 baseline approval is complete after this commit; repository-wide formatting drift outside the changed files predates this patch.
