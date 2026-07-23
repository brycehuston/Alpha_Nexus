# Objective

Preserve and make Generation 1 operable as a verified, dry-run shadow signal collector while retaining its execution implementation as evidence and fallback; do not build Generation 2 in this repository.

# Definition of Done

- Repository state, runtime mode, and material architecture are verified against code and configuration.
- One concise operating guide and plan are the source of project workflow state.
- Normal development has one concrete, non-capital next task and no undocumented execution-safety claim.

# Current Verified State

- Last verified: 2026-07-22.
- Branch: `test/shadow-pipeline-replay-clean`, created from baseline commit `4703fc3`.
- Commit state: the deterministic replay commit is amended with the validated fail-closed shadow-startup correction; use `git log -1` for its current hash.
- Worktree: clean after the amended commit. The separate original dirty worktree remains untouched.
- Toolchain: Cargo 1.97.1 and rustc 1.97.1; Python 3.13.5. Cargo is installed locally but was not on this shell's `PATH`.
- Build: `cargo check --locked` passes. Dormant execution and exit code produces expected unused-code warnings; Solana client 1.18.26 also has a future-incompatibility notice.
- Tests: `cargo test --locked` passes with five deterministic tests: four startup-policy cases and one shadow-pipeline replay.
- Formatting: repository-wide `cargo fmt --check` still fails from verified pre-existing drift. Changed Rust hunks conform to rustfmt output, and `git diff --check` is clean.
- Runtime mode: startup requires explicit `DRY_RUN=true` (or `1`) before any clients, database initialization, recovery, or listener startup. Missing, invalid, or false values terminate startup. The resulting policy forbids position recovery and capital execution.
- Deployment: `deploy.sh` and `alphanexus-daemon.service` exist; no local runtime or VPS evidence establishes a deployed daemon.
- Data/providers: Redis is hard-coded to local `redis://127.0.0.1/`; the wallet pipeline uses Helius/Dune where configured. PumpPortal WebSocket and external price/alert services are part of the daemon path.
- Primary blocker: none for the fail-closed startup and deterministic replay scope.

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
4. The active listener writes an accepted-buy `ShadowReceipt` to `shadow_ledger.jsonl` rather than calling `execution::execute_pump_buy`.
5. Retained but inactive execution and exit code remains compiled as Generation 1 evidence and fallback.
6. Startup fails closed unless shadow mode is explicitly enabled; position recovery, `monitor_and_sell`, buy execution, and sell execution have no active call path.

# Execution Plan

- [x] Inspect repository, Git history/status, configuration mode, architecture, tests, deployment files, and provider usage.
- [x] Establish this operating baseline without changing bot behaviour.
- [x] Add a deterministic, non-network replay test for the active shadow signal pipeline: verify one eligible buy receipt and representative sell, dust, token-age/market-cap, duplicate-mint, position-cap, circuit-breaker, and bag rejections through the production decision path.
- [x] Enforce fail-closed shadow startup before position recovery, monitor startup, or capital execution.
- [~] Independently review the fail-closed shadow startup and replay-test commit.
- [-] Generation 2 redesign or migration: explicitly deferred pending an independent design and shadow-testing decision.

# Current Action

Independently review the fail-closed shadow startup and replay-test commit.

# Validation Evidence

- `cargo check --locked`: passed; dormant execution/exit warnings and one future-incompatibility notice remain.
- `cargo test --locked`: passed; 5 passed, 0 failed. Startup-policy and replay tests are deterministic and require no external services, credentials, or filesystem state.
- `cargo fmt --check`: fails on the untouched baseline's repository-wide formatting drift; changed replay-test files were reviewed separately and introduce no whitespace errors.
- `python -m py_compile data_pipeline\\backtest_wallets.py data_pipeline\\update_whitelist.py data_pipeline\\backtest_wallets2.py`: passed.
- Daemon startup/replay: not run. It would require external PumpPortal, provider, alert, and local-write isolation not supplied by the repository.

# Blockers

- No implementation blocker remains; repository-wide Rust formatting drift predates this scoped patch.
