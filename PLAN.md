# Objective

Preserve and make Generation 1 operable as a verified, dry-run shadow signal collector while retaining its execution implementation as evidence and fallback; do not build Generation 2 in this repository.

# Definition of Done

- Repository state, runtime mode, and material architecture are verified against code and configuration.
- One concise operating guide and plan are the source of project workflow state.
- Normal development has one concrete, non-capital next task and no undocumented execution-safety claim.

# Current Verified State

- Last verified: 2026-07-23.
- Branch: `docs/api-budget-policy`, created from `main` at `2cba4c1`.
- Commit state: HEAD is `2cba4c1`; the previous parser task is complete and merged.
- Worktree: clean before this documentation task.
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
- [x] Push the approved baseline branch and open a pull request.
- [x] Correct and validate wallet backtester UTF-8 output and fail-closed native-balance trade parsing.
- [x] Independently review the wallet backtester parser correction and offline validation evidence.
- [x] Add and validate the paid API cost-control policy.
- [~] Resume cached Dune candidate validation after this policy branch is merged.
- [-] Generation 2 redesign or migration: explicitly deferred pending an independent design and shadow-testing decision.

# Current Action

After merging this documentation branch, validate the existing cached Dune
results locally and determine the minimum additional paid requests required.

# Budget Constraints

- Reuse the five existing cached Dune JSON responses.
- Do not redownload an existing result page.
- Do not execute or refresh any Dune query without explicit approval.
- Do not override Dune credit limits.
- Make only minimal metadata or missing-page requests after validating the
  trader-wallet column.
- Do not call Helius until Dune candidates are validated, deduplicated, and
  screened locally.
- Checkpoint every paid response immediately.
- Paid executions currently authorized: zero.

# Validation Evidence

- Paid API policy documentation: `git diff --check` passed; no paid API calls made.
- Paid API policy documentation review: passed.
- `python -m unittest data_pipeline.test_backtest_wallets -v`: passed; 10 offline parser and stdout tests passed.
- Supplied `test_tx.json`: parsed as a sell of `18,969,867.095602` tokens for `0.800328221` SOL.
- Fail-closed parser matrix: passed for missing, null, non-list, malformed, unmatched, and duplicate account data; missing, null, non-numeric, boolean, and zero native balance changes; ambiguous transfers; direction disagreement; and fee-only balance loss.
- UTF-8 configuration: passed with redirected `TextIOWrapper`, `StringIO`, and a replacement stream that rejects reconfiguration.
- `python -m py_compile data_pipeline\\backtest_wallets.py`: passed.
- `git diff --check`: passed.
- Independent Claude review: **YES — approve as-is**.
- Network requests and live backtest: not run.
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
