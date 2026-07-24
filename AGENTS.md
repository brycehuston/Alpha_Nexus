# Alpha Nexus Operating Guide

## Project mission

Alpha Nexus Generation 1 is an experimental Solana smart-wallet signal collector and copy-trading daemon. The current worktree is configured for dry-run shadow collection: accepted signals are recorded as hypothetical trades while the retained live execution code remains inactive.

## FRUX NAV Task Continuity Protocol

FRUX NAV is used only for tasks that span multiple sessions, branches,
worktrees, agents, or high-consequence implementation. Do not use it for
small single-session changes.

When FRUX NAV is active, `PLAN.md` must contain one concise continuity line:

`Task: <TASK_ID> | Checkpoint: <CXX> | State: <STATE> | Branch: <BRANCH> | Commit: <SHORT_COMMIT> | Resume: <TASK_ID>.<CXX>@<SHORT_COMMIT>`

Rules:

- Read `AGENTS.md` and `PLAN.md`, then verify repository, branch, commit,
  worktree, and Current Action before changing files.
- The recorded Task ID and Current Action define the authorized scope.
- Continuity metadata does not authorize commits, pushes, deployments,
  service changes, or broader work.
- Keep exactly one active `[~]` item and exactly one Current Action.
- Advance a checkpoint only after verified progress that materially changes
  the resumable state.
- Do not increment checkpoints for discussion, planning, failed commands,
  or unverified edits.
- If the recorded continuity state conflicts with Git or runtime evidence,
  stop and report the mismatch instead of silently rewriting it.
- Record concise handoff evidence in the existing `Validation Evidence`
  section of `PLAN.md`. Do not create a separate ops-ledger file.
- Remove or archive FRUX NAV metadata when the task is completed.

## Required workflow

Before modifying code:

1. Read `AGENTS.md` and `PLAN.md`.
2. Inspect Git status and the relevant implementation.
3. Verify every material claim in `PLAN.md` against code, configuration, and current validation evidence.
4. Correct stale plan state before proceeding.

During implementation:

1. Work only on the single `[~]` item in `PLAN.md`.
2. Use the shortest complete implementation path; expand scope only when correctness requires it.
3. Do not perform unrelated refactors, introduce placeholders, or change architecture without authorization.
4. Preserve Generation 1 as evidence, collector, and fallback. Do not start a Generation 2 rewrite here.

Before finishing:

1. Run proportional validation and inspect the complete diff.
2. Confirm Git status and stage only intended paths; never use `git add .`.
3. Update `PLAN.md`, mark completed work `[x]`, and leave exactly one next item `[~]`.
4. Report behavioural evidence and unresolved blockers explicitly.

## Safety rules

- Never enable live trading implicitly; verify execution mode before any runtime check.
- Never insert, print, commit, or expose private keys, API keys, or other secrets.
- Never replace missing production data with mock values in a production path.
- Never bypass circuit breakers, double-buy guards, position-state protections, or buy/sell confirmation checks without explicit authorization.
- Compilation alone is not evidence that execution is safe.
- Preserve a clear paper/dry-run mode.

## Paid API and Quota Cost Control

Paid API credits, RPC quota, model usage, compute credits, and rate limits are
project capital. Optimize for the cheapest reliable path that preserves data
quality and correctness.

Rules:

- Check local caches and existing artifacts before making any network request.
- Reuse previously downloaded responses instead of purchasing the same data
  again.
- Inspect metadata, schemas, or a minimal sample before downloading full
  results.
- Request only the required columns, rows, wallets, time ranges, and pages.
- Validate and deduplicate inputs locally before sending them to a paid API.
- Save every paid response incrementally with source and request provenance.
- Never execute, refresh, or rerun a paid Dune query without explicit user
  approval.
- Never override a provider credit ceiling, spending limit, or safety limit
  without explicit user approval.
- Stop and report before continuing after HTTP 402, repeated 429 responses,
  unexpected cost, or unexpectedly large result estimates.
- Multiple agents must analyze the same cached dataset. Do not allow separate
  agents to independently repeat paid downloads or backtests.
- Use cheap screening before expensive enrichment. Reserve Helius enhanced
  transaction history and similar high-cost calls for candidates that survive
  local screening.
- Before a material paid operation, report:
  - service
  - data or action requested
  - reason it is necessary
  - cached or cheaper alternative
  - expected requests, rows, or wallets
  - known or possible credit impact
  - whether explicit approval is required
- Prefer zero-cost local analysis when it can answer the question reliably.
- Cost optimization must not justify fabricated data, skipped validation, or
  weakened safety checks.

## Git rules

- Do not commit directly to `main` unless explicitly authorized; use a scoped branch.
- Preserve unrelated dirty or untracked work and stage only intended files.
- Show diff, validation, and status before a commit or push.
- Do not rewrite history or force-push without explicit authorization.

## Validation commands

Run Rust commands from `rust_daemon` with a Rust toolchain available on `PATH`:

```powershell
cargo fmt --check
cargo check --locked
cargo test --locked
```

Run the supported Python syntax check from the repository root:

```powershell
python -m py_compile data_pipeline\backtest_wallets.py data_pipeline\update_whitelist.py
```

Do not run the daemon as a baseline smoke test unless its external providers, Telegram alerts, local database writes, and execution mode are explicitly isolated and verified safe.

## Completion standard

A task is complete only when the requested scope is implemented, relevant validation passes, the diff is inspected, runtime or behavioural evidence is supplied when applicable, and `PLAN.md` reflects verified reality.
