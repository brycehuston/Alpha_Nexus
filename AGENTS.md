# Alpha Nexus Operating Guide

## Project mission

Alpha Nexus Generation 1 is an experimental Solana smart-wallet signal collector and copy-trading daemon. The current worktree is configured for dry-run shadow collection: accepted signals are recorded as hypothetical trades while the retained live execution code remains inactive.

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
