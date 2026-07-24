# PumpSwap Backtester Parser

Task: AN-PUMPSWAP-PARSER-01 | Checkpoint: C02 | State: BLOCKED | Branch: fix/pumpswap-backtester-parser | Commit: 5447b7c | Resume: AN-PUMPSWAP-PARSER-01.C02@5447b7c

# Objective

Extend the existing wallet backtester to recognize and safely parse PumpSwap
trades using only the completed ten-wallet Helius pilot cache.

# Scope

- `PLAN.md`
- `data_pipeline/backtest_wallets.py`
- `data_pipeline/test_backtest_wallets.py`

# Constraints

- Preserve existing Pump.fun parsing and canonical scoring behavior.
- Reuse native-balance and token-transfer direction accounting when valid.
- Reject ambiguous multi-token, multi-leg, or direction-conflicting activity.
- Do not add a helper module, decoder framework, or permanent replay machinery.
- Network and paid requests authorized: zero.

# Execution Plan

- [x] Inspect cached PumpSwap evidence, implement the narrow parser patch, add focused tests, and run the offline ten-wallet replay.
- [~] Obtain cached direct PumpSwap evidence with both a reliable target-owned token delta and usable native SOL change before extending the parser.

# Current Action

Await direct PumpSwap evidence that satisfies unchanged native-SOL, dust, fee, ownership, and direction requirements.

# Validation Evidence

- Base verified: `main` at `5447b7c`; worktree clean.
- Created branch `fix/pumpswap-backtester-parser`.
- Cached evidence: 83 records match the unchanged Pump.fun gate; 324 contain
  PumpSwap, including six routed records.
- The unchanged accounting parser accepted four routed PumpSwap sells and
  rejected all 318 direct `PUMP_AMM` records without inference.
- `python -m unittest data_pipeline.test_backtest_wallets -v`: passed; 22 tests.
- Both scoped Python files pass `py_compile`.
- `git diff --check`: passed.
- Offline replay: 11 analyzed unique-mint trades; 0 of 10 wallets qualified.
- Replay artifacts written to `AN-HELIUS-PILOT-01-PUMPSWAP-REPLAY`.
- No network, paid request, cache mutation, manifest mutation, stage, or commit.
- C02 review rejected because all 318 cached direct `PUMP_AMM` transactions
  remained unparsed; target-owned token balance evidence requires inspection.
- Complete offline inspection: 10 complete wallets, 10 cached pages, 577
  transactions; no missing page or network request.
- Recognition: 318 direct `PUMP_AMM`; 6 routed PumpSwap-program transactions.
- Target-owned balance classification accepted 0 direct and 4 routed records.
- Direct rejection partition: 109 zero native balance, 138 native-SOL dust,
  6 fee-only native loss, and 65 usable-native records with no attributable
  non-WSOL target token delta.
- Routed rejection partition: 2 records with multiple target-owned non-WSOL
  mints; the other 4 remain safely parseable sells.

# Blockers

- The cache contains no direct `PUMP_AMM` record with exactly one reliable
  target-owned non-WSOL token delta, a non-dust/non-fee-only native SOL change,
  and agreeing directions. Parsing a direct record would require inference
  from WSOL or pool/intermediary transfers, which is outside the authorized
  accounting rules.
