# Helius Pilot Safety

Task: AN-HELIUS-SAFE-01 | Checkpoint: C02 | State: READY_FOR_REVIEW | Branch: fix/helius-pilot-budget-safety | Commit: cb07b6a | Resume: AN-HELIUS-SAFE-01.C02@cb07b6a

# Objective

Add the smallest bounded, cached, resumable acquisition path needed for the
ten-wallet Helius pilot without making any network request during implementation.

# Scope

- `PLAN.md`
- `data_pipeline/backtest_wallets.py`
- `data_pipeline/test_backtest_wallets.py`

# Constraints

- Maximum 10 unique wallets, 10 pages per wallet, and 120 total attempts.
- Every attempt is estimated at 100 credits; local ceiling is 12,000 credits.
- Cache each successful raw page immediately and resume from valid cache.
- Abort globally on fatal provider, budget, cursor, cache, or manifest failure.
- Only complete histories may enter unchanged canonical scoring.
- Paid network requests authorized during implementation: zero.

# Execution Plan

- [x] Verify the requested branch base and discard only the rejected three-file patch.
- [x] Implement and validate the minimal safe pilot patch.
- [~] Review the final three-file diff; the paid pilot remains unauthorized.

# Current Action

Review the minimal uncommitted patch and validation evidence.

# Validation Evidence

- Branch: `fix/helius-pilot-budget-safety`; HEAD: `cb07b6a`.
- Rejected changes were restored only from the three authorized paths.
- Worktree was clean and nothing was staged before reimplementation.
- `python -m unittest data_pipeline.test_backtest_wallets -v`: passed; 18 tests.
- Both scoped Python files pass `py_compile`.
- `git diff --check`: passed.
- Network requests and the real pilot: not run.

# Blockers

- None.
