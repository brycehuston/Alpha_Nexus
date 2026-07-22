"""
backtest_wallets.py — Alpha Nexus Wallet Backtester (Helius-Powered)
=====================================================================
Replaces Dune Analytics entirely. Uses your existing Helius API key to:
  1. Pull full pump.fun swap history for each candidate wallet
  2. Calculate win rate, trade count, net PnL (SOL), and avg return
  3. Score and rank wallets against Alpha Nexus criteria
  4. Optionally seed Redis with the top wallets that pass

Usage:
  1. Add wallet addresses (one per line) to: data_pipeline/candidates.txt
  2. Run: python data_pipeline/backtest_wallets.py
  3. Results are saved to: data_pipeline/out/backtest_results.csv

Environment variables (loaded from .env automatically):
  HELIUS_API_KEY  — your Helius API key
  REDIS_SEED      — set to "true" to auto-seed Redis with passing wallets
"""

import os
import sys
import time
import csv
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env ──────────────────────────────────────────────────────────────────
load_dotenv(Path(__file__).parent.parent / ".env")

HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
if not HELIUS_API_KEY:
    print("ERROR: HELIUS_API_KEY not set in .env")
    sys.exit(1)

REDIS_SEED = os.environ.get("REDIS_SEED", "false").lower() == "true"

# ── Constants ──────────────────────────────────────────────────────────────────
HELIUS_TX_URL   = f"https://api.helius.xyz/v0/addresses/{{address}}/transactions"
WSOL_MINT       = "So11111111111111111111111111111111111111112"
TX_LIMIT        = 100          # max per page (Helius max)
MAX_PAGES       = 10           # cap at 1000 txs per wallet to avoid rate limit burn
RATE_LIMIT_WAIT = 0.25         # seconds between requests (4 req/s, well under free limit)

# ── Alpha Nexus filtering criteria (mirrors update_whitelist.py) ───────────────
MIN_WIN_RATE    = 0.65
MIN_TRADES      = 20           # lowered from 50 — small sample from GMGN is expected
MIN_NET_SOL     = 1.0          # minimum net profit in SOL to qualify
TOP_N           = 50           # max wallets to seed into Redis

# ── File paths ─────────────────────────────────────────────────────────────────
BASE_DIR        = Path(__file__).parent
CANDIDATES_FILE = BASE_DIR / "candidates.txt"
OUT_DIR         = BASE_DIR / "out"
RESULTS_CSV     = OUT_DIR / "backtest_results.csv"


def fetch_transactions(address: str) -> list:
    """
    Fetches all SWAP transactions for a wallet via Helius Enhanced TX API.
    Handles pagination automatically up to MAX_PAGES.
    """
    all_txs = []
    params = {
        "api-key": HELIUS_API_KEY,
        "type":    "SWAP",
        "limit":   TX_LIMIT,
    }

    url = HELIUS_TX_URL.format(address=address)
    for page in range(MAX_PAGES):
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                print(f"    ⏳ Rate limited — waiting 5s...")
                time.sleep(5)
                resp = requests.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                print(f"    ⚠️  Helius returned {resp.status_code} for {address[:8]}...")
                break

            txs = resp.json()
            if not txs:
                break  # no more pages

            all_txs.extend(txs)

            # Paginate using the last tx signature as the cursor
            params["before"] = txs[-1]["signature"]
            time.sleep(RATE_LIMIT_WAIT)

            if len(txs) < TX_LIMIT:
                break  # last page

        except Exception as e:
            print(f"    ❌ Request error for {address[:8]}...: {e}")
            break

    return all_txs


def is_pump_fun(tx: dict) -> bool:
    """Returns True if the transaction originated from pump.fun."""
    source = tx.get("source", "")
    # Also check instructions for pump.fun program ID
    PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
    if source == "PUMP_FUN":
        return True
    for ix in tx.get("instructions", []):
        if ix.get("programId") == PUMP_FUN_PROGRAM:
            return True
    return False


def parse_trade(tx: dict, wallet: str) -> dict | None:
    """
    Parses a single SWAP transaction and returns a trade dict:
      { mint, direction: 'buy'|'sell', sol_amount, token_amount }
    Returns None if the transaction can't be parsed cleanly.
    """
    token_transfers = tx.get("tokenTransfers", [])
    native_transfers = tx.get("nativeTransfers", [])

    # Calculate net SOL movement for this wallet
    sol_out = sum(t["amount"] for t in native_transfers
                  if t.get("fromUserAccount") == wallet) / 1e9
    sol_in  = sum(t["amount"] for t in native_transfers
                  if t.get("toUserAccount") == wallet) / 1e9
    net_sol = sol_in - sol_out  # positive = received SOL (sell), negative = spent SOL (buy)

    # Find the non-WSOL token mint involved
    mint = None
    token_amount = 0.0
    for t in token_transfers:
        if t.get("mint") == WSOL_MINT:
            continue
        if t.get("fromUserAccount") == wallet or t.get("toUserAccount") == wallet:
            mint = t.get("mint")
            token_amount = t.get("tokenAmount", 0)
            break

    if not mint or abs(net_sol) < 0.001:
        return None  # dust or unparseable

    direction = "sell" if net_sol > 0 else "buy"
    sol_amount = abs(net_sol)

    return {
        "mint":         mint,
        "direction":    direction,
        "sol_amount":   sol_amount,
        "token_amount": token_amount,
        "signature":    tx.get("signature", ""),
        "timestamp":    tx.get("timestamp", 0),
    }


def score_wallet(address: str) -> dict:
    """
    Fetches and scores a single wallet's pump.fun trading history.
    Returns a metrics dict.
    """
    print(f"  Fetching txs for {address[:12]}...")
    txs = fetch_transactions(address)

    pump_txs = [tx for tx in txs if is_pump_fun(tx)]
    print(f"    Found {len(txs)} total swaps, {len(pump_txs)} pump.fun")

    trades: dict[str, dict] = {}  # mint → {buy_sol, sell_sol, complete}

    for tx in pump_txs:
        trade = parse_trade(tx, address)
        if not trade:
            continue

        mint = trade["mint"]
        if mint not in trades:
            trades[mint] = {"buy_sol": 0.0, "sell_sol": 0.0}

        if trade["direction"] == "buy":
            trades[mint]["buy_sol"] += trade["sol_amount"]
        else:
            trades[mint]["sell_sol"] += trade["sol_amount"]

    if not trades:
        return {"wallet": address, "total_trades": 0, "win_rate": 0.0,
                "net_sol": 0.0, "avg_return_pct": 0.0, "passes": False}

    wins = 0
    total_buy_sol = 0.0
    total_sell_sol = 0.0
    returns = []

    for mint, data in trades.items():
        buy  = data["buy_sol"]
        sell = data["sell_sol"]
        total_buy_sol  += buy
        total_sell_sol += sell

        if buy > 0:
            ret = (sell - buy) / buy
            returns.append(ret)
            if sell > buy:
                wins += 1

    total_trades   = len(trades)
    win_rate       = wins / total_trades if total_trades > 0 else 0.0
    net_sol        = total_sell_sol - total_buy_sol
    avg_return_pct = (sum(returns) / len(returns) * 100) if returns else 0.0

    passes = (
        win_rate   >= MIN_WIN_RATE and
        total_trades >= MIN_TRADES  and
        net_sol    >= MIN_NET_SOL
    )

    return {
        "wallet":          address,
        "total_trades":    total_trades,
        "win_rate":        round(win_rate, 4),
        "net_sol":         round(net_sol, 4),
        "avg_return_pct":  round(avg_return_pct, 2),
        "passes":          passes,
    }


def main():
    print("=" * 60)
    print("  Alpha Nexus — Helius Wallet Backtester")
    print("=" * 60)

    # ── Load candidate wallets ─────────────────────────────────────────────────
    if not CANDIDATES_FILE.exists():
        print(f"\n❌ {CANDIDATES_FILE} not found.")
        print("Create it with one wallet address per line and re-run.")
        sys.exit(1)

    candidates = [
        line.strip() for line in CANDIDATES_FILE.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]

    if not candidates:
        print("❌ candidates.txt is empty. Add wallet addresses and re-run.")
        sys.exit(1)

    print(f"\n📋 Loaded {len(candidates)} candidate wallets.")
    print(f"   Criteria: win_rate≥{MIN_WIN_RATE} | trades≥{MIN_TRADES} | net_sol≥{MIN_NET_SOL}")
    print()

    # ── Score each wallet ──────────────────────────────────────────────────────
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    results = []

    for i, wallet in enumerate(candidates, 1):
        print(f"[{i}/{len(candidates)}] Scoring {wallet[:12]}...")
        try:
            score = score_wallet(wallet)
            results.append(score)
            status = "✅ PASSES" if score["passes"] else "❌ fails"
            print(f"    {status} | trades={score['total_trades']} "
                  f"| win={score['win_rate']:.0%} "
                  f"| net={score['net_sol']:+.2f} SOL "
                  f"| avg_return={score['avg_return_pct']:+.1f}%")
        except Exception as e:
            print(f"    💥 Error: {e}")
        print()

    # ── Sort and save results ──────────────────────────────────────────────────
    results.sort(key=lambda r: (r["passes"], r["net_sol"]), reverse=True)

    with open(RESULTS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    passing = [r for r in results if r["passes"]]

    print("=" * 60)
    print(f"  RESULTS: {len(passing)}/{len(results)} wallets pass criteria")
    print("=" * 60)
    for r in results[:10]:
        tag = "✅" if r["passes"] else "❌"
        print(f"  {tag} {r['wallet'][:16]}... "
              f"win={r['win_rate']:.0%} trades={r['total_trades']} "
              f"net={r['net_sol']:+.2f} SOL")

    print(f"\n💾 Full results saved to: {RESULTS_CSV}")

    # ── Optionally seed Redis ──────────────────────────────────────────────────
    if REDIS_SEED and passing:
        try:
            import redis as redis_lib
            top = [r["wallet"] for r in passing[:TOP_N]]
            r = redis_lib.Redis(host="localhost", port=6379, db=0, decode_responses=True)
            pipe = r.pipeline()
            pipe.delete("smart_herd_wallets")
            pipe.sadd("smart_herd_wallets", *top)
            pipe.execute()
            print(f"\n🚀 Redis seeded with {len(top)} elite wallets!")
            print("   Run `cargo run --release` from rust_daemon/ to start the bot.")
        except Exception as e:
            print(f"\n⚠️  Redis seed failed: {e}")
            print("   Start Redis first: run redis-server.exe from %TEMP%\\redis\\")
    elif passing:
        print(f"\n💡 To auto-seed Redis, re-run with: $env:REDIS_SEED='true'")
        print(f"   Or manually paste the passing wallets into candidates.txt")


if __name__ == "__main__":
    main()
