"""
backtest_wallets.py — Alpha Nexus Wallet Backtester (Helius-Powered)
=====================================================================
Replaces Dune Analytics entirely. Uses your existing Helius API key to:
  1. Pull full pump.fun swap history for each candidate wallet
  2. Calculate win rate, trade count, net PnL (SOL), and avg return
  3. Score and rank wallets against Alpha Nexus criteria
  4. Optionally seed Redis with the top wallets that pass

Usage:
  python data_pipeline/backtest_wallets.py \
    --candidates <wallets.txt> --output-dir <pilot-output>

Environment variables (loaded from .env automatically):
  HELIUS_API_KEY  — your Helius API key
  REDIS_SEED      — set to "true" to auto-seed Redis with passing wallets
"""

import os
import sys
import time
import csv
import json
import math
import argparse
import requests
from pathlib import Path
from dotenv import load_dotenv


def configure_stdout_utf8(stream) -> None:
    """Use UTF-8 when a standard text stream supports reconfiguration."""
    encoding = getattr(stream, "encoding", None)
    reconfigure = getattr(stream, "reconfigure", None)
    if (
        callable(reconfigure)
        and isinstance(encoding, str)
        and encoding.lower().replace("_", "-") not in {"utf-8", "utf8"}
    ):
        try:
            reconfigure(encoding="utf-8")
        except (AttributeError, OSError, TypeError, ValueError):
            pass


configure_stdout_utf8(sys.stdout)

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
PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_AMM_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
TX_LIMIT        = 100          # max per page (Helius max)
MAX_PAGES       = 10           # cap at 1000 txs per wallet to avoid rate limit burn
MAX_WALLETS     = 10
MAX_ATTEMPTS    = 120
CREDITS_PER_ATTEMPT = 100
MAX_CREDITS     = 12_000
RATE_LIMIT_WAIT = 0.25         # seconds between requests (4 req/s, well under free limit)

# ── Alpha Nexus filtering criteria (mirrors update_whitelist.py) ───────────────
MIN_WIN_RATE    = 0.65
MIN_TRADES      = 20           # lowered from 50 — small sample from GMGN is expected
MIN_NET_SOL     = 1.0          # minimum net profit in SOL to qualify
TOP_N           = 50           # max wallets to seed into Redis

class PilotFatalError(RuntimeError):
    pass


def write_json(path: Path, data, *, refuse_existing=False) -> None:
    """Write JSON through a temporary file; cache pages are never overwritten."""
    temp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if refuse_existing and path.exists():
            raise PilotFatalError(f"Cache page already exists: {path.name}")
        with open(temp_path, "w", encoding="utf-8") as stream:
            json.dump(data, stream, ensure_ascii=False)
            stream.flush()
            os.fsync(stream.fileno())
        if refuse_existing and path.exists():
            raise PilotFatalError(f"Cache page already exists: {path.name}")
        os.replace(temp_path, path)
    except PilotFatalError:
        temp_path.unlink(missing_ok=True)
        raise
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise PilotFatalError(f"Could not write {path.name}") from None


def load_manifest(output_dir: Path) -> dict:
    path = output_dir / "manifest.json"
    if not path.exists():
        return {"total_attempts": 0, "estimated_credits": 0, "wallets": {}}
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        raise PilotFatalError("Manifest is not valid JSON") from None

    attempts = manifest.get("total_attempts")
    wallets = manifest.get("wallets")
    if (
        not isinstance(attempts, int)
        or isinstance(attempts, bool)
        or not 0 <= attempts <= MAX_ATTEMPTS
        or manifest.get("estimated_credits") != attempts * CREDITS_PER_ATTEMPT
        or not isinstance(wallets, dict)
    ):
        raise PilotFatalError("Manifest contents are invalid")
    for state in wallets.values():
        if (
            not isinstance(state, dict)
            or state.get("status")
            not in {
                "NOT_STARTED", "ACTIVE", "REQUESTING",
                "COMPLETE", "INCOMPLETE", "FATAL",
            }
            or not isinstance(state.get("pages_cached"), int)
            or not 0 <= state["pages_cached"] <= MAX_PAGES
            or (
                state.get("last_cursor") is not None
                and not isinstance(state["last_cursor"], str)
            )
        ):
            raise PilotFatalError("Manifest wallet state is invalid")
    return manifest


def save_manifest(output_dir: Path, manifest: dict) -> None:
    write_json(output_dir / "manifest.json", manifest)


def load_cached_pages(output_dir: Path, address: str) -> tuple[list, str, str | None]:
    """Load a contiguous, valid cache and derive acquisition status/cursor."""
    cache_dir = output_dir / "cache" / address
    paths = list(cache_dir.glob("page_*.json")) if cache_dir.exists() else []
    numbered_paths = []
    for path in paths:
        try:
            number = int(path.stem.removeprefix("page_"))
        except ValueError:
            raise PilotFatalError(f"Invalid cache filename for {address[:8]}") from None
        if number < 1 or path.name != f"page_{number}.json":
            raise PilotFatalError(f"Invalid cache filename for {address[:8]}")
        numbered_paths.append((number, path))
    numbered_paths.sort()
    if [number for number, _ in numbered_paths] != list(
        range(1, len(numbered_paths) + 1)
    ) or len(numbered_paths) > MAX_PAGES:
        raise PilotFatalError(f"Cached pages are not contiguous for {address[:8]}")

    all_txs = []
    last_cursor = None
    seen_cursors = set()
    terminal = False
    for _, path in numbered_paths:
        try:
            page = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            raise PilotFatalError(f"Invalid cached page for {address[:8]}") from None
        if (
            not isinstance(page, list)
            or len(page) > TX_LIMIT
            or not all(isinstance(tx, dict) for tx in page)
            or terminal
        ):
            raise PilotFatalError(f"Invalid cached page for {address[:8]}")
        if page:
            cursor = page[-1].get("signature")
            if not isinstance(cursor, str) or not cursor or cursor in seen_cursors:
                raise PilotFatalError(f"Invalid cached cursor for {address[:8]}")
            seen_cursors.add(cursor)
            last_cursor = cursor
        terminal = len(page) < TX_LIMIT
        all_txs.extend(page)

    if terminal:
        status = "COMPLETE"
    elif len(numbered_paths) == MAX_PAGES:
        status = "INCOMPLETE"
    else:
        status = "NOT_STARTED" if not numbered_paths else "ACTIVE"
    return all_txs, status, last_cursor


def reserve_attempt(output_dir: Path, manifest: dict, address: str) -> None:
    if (
        manifest["total_attempts"] >= MAX_ATTEMPTS
        or (manifest["total_attempts"] + 1) * CREDITS_PER_ATTEMPT > MAX_CREDITS
    ):
        raise PilotFatalError("Pilot request and credit budget exhausted")
    manifest["total_attempts"] += 1
    manifest["estimated_credits"] = (
        manifest["total_attempts"] * CREDITS_PER_ATTEMPT
    )
    manifest["wallets"][address]["status"] = "REQUESTING"
    save_manifest(output_dir, manifest)


def request_page(
    address: str,
    cursor: str | None,
    output_dir: Path,
    manifest: dict,
    request_get,
    sleeper,
) -> list:
    params = {"api-key": HELIUS_API_KEY, "type": "SWAP", "limit": TX_LIMIT}
    if cursor:
        params["before"] = cursor
    url = HELIUS_TX_URL.format(address=address)

    def attempt():
        reserve_attempt(output_dir, manifest, address)
        try:
            return request_get(url, params=params, timeout=15)
        except Exception:
            raise PilotFatalError(
                f"Sanitized request failure for {address[:8]}"
            ) from None

    response = attempt()
    status = response.status_code
    if not isinstance(status, int):
        raise PilotFatalError("Helius response status is invalid")
    if status in {401, 402, 403}:
        raise PilotFatalError(f"Fatal Helius response HTTP {status}")

    retryable = status == 429 or 500 <= status <= 599
    if retryable:
        delay = 2.0
        if status == 429:
            try:
                delay = float(response.headers.get("Retry-After", 5))
            except (TypeError, ValueError):
                delay = 5.0
        sleeper(max(0.0, delay))
        response = attempt()
        retry_status = response.status_code
        if not isinstance(retry_status, int):
            raise PilotFatalError("Helius retry status is invalid")
        if retry_status in {401, 402, 403}:
            raise PilotFatalError(f"Fatal Helius response HTTP {retry_status}")
        if retry_status == 429:
            raise PilotFatalError("Second HTTP 429")
        if 500 <= retry_status <= 599:
            raise PilotFatalError("Repeated HTTP 5xx")
        if retry_status != 200:
            raise PilotFatalError(f"Fatal Helius response HTTP {retry_status}")
    elif status != 200:
        raise PilotFatalError(f"Fatal Helius response HTTP {status}")

    try:
        page = response.json()
    except Exception:
        raise PilotFatalError("Helius response is not valid JSON") from None
    if (
        not isinstance(page, list)
        or len(page) > TX_LIMIT
        or not all(isinstance(tx, dict) for tx in page)
    ):
        raise PilotFatalError("Helius response schema is invalid")
    if page:
        cursor_value = page[-1].get("signature")
        if not isinstance(cursor_value, str) or not cursor_value:
            raise PilotFatalError("Helius response cursor is invalid")
    return page


def sync_wallet_state(output_dir: Path, manifest: dict, address: str):
    all_txs, status, cursor = load_cached_pages(output_dir, address)
    state = manifest["wallets"].setdefault(
        address,
        {"status": "NOT_STARTED", "pages_cached": 0, "last_cursor": None},
    )
    prior_status = state.get("status")
    if prior_status == "FATAL":
        raise PilotFatalError(f"Previous fatal stop requires review for {address[:8]}")
    cached_pages = len(list((output_dir / "cache" / address).glob("page_*.json")))
    if prior_status == "REQUESTING" and state.get("pages_cached") == cached_pages:
        raise PilotFatalError(f"Ambiguous prior request requires review for {address[:8]}")
    if state.get("pages_cached", 0) > cached_pages:
        raise PilotFatalError(f"Manifest references missing cache for {address[:8]}")
    if state.get("status") == "COMPLETE" and status != "COMPLETE":
        raise PilotFatalError(f"Completed cache is missing for {address[:8]}")
    if state.get("status") == "INCOMPLETE" and status != "INCOMPLETE":
        raise PilotFatalError(f"Incomplete cache is missing for {address[:8]}")
    state.update(
        {"status": status, "pages_cached": cached_pages, "last_cursor": cursor}
    )
    return all_txs, status, cursor, cached_pages


def fetch_transactions(
    address: str,
    output_dir: Path,
    manifest: dict,
    request_get=requests.get,
    sleeper=time.sleep,
) -> tuple[list, bool]:
    """
    Fetches all SWAP transactions for a wallet via Helius Enhanced TX API.
    Handles pagination automatically up to MAX_PAGES.
    """
    all_txs, status, cursor, cached_pages = sync_wallet_state(
        output_dir, manifest, address
    )
    state = manifest["wallets"][address]
    save_manifest(output_dir, manifest)
    if status == "COMPLETE":
        return all_txs, True
    if status == "INCOMPLETE":
        return all_txs, False

    state["status"] = "ACTIVE"
    save_manifest(output_dir, manifest)
    seen_cursors = {
        page[-1]["signature"]
        for page in [
            json.loads(path.read_text(encoding="utf-8"))
            for path in sorted(
                (output_dir / "cache" / address).glob("page_*.json"),
                key=lambda item: int(item.stem.removeprefix("page_")),
            )
        ]
        if page
    }
    try:
        for page_number in range(cached_pages + 1, MAX_PAGES + 1):
            page = request_page(
                address,
                cursor,
                output_dir,
                manifest,
                request_get,
                sleeper,
            )
            next_cursor = page[-1]["signature"] if page else cursor
            if page and (next_cursor == cursor or next_cursor in seen_cursors):
                raise PilotFatalError(f"Repeated cursor for {address[:8]}")

            cache_path = (
                output_dir / "cache" / address / f"page_{page_number}.json"
            )
            write_json(cache_path, page, refuse_existing=True)
            all_txs.extend(page)
            state["pages_cached"] = page_number
            state["last_cursor"] = next_cursor
            state["status"] = "ACTIVE"

            if len(page) < TX_LIMIT:
                state["status"] = "COMPLETE"
                save_manifest(output_dir, manifest)
                return all_txs, True
            seen_cursors.add(next_cursor)
            cursor = next_cursor

            if page_number == MAX_PAGES:
                state["status"] = "INCOMPLETE"
                save_manifest(output_dir, manifest)
                return all_txs, False
            save_manifest(output_dir, manifest)
    except PilotFatalError:
        state["status"] = "FATAL"
        try:
            save_manifest(output_dir, manifest)
        except PilotFatalError:
            pass
        raise

    return all_txs, False


def contains_program(instructions, program_id: str) -> bool:
    """Search Helius top-level and inner instructions for a program ID."""
    if not isinstance(instructions, list):
        return False
    for instruction in instructions:
        if not isinstance(instruction, dict):
            continue
        if instruction.get("programId") == program_id:
            return True
        if contains_program(instruction.get("innerInstructions"), program_id):
            return True
    return False


def is_pump_fun(tx: dict) -> bool:
    """Returns True if the transaction originated from pump.fun."""
    return (
        tx.get("source") == "PUMP_FUN"
        or any(
            isinstance(instruction, dict)
            and instruction.get("programId") == PUMP_FUN_PROGRAM
            for instruction in tx.get("instructions", [])
        )
    )


def is_pump_swap(tx: dict) -> bool:
    """Returns True for direct or program-proven routed PumpSwap activity."""
    return (
        tx.get("source") == "PUMP_AMM"
        or contains_program(tx.get("instructions"), PUMP_AMM_PROGRAM)
    )


def parse_trade(tx: dict, wallet: str) -> dict | None:
    """
    Parses a single SWAP transaction and returns a trade dict:
      { mint, direction: 'buy'|'sell', sol_amount, token_amount }
    Returns None if the transaction can't be parsed cleanly.
    """
    account_data = tx.get("accountData")
    if (
        not isinstance(account_data, list)
        or not all(isinstance(account, dict) for account in account_data)
    ):
        return None

    wallet_accounts = [
        account for account in account_data
        if account.get("account") == wallet
    ]
    if len(wallet_accounts) != 1:
        return None

    native_balance_change = wallet_accounts[0].get("nativeBalanceChange")
    if (
        isinstance(native_balance_change, bool)
        or not isinstance(native_balance_change, (int, float))
        or (
            isinstance(native_balance_change, float)
            and not math.isfinite(native_balance_change)
        )
        or native_balance_change == 0
    ):
        return None

    # Wallet-native balance change is already net of all lamport movements.
    net_sol = native_balance_change / 1e9
    sol_direction = "sell" if net_sol > 0 else "buy"

    transaction_fee = tx.get("fee")
    if (
        sol_direction == "buy"
        and tx.get("feePayer") == wallet
        and not isinstance(transaction_fee, bool)
        and isinstance(transaction_fee, (int, float))
        and (
            not isinstance(transaction_fee, float)
            or math.isfinite(transaction_fee)
        )
        and transaction_fee >= abs(native_balance_change)
    ):
        return None

    # Find the non-WSOL token mint involved
    token_transfers = tx.get("tokenTransfers")
    if (
        not isinstance(token_transfers, list)
        or not all(isinstance(transfer, dict) for transfer in token_transfers)
    ):
        return None

    wallet_token_transfers = []
    for t in token_transfers:
        if t.get("mint") == WSOL_MINT:
            continue

        from_wallet = t.get("fromUserAccount") == wallet
        to_wallet = t.get("toUserAccount") == wallet
        if from_wallet and to_wallet:
            return None
        if not from_wallet and not to_wallet:
            continue

        mint = t.get("mint")
        token_amount = t.get("tokenAmount")
        if (
            not mint
            or isinstance(token_amount, bool)
            or not isinstance(token_amount, (int, float))
            or (isinstance(token_amount, float) and not math.isfinite(token_amount))
            or token_amount <= 0
        ):
            return None

        token_direction = "sell" if from_wallet else "buy"
        wallet_token_transfers.append((mint, token_amount, token_direction))

    if len(wallet_token_transfers) != 1 or abs(net_sol) < 0.001:
        return None  # dust or unparseable

    mint, token_amount, token_direction = wallet_token_transfers[0]
    if token_direction != sol_direction:
        return None

    sol_amount = abs(net_sol)

    return {
        "mint":         mint,
        "direction":    sol_direction,
        "sol_amount":   sol_amount,
        "token_amount": token_amount,
        "signature":    tx.get("signature", ""),
        "timestamp":    tx.get("timestamp", 0),
    }


def score_wallet(address: str, txs: list) -> dict:
    """
    Scores a complete wallet's pump.fun and PumpSwap trading history.
    Returns a metrics dict.
    """
    pump_txs = [tx for tx in txs if is_pump_fun(tx) or is_pump_swap(tx)]
    print(f"    Found {len(txs)} total swaps, {len(pump_txs)} pump trades")

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


def load_candidates(path: Path) -> list[str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        raise PilotFatalError("Candidate input could not be read") from None
    candidates = list(
        dict.fromkeys(
            line.strip()
            for line in lines
            if line.strip() and not line.lstrip().startswith("#")
        )
    )
    if not candidates:
        raise PilotFatalError("Candidate input is empty")
    if len(candidates) > MAX_WALLETS:
        raise PilotFatalError("Candidate input exceeds 10 unique wallets")
    return candidates


def print_usage(manifest: dict, candidates: list[str], fatal=None) -> None:
    statuses = [
        manifest["wallets"].get(wallet, {}).get("status", "NOT_STARTED")
        for wallet in candidates
    ]
    if fatal:
        run_status = "FATAL"
    elif "INCOMPLETE" in statuses:
        run_status = "INCOMPLETE"
    else:
        run_status = "COMPLETE"
    print(
        "Pilot usage: "
        f"status={run_status} attempts={manifest['total_attempts']} "
        f"estimated_credits={manifest['estimated_credits']} "
        f"complete={statuses.count('COMPLETE')} "
        f"incomplete={statuses.count('INCOMPLETE')}"
    )


def run_pilot(
    candidate_path: Path,
    output_dir: Path,
    request_get=requests.get,
    sleeper=time.sleep,
) -> tuple[list[dict], dict]:
    candidates = load_candidates(candidate_path)
    if output_dir.exists() and not output_dir.is_dir():
        raise PilotFatalError("Output path is not a directory")
    manifest = load_manifest(output_dir)

    try:
        cached_pages = 0
        for wallet in candidates:
            _, _, _, page_count = sync_wallet_state(
                output_dir, manifest, wallet
            )
            cached_pages += page_count
        save_manifest(output_dir, manifest)
        print(
            "Pilot budget: "
            f"wallets={len(candidates)} max_pages_per_wallet={MAX_PAGES} "
            f"max_attempts={MAX_ATTEMPTS} credits_per_attempt={CREDITS_PER_ATTEMPT} "
            f"max_credits={MAX_CREDITS} cached_pages={cached_pages}"
        )

        results = []
        for index, wallet in enumerate(candidates, 1):
            print(f"[{index}/{len(candidates)}] Acquiring {wallet[:12]}...")
            txs, complete = fetch_transactions(
                wallet,
                output_dir,
                manifest,
                request_get,
                sleeper,
            )
            if complete:
                results.append(score_wallet(wallet, txs))
    except PilotFatalError as error:
        print_usage(manifest, candidates, fatal=str(error))
        raise

    print_usage(manifest, candidates)
    return results, manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    try:
        results, _ = run_pilot(args.candidates, args.output_dir)
    except PilotFatalError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    results.sort(key=lambda r: (r["passes"], r["net_sol"]), reverse=True)
    results_csv = args.output_dir / "backtest_results.csv"
    fields = [
        "wallet",
        "total_trades",
        "win_rate",
        "net_sol",
        "avg_return_pct",
        "passes",
    ]
    with open(results_csv, "w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)

    passing = [result for result in results if result["passes"]]
    if REDIS_SEED and passing:
        try:
            import redis as redis_lib
            top = [result["wallet"] for result in passing[:TOP_N]]
            redis_client = redis_lib.Redis(host="localhost", port=6379, db=0,
                                           decode_responses=True)
            pipe = redis_client.pipeline()
            pipe.delete("smart_herd_wallets")
            pipe.sadd("smart_herd_wallets", *top)
            pipe.execute()
            print(f"Redis seeded with {len(top)} elite wallets")
        except Exception as error:
            print(f"Redis seed failed: {error}")
    elif passing:
        print("Set REDIS_SEED=true to seed passing wallets")
    print(f"Results saved to: {results_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
