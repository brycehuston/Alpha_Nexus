# ⚡ Alpha Nexus

> **A high-performance, copy-trading bot for Solana meme coins built in Rust + Python.**

> **Current worktree note (verified 2026-07-22):** accepted signals are routed to a local `shadow_ledger.jsonl` receipt in the active shadow-mode worktree; they do not invoke the retained PumpPortal buy path. Treat the execution and dry-run sections below as Generation 1 reference behaviour until the shadow pipeline has deterministic replay validation.
> Alpha Nexus monitors a curated list of elite on-chain wallets ("smart money") in real time and mirrors their pump.fun buys instantly — before the market can react.

---

## 📖 Table of Contents

- [How It Works](#-how-it-works)
- [The 5-Gate Safety Pipeline](#-the-5-gate-safety-pipeline)
- [Architecture](#-architecture)
- [Project Structure](#-project-structure)
- [Quick Start](#-quick-start)
- [Configuration](#-configuration)
- [Finding Elite Wallets](#-finding-elite-wallets)
- [Deploying to a VPS](#-deploying-to-a-vps)
- [Live Log Reference](#-live-log-reference)
- [Safety Features](#-safety-features)

---

## 🧠 How It Works

Alpha Nexus operates in two independent layers:

### 🐍 Python Data Pipeline (`data_pipeline/`)
Runs once per day (via cron or manually). It sources and vets elite Solana wallets using either:
- **Dune Analytics** — SQL-based historical on-chain analysis
- **Helius Backtester** *(recommended, free)* — pulls full swap history per wallet using your Helius API key, calculates win rate, net PnL, and trade count, then seeds Redis with qualifying wallets

Wallets that pass the filter criteria are stored in a Redis set (`smart_herd_wallets`).

### 🦀 Rust Daemon (`rust_daemon/`)
A production-grade async daemon built on Tokio. It:
1. Loads the elite wallet list from Redis on startup
2. Opens a persistent WebSocket to [PumpPortal](https://pumpportal.fun) and subscribes to live trade events for every wallet in the whitelist
3. Passes every incoming event through the 5-Gate Safety Pipeline
4. Executes mirror buys via PumpPortal's `/trade-local` API when all gates pass
5. Spawns an independent exit watcher per position to handle stop-losses and take-profits

---

## 🔒 The 5-Gate Safety Pipeline

Every incoming trade event must pass **all 5 gates in order** before the bot spends a single lamport.

```
Incoming WebSocket Event
        │
        ▼
┌───────────────────────────────┐
│  GATE 1 — Whitelist Filter    │  Is the wallet in Redis smart_herd?
│  websocket.rs + redis_client  │  ❌ Not whitelisted → silent discard
└───────────────┬───────────────┘
                │ ✅
                ▼
┌───────────────────────────────┐
│  GATE 2 — State-Change Check  │  Did they actually BUY? (tx_type == "buy")
│  websocket.rs:154             │  ❌ Sell/unknown → monitor only, no execute
└───────────────┬───────────────┘
                │ ✅
                ▼
┌───────────────────────────────┐
│  GATE 3 — Whale Dust Filter   │  sol_amount >= 0.5 SOL?
│  websocket.rs:160             │  ❌ Dust trade → continue 'connection
└───────────────┬───────────────┘
                │ ✅
                ▼
┌───────────────────────────────┐
│  GATE 4 — Double-Buy Guard    │  Is this mint already in traded_mints?
│  state.rs:83 (RwLock HashMap) │  ❌ Already traded → drop(permit), skip
└───────────────┬───────────────┘
                │ ✅
                ▼
┌───────────────────────────────┐
│  GATE 5 — Circuit Breaker     │  consecutive_losses < 3?
│  state.rs:138 (AtomicUsize)   │  ❌ Breaker active → paused 30 min
└───────────────┬───────────────┘
                │ ✅
                ▼
        🚀 EXECUTE BUY
   (via PumpPortal /trade-local)
```

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────┐
│                     VPS (Ubuntu)                    │
│                                                     │
│  ┌─────────────────┐    ┌──────────────────────┐   │
│  │  Redis (local)  │◄───│  Python Pipeline     │   │
│  │  smart_herd_    │    │  update_whitelist.py │   │
│  │  wallets (SET)  │    │  backtest_wallets.py │   │
│  └────────┬────────┘    └──────────────────────┘   │
│           │ on startup                              │
│  ┌────────▼────────────────────────────────────┐   │
│  │         alphanexus-daemon (Rust/Tokio)      │   │
│  │                                             │   │
│  │  WebSocket ──► 5-Gate Pipeline ──► Execute  │   │
│  │  (PumpPortal)   (websocket.rs)   (execution │   │
│  │                                   .rs)      │   │
│  │                     │                       │   │
│  │               Exit Watchers                 │   │
│  │               (exits.rs, per position)      │   │
│  └─────────────────────────────────────────────┘   │
│                         │                          │
│                  Telegram Alerts                    │
└─────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
alphanexus_bot/
├── rust_daemon/               # 🦀 Core trading engine
│   ├── src/
│   │   ├── main.rs            # Entry point, startup, graceful shutdown
│   │   ├── config.rs          # Environment variable loading & validation
│   │   ├── websocket.rs       # PumpPortal WS listener + 5-gate pipeline
│   │   ├── execution.rs       # Buy execution via PumpPortal /trade-local
│   │   ├── exits.rs           # Per-position exit watcher (TP/SL)
│   │   ├── state.rs           # BotState: circuit breaker, position semaphore, mint lock
│   │   ├── redis_client.rs    # Loads smart_herd_wallets from Redis
│   │   ├── db.rs              # SQLite telemetry logging
│   │   ├── telegram.rs        # Trade alert notifications
│   │   ├── utils.rs           # Dynamic priority fee (Helius)
│   │   ├── types.rs           # PumpTradeEvent struct
│   │   └── error.rs           # BotError enum
│   └── Cargo.toml
│
├── data_pipeline/             # 🐍 Wallet sourcing & backtesting
│   ├── update_whitelist.py    # Pulls elite wallets → seeds Redis
│   ├── backtest_wallets.py    # ⭐ Helius-powered wallet backtester (replaces Dune)
│   ├── candidates.txt         # Paste wallet addresses here before backtesting
│   ├── out/                   # Backtest results (CSV)
│   └── requirements.txt
│
├── alphanexus-daemon.service  # systemd unit file for VPS auto-start
├── deploy.sh                  # One-command build + deploy to VPS
├── example.env                # ← Copy this to .env and fill in your keys
└── .gitignore
```

---

## 🚀 Quick Start

### Prerequisites

- [Rust](https://rustup.rs) (stable)
- Python 3.10+
- Redis (running locally — see below)
- A [Helius](https://helius.dev) API key
- A [PumpPortal](https://pumpportal.fun) API key
- A Solana wallet with SOL for trading

### 1. Clone & Configure

```bash
git clone https://github.com/brycehuston/Alpha_Nexus.git
cd Alpha_Nexus

# Copy the template and fill in your keys
cp example.env .env
```

Edit `.env` with your real values. See [Configuration](#-configuration) below.

### 2. Start Redis

**Windows (via the included binary):**
```powershell
# Download and extract Redis (one-time setup)
Invoke-WebRequest -Uri "https://github.com/microsoftarchive/redis/releases/download/win-3.0.504/Redis-x64-3.0.504.zip" -OutFile "$env:TEMP\redis.zip"
Expand-Archive "$env:TEMP\redis.zip" -DestinationPath "$env:TEMP\redis" -Force
Start-Process -FilePath "$env:TEMP\redis\redis-server.exe" -WindowStyle Minimized
```

**Linux/macOS:**
```bash
sudo apt install redis-server && sudo service redis-server start
# or: brew install redis && brew services start redis
```

### 3. Find & Backtest Elite Wallets

```bash
# Install Python deps
pip install -r data_pipeline/requirements.txt

# 1. Go to https://gmgn.ai/sol/wallets and copy top pump.fun wallet addresses
# 2. Paste them into data_pipeline/candidates.txt (one per line)
# 3. Run the backtester
python data_pipeline/backtest_wallets.py

# Results are saved to data_pipeline/out/backtest_results.csv
# To auto-seed Redis with passing wallets:
REDIS_SEED=true python data_pipeline/backtest_wallets.py
```

### 4. Run the Bot (Dry Run)

```bash
cd rust_daemon
cargo run --release
```

You should see:
```
Initialize Alpha Nexus Daemon...
🔑 Loaded trading wallet: <your-pubkey>
🧪 DRY RUN MODE ENABLED: Bot will simulate trades without risking real capital.
🟢 WS Connected. Subscribing to 47 elite wallets...
```

### 5. Go Live

When you've validated the dry run, set `DRY_RUN=false` in your `.env` and redeploy.

---

## ⚙️ Configuration

Copy `example.env` to `.env` and fill in your values:

| Variable | Required | Description |
|----------|----------|-------------|
| `HELIUS_API_KEY` | ✅ | Helius RPC + analytics API key |
| `RPC_URL` | ✅ | Your Helius RPC endpoint (include API key) |
| `BOT_PRIVATE_KEY` | ✅ | Base58-encoded private key of your trading wallet |
| `PUMPPORTAL_API_KEY` | ✅ | PumpPortal API key for trade execution |
| `TELEGRAM_BOT_TOKEN` | ⚠️ Optional | Telegram bot token for trade alerts |
| `TELEGRAM_CHAT_ID` | ⚠️ Optional | Your Telegram chat ID |
| `DUNE_API_KEY` | ⚠️ Optional | Only needed if using `update_whitelist.py` |
| `DUNE_QUERY_ID` | ⚠️ Optional | Your Dune query ID |
| `TRADE_SIZE_SOL` | ✅ | SOL amount per mirror trade (e.g. `0.1`) |
| `DRY_RUN` | ✅ | `true` = simulate only, `false` = live trading |
| `AUTO_EXECUTE` | ✅ | Master kill switch for execution |

> ⚠️ **Never commit your `.env` file.** It is git-ignored by default.

---

## 🔍 Finding Elite Wallets

Alpha Nexus ships with two wallet sourcing methods:

### Option A — Helius Backtester *(Recommended, Free)*

No Dune subscription needed. Uses your existing Helius API key.

```bash
# 1. Collect candidate wallet addresses from any of these sources:
#    - https://gmgn.ai/sol/wallets         (pump.fun smart money leaderboard)
#    - https://cielo.finance                (top Solana traders by PnL)
#    - https://bullx.io                     (smart money alerts)
#    - https://photon-sol.twitfi.com        (pump.fun whale tracker)

# 2. Paste addresses into:
nano data_pipeline/candidates.txt

# 3. Run the backtester — it will score each wallet and rank them
python data_pipeline/backtest_wallets.py

# 4. Seed Redis with the wallets that pass the criteria
REDIS_SEED=true python data_pipeline/backtest_wallets.py
```

**Filtering criteria (configurable in `backtest_wallets.py`):**

| Metric | Threshold |
|--------|-----------|
| Win Rate | ≥ 65% |
| Total Trades | ≥ 20 |
| Net PnL | ≥ 1.0 SOL |

### Option B — Dune Analytics *(Requires Paid Plan)*

```bash
# Set DUNE_API_KEY and DUNE_QUERY_ID in .env, then:
python data_pipeline/update_whitelist.py
```

---

## 🖥️ Deploying to a VPS

The `deploy.sh` script automates a full production deploy:

```bash
# From your local machine (Git Bash or WSL):
./deploy.sh
```

**What it does:**
1. Builds the release binary locally with `cargo build --release`
2. Smoke-tests the binary
3. Checks server dependencies (Redis, Python)
4. Uploads binary, `.env`, and data pipeline via `scp`
5. Installs and starts the systemd service

**Requirements:**
- SSH key at `~/.ssh/Fruxfi-key.pem`
- Target server: `ubuntu@<your-vps-ip>` (update `SERVER` in `deploy.sh`)

**Live log monitoring:**
```bash
ssh -i ~/.ssh/Fruxfi-key.pem ubuntu@<your-vps-ip>
journalctl -u alphanexus-daemon -f
```

---

## 📋 Live Log Reference

| Log Message | Meaning |
|-------------|---------|
| `🟢 WS Connected. Subscribing to N elite wallets...` | Startup successful |
| `🚨 Smart Money Alert: WALLET BUY MINT (Size: X SOL)` | Signal detected, pipeline starting |
| `🔕 Dust filter: ignoring X SOL buy...` | Trade too small, skipped |
| `📊 Position slot acquired (N/3) for MINT` | All gates passed, executing |
| `🚀 [DRY RUN] Simulating buy for MINT` | Dry run mode — no real trade |
| `🚀 Firing execution for MINT` | Live trade sent to chain |
| `🎯 Buy CONFIRMED on-chain for MINT` | Transaction landed |
| `🔴 CIRCUIT BREAKER TRIPPED` | 3 consecutive losses — trading paused |
| `🔄 Circuit breaker AUTO-RESET` | 30-min cooldown expired, trading resumed |
| `⏰ WS receive timeout — Forcing reconnect` | Half-open connection detected, reconnecting |
| `💓 Heartbeat received from PumpPortal` | WebSocket keepalive OK |

---

## 🛡️ Safety Features

| Feature | Implementation |
|---------|---------------|
| **Dry Run Mode** | `DRY_RUN=true` simulates all trades end-to-end with zero capital risk |
| **Circuit Breaker** | Halts all trading for 30 min after 3 consecutive stop-losses (`state.rs`) |
| **Position Cap** | Max 3 concurrent open positions via Tokio `Semaphore` (`state.rs`) |
| **Dust Filter** | Ignores trades < 0.5 SOL to avoid bait signals (`websocket.rs`) |
| **Double-Buy Guard** | `RwLock<HashMap>` prevents entering the same token twice (`state.rs`) |
| **Token Age Filter** | Skips tokens < 60 seconds old to avoid sub-minute rug pulls (`websocket.rs`) |
| **Market Cap Filter** | Only trades tokens with MC between $10k and $2M (`websocket.rs`) |
| **Half-Open WS Detection** | 15-second receive timeout + application-level ping (`websocket.rs`) |
| **Buy Confirmation Gate** | Exit watcher only spawns after on-chain confirmation (`execution.rs`) |
| **Graceful Shutdown** | SIGINT/SIGTERM handled cleanly with open position warnings (`main.rs`) |

---

## ⚠️ Disclaimer

Alpha Nexus is experimental software for educational purposes. Meme coin trading carries **extreme financial risk**. You can lose your entire trading balance. Always start with `DRY_RUN=true`, use only capital you can afford to lose, and never trade with your primary wallet.
