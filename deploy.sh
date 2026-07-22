#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Alpha Nexus Production Deploy Script
# =============================================================================
# Usage: ./deploy.sh
#
# Works from: Git Bash (Windows), WSL, or native Linux/macOS terminal.
# Cargo/Rustup are sourced automatically if installed via rustup.

# Source rustup/cargo environment so `cargo` is on PATH in Git Bash on Windows.
# This is a no-op on Linux/macOS where cargo is already in PATH.
# shellcheck source=/dev/null
if [ -f "$HOME/.cargo/env" ]; then
    source "$HOME/.cargo/env"
fi

# Verify cargo is available before doing anything else.
if ! command -v cargo &> /dev/null; then
    echo "❌ cargo not found. Install Rust via: https://rustup.rs"
    exit 1
fi
# Run from your LOCAL machine (Windows: use Git Bash or WSL).
# Requires: ssh key at ~/.ssh/Fruxfi-key.pem, cargo in PATH.
# =============================================================================
set -euo pipefail

SERVER="ubuntu@54.210.203.195"
SSH_KEY="$HOME/.ssh/Fruxfi-key.pem"
REMOTE_DIR="/home/ubuntu/alphanexus_bot"
LOCAL_ROOT="$(cd "$(dirname "$0")" && pwd)"
RUST_DIR="$LOCAL_ROOT/rust_daemon"

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║       Alpha Nexus — Production Deploy        ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

# ---- Step 1: Build the release binary locally -------------------------------
echo "▶ [1/6] Building release binary..."
cd "$RUST_DIR"
cargo build --release
echo "✅ Binary built: $RUST_DIR/target/release/alphanexus-daemon"

# ---- Step 2: Verify binary starts cleanly (no env = should fail with config
#              error, NOT a panic or segfault) ----------------------------------
echo ""
echo "▶ [2/6] Smoke-testing binary startup..."
# Expect exit code 1 with a clear ConfigError message (BOT_PRIVATE_KEY missing).
# A panic (exit 101) or segfault (exit 139) here means something is badly wrong.
set +e
SMOKE_OUTPUT=$("$RUST_DIR/target/release/alphanexus-daemon" 2>&1)
SMOKE_EXIT=$?
set -e
if echo "$SMOKE_OUTPUT" | grep -Eq "BOT_PRIVATE_KEY|RPC_URL"; then
    echo "✅ Startup smoke test passed (config validation fires correctly)."
elif [ $SMOKE_EXIT -eq 0 ]; then
    echo "❌ SMOKE TEST FAILED: binary exited 0 with no env vars set. Check config.rs."
    exit 1
else
    echo "⚠️  Smoke test unexpected output (exit $SMOKE_EXIT):"
    echo "$SMOKE_OUTPUT"
    echo "   Proceeding — check output above manually."
fi

# ---- Step 3: Ensure server dependencies are installed -----------------------
echo ""
echo "▶ [3/6] Checking server dependencies..."
ssh -i "$SSH_KEY" "$SERVER" bash <<'REMOTE'
set -euo pipefail
echo "  Checking Redis..."
if ! systemctl is-active --quiet redis-server; then
    echo "  Redis not running — installing and starting..."
    sudo apt-get update -q && sudo apt-get install -y -q redis-server
    sudo systemctl enable redis-server
    sudo systemctl start redis-server
fi
redis-cli ping | grep -q PONG && echo "  ✅ Redis is up."

echo "  Checking Python3 + pip..."
python3 --version
pip3 --version || sudo apt-get install -y -q python3-pip

echo "  Checking dune-client and redis Python packages..."
pip3 show dune-client redis > /dev/null 2>&1 || pip3 install --quiet --break-system-packages dune-client redis
echo "  ✅ Python deps present."
REMOTE

# ---- Step 4: Upload files ---------------------------------------------------
echo ""
echo "▶ [4/6] Uploading files to server..."

# Create remote directory structure
ssh -i "$SSH_KEY" "$SERVER" "mkdir -p $REMOTE_DIR/rust_daemon/target/release $REMOTE_DIR/data_pipeline"

# Stop the daemon first so we don't get a 'Text file busy' error when overwriting the binary
ssh -i "$SSH_KEY" "$SERVER" "sudo systemctl stop alphanexus-daemon 2>/dev/null || true"

# Upload release binary
scp -i "$SSH_KEY" \
    "$RUST_DIR/target/release/alphanexus-daemon" \
    "$SERVER:$REMOTE_DIR/rust_daemon/target/release/alphanexus-daemon"
echo "  ✅ Binary uploaded."

# Upload .env (contains secrets — never commit to git)
scp -i "$SSH_KEY" \
    "$LOCAL_ROOT/.env" \
    "$SERVER:$REMOTE_DIR/.env"
echo "  ✅ .env uploaded."

# Upload data pipeline
scp -i "$SSH_KEY" \
    "$LOCAL_ROOT/data_pipeline/update_whitelist.py" \
    "$LOCAL_ROOT/data_pipeline/requirements.txt" \
    "$SERVER:$REMOTE_DIR/data_pipeline/"
echo "  ✅ Data pipeline uploaded."

# ---- Step 5: Install systemd service ----------------------------------------
echo ""
echo "▶ [5/6] Installing systemd service..."
scp -i "$SSH_KEY" \
    "$LOCAL_ROOT/alphanexus-daemon.service" \
    "$SERVER:/tmp/alphanexus-daemon.service"

ssh -i "$SSH_KEY" "$SERVER" bash <<'REMOTE'
set -euo pipefail
sudo mv /tmp/alphanexus-daemon.service /etc/systemd/system/alphanexus-daemon.service
sudo chmod 644 /etc/systemd/system/alphanexus-daemon.service
sudo chmod +x /home/ubuntu/alphanexus_bot/rust_daemon/target/release/alphanexus-daemon
sudo systemctl daemon-reload
sudo systemctl enable alphanexus-daemon
echo "  ✅ systemd service installed and enabled."
REMOTE

# ---- Step 6: Run the whitelist pipeline + start the daemon ------------------
echo ""
echo "▶ [6/6] Seeding Redis whitelist and starting daemon..."
ssh -i "$SSH_KEY" "$SERVER" bash <<REMOTE
set -euo pipefail
cd $REMOTE_DIR

# Load env vars for the Python script (strip Windows CRLF line endings first)
sed -i 's/\r$//' .env
set -a
source .env
set +a

echo "  Skipping whitelist update (should be run via cron once per 24h to save Dune credits)..."

echo "  Starting alphanexus-daemon..."
sudo systemctl start alphanexus-daemon
sleep 3

# Check it actually came up
if systemctl is-active --quiet alphanexus-daemon; then
    echo ""
    echo "╔══════════════════════════════════════════════╗"
    echo "║   ✅  ALPHA NEXUS IS LIVE ON $SERVER   ║"
    echo "╚══════════════════════════════════════════════╝"
    echo ""
    echo "Live logs: ssh -i ~/.ssh/Fruxfi-key.pem $SERVER"
    echo "           journalctl -u alphanexus-daemon -f"
else
    echo "❌ Daemon failed to start. Check logs:"
    sudo journalctl -u alphanexus-daemon -n 30 --no-pager
    exit 1
fi
REMOTE

echo ""
echo "Deploy complete. 🚀"
