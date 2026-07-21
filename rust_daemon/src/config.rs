use solana_sdk::{signature::Keypair, signer::Signer};
use std::env;

pub struct AppConfig {
    pub rpc_url: String,
    pub bot_keypair: Keypair,
    pub pumpportal_api_key: String,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
}

impl AppConfig {
    pub fn load_from_env() -> Result<Self, crate::error::BotError> {
        // -----------------------------------------------------------------------
        // HARDENING: RPC_URL is now REQUIRED — no silent public-node fallback.
        //
        // WHY: The public Solana RPC (api.mainnet-beta.solana.com) does NOT
        // support `getPriorityFeeEstimate` (Helius-specific), rate-limits
        // aggressively, and silently degrades every exit transaction to the
        // FALLBACK_PRIORITY_FEE with no warning. A misconfigured deploy would
        // produce real trades with systematically underpriced sell fees.
        // -----------------------------------------------------------------------
        let rpc_url = env::var("RPC_URL").map_err(|_| {
            crate::error::BotError::ConfigError(
                "RPC_URL environment variable is REQUIRED. \
                 Set it to your Helius (or equivalent) RPC endpoint. \
                 Example: https://mainnet.helius-rpc.com/?api-key=YOUR_KEY"
                    .to_string(),
            )
        })?;

        // -----------------------------------------------------------------------
        // HARDENING: Keypair validation with loud startup failure.
        //
        // WHY: `Keypair::from_base58_string` panics on invalid input in some
        // SDK versions and silently produces an unusable keypair in others.
        // We parse explicitly and immediately verify the derived pubkey is
        // non-zero (the zero pubkey indicates a failed/default keypair).
        //
        // The PUBLIC KEY is logged at startup so operators can visually confirm
        // the correct wallet is loaded without ever exposing the private key.
        // -----------------------------------------------------------------------
        let pk_string = env::var("BOT_PRIVATE_KEY").map_err(|_| {
            crate::error::BotError::ConfigError(
                "BOT_PRIVATE_KEY environment variable is REQUIRED. \
                 Set it to the base58-encoded private key of your trading wallet."
                    .to_string(),
            )
        })?;

        if pk_string.trim().is_empty() {
            return Err(crate::error::BotError::ConfigError(
                "BOT_PRIVATE_KEY is set but empty.".to_string(),
            ));
        }

        // from_base58_string can panic — catch it via std::panic::catch_unwind
        // so we give a clean error message instead of a crash dump.
        let keypair_result = std::panic::catch_unwind(|| {
            Keypair::from_base58_string(&pk_string)
        });

        let bot_keypair = match keypair_result {
            Ok(kp) => kp,
            Err(_) => {
                return Err(crate::error::BotError::ConfigError(
                    "BOT_PRIVATE_KEY is not valid base58. \
                     Verify the key was not truncated or corrupted."
                        .to_string(),
                ));
            }
        };

        // Sanity check: the zero pubkey means keypair construction silently failed.
        let pubkey = bot_keypair.pubkey();
        if pubkey == solana_sdk::pubkey::Pubkey::default() {
            return Err(crate::error::BotError::ConfigError(
                "BOT_PRIVATE_KEY produced a zero/default pubkey — \
                 the key is invalid. Check for truncation or encoding errors."
                    .to_string(),
            ));
        }

        // Log the pubkey so operators can confirm the right wallet is loaded.
        // NEVER log pk_string — it is the raw private key.
        println!("🔑 Loaded trading wallet: {}", pubkey);

        let pumpportal_api_key = env::var("PUMPPORTAL_API_KEY")
            .unwrap_or_else(|_| "".to_string()); // Free tier: leave blank

        let telegram_bot_token = env::var("TELEGRAM_BOT_TOKEN").ok().filter(|s| !s.is_empty());
        let telegram_chat_id = env::var("TELEGRAM_CHAT_ID").ok().filter(|s| !s.is_empty());

        if telegram_bot_token.is_none() || telegram_chat_id.is_none() {
            println!("⚠️  TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set — alerts disabled.");
        }

        Ok(Self {
            rpc_url,
            bot_keypair,
            pumpportal_api_key,
            telegram_bot_token,
            telegram_chat_id,
        })
    }
}
