use solana_sdk::signature::Keypair;
use std::env;

pub struct AppConfig {
    pub rpc_url: String,
    pub bot_keypair: Keypair,
    pub pumpportal_api_key: String,
}

impl AppConfig {
    pub fn load_from_env() -> Result<Self, crate::error::BotError> {
        let rpc_url = env::var("RPC_URL").unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
        
        let pk_string = env::var("BOT_PRIVATE_KEY")
            .map_err(|_| crate::error::BotError::ConfigError("BOT_PRIVATE_KEY missing".to_string()))?;
        
        let bot_keypair = Keypair::from_base58_string(&pk_string);
        
        let pumpportal_api_key = env::var("PUMPPORTAL_API_KEY")
            .unwrap_or_else(|_| "".to_string()); // Leave blank if using free tier limits

        Ok(Self {
            rpc_url,
            bot_keypair,
            pumpportal_api_key,
        })
    }
}
