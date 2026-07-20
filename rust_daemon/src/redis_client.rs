use crate::error::BotError;
use redis::AsyncCommands;

const REDIS_KEY: &str = "smart_herd_wallets";

pub async fn fetch_elite_wallets() -> Result<Vec<String>, BotError> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut redis_conn = client.get_multiplexed_tokio_connection().await?;
    let wallets: Vec<String> = redis_conn.smembers(REDIS_KEY).await?;
    Ok(wallets)
}
