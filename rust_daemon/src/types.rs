use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct PumpTradeEvent {
    #[serde(rename = "txType")]
    pub tx_type: String,
    pub mint: String,
    #[serde(rename = "traderPublicKey")]
    pub trader_public_key: String,
    #[serde(rename = "solAmount")]
    pub sol_amount: f64,
    #[serde(rename = "tokenAmount")]
    #[allow(dead_code)] // Deserialized from WS payload; reserved for future position-sizing filters
    pub token_amount: f64,
    pub signature: String,
}
