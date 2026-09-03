//! SOL/USD 报价缓存；网络请求独立于 Geyser 热路径。

use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const PRICE_URL: &str =
    "https://lite-api.jup.ag/price/v3?ids=So11111111111111111111111111111111111111112";

#[derive(Deserialize)]
struct PriceEntry {
    #[serde(rename = "usdPrice")]
    usd_price: f64,
}

pub async fn start() {
    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
    else {
        return;
    };

    refresh(&client).await;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.tick().await;
        loop {
            interval.tick().await;
            refresh(&client).await;
        }
    });
}

async fn refresh(client: &reqwest::Client) {
    let result = async {
        let response = client.get(PRICE_URL).send().await?.error_for_status()?;
        let prices = response.json::<HashMap<String, PriceEntry>>().await?;
        Ok::<_, reqwest::Error>(prices.get(SOL_MINT).map(|entry| entry.usd_price))
    }
    .await;

    if let Ok(Some(price)) = result {
        if price.is_finite() && price > 0.0 {
            crate::display::set_sol_usd(price);
        }
    }
}
