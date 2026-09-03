//! 单源事件去重：仅按 (signature, mint) key 集合去重，避免 Geyser 重连
//! 边界 slot 同一笔事件被推送两次。多源指纹校验已移除，依赖单 Geyser。

use crate::pump::PumpEvent;
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashSet, VecDeque};
use tokio::sync::mpsc;

const DEDUP_CACHE_CAPACITY: usize = 100_000;

pub async fn run(
    mut input: mpsc::Receiver<PumpEvent>,
    output: mpsc::Sender<PumpEvent>,
) -> anyhow::Result<()> {
    let mut seen = HashSet::<(String, Pubkey)>::with_capacity(DEDUP_CACHE_CAPACITY);
    let mut order = VecDeque::<(String, Pubkey)>::with_capacity(DEDUP_CACHE_CAPACITY);
    while let Some(event) = input.recv().await {
        let key = (event.signature.clone(), event.mint);
        if !seen.insert(key.clone()) {
            continue;
        }
        order.push_back(key);
        if order.len() > DEDUP_CACHE_CAPACITY {
            if let Some(expired) = order.pop_front() {
                seen.remove(&expired);
            }
        }
        if output.send(event).await.is_err() {
            break;
        }
    }
    Ok(())
}
