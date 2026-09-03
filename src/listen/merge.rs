//! 多源事件去重：按 (signature, mint) 去重，避免 ShredStream 与 Geyser
//! 重复触发策略，也覆盖 Geyser 重连边界的重复推送。

use crate::pump::PumpEvent;
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;

const DEDUP_CACHE_CAPACITY: usize = 100_000;

pub async fn run(
    mut input: mpsc::Receiver<PumpEvent>,
    output: mpsc::Sender<PumpEvent>,
) -> anyhow::Result<()> {
    // value: 首报来源，以及 Shred 首报后是否已转发过一次完整来源用于核账。
    let mut seen = HashMap::<(String, Pubkey), (u8, bool)>::with_capacity(DEDUP_CACHE_CAPACITY);
    let mut order = VecDeque::<(String, Pubkey)>::with_capacity(DEDUP_CACHE_CAPACITY);
    while let Some(mut event) = input.recv().await {
        let key = (event.signature.clone(), event.mint);
        if let Some((first_source, enriched)) = seen.get_mut(&key) {
            super::record_feed_result(event.source_id, true);
            // Entry/Shred 没有 transaction meta 和 inner logs。若它抢到首报，必须让随后
            // 的 Geyser/RPC 完整事件继续进入引擎核账；标为 replayed 可禁止二次触发策略。
            if should_forward_enrichment(*first_source, event.source_id, *enriched) {
                *enriched = true;
                event.replayed = true;
                if output.send(event).await.is_err() {
                    break;
                }
            }
            continue;
        }
        super::record_feed_result(event.source_id, false);
        seen.insert(key.clone(), (event.source_id, false));
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

fn should_forward_enrichment(first_source: u8, incoming_source: u8, enriched: bool) -> bool {
    first_source == super::shred::SHRED_SOURCE_ID
        && incoming_source != super::shred::SHRED_SOURCE_ID
        && !enriched
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn full_event_is_forwarded_once_after_shred_first_report() {
        let (input_tx, input_rx) = mpsc::channel(4);
        let (output_tx, mut output_rx) = mpsc::channel(4);
        let task = tokio::spawn(run(input_rx, output_tx));
        let shred = event(super::super::shred::SHRED_SOURCE_ID);
        let geyser = event(0);
        input_tx.send(shred).await.unwrap();
        input_tx.send(geyser).await.unwrap();
        input_tx.send(event(1)).await.unwrap();
        drop(input_tx);

        let first = output_rx.recv().await.unwrap();
        let enrichment = output_rx.recv().await.unwrap();
        assert_eq!(first.source_id, super::super::shred::SHRED_SOURCE_ID);
        assert!(!first.replayed);
        assert_eq!(enrichment.source_id, 0);
        assert!(enrichment.replayed, "核账事件不得二次触发策略");
        assert!(output_rx.recv().await.is_none(), "完整事件只转发一次");
        task.await.unwrap().unwrap();
    }

    fn event(source_id: u8) -> PumpEvent {
        PumpEvent {
            source_id,
            source_mask: 1u64 << source_id,
            replayed: false,
            repaired: false,
            slot: 443_949_209,
            transaction_index: 9,
            signature: "same-signature".into(),
            fee_lamports: 0,
            signer: Pubkey::new_unique(),
            mint: Pubkey::new_from_array([7; 32]),
            bonding_curve: Pubkey::new_unique(),
            creator: Pubkey::new_unique(),
            token_program: spl_token::ID,
            quote_mint: spl_token::native_mint::ID,
            quote_decimals: Some(9),
            name: None,
            symbol: None,
            uri: None,
            kind: crate::pump::PumpIxKind::BuyExactSolIn,
            buy_quote_amount: Some(5_000_000),
            buy_instruction_count: 1,
            buys: vec![],
            sells: vec![],
            jito_tip_lamports: None,
            jito_dont_front: false,
            is_create: false,
            is_buy: true,
            is_sell: false,
            seen_ns: 0,
        }
    }

    #[test]
    fn shred_duplicate_is_not_forwarded_after_geyser_first_report() {
        assert!(!should_forward_enrichment(
            0,
            super::super::shred::SHRED_SOURCE_ID,
            false
        ));
    }
}
