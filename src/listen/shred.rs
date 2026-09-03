//! Jito ShredStream Entry gRPC listener.
//!
//! The proxy performs raw-shred/FEC reconstruction and exposes serialized
//! `Vec<solana_entry::entry::Entry>` values. We turn the contained transactions
//! into the same protobuf shape used by the existing Pump decoder, then feed the
//! shared merge channel.

use crate::config::ShredCfg;
use crate::constants::{PUMP_AMM_PROGRAM_ID, PUMP_PROGRAM_ID};
use crate::listen::repair::versioned_transaction_to_update;
use crate::pump::{decode_transactions, PumpEvent};
use futures::StreamExt;
use solana_entry::entry::Entry;
use solana_sdk::transaction::VersionedTransaction;
use solana_streamer_sdk::protos::shredstream::{
    shredstream_proxy_client::ShredstreamProxyClient, SubscribeEntriesRequest,
};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Source IDs 0..61 belong to Geyser and 63 belongs to RPC repair.
pub const SHRED_SOURCE_ID: u8 = 62;
const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

pub async fn run(cfg: ShredCfg, output: mpsc::Sender<PumpEvent>) -> anyhow::Result<()> {
    let endpoint = cfg.endpoint.trim().to_owned();
    let reconnect = Duration::from_millis(cfg.reconnect_ms);

    let mut last_report = Instant::now();
    loop {
        let client = ShredstreamProxyClient::connect(endpoint.clone()).await;
        let mut client = match client {
            Ok(client) => client.max_decoding_message_size(MAX_MESSAGE_SIZE),
            Err(error) => {
                crate::telemetry::warn("ShredStream", format!("连接失败: {error}"));
                tokio::time::sleep(reconnect).await;
                continue;
            }
        };

        let response = client.subscribe_entries(SubscribeEntriesRequest {}).await;
        let mut stream = match response {
            Ok(response) => {
                crate::telemetry::info("ShredStream", format!("已连接 {endpoint}"));
                response.into_inner()
            }
            Err(error) => {
                crate::telemetry::warn("ShredStream", format!("订阅失败: {error}"));
                tokio::time::sleep(reconnect).await;
                continue;
            }
        };

        while let Some(message) = stream.next().await {
            let message = match message {
                Ok(message) => message,
                Err(error) => {
                    crate::telemetry::warn("ShredStream", format!("读取失败: {error}"));
                    break;
                }
            };
            if !process_entries(message.slot, &message.entries, &output).await? {
                return Ok(());
            }
            if last_report.elapsed() >= Duration::from_secs(10) {
                let stats = crate::listen::feed_stats();
                crate::telemetry::info(
                    "Shred统计",
                    format!(
                        "slot={} batches={} tx={} pump={} decoded={} accepted={} shred_late={} geyser_late={}",
                        stats.last_shred_slot.unwrap_or_default(),
                        stats.shred_batches,
                        stats.shred_transactions,
                        stats.shred_candidates,
                        stats.shred_events,
                        stats.shred_first,
                        stats.shred_duplicates,
                        stats.geyser_duplicates,
                    ),
                );
                last_report = Instant::now();
            }
        }

        crate::telemetry::warn("ShredStream", "连接中断，准备重连");
        tokio::time::sleep(reconnect).await;
    }
}

async fn process_entries(
    slot: u64,
    bytes: &[u8],
    output: &mpsc::Sender<PumpEvent>,
) -> anyhow::Result<bool> {
    let entries: Vec<Entry> = match wincode::deserialize(bytes) {
        Ok(entries) => entries,
        Err(error) => {
            crate::listen::record_shred_batch(slot, 0, 0, 0);
            crate::telemetry::warn(
                "ShredStream",
                format!("Entry 解码失败 slot={slot}: {error}"),
            );
            return Ok(true);
        }
    };

    let mut transaction_index = 0u64;
    let mut candidates = 0u64;
    let mut decoded = 0u64;
    for entry in entries {
        for transaction in &entry.transactions {
            let index = transaction_index;
            transaction_index = transaction_index.saturating_add(1);
            if !may_contain_pump(transaction) {
                continue;
            }
            candidates = candidates.saturating_add(1);
            let Some(update) = versioned_transaction_to_update(slot, index, transaction) else {
                continue;
            };
            for mut event in decode_transactions(slot, &update) {
                decoded = decoded.saturating_add(1);
                event.source_id = SHRED_SOURCE_ID;
                event.source_mask = 1u64 << SHRED_SOURCE_ID;
                event.replayed = false;
                event.repaired = false;
                if output.send(event).await.is_err() {
                    crate::listen::record_shred_batch(slot, transaction_index, candidates, decoded);
                    return Ok(false);
                }
            }
        }
    }
    crate::listen::record_shred_batch(slot, transaction_index, candidates, decoded);
    Ok(true)
}

#[inline]
fn may_contain_pump(transaction: &VersionedTransaction) -> bool {
    transaction
        .message
        .static_account_keys()
        .iter()
        .any(|key| key == &*PUMP_PROGRAM_ID || key == &*PUMP_AMM_PROGRAM_ID)
}

#[cfg(test)]
mod tests {
    use super::may_contain_pump;
    use crate::constants::PUMP_PROGRAM_ID;
    use solana_sdk::{
        hash::Hash,
        message::Message,
        signature::{Keypair, Signer},
        transaction::{Transaction, VersionedTransaction},
    };

    #[test]
    fn prefilter_accepts_pump_program_account() {
        let payer = Keypair::new();
        let message = Message::new_with_blockhash(
            &[solana_sdk::instruction::Instruction {
                program_id: *PUMP_PROGRAM_ID,
                accounts: vec![],
                data: vec![],
            }],
            Some(&payer.pubkey()),
            &Hash::new_unique(),
        );
        let transaction = Transaction::new_unsigned(message);
        let versioned = VersionedTransaction::from(transaction);
        assert!(may_contain_pump(&versioned));
    }
}
