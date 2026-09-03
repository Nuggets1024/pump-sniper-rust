//! Geyser slot gap 的后台 RPC block 回补任务。

use crate::pump::{decode_transactions, PumpEvent};
use solana_client::rpc_config::RpcBlockConfig;
use solana_commitment_config::CommitmentConfig;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::message::VersionedMessage;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;
use solana_transaction_status::{
    EncodedTransactionWithStatusMeta, TransactionDetails, UiInstruction, UiTransactionEncoding,
    UiTransactionStatusMeta, UiTransactionTokenBalance,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use yellowstone_grpc_proto::prelude::{
    CompiledInstruction as ProtoCompiledInstruction, InnerInstruction as ProtoInnerInstruction,
    InnerInstructions as ProtoInnerInstructions, Message as ProtoMessage,
    MessageAddressTableLookup as ProtoAddressLookup, MessageHeader as ProtoHeader,
    SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo, TokenBalance as ProtoTokenBalance,
    Transaction as ProtoTransaction, TransactionError as ProtoTransactionError,
    TransactionStatusMeta as ProtoMeta, UiTokenAmount as ProtoTokenAmount,
};

pub(crate) const MAX_GAP_SLOTS: u64 = 256;
const REPAIRED_SLOT_CACHE: usize = 100_000;
const MAX_RETRY_BACKOFF_SECS: u64 = 30;
pub const RPC_REPAIR_SOURCE_ID: u8 = 63;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct GapRange {
    pub source_id: u8,
    pub start: u64,
    pub end: u64,
}

#[derive(Clone)]
pub struct GapStore {
    path: PathBuf,
    pending: Arc<parking_lot::Mutex<Vec<GapRange>>>,
}

impl GapStore {
    pub fn open(path: PathBuf) -> Self {
        let pending = match std::fs::read(&path) {
            Ok(raw) => serde_json::from_slice(&raw).unwrap_or_else(|error| {
                crate::listen::fail_integrity(format!(
                    "pending gap 文件损坏 path={} error={error}",
                    path.display()
                ));
                Vec::new()
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => {
                crate::listen::fail_integrity(format!(
                    "pending gap 读取失败 path={} error={error}",
                    path.display()
                ));
                Vec::new()
            }
        };
        Self {
            path,
            pending: Arc::new(parking_lot::Mutex::new(pending)),
        }
    }

    /// scan 每次启动都是新会话，不继承上次的市场缺口。
    pub fn fresh(path: PathBuf) -> anyhow::Result<Self> {
        persist_gaps(&path, &[])?;
        Ok(Self {
            path,
            pending: Arc::new(parking_lot::Mutex::new(Vec::new())),
        })
    }

    pub fn pending(&self) -> Vec<GapRange> {
        self.pending.lock().clone()
    }

    pub fn add(&self, gap: GapRange) -> anyhow::Result<bool> {
        let mut pending = self.pending.lock();
        if pending.contains(&gap) {
            return Ok(false);
        }
        pending.push(gap);
        persist_gaps(&self.path, &pending)?;
        Ok(true)
    }

    pub fn complete(&self, gap: GapRange) -> anyhow::Result<()> {
        let mut pending = self.pending.lock();
        pending.retain(|candidate| *candidate != gap);
        persist_gaps(&self.path, &pending)
    }
}

fn persist_gaps(path: &PathBuf, gaps: &[GapRange]) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("pending gap 路径没有父目录"))?;
    std::fs::create_dir_all(parent)?;
    let temp = path.with_extension(format!("json.tmp.{}", std::process::id()));
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)?;
    file.write_all(&serde_json::to_vec(gaps)?)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temp, path)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

pub async fn run(
    rpc_url: String,
    mut gaps: mpsc::UnboundedReceiver<GapRange>,
    retry_tx: mpsc::UnboundedSender<GapRange>,
    events: mpsc::Sender<PumpEvent>,
    store: GapStore,
) {
    let rpc = Arc::new(crate::rpc::nonblocking(
        rpc_url,
        Duration::from_secs(30),
        CommitmentConfig::confirmed(),
    ));
    let mut repaired = HashSet::<u64>::with_capacity(REPAIRED_SLOT_CACHE);
    let mut order = VecDeque::<u64>::with_capacity(REPAIRED_SLOT_CACHE);
    let mut retry_attempts = HashMap::<GapRange, u32>::new();

    while let Some(gap) = gaps.recv().await {
        if gap.end < gap.start {
            continue;
        }
        let count = gap.end.saturating_sub(gap.start).saturating_add(1);
        if count > MAX_GAP_SLOTS {
            crate::listen::fail_integrity(format!(
                "slot gap 超过回补上限 source={} range={}..={} count={count}",
                gap.source_id, gap.start, gap.end
            ));
            crate::listen::finish_gap_repair();
            continue;
        }
        crate::telemetry::info(
            "RPC回补",
            format!(
                "开始 source={} range={}..={} slots={count}",
                gap.source_id, gap.start, gap.end
            ),
        );
        tokio::time::sleep(Duration::from_millis(800)).await;
        let produced = match fetch_produced_slots(&rpc, gap.start, gap.end).await {
            Ok(slots) => slots,
            Err(error) => {
                crate::listen::fail_integrity(format!(
                    "RPC 查询缺失区块失败 range={}..={} error={error}",
                    gap.start, gap.end
                ));
                schedule_retry(gap, &retry_tx, &mut retry_attempts);
                continue;
            }
        };
        let produced_count = produced.len();
        let mut emitted = 0u64;
        let mut succeeded = true;
        for (position, slot) in produced.into_iter().enumerate() {
            if repaired.contains(&slot) {
                continue;
            }
            match repair_slot(&rpc, slot, &events).await {
                Ok(count) => {
                    emitted += count;
                    repaired.insert(slot);
                    order.push_back(slot);
                    if order.len() > REPAIRED_SLOT_CACHE {
                        if let Some(expired) = order.pop_front() {
                            repaired.remove(&expired);
                        }
                    }
                }
                Err(error) => {
                    succeeded = false;
                    crate::listen::fail_integrity(format!(
                        "RPC 回补区块失败 slot={slot} error={error}"
                    ));
                }
            }
            if (position + 1).is_multiple_of(16) {
                crate::telemetry::info(
                    "RPC回补进度",
                    format!(
                        "range={}..{} blocks={}/{} events={emitted}",
                        gap.start,
                        gap.end,
                        position + 1,
                        produced_count
                    ),
                );
            }
        }
        crate::telemetry::info(
            "RPC回补",
            format!(
                "source={} range={}..={} blocks={produced_count} events={emitted}",
                gap.source_id, gap.start, gap.end
            ),
        );
        if succeeded {
            if let Err(error) = store.complete(gap) {
                crate::listen::fail_integrity(format!("pending gap 完成状态持久化失败: {error}"));
                schedule_retry(gap, &retry_tx, &mut retry_attempts);
                continue;
            }
            retry_attempts.remove(&gap);
            crate::listen::finish_gap_repair();
        } else {
            schedule_retry(gap, &retry_tx, &mut retry_attempts);
        }
    }
}

fn schedule_retry(
    gap: GapRange,
    retry_tx: &mpsc::UnboundedSender<GapRange>,
    attempts: &mut HashMap<GapRange, u32>,
) {
    let attempt = attempts.entry(gap).or_default();
    *attempt = attempt.saturating_add(1);
    let delay = 1u64
        .checked_shl((*attempt).min(5))
        .unwrap_or(MAX_RETRY_BACKOFF_SECS)
        .min(MAX_RETRY_BACKOFF_SECS);
    let retry_tx = retry_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(delay)).await;
        if retry_tx.send(gap).is_err() {
            crate::listen::finish_gap_repair();
            crate::listen::fail_integrity(format!(
                "RPC 回补重试队列已关闭 source={} range={}..={}",
                gap.source_id, gap.start, gap.end
            ));
        }
    });
}

async fn fetch_produced_slots(rpc: &RpcClient, start: u64, end: u64) -> anyhow::Result<Vec<u64>> {
    let mut last_error = None;
    for _ in 0..3 {
        match tokio::time::timeout(
            Duration::from_secs(10),
            rpc.get_blocks_with_commitment(start, Some(end), CommitmentConfig::confirmed()),
        )
        .await
        {
            Ok(Ok(slots)) => return Ok(slots),
            Ok(Err(error)) => last_error = Some(error.to_string()),
            Err(_) => last_error = Some("RPC getBlocks 超时".into()),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(anyhow::anyhow!(
        "{}",
        last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "unknown RPC error".into())
    ))
}

async fn repair_slot(
    rpc: &RpcClient,
    slot: u64,
    output: &mpsc::Sender<PumpEvent>,
) -> anyhow::Result<u64> {
    let block = tokio::time::timeout(
        Duration::from_secs(10),
        rpc.get_block_with_config(
            slot,
            RpcBlockConfig {
                encoding: Some(UiTransactionEncoding::Base64),
                transaction_details: Some(TransactionDetails::Full),
                rewards: Some(false),
                commitment: Some(CommitmentConfig::confirmed()),
                max_supported_transaction_version: Some(0),
            },
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("getBlock 超时"))??;
    let mut emitted = 0u64;
    for (index, transaction) in block
        .transactions
        .unwrap_or_default()
        .into_iter()
        .enumerate()
    {
        let Some(update) = rpc_transaction_to_update(slot, index as u64, transaction) else {
            continue;
        };
        for mut event in decode_transactions(slot, &update) {
            event.source_id = RPC_REPAIR_SOURCE_ID;
            event.source_mask = 1u64 << RPC_REPAIR_SOURCE_ID;
            event.replayed = true;
            event.repaired = true;
            output
                .send(event)
                .await
                .map_err(|_| anyhow::anyhow!("事件合并通道已关闭"))?;
            emitted += 1;
        }
    }
    Ok(emitted)
}

fn rpc_transaction_to_update(
    slot: u64,
    index: u64,
    encoded: EncodedTransactionWithStatusMeta,
) -> Option<SubscribeUpdateTransaction> {
    let transaction = encoded.transaction.decode()?;
    let signature = transaction.signatures.first()?.as_ref().to_vec();
    let message = proto_message(&transaction.message);
    let meta = encoded.meta.map(proto_meta);
    Some(SubscribeUpdateTransaction {
        slot,
        transaction: Some(SubscribeUpdateTransactionInfo {
            signature,
            is_vote: false,
            transaction: Some(ProtoTransaction {
                signatures: transaction
                    .signatures
                    .iter()
                    .map(|signature| signature.as_ref().to_vec())
                    .collect(),
                message: Some(message),
            }),
            meta,
            index,
        }),
    })
}

pub(crate) fn versioned_transaction_to_update(
    slot: u64,
    index: u64,
    transaction: &VersionedTransaction,
) -> Option<SubscribeUpdateTransaction> {
    let signature = transaction.signatures.first()?.as_ref().to_vec();
    Some(SubscribeUpdateTransaction {
        slot,
        transaction: Some(SubscribeUpdateTransactionInfo {
            signature,
            is_vote: false,
            transaction: Some(ProtoTransaction {
                signatures: transaction
                    .signatures
                    .iter()
                    .map(|signature| signature.as_ref().to_vec())
                    .collect(),
                message: Some(proto_message(&transaction.message)),
            }),
            // Shred/Entry 是执行前数据，没有 fee、logs、CPI 或 loaded addresses。
            meta: None,
            index,
        }),
    })
}

fn proto_message(message: &VersionedMessage) -> ProtoMessage {
    let header = message.header();
    let (versioned, lookups) = match message {
        VersionedMessage::Legacy(_) => (false, Vec::new()),
        VersionedMessage::V0(message) => (
            true,
            message
                .address_table_lookups
                .iter()
                .map(|lookup| ProtoAddressLookup {
                    account_key: lookup.account_key.to_bytes().to_vec(),
                    writable_indexes: lookup.writable_indexes.clone(),
                    readonly_indexes: lookup.readonly_indexes.clone(),
                })
                .collect(),
        ),
        VersionedMessage::V1(_) => (true, Vec::new()),
    };
    ProtoMessage {
        header: Some(ProtoHeader {
            num_required_signatures: header.num_required_signatures.into(),
            num_readonly_signed_accounts: header.num_readonly_signed_accounts.into(),
            num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts.into(),
        }),
        account_keys: message
            .static_account_keys()
            .iter()
            .map(|key| key.to_bytes().to_vec())
            .collect(),
        recent_blockhash: message.recent_blockhash().to_bytes().to_vec(),
        instructions: message
            .instructions()
            .iter()
            .map(|instruction| ProtoCompiledInstruction {
                program_id_index: instruction.program_id_index.into(),
                accounts: instruction.accounts.clone(),
                data: instruction.data.clone(),
            })
            .collect(),
        versioned,
        address_table_lookups: lookups,
        config: None,
    }
}

fn proto_meta(meta: UiTransactionStatusMeta) -> ProtoMeta {
    let inner_none = !meta.inner_instructions.is_some();
    let logs_none = !meta.log_messages.is_some();
    let loaded = meta.loaded_addresses.unwrap_or(Default::default());
    ProtoMeta {
        err: meta.err.map(|error| ProtoTransactionError {
            err: bincode::serialize(&error).unwrap_or_default(),
        }),
        fee: meta.fee,
        pre_balances: meta.pre_balances,
        post_balances: meta.post_balances,
        inner_instructions: meta
            .inner_instructions
            .unwrap_or(Vec::new())
            .into_iter()
            .map(|inner| ProtoInnerInstructions {
                index: inner.index.into(),
                instructions: inner
                    .instructions
                    .into_iter()
                    .filter_map(|instruction| match instruction {
                        UiInstruction::Compiled(instruction) => Some(ProtoInnerInstruction {
                            program_id_index: instruction.program_id_index.into(),
                            accounts: instruction.accounts,
                            data: bs58::decode(instruction.data).into_vec().ok()?,
                            stack_height: instruction.stack_height,
                        }),
                        UiInstruction::Parsed(_) => None,
                    })
                    .collect(),
            })
            .collect(),
        inner_instructions_none: inner_none,
        log_messages: meta.log_messages.unwrap_or(Vec::new()),
        log_messages_none: logs_none,
        pre_token_balances: meta
            .pre_token_balances
            .unwrap_or(Vec::new())
            .into_iter()
            .map(proto_token_balance)
            .collect(),
        post_token_balances: meta
            .post_token_balances
            .unwrap_or(Vec::new())
            .into_iter()
            .map(proto_token_balance)
            .collect(),
        rewards: Vec::new(),
        loaded_writable_addresses: loaded
            .writable
            .into_iter()
            .filter_map(|key| Pubkey::from_str(&key).ok())
            .map(|key| key.to_bytes().to_vec())
            .collect(),
        loaded_readonly_addresses: loaded
            .readonly
            .into_iter()
            .filter_map(|key| Pubkey::from_str(&key).ok())
            .map(|key| key.to_bytes().to_vec())
            .collect(),
        return_data: None,
        return_data_none: true,
        compute_units_consumed: meta.compute_units_consumed.map(|value| value),
        cost_units: None,
    }
}

fn proto_token_balance(balance: UiTransactionTokenBalance) -> ProtoTokenBalance {
    ProtoTokenBalance {
        account_index: balance.account_index.into(),
        mint: balance.mint,
        ui_token_amount: Some(ProtoTokenAmount {
            ui_amount: balance.ui_token_amount.ui_amount.unwrap_or_default(),
            decimals: balance.ui_token_amount.decimals.into(),
            amount: balance.ui_token_amount.amount,
            ui_amount_string: balance.ui_token_amount.ui_amount_string,
        }),
        owner: balance.owner.unwrap_or(String::new()),
        program_id: balance.program_id.unwrap_or(String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::instruction::Instruction;
    use solana_sdk::message::Message;

    #[test]
    fn legacy_message_converts_to_yellowstone_shape() {
        let payer = Pubkey::new_unique();
        let program = Pubkey::new_unique();
        let message = VersionedMessage::Legacy(Message::new(
            &[Instruction {
                program_id: program,
                accounts: Vec::new(),
                data: vec![1, 2, 3],
            }],
            Some(&payer),
        ));
        let converted = proto_message(&message);
        assert!(!converted.versioned);
        assert_eq!(converted.account_keys[0], payer.to_bytes());
        assert_eq!(converted.instructions[0].data, vec![1, 2, 3]);
    }

    #[test]
    fn pending_gap_survives_restart_until_completed() {
        let path = std::env::temp_dir().join(format!(
            "pump-sniper-gaps-{}-{}.json",
            std::process::id(),
            rand::random::<u64>()
        ));
        let gap = GapRange {
            source_id: 1,
            start: 10,
            end: 20,
        };
        let store = GapStore::open(path.clone());
        assert!(store.add(gap).unwrap());
        assert!(!store.add(gap).unwrap());
        assert_eq!(GapStore::open(path.clone()).pending(), vec![gap]);
        store.complete(gap).unwrap();
        assert!(GapStore::open(path.clone()).pending().is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn fresh_gap_store_discards_previous_session() {
        let path = std::env::temp_dir().join(format!(
            "pump-sniper-fresh-gaps-{}-{}.json",
            std::process::id(),
            rand::random::<u64>()
        ));
        let gap = GapRange {
            source_id: 0,
            start: 100,
            end: 200,
        };
        let old = GapStore::open(path.clone());
        old.add(gap).unwrap();

        let fresh = GapStore::fresh(path.clone()).unwrap();
        assert!(fresh.pending().is_empty());
        assert!(GapStore::open(path.clone()).pending().is_empty());
        let _ = std::fs::remove_file(path);
    }
}
