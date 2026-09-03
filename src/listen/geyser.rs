use crate::config::{AppConfig, BotMode, CommitmentCfg};
use crate::constants::{PUMP_AMM_PROGRAM, PUMP_MINT_AUTHORITY, PUMP_PROGRAM};
use crate::listen::repair::{GapRange, GapStore};
use crate::pump::{contains_create_instruction, decode_transactions, PumpEvent};
use crate::strategy::ScanTarget;
use anyhow::Context;
use futures::{SinkExt, StreamExt};
use solana_commitment_config::CommitmentConfig;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use yellowstone_grpc_client::{GeyserGrpcClient, Interceptor};
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions, SubscribeRequestPing,
};

/// Geyser actor 的最小配置；刻意不包含钱包路径和落地供应商密钥。
#[derive(Clone)]
pub struct RuntimeConfig {
    endpoint: String,
    x_token: String,
    commitment: CommitmentCfg,
    reconnect_ms: u64,
    log_directory: String,
    rpc_url: String,
    direct_create: bool,
    fresh_session: bool,
    follow_addresses: Vec<String>,
    max_event_slot_lag: u64,
}

impl RuntimeConfig {
    pub fn new(cfg: &AppConfig, endpoint: String, x_token: String) -> Self {
        Self {
            endpoint,
            x_token,
            commitment: cfg.geyser.commitment.clone(),
            reconnect_ms: cfg.geyser.reconnect_ms,
            log_directory: cfg.log.directory.clone(),
            rpc_url: cfg.rpc.url.clone(),
            direct_create: cfg.follow.is_empty(),
            fresh_session: cfg.bot.mode == BotMode::Scan,
            follow_addresses: cfg
                .follow
                .iter()
                .map(|follow| follow.address.clone())
                .collect(),
            max_event_slot_lag: cfg.landing.lighthouse_slot_slack,
        }
    }
}

pub async fn run(
    cfg: RuntimeConfig,
    source_id: u8,
    watched_wallet: solana_sdk::pubkey::Pubkey,
    tx: mpsc::Sender<PumpEvent>,
    gaps: mpsc::UnboundedSender<GapRange>,
    gap_store: GapStore,
    scan_target: Option<watch::Receiver<Vec<ScanTarget>>>,
) -> anyhow::Result<()> {
    let cursor_path = std::path::Path::new(&cfg.log_directory)
        .join("cursors")
        .join(format!("geyser-{source_id}.json"));
    let (persisted_slot, cursor_tx) = crate::listen::cursor::start(cursor_path, !cfg.fresh_session);
    let mut cursor = StreamCursor {
        last_slot: persisted_slot,
        ..Default::default()
    };
    let rpc_tip = if cfg.fresh_session {
        let initial_tip = fetch_rpc_tip(&cfg).await.ok();
        Some(spawn_rpc_tip_sampler(cfg.rpc_url.clone(), initial_tip))
    } else {
        None
    };
    loop {
        if cfg.fresh_session {
            // scan 重连也只接实时流，不回放、不为停机期创建 gap。
            cursor.last_slot = None;
            cursor.skip_replay_once = false;
            cursor_tx.send_replace(None);
        }
        match connect_once(
            &cfg,
            source_id,
            watched_wallet,
            &tx,
            &gaps,
            &gap_store,
            &cursor_tx,
            &mut cursor,
            scan_target.as_ref(),
            rpc_tip.as_ref(),
        )
        .await
        {
            Ok(()) => crate::telemetry::warn("重连", "Geyser 流结束"),
            Err(e) => {
                let message = format!("{e:#}");
                if let Some(floor) = replay_floor(&message) {
                    if cfg.fresh_session {
                        cursor.last_slot = None;
                        cursor.skip_replay_once = false;
                        cursor_tx.send_replace(None);
                        crate::telemetry::warn(
                            "实时重连",
                            format!("source={source_id} replay_floor={floor}，scan 忽略历史缺口"),
                        );
                        tokio::time::sleep(Duration::from_millis(cfg.reconnect_ms.max(200))).await;
                        continue;
                    }
                    if let Some(last) = cursor.last_slot {
                        if floor > last.saturating_add(1) {
                            enqueue_gap_chunks(
                                source_id,
                                last.saturating_add(1),
                                floor - 1,
                                &gaps,
                                &gap_store,
                            );
                        }
                    }
                    // 下一次直接接实时流；把 cursor 留在 floor 前一位，首个实时 slot
                    // 会把 replay floor 到实时尖端之间的全部区间交给 RPC 回补。
                    let resume_cursor = resume_cursor_after_floor(cursor.last_slot, floor);
                    cursor.last_slot = Some(resume_cursor);
                    cursor.skip_replay_once = true;
                    cursor_tx.send_replace(Some(resume_cursor));
                    crate::telemetry::warn(
                        "重放越界",
                        format!("source={source_id} replay_floor={floor}，缺口已交给RPC回补"),
                    );
                } else {
                    crate::telemetry::error_fields(
                        "失败",
                        ["Geyser".to_owned(), source_id.to_string(), message],
                    );
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(cfg.reconnect_ms.max(200))).await;
    }
}

async fn connect_once(
    cfg: &RuntimeConfig,
    source_id: u8,
    watched_wallet: solana_sdk::pubkey::Pubkey,
    out: &mpsc::Sender<PumpEvent>,
    gaps: &mpsc::UnboundedSender<GapRange>,
    gap_store: &GapStore,
    cursor_tx: &watch::Sender<Option<u64>>,
    cursor: &mut StreamCursor,
    scan_target: Option<&watch::Receiver<Vec<ScanTarget>>>,
    rpc_tip: Option<&watch::Receiver<Option<u64>>>,
) -> anyhow::Result<()> {
    let mut builder =
        GeyserGrpcClient::build_from_shared(cfg.endpoint.clone()).context("Geyser 构建器")?;
    if !cfg.x_token.is_empty() {
        builder = builder.x_token(Some(cfg.x_token.clone()))?;
    }
    let mut client = builder.connect().await.context("连接 Geyser")?;

    let (mut sink, mut stream) = client.subscribe().await.context("订阅")?;

    let accounts = subscription_accounts(cfg, watched_wallet);

    let commitment = match cfg.commitment {
        CommitmentCfg::Finalized => CommitmentLevel::Finalized,
        CommitmentCfg::Confirmed => CommitmentLevel::Confirmed,
        CommitmentCfg::Processed => CommitmentLevel::Processed,
    };

    let commitment = commitment as i32;
    let replay_was_skipped = cursor.skip_replay_once;
    cursor.skip_replay_once = false;
    let replay_until = if !cfg.fresh_session && cursor.last_slot.is_some() && !replay_was_skipped {
        fetch_rpc_tip(cfg).await.ok()
    } else {
        None
    };
    let replay_from = replay_until.and(cursor.last_slot.map(safe_replay_slot));
    if let Some(slot) = replay_from {
        crate::telemetry::info(
            "恢复订阅",
            format!(
                "from_slot={slot}  replay_until={}",
                replay_until.expect("replay_from requires replay_until")
            ),
        );
    } else if cursor.last_slot.is_some() && !replay_was_skipped {
        crate::telemetry::warn(
            "恢复订阅",
            "RPC tip 不可得，为避免历史信号触发下单，本次不重放",
        );
    }
    let discovery_accounts = accounts;
    let required_accounts = discovery_required_accounts(cfg);
    let mut scan_target = scan_target.cloned();
    let mut active_targets = scan_target
        .as_ref()
        .map(|target| target.borrow().clone())
        .unwrap_or_default();
    let target_accounts = active_targets
        .iter()
        .map(|target| target.mint.to_string())
        .collect();
    let request_from_slot = active_targets
        .iter()
        .map(|target| target.create_slot)
        .min()
        .or(replay_from);
    sink.send(subscription_request(
        discovery_accounts.clone(),
        required_accounts.clone(),
        target_accounts,
        commitment,
        request_from_slot,
    ))
    .await
    .context("发送订阅")?;

    crate::telemetry::info_fields(
        "订阅",
        [
            "Geyser".to_owned(),
            source_id.to_string(),
            "active".to_owned(),
        ],
    );

    loop {
        let maybe_msg = tokio::select! {
            biased;
            changed = async {
                match scan_target.as_mut() {
                    Some(target) => target.changed().await,
                    None => std::future::pending().await,
                }
            }, if scan_target.is_some() => {
                changed.context("target mint 通道已关闭")?;
                let targets = scan_target
                    .as_ref()
                    .map(|target| target.borrow().clone())
                    .unwrap_or_default();
                let new_from_slot = targets
                    .iter()
                    .filter(|target| !active_targets.iter().any(|active| active.mint == target.mint))
                    .map(|target| target.create_slot)
                    .min();
                if targets == active_targets {
                    continue;
                }
                sink.send(subscription_request(
                    discovery_accounts.clone(),
                    required_accounts.clone(),
                    targets.iter().map(|target| target.mint.to_string()).collect(),
                    commitment,
                    new_from_slot,
                ))
                .await
                .context("单连接追加目标 mint 订阅")?;
                active_targets = targets;
                crate::telemetry::info_fields(
                    "订阅",
                    [
                        "target".to_owned(),
                        source_id.to_string(),
                        active_targets.len().to_string(),
                        new_from_slot.map_or_else(|| "实时".to_owned(), |slot| slot.to_string()),
                    ],
                );
                continue;
            }
            message = stream.next() => message,
        };
        let Some(msg) = maybe_msg else { break };
        let upd = match msg {
            Ok(u) => u,
            Err(e) => return Err(e.into()),
        };
        match upd.update_oneof {
            Some(UpdateOneof::Ping(_)) => {
                let _ = sink
                    .send(SubscribeRequest {
                        ping: Some(SubscribeRequestPing { id: 1 }),
                        ..Default::default()
                    })
                    .await;
            }
            Some(UpdateOneof::Slot(slot_update)) => {
                let slot = slot_update.slot;
                crate::admin::set_latest_slot(slot);
                if let Some(last) = cursor.last_slot.filter(|_| !cfg.fresh_session) {
                    if slot > last.saturating_add(1) {
                        enqueue_gap_chunks(
                            source_id,
                            last.saturating_add(1),
                            slot.saturating_sub(1),
                            gaps,
                            gap_store,
                        );
                    }
                }
                if cursor.last_slot.is_none_or(|last| slot > last) {
                    cursor.last_slot = Some(slot);
                    cursor_tx.send_replace(Some(slot));
                }
            }
            Some(UpdateOneof::Transaction(txu)) => {
                let slot = txu.slot;
                crate::admin::set_latest_slot(slot);
                if cfg.fresh_session {
                    if active_targets.is_empty() {
                        if !contains_create_instruction(&txu) {
                            continue;
                        }
                        let Some(tip) = rpc_tip.and_then(|tip| *tip.borrow()) else {
                            crate::telemetry::warn("CREATE检查", "RPC tip 不可得，跳过下单");
                            continue;
                        };
                        let lag = tip.saturating_sub(slot);
                        if lag > cfg.max_event_slot_lag {
                            crate::telemetry::warn(
                                "过期CREATE",
                                format!(
                                    "source={source_id} event_slot={slot} rpc_tip={tip} lag={lag}，跳过下单"
                                ),
                            );
                            continue;
                        }
                    }
                }
                let replaying = replay_until.is_some_and(|tip| slot <= tip);
                if cursor.last_slot.is_none_or(|last| slot > last) {
                    cursor.last_slot = Some(slot);
                    cursor_tx.send_replace(Some(slot));
                }
                let raw_signature = txu
                    .transaction
                    .as_ref()
                    .map(|info| bs58::encode(&info.signature).into_string())
                    .unwrap_or_default();
                if !raw_signature.is_empty() && !cursor.seen.insert(raw_signature) {
                    continue;
                }
                let decoded = decode_transactions(slot, &txu);
                for mut ev in decoded {
                    if !active_targets.is_empty()
                        && !ev.is_create
                        && !active_targets.iter().any(|target| ev.mint == target.mint)
                    {
                        continue;
                    }
                    ev.source_id = source_id;
                    ev.source_mask = 1u64.checked_shl(source_id.into()).unwrap_or(0);
                    ev.replayed = replaying;
                    if out.send(ev).await.is_err() {
                        return Err(anyhow::anyhow!("事件处理通道已关闭"));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// scan 的第二条独立连接：启动时仅保持轻量 slot 流，锁定后切换为目标 mint。
pub async fn run_scan_target(
    cfg: RuntimeConfig,
    source_id: u8,
    target_rx: watch::Receiver<Vec<ScanTarget>>,
    out: mpsc::Sender<PumpEvent>,
) -> anyhow::Result<()> {
    loop {
        match connect_scan_target_once(&cfg, source_id, target_rx.clone(), &out).await {
            Ok(()) => {
                crate::telemetry::warn("目标重连", "目标 mint Geyser 流结束");
            }
            Err(error) => {
                let message = format!("{error:#}");
                crate::telemetry::error_fields(
                    "失败",
                    ["目标监听".to_owned(), source_id.to_string(), message],
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(cfg.reconnect_ms.max(200))).await;
    }
}

async fn connect_scan_target_once(
    cfg: &RuntimeConfig,
    source_id: u8,
    mut target_rx: watch::Receiver<Vec<ScanTarget>>,
    out: &mpsc::Sender<PumpEvent>,
) -> anyhow::Result<()> {
    let mut builder =
        GeyserGrpcClient::build_from_shared(cfg.endpoint.clone()).context("目标 Geyser 构建器")?;
    if !cfg.x_token.is_empty() {
        builder = builder.x_token(Some(cfg.x_token.clone()))?;
    }
    let mut client = builder.connect().await.context("连接目标 Geyser")?;
    let (mut sink, mut stream) = client.subscribe().await.context("订阅目标 Geyser")?;
    let commitment = match cfg.commitment {
        CommitmentCfg::Finalized => CommitmentLevel::Finalized,
        CommitmentCfg::Confirmed => CommitmentLevel::Confirmed,
        CommitmentCfg::Processed => CommitmentLevel::Processed,
    } as i32;
    sink.send(slot_only_request(commitment)).await?;
    crate::telemetry::info_fields(
        "订阅",
        [
            "target".to_owned(),
            source_id.to_string(),
            "standby".to_owned(),
        ],
    );

    let mut active_targets = loop {
        if !target_rx.borrow().is_empty() {
            break target_rx.borrow().clone();
        }
        tokio::select! {
            changed = target_rx.changed() => changed.context("scan target 通道已关闭")?,
            update = stream.next() => {
                let Some(update) = update else { anyhow::bail!("目标 Geyser 流结束") };
                if matches!(update?.update_oneof, Some(UpdateOneof::Ping(_))) {
                    let _ = sink.send(SubscribeRequest {
                        ping: Some(SubscribeRequestPing { id: 1 }),
                        ..Default::default()
                    }).await;
                }
            }
        }
    };

    sink.send(subscription_request(
        vec![],
        vec![],
        active_targets
            .iter()
            .map(|target| target.mint.to_string())
            .collect(),
        commitment,
        None,
    ))
    .await
    .context("切换目标 mint 订阅")?;
    crate::telemetry::info_fields(
        "订阅",
        [
            "target".to_owned(),
            source_id.to_string(),
            active_targets.len().to_string(),
            "实时".to_owned(),
        ],
    );

    let mut seen = SeenSignatures::default();
    loop {
        let maybe_update = tokio::select! {
            biased;
            changed = target_rx.changed() => {
                changed.context("scan target 通道已关闭")?;
                let targets = target_rx.borrow().clone();
                if targets == active_targets {
                    continue;
                }
                if targets.is_empty() {
                    sink.send(slot_only_request(commitment)).await.context("切换目标 standby")?;
                    active_targets = targets;
                    seen = SeenSignatures::default();
                    crate::telemetry::info_fields(
                        "订阅",
                        ["target".to_owned(), source_id.to_string(), "standby".to_owned()],
                    );
                    continue;
                }
                sink.send(subscription_request(
                    vec![],
                    vec![],
                    targets.iter().map(|target| target.mint.to_string()).collect(),
                    commitment,
                    None,
                ))
                .await
                .context("切换目标 mint 订阅")?;
                active_targets = targets;
                seen = SeenSignatures::default();
                crate::telemetry::info_fields(
                    "订阅",
                    [
                        "target".to_owned(),
                        source_id.to_string(),
                        active_targets.len().to_string(),
                        "实时".to_owned(),
                    ],
                );
                continue;
            }
            update = stream.next() => update,
        };
        let Some(update) = maybe_update else { break };
        match update?.update_oneof {
            Some(UpdateOneof::Ping(_)) => {
                let _ = sink
                    .send(SubscribeRequest {
                        ping: Some(SubscribeRequestPing { id: 1 }),
                        ..Default::default()
                    })
                    .await;
            }
            Some(UpdateOneof::Slot(slot_update)) => {
                crate::admin::set_latest_slot(slot_update.slot);
            }
            Some(UpdateOneof::Transaction(txu)) => {
                crate::admin::set_latest_slot(txu.slot);
                let raw_signature = txu
                    .transaction
                    .as_ref()
                    .map(|info| bs58::encode(&info.signature).into_string())
                    .unwrap_or_default();
                if !raw_signature.is_empty() && !seen.insert(raw_signature) {
                    continue;
                }
                for mut event in decode_transactions(txu.slot, &txu) {
                    if !active_targets
                        .iter()
                        .any(|target| event.mint == target.mint)
                    {
                        continue;
                    }
                    event.source_id = source_id;
                    event.source_mask = 1u64.checked_shl(source_id.into()).unwrap_or(0);
                    event.replayed = false;
                    if out.send(event).await.is_err() {
                        return Err(anyhow::anyhow!("目标事件处理通道已关闭"));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn replay_floor(message: &str) -> Option<u64> {
    let (_, suffix) = message.split_once("last available:")?;
    suffix
        .trim_start()
        .split(|character: char| !character.is_ascii_digit())
        .next()?
        .parse()
        .ok()
}

fn safe_replay_slot(last_slot: u64) -> u64 {
    last_slot.saturating_sub(CURSOR_REPLAY_OVERLAP_SLOTS)
}

fn resume_cursor_after_floor(last_slot: Option<u64>, floor: u64) -> u64 {
    last_slot.unwrap_or_default().max(floor.saturating_sub(1))
}

fn enqueue_gap_chunks(
    source_id: u8,
    mut start: u64,
    end: u64,
    gaps: &mpsc::UnboundedSender<GapRange>,
    gap_store: &GapStore,
) {
    while start <= end {
        let chunk_end = end.min(start.saturating_add(crate::listen::repair::MAX_GAP_SLOTS - 1));
        let gap = GapRange {
            source_id,
            start,
            end: chunk_end,
        };
        enqueue_gap(gap, gaps, gap_store);
        start = chunk_end.saturating_add(1);
    }
}

fn enqueue_gap(gap: GapRange, gaps: &mpsc::UnboundedSender<GapRange>, gap_store: &GapStore) {
    match gap_store.add(gap) {
        Ok(false) => return,
        Err(error) => {
            crate::listen::fail_integrity(format!("pending gap 持久化失败: {error}"));
            return;
        }
        Ok(true) => {}
    }
    crate::listen::begin_gap_repair();
    if gaps.send(gap).is_err() {
        crate::listen::finish_gap_repair();
        crate::listen::fail_integrity(format!(
            "RPC 回补队列已满 source={} range={}..={}",
            gap.source_id, gap.start, gap.end
        ));
    }
}

async fn fetch_rpc_tip(cfg: &RuntimeConfig) -> anyhow::Result<u64> {
    let url = cfg.rpc_url.clone();
    tokio::task::spawn_blocking(move || {
        crate::rpc::blocking(url, Duration::from_secs(8), CommitmentConfig::processed())
            .get_slot()
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    })
    .await
    .context("等待 RPC slot 任务")?
    .context("读取 RPC slot")
}

fn spawn_rpc_tip_sampler(
    rpc_url: String,
    initial_tip: Option<u64>,
) -> watch::Receiver<Option<u64>> {
    let (tx, rx) = watch::channel(initial_tip);
    let client = std::sync::Arc::new(crate::rpc::blocking(
        rpc_url,
        Duration::from_secs(8),
        CommitmentConfig::processed(),
    ));
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let client = client.clone();
            if let Ok(Ok(slot)) = tokio::task::spawn_blocking(move || client.get_slot()).await {
                tx.send_replace(Some(slot));
            }
            if tx.is_closed() {
                break;
            }
        }
    });
    rx
}

fn slot_only_request(commitment: i32) -> SubscribeRequest {
    let mut slots = HashMap::new();
    slots.insert(
        "standby".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
            interslot_updates: Some(false),
        },
    );
    SubscribeRequest {
        slots,
        commitment: Some(commitment),
        ..Default::default()
    }
}

fn subscription_request(
    accounts: Vec<String>,
    required_accounts: Vec<String>,
    target_accounts: Vec<String>,
    commitment: i32,
    from_slot: Option<u64>,
) -> SubscribeRequest {
    let mut slots = HashMap::new();
    slots.insert(
        "integrity".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
            interslot_updates: Some(false),
        },
    );
    let mut transactions = HashMap::new();
    if !accounts.is_empty() || !required_accounts.is_empty() {
        transactions.insert(
            "create".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include: accounts,
                account_exclude: vec![],
                account_required: required_accounts,
                cuckoo_account_include: None,
                token_accounts: None,
            },
        );
    }
    if !target_accounts.is_empty() {
        transactions.insert(
            "targets".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include: target_accounts,
                account_exclude: vec![],
                account_required: vec![],
                cuckoo_account_include: None,
                token_accounts: None,
            },
        );
    }
    SubscribeRequest {
        slots,
        transactions,
        commitment: Some(commitment),
        from_slot,
        ..Default::default()
    }
}

fn discovery_required_accounts(cfg: &RuntimeConfig) -> Vec<String> {
    (cfg.fresh_session || !cfg.follow_addresses.is_empty())
        .then(|| vec![PUMP_MINT_AUTHORITY.to_string()])
        .unwrap_or_default()
}

const SEEN_SIGNATURE_CAPACITY: usize = 100_000;
/// Geyser 的 slot/transaction 通知可能乱序；恢复时回退一小段安全窗口。
/// 回放事件带 `replayed=true`，只用于补齐状态和审计，不触发新策略。
const CURSOR_REPLAY_OVERLAP_SLOTS: u64 = 64;

#[derive(Default)]
struct StreamCursor {
    last_slot: Option<u64>,
    skip_replay_once: bool,
    seen: SeenSignatures,
}

struct SeenSignatures {
    values: HashSet<String>,
    order: VecDeque<String>,
    capacity: usize,
}

impl Default for SeenSignatures {
    fn default() -> Self {
        Self {
            values: HashSet::with_capacity(SEEN_SIGNATURE_CAPACITY),
            order: VecDeque::with_capacity(SEEN_SIGNATURE_CAPACITY),
            capacity: SEEN_SIGNATURE_CAPACITY,
        }
    }
}

impl SeenSignatures {
    fn insert(&mut self, signature: String) -> bool {
        if !self.values.insert(signature.clone()) {
            return false;
        }
        self.order.push_back(signature);
        if self.order.len() > self.capacity {
            if let Some(expired) = self.order.pop_front() {
                self.values.remove(&expired);
            }
        }
        true
    }
}

fn subscription_accounts(
    cfg: &RuntimeConfig,
    watched_wallet: solana_sdk::pubkey::Pubkey,
) -> Vec<String> {
    let mut accounts = if cfg.direct_create && cfg.fresh_session {
        // scan 发现流只需要 Pump 主程序；PumpSwap 不会产生 create 信号。
        vec![PUMP_PROGRAM.to_string()]
    } else if cfg.direct_create {
        vec![PUMP_PROGRAM.to_string(), PUMP_AMM_PROGRAM.to_string()]
    } else {
        cfg.follow_addresses.clone()
    };
    if !cfg.fresh_session && watched_wallet != solana_sdk::pubkey::Pubkey::default() {
        accounts.push(watched_wallet.to_string());
    }
    accounts.sort_unstable();
    accounts.dedup();
    accounts
}

// 压住未使用的 Interceptor 导入（类型推断需要）
#[allow(dead_code)]
fn _interceptor_bound<I: Interceptor>(_: I) {}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;

    fn runtime_config(direct_create: bool, follow_addresses: Vec<String>) -> RuntimeConfig {
        RuntimeConfig {
            endpoint: "https://geyser.example".into(),
            x_token: String::new(),
            commitment: CommitmentCfg::Processed,
            reconnect_ms: 500,
            log_directory: "logs".into(),
            rpc_url: "https://rpc.example".into(),
            direct_create,
            fresh_session: false,
            follow_addresses,
            max_event_slot_lag: 12,
        }
    }

    #[test]
    fn empty_follow_list_subscribes_to_pump_programs() {
        let accounts = subscription_accounts(&runtime_config(true, vec![]), Pubkey::default());
        assert!(accounts.contains(&PUMP_PROGRAM.to_string()));
        assert!(accounts.contains(&PUMP_AMM_PROGRAM.to_string()));
    }

    #[test]
    fn scan_discovery_subscribes_only_to_pump_main_program() {
        let mut cfg = runtime_config(true, vec![]);
        cfg.fresh_session = true;
        let accounts = subscription_accounts(&cfg, Pubkey::new_unique());
        assert_eq!(accounts, vec![PUMP_PROGRAM.to_string()]);
    }

    #[test]
    fn scan_discovery_filter_requires_create_mint_authority() {
        let request = subscription_request(
            vec![PUMP_PROGRAM.to_string()],
            vec![PUMP_MINT_AUTHORITY.to_string()],
            vec![],
            CommitmentLevel::Processed as i32,
            None,
        );
        let filter = request.transactions.get("create").unwrap();
        assert_eq!(filter.account_include, vec![PUMP_PROGRAM.to_string()]);
        assert_eq!(
            filter.account_required,
            vec![PUMP_MINT_AUTHORITY.to_string()]
        );
    }

    #[test]
    fn follow_list_replaces_direct_pump_subscription() {
        let follow = Pubkey::new_unique();
        let cfg = runtime_config(false, vec![follow.to_string()]);
        let accounts = subscription_accounts(&cfg, Pubkey::default());
        assert_eq!(accounts, vec![follow.to_string()]);
        assert_eq!(
            discovery_required_accounts(&cfg),
            vec![PUMP_MINT_AUTHORITY.to_string()]
        );
    }

    #[test]
    fn one_stream_can_keep_create_and_target_filters() {
        let target = Pubkey::new_unique();
        let request = subscription_request(
            vec![PUMP_PROGRAM.to_string()],
            vec![PUMP_MINT_AUTHORITY.to_string()],
            vec![target.to_string()],
            CommitmentLevel::Processed as i32,
            Some(42),
        );
        assert_eq!(request.transactions.len(), 2);
        assert!(request.transactions.contains_key("create"));
        assert_eq!(
            request.transactions["targets"].account_include,
            vec![target.to_string()]
        );
        assert_eq!(request.from_slot, Some(42));
    }

    #[test]
    fn replay_cursor_deduplicates_signatures_with_bounded_memory() {
        let mut seen = SeenSignatures {
            values: HashSet::new(),
            order: VecDeque::new(),
            capacity: 2,
        };
        assert!(seen.insert("a".into()));
        assert!(!seen.insert("a".into()));
        assert!(seen.insert("b".into()));
        assert!(seen.insert("c".into()));
        assert!(!seen.values.contains("a"));
        assert!(seen.insert("a".into()));
    }

    #[test]
    fn reconnect_subscription_requests_replay_from_last_slot() {
        let request = subscription_request(
            vec![PUMP_PROGRAM.into()],
            vec![],
            vec![],
            0,
            Some(safe_replay_slot(123)),
        );
        assert_eq!(request.from_slot, Some(59));
        assert!(request.slots.contains_key("integrity"));
    }

    #[test]
    fn reconnect_overlap_saturates_at_genesis() {
        assert_eq!(safe_replay_slot(12), 0);
    }

    #[test]
    fn replay_floor_never_moves_cursor_backwards() {
        let last = 443_442_727;
        let floor = 443_442_681;
        assert_eq!(resume_cursor_after_floor(Some(last), floor), last);
    }

    #[test]
    fn parses_geyser_replay_floor_from_out_of_range_error() {
        assert_eq!(
            replay_floor(
                "status: OutOfRange, message: broadcast is not available, last available: 443176061"
            ),
            Some(443_176_061)
        );
        assert_eq!(replay_floor("connection reset"), None);
    }
}
