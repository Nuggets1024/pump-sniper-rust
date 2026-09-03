//! 每个 mint 一份异步交易流水，热路径只投递事件，不直接做文件 I/O。

use crate::display::{compact_market_cap, compact_quote as compact_quote_amount, compact_token};
use crate::pump::{
    format_raw_amount, is_native_quote, raw_amount_as_f64, PumpBuy, PumpEvent, PumpSell,
};
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

static DROPPED_JOURNAL_MESSAGES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct TradeJournal {
    tx: mpsc::Sender<JournalMessage>,
}

enum JournalMessage {
    Chain(Box<PumpEvent>),
    Note {
        mint: Pubkey,
        tag: &'static str,
        body: String,
    },
    Submitted {
        mint: Pubkey,
        side: &'static str,
        signature: String,
        signatures: Vec<String>,
        channel: String,
        submitted: Instant,
    },
    Coverage {
        mint: Pubkey,
        signature: String,
        source_mask: u64,
        conflict: bool,
    },
}

impl TradeJournal {
    pub fn start(directory: impl AsRef<Path>, our_wallet: Pubkey) -> Self {
        let (tx, rx) = mpsc::channel(16_384);
        let directory = directory.as_ref().to_path_buf();
        tokio::spawn(async move {
            if let Err(error) = run(directory, our_wallet, rx).await {
                crate::telemetry::error("交易日志", error.to_string());
            }
        });
        Self { tx }
    }

    pub fn record(&self, event: &PumpEvent) -> bool {
        self.send(JournalMessage::Chain(Box::new(event.clone())))
    }

    pub fn note(&self, mint: Pubkey, tag: &'static str, body: impl Into<String>) -> bool {
        self.send(JournalMessage::Note {
            mint,
            tag,
            body: body.into(),
        })
    }

    pub fn submitted(
        &self,
        mint: Pubkey,
        side: &'static str,
        signature: impl Into<String>,
        signatures: Vec<String>,
        channel: impl Into<String>,
    ) -> bool {
        self.send(JournalMessage::Submitted {
            mint,
            side,
            signature: signature.into(),
            signatures,
            channel: channel.into(),
            submitted: Instant::now(),
        })
    }

    pub fn coverage(
        &self,
        mint: Pubkey,
        signature: String,
        source_mask: u64,
        conflict: bool,
    ) -> bool {
        self.send(JournalMessage::Coverage {
            mint,
            signature,
            source_mask,
            conflict,
        })
    }

    /// 审计通道一旦发生丢弃或关闭，进程本次生命周期内停止允许新开仓。
    pub fn allows_new_orders(&self) -> bool {
        DROPPED_JOURNAL_MESSAGES.load(Ordering::Acquire) == 0
    }

    fn send(&self, message: JournalMessage) -> bool {
        match self.tx.try_send(message) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                let dropped = DROPPED_JOURNAL_MESSAGES.fetch_add(1, Ordering::Relaxed) + 1;
                if dropped <= 10 || dropped.is_power_of_two() {
                    crate::telemetry::error(
                        "交易日志",
                        format!("日志队列已满，丢弃数={dropped}；已暂停新开仓，请检查磁盘"),
                    );
                }
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                DROPPED_JOURNAL_MESSAGES.fetch_add(1, Ordering::Release);
                crate::telemetry::error("交易日志", "日志写入任务已停止");
                false
            }
        }
    }
}

struct TokenHistory {
    creator: Pubkey,
    create_slot: u64,
    buys: Vec<ObservedBuy>,
    sells: Vec<ObservedSell>,
    // 最后一位区分指令级估算与 Geyser TradeEvent 精确数据，允许后者补充核账。
    seen: HashSet<(String, u32, u16, Pubkey, bool, bool)>,
}

struct PendingSubmission {
    side: &'static str,
    submitted: Instant,
}

#[derive(Clone, Copy)]
enum CostPrice {
    Flat,
    Known(f64),
    Unknown,
}

struct ObservedBuy {
    slot: u64,
    transaction_index: u64,
    signature: String,
    buy: PumpBuy,
    fee_payer: Pubkey,
    fee_lamports: u64,
    jito_tip_lamports: u64,
}

struct ObservedSell {
    slot: u64,
    transaction_index: u64,
    signature: String,
    sell: PumpSell,
    fee_payer: Pubkey,
    fee_lamports: u64,
    jito_tip_lamports: u64,
}

async fn run(
    directory: PathBuf,
    our_wallet: Pubkey,
    mut rx: mpsc::Receiver<JournalMessage>,
) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(&directory).await?;
    let wal_path = directory.join("events.wal.jsonl");
    let wal_file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&wal_path)
        .await?;
    let mut wal = tokio::io::BufWriter::new(wal_file);
    let mut tokens = HashMap::<Pubkey, TokenHistory>::new();
    let mut pending = HashMap::<(Pubkey, String), PendingSubmission>::new();

    while let Some(message) = rx.recv().await {
        let event = match message {
            JournalMessage::Chain(event) => {
                append_wal(
                    &mut wal,
                    &serde_json::json!({ "type": "event", "event": event }),
                )
                .await?;
                *event
            }
            JournalMessage::Note { mint, tag, body } => {
                append_report(&directory, mint, &format_note(tag, &body)).await;
                let row = crate::admin::TokenLogRow {
                    ts: json_timestamp(),
                    kind: "note".into(),
                    slot: None,
                    signature: None,
                    message: format!("{tag}: {body}"),
                    data: serde_json::json!({ "tag": tag, "body": body }),
                };
                crate::admin::record_token_event(
                    mint.to_string(),
                    String::new(),
                    None,
                    None,
                    None,
                    row.slot,
                    row.signature.clone(),
                    row.kind.clone(),
                    row.message.clone(),
                    row.data.clone(),
                );
                continue;
            }
            JournalMessage::Submitted {
                mint,
                side,
                signature,
                signatures,
                channel,
                submitted,
            } => {
                let acceptance = submission_acceptance(&channel);
                let sent = format!(
                    "{}{}",
                    crate::telemetry::compact_line(
                        &format!("执行{side}"),
                        vec![
                            mint.to_string(),
                            channel.clone(),
                            acceptance.to_owned(),
                            signature.clone(),
                        ],
                    ),
                    crate::telemetry::compact_line(
                        "上链确认",
                        vec![mint.to_string(), "等待Geyser".to_owned(), signature.clone(),],
                    ),
                );
                append_report(&directory, mint, &sent).await;
                let row = crate::admin::TokenLogRow {
                    ts: json_timestamp(),
                    kind: "submitted".into(),
                    slot: None,
                    signature: Some(signature.clone()),
                    message: format!("{side} 已提交到 {channel}"),
                    data: serde_json::json!({
                        "side": side,
                        "signature": signature,
                        "signatures": signatures.clone(),
                        "channel": channel,
                        "acceptance": acceptance,
                    }),
                };
                crate::admin::record_token_event(
                    mint.to_string(),
                    String::new(),
                    None,
                    None,
                    None,
                    row.slot,
                    row.signature.clone(),
                    row.kind.clone(),
                    row.message.clone(),
                    row.data.clone(),
                );
                let pending_signatures = if signatures.is_empty() {
                    vec![signature.clone()]
                } else {
                    signatures.clone()
                };
                for pending_signature in pending_signatures {
                    pending.insert(
                        (mint, pending_signature),
                        PendingSubmission { side, submitted },
                    );
                }
                if let Some(history) = tokens.get(&mint) {
                    for candidate in &signatures {
                        if let Some(slot) = observed_slot(history, candidate) {
                            if let Some(line) = take_confirmation(
                                &mut pending,
                                candidate,
                                slot,
                                mint,
                                history.create_slot,
                            ) {
                                append_report(&directory, mint, &line).await;
                            }
                        }
                    }
                }
                continue;
            }
            JournalMessage::Coverage {
                mint,
                signature,
                source_mask,
                conflict,
            } => {
                append_wal(
                    &mut wal,
                    &serde_json::json!({
                        "type": "coverage",
                        "mint": mint.to_string(),
                        "signature": signature,
                        "source_mask": source_mask,
                        "conflict": conflict,
                    }),
                )
                .await?;
                continue;
            }
        };
        if !tokens.contains_key(&event.mint) {
            if event.is_create {
                let fields = compact_create_fields(&event);
                let header = crate::telemetry::compact_line("创建", fields.clone());
                publish_compact_line("创建", fields);
                append_report(&directory, event.mint, &header).await;
                let row = crate::admin::TokenLogRow {
                    ts: json_timestamp(),
                    kind: "create".into(),
                    slot: Some(event.slot),
                    signature: Some(event.signature.clone()),
                    message: format!(
                        "创建 {} {}",
                        clean(event.name.as_deref()),
                        clean(event.symbol.as_deref())
                    ),
                    data: serde_json::json!({ "event": event }),
                };
                publish_token_row(&event, &row);
            }
            tokens.insert(
                event.mint,
                TokenHistory {
                    creator: event.creator,
                    create_slot: event.slot,
                    buys: Vec::new(),
                    sells: Vec::new(),
                    seen: HashSet::new(),
                },
            );
        }

        let Some(history) = tokens.get_mut(&event.mint) else {
            continue;
        };
        for buy in &event.buys {
            // Entry/Shred 只有指令参数，没有执行结果。它可能执行失败，也没有实际
            // token 数量，因此不能以“买入成交”写日志或进入管理页。
            if !buy.exact {
                continue;
            }
            let key = (
                event.signature.clone(),
                buy.instruction_index,
                buy.event_index,
                buy.wallet,
                true,
                buy.exact,
            );
            if !history.seen.insert(key) {
                continue;
            }
            history.buys.push(ObservedBuy {
                slot: event.slot,
                transaction_index: event.transaction_index,
                signature: event.signature.clone(),
                buy: buy.clone(),
                fee_payer: event.signer,
                fee_lamports: event.fee_lamports,
                jito_tip_lamports: event.jito_tip_lamports.unwrap_or(0),
            });
            history.buys.sort_by_key(order_key);

            let observed = history
                .buys
                .iter()
                .find(|item| same_buy(item, &event, buy))
                .expect("刚插入的买入必须存在");
            let fields = compact_buy_fields(event.mint, history, observed, our_wallet);
            let line = crate::telemetry::compact_line("买入", fields.clone());
            publish_compact_line("买入", fields);
            append_report(&directory, event.mint, &line).await;
            let row = crate::admin::TokenLogRow {
                ts: json_timestamp(),
                kind: "buy".into(),
                slot: Some(event.slot),
                signature: Some(event.signature.clone()),
                message: format!("买入 wallet={} slot={}", buy.wallet, event.slot),
                data: serde_json::json!({
                    "event": event,
                    "buy": buy,
                    "is_our": buy.wallet == our_wallet,
                    "price": buy_price(buy),
                }),
            };
            publish_token_row(&event, &row);
            if let Some(line) = take_confirmation(
                &mut pending,
                &event.signature,
                event.slot,
                event.mint,
                history.create_slot,
            ) {
                append_report(&directory, event.mint, &line).await;
            }
        }
        for sell in &event.sells {
            if !sell.exact {
                continue;
            }
            let key = (
                event.signature.clone(),
                sell.instruction_index,
                sell.event_index,
                sell.wallet,
                false,
                sell.exact,
            );
            if !history.seen.insert(key) {
                continue;
            }
            history.sells.push(ObservedSell {
                slot: event.slot,
                transaction_index: event.transaction_index,
                signature: event.signature.clone(),
                sell: sell.clone(),
                fee_payer: event.signer,
                fee_lamports: event.fee_lamports,
                jito_tip_lamports: event.jito_tip_lamports.unwrap_or(0),
            });
            history.sells.sort_by_key(sell_order_key);
            let observed = history
                .sells
                .iter()
                .find(|item| {
                    item.signature == event.signature
                        && item.sell.instruction_index == sell.instruction_index
                        && item.sell.event_index == sell.event_index
                        && item.sell.wallet == sell.wallet
                })
                .expect("刚插入的卖出必须存在");
            let fields = compact_sell_fields(event.mint, history, observed, our_wallet);
            let line = crate::telemetry::compact_line("卖出", fields.clone());
            publish_compact_line("卖出", fields);
            append_report(&directory, event.mint, &line).await;
            let row = crate::admin::TokenLogRow {
                ts: json_timestamp(),
                kind: "sell".into(),
                slot: Some(event.slot),
                signature: Some(event.signature.clone()),
                message: format!("卖出 wallet={} slot={}", sell.wallet, event.slot),
                data: serde_json::json!({
                    "event": event,
                    "sell": sell,
                    "is_our": sell.wallet == our_wallet,
                    "price": sell_price(sell),
                }),
            };
            publish_token_row(&event, &row);
            if let Some(line) = take_confirmation(
                &mut pending,
                &event.signature,
                event.slot,
                event.mint,
                history.create_slot,
            ) {
                append_report(&directory, event.mint, &line).await;
            }
        }
    }
    Ok(())
}

fn submission_acceptance(channel: &str) -> &'static str {
    if channel.ends_with("/udp-dispatched") {
        "UDP已发送，未获服务端确认"
    } else {
        "落地节点已接受"
    }
}

async fn append_wal(
    wal: &mut tokio::io::BufWriter<tokio::fs::File>,
    value: &serde_json::Value,
) -> anyhow::Result<()> {
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    wal.write_all(&line).await?;
    wal.flush().await?;
    wal.get_ref().sync_data().await?;
    Ok(())
}

fn publish_compact_line(kind: &str, fields: Vec<String>) {
    crate::telemetry::market(crate::telemetry::compact_line(kind, fields));
}

fn main_log_content(line: &str) -> &str {
    line.strip_suffix('\n').unwrap_or(line)
}

fn format_note(tag: &str, body: &str) -> String {
    crate::telemetry::compact_line(tag, vec![body.to_owned()])
}

fn observed_slot(history: &TokenHistory, signature: &str) -> Option<u64> {
    history
        .buys
        .iter()
        .find(|item| item.signature == signature)
        .map(|item| item.slot)
        .or_else(|| {
            history
                .sells
                .iter()
                .find(|item| item.signature == signature)
                .map(|item| item.slot)
        })
}

fn take_confirmation(
    pending: &mut HashMap<(Pubkey, String), PendingSubmission>,
    signature: &str,
    slot: u64,
    mint: Pubkey,
    create_slot: u64,
) -> Option<String> {
    let pending = pending.remove(&(mint, signature.to_owned()))?;
    Some(crate::telemetry::compact_line(
        "确认交易",
        vec![
            mint.to_string(),
            format!("{}上链", pending.side),
            signature.to_owned(),
            format!("间隔区块={}", slot.saturating_sub(create_slot)),
            format!("等待={}ms", pending.submitted.elapsed().as_millis()),
        ],
    ))
}

async fn append_report(directory: &Path, mint: Pubkey, content: &str) {
    if let Err(error) = append(directory, mint, content).await {
        crate::telemetry::error("交易日志", format!("mint={mint}  {error}"));
    }
}

fn publish_token_row(event: &PumpEvent, row: &crate::admin::TokenLogRow) {
    crate::admin::record_token_event(
        event.mint.to_string(),
        event.creator.to_string(),
        event.name.clone(),
        event.symbol.clone(),
        event.is_create.then_some(event.slot),
        row.slot,
        row.signature.clone(),
        row.kind.clone(),
        row.message.clone(),
        row.data.clone(),
    );
}

fn json_timestamp() -> String {
    let offset = chrono::FixedOffset::east_opt(8 * 60 * 60).expect("valid UTC+8 offset");
    chrono::Utc::now()
        .with_timezone(&offset)
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
}

fn order_key(item: &ObservedBuy) -> (u64, u64, u32, u16) {
    (
        item.slot,
        item.transaction_index,
        item.buy.instruction_index,
        item.buy.event_index,
    )
}

fn sell_order_key(item: &ObservedSell) -> (u64, u64, u32, u16) {
    (
        item.slot,
        item.transaction_index,
        item.sell.instruction_index,
        item.sell.event_index,
    )
}

fn buy_price(buy: &PumpBuy) -> Option<f64> {
    let token_amount = buy.token_amount?;
    if token_amount == 0 {
        return None;
    }
    raw_amount_as_f64(buy.quote_amount, buy.quote_decimals)
        .map(|quote| quote / (token_amount as f64 / 1e6))
}

fn sell_price(sell: &PumpSell) -> Option<f64> {
    if sell.token_amount == 0 {
        return None;
    }
    sell.quote_amount
        .and_then(|amount| raw_amount_as_f64(amount, sell.quote_decimals))
        .map(|quote| quote / (sell.token_amount as f64 / 1e6))
}

async fn append(directory: &Path, mint: Pubkey, content: &str) -> anyhow::Result<()> {
    let path = directory.join(format!("{mint}.log"));
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    file.write_all(content.as_bytes()).await?;
    file.flush().await?;
    Ok(())
}

#[allow(dead_code)]
fn format_create(event: &PumpEvent, our_wallet: Pubkey) -> String {
    let timestamp = timestamp();
    let detected = if event.is_buy {
        "create + buy"
    } else {
        "create"
    };
    format!(
        "\n==================== TOKEN MONITOR ====================\n{} [监控交易]: detected {} / {}\n{} [代币创建]: [{}] name={} symbol={} dev={} slot={} tx_index={}\n{} [创建详情]: curve={} token_program={} uri={} 同笔买入={} quote={} {} Jito_tip={} dontfront={} bundle_id=Geyser不可得\n{} [监听状态]: 已锁定该 mint；此后若无 BUY/SELL，表示 Geyser 未观察到链上成交\n{} [我的钱包]: [{}] wallet={} 排名=等待链上买入\n=======================================================\n",
        timestamp,
        detected,
        event.signature,
        timestamp,
        event.mint,
        clean(event.name.as_deref()),
        clean(event.symbol.as_deref()),
        event.creator,
        event.slot,
        event.transaction_index,
        timestamp,
        event.bonding_curve,
        event.token_program,
        clean(event.uri.as_deref()),
        event.buy_instruction_count,
        quote_label(event.quote_mint),
        event
            .buy_quote_amount
            .map(|amount| format_raw_amount(amount, event.quote_decimals))
            .unwrap_or_else(|| "无".into()),
        amount(event.jito_tip_lamports),
        yn(event.jito_dont_front),
        timestamp,
        timestamp,
        event.mint,
        our_wallet,
    )
}

fn format_compact_create(event: &PumpEvent) -> String {
    crate::telemetry::compact_line("创建", compact_create_fields(event))
}

fn compact_create_fields(event: &PumpEvent) -> Vec<String> {
    vec![
        format!(
            "{}/{}",
            compact_text(event.symbol.as_deref(), 12),
            compact_text(event.name.as_deref(), 20)
        ),
        event.mint.to_string(),
        event.creator.to_string(),
        format!("slot={}", event.slot),
        format!("tx={}", event.transaction_index),
        event.signature.clone(),
    ]
}

fn format_compact_buy(history: &TokenHistory, item: &ObservedBuy, our_wallet: Pubkey) -> String {
    crate::telemetry::compact_line(
        "买入",
        compact_buy_fields(Pubkey::default(), history, item, our_wallet),
    )
}

fn compact_buy_fields(
    mint: Pubkey,
    history: &TokenHistory,
    item: &ObservedBuy,
    our_wallet: Pubkey,
) -> Vec<String> {
    let token_amount = item
        .buy
        .token_amount
        .map(compact_token)
        .unwrap_or_else(|| "未知".into());
    let current_price = trade_price(
        item.buy.virtual_quote_reserves,
        item.buy.quote_decimals,
        item.buy.virtual_token_reserves,
        Some(item.buy.quote_amount),
        item.buy.token_amount,
    );
    compact_trade_fields(
        mint,
        item.buy.wallet,
        compact_trade_quote(
            item.buy.quote_amount,
            item.buy.quote_mint,
            item.buy.quote_decimals,
        ),
        token_amount,
        compact_market_cap(
            item.buy.virtual_quote_reserves,
            item.buy.quote_mint,
            item.buy.quote_decimals,
            item.buy.virtual_token_reserves,
        ),
        current_price,
        remaining_cost_price(history, our_wallet, order_key(item), true),
        item.slot,
        identity(item.buy.wallet, history.creator, our_wallet),
        &item.signature,
    )
}

fn format_compact_sell(history: &TokenHistory, item: &ObservedSell, our_wallet: Pubkey) -> String {
    crate::telemetry::compact_line(
        "卖出",
        compact_sell_fields(Pubkey::default(), history, item, our_wallet),
    )
}

fn compact_sell_fields(
    mint: Pubkey,
    history: &TokenHistory,
    item: &ObservedSell,
    our_wallet: Pubkey,
) -> Vec<String> {
    let current_price = trade_price(
        item.sell.virtual_quote_reserves,
        item.sell.quote_decimals,
        item.sell.virtual_token_reserves,
        item.sell.quote_amount,
        Some(item.sell.token_amount),
    );
    compact_trade_fields(
        mint,
        item.sell.wallet,
        item.sell
            .quote_amount
            .map(|amount| {
                compact_trade_quote(amount, item.sell.quote_mint, item.sell.quote_decimals)
            })
            .unwrap_or_else(|| "未知".into()),
        compact_token(item.sell.token_amount),
        compact_market_cap(
            item.sell.virtual_quote_reserves,
            item.sell.quote_mint,
            item.sell.quote_decimals,
            item.sell.virtual_token_reserves,
        ),
        current_price,
        remaining_cost_price(
            history,
            our_wallet,
            sell_order_key(item),
            item.sell.wallet != our_wallet,
        ),
        item.slot,
        identity(item.sell.wallet, history.creator, our_wallet),
        &item.signature,
    )
}

#[allow(clippy::too_many_arguments)]
fn compact_trade_fields(
    mint: Pubkey,
    wallet: Pubkey,
    quote: String,
    tokens: String,
    market_cap: String,
    current_price: Option<f64>,
    cost_price: CostPrice,
    slot: u64,
    identity: &str,
    signature: &str,
) -> Vec<String> {
    let (cost, pnl) = match (current_price, cost_price) {
        (_, CostPrice::Flat) => ("0".into(), "PNL 0%".into()),
        (Some(current), CostPrice::Known(cost)) => {
            let percent = ((current / cost - 1.0) * 100.0).round();
            if percent.is_finite() {
                (
                    format_compact_price(cost),
                    format!("PNL {}%", percent as i64),
                )
            } else {
                (format_compact_price(cost), "PNL 未知".into())
            }
        }
        (None, CostPrice::Known(cost)) => (format_compact_price(cost), "PNL 未知".into()),
        (_, CostPrice::Unknown) => ("未知".into(), "PNL 未知".into()),
    };
    let current = current_price
        .map(format_compact_price)
        .unwrap_or_else(|| "未知".into());
    let prices = format!("{current}/{cost}");
    vec![
        wallet.to_string(),
        quote,
        tokens,
        format!("MCAP {market_cap}"),
        prices,
        pnl,
        slot.to_string(),
        identity.to_owned(),
        mint.to_string(),
        signature.to_owned(),
    ]
}

fn format_compact_price(price: f64) -> String {
    if !price.is_finite() || price <= 0.0 {
        "0".into()
    } else {
        format!("{price:.11}")
    }
}

fn trade_price(
    virtual_quote_reserves: Option<u64>,
    quote_decimals: Option<u8>,
    virtual_token_reserves: Option<u64>,
    trade_quote_amount: Option<u64>,
    trade_token_amount: Option<u64>,
) -> Option<f64> {
    let price = virtual_quote_reserves
        .zip(virtual_token_reserves)
        .and_then(|(quote, tokens)| unit_price(quote, quote_decimals, tokens))
        .or_else(|| {
            trade_quote_amount
                .zip(trade_token_amount)
                .and_then(|(quote, tokens)| unit_price(quote, quote_decimals, tokens))
        })?;
    (price.is_finite() && price > 0.0).then_some(price)
}

fn unit_price(quote_amount: u64, quote_decimals: Option<u8>, token_amount: u64) -> Option<f64> {
    if token_amount == 0 {
        return None;
    }
    let quote = raw_amount_as_f64(quote_amount, quote_decimals)?;
    Some(quote / (token_amount as f64 / 1_000_000.0))
}

/// 本钱包剩余持仓的移动平均成本价；未买入或已经清仓时为零。
fn remaining_cost_price(
    history: &TokenHistory,
    our_wallet: Pubkey,
    cutoff: (u64, u64, u32, u16),
    include_cutoff: bool,
) -> CostPrice {
    let mut holdings_raw = 0.0f64;
    let mut remaining_cost = 0.0f64;
    let mut incomplete = false;
    let mut our_trades = history
        .buys
        .iter()
        .filter(|item| item.buy.wallet == our_wallet)
        .filter(|item| {
            let order = order_key(item);
            order < cutoff || (include_cutoff && order == cutoff)
        })
        .map(TradeRef::Buy)
        .chain(
            history
                .sells
                .iter()
                .filter(|item| item.sell.wallet == our_wallet)
                .filter(|item| {
                    let order = sell_order_key(item);
                    order < cutoff || (include_cutoff && order == cutoff)
                })
                .map(TradeRef::Sell),
        )
        .collect::<Vec<_>>();
    our_trades.sort_by_key(TradeRef::order);
    for trade in our_trades {
        match trade {
            TradeRef::Buy(item) => {
                let Some(tokens) = item.buy.token_amount.filter(|tokens| *tokens > 0) else {
                    incomplete = true;
                    continue;
                };
                let Some(cost) = raw_amount_as_f64(item.buy.quote_amount, item.buy.quote_decimals)
                else {
                    incomplete = true;
                    continue;
                };
                holdings_raw += tokens as f64;
                remaining_cost += cost;
            }
            TradeRef::Sell(item) => {
                if holdings_raw <= 0.0 {
                    continue;
                }
                let sold_raw = (item.sell.token_amount as f64).min(holdings_raw);
                remaining_cost -= remaining_cost * sold_raw / holdings_raw;
                holdings_raw -= sold_raw;
            }
        }
    }
    if incomplete {
        CostPrice::Unknown
    } else if holdings_raw <= 0.0 || remaining_cost <= 0.0 {
        CostPrice::Flat
    } else {
        CostPrice::Known(remaining_cost / (holdings_raw / 1_000_000.0))
    }
}

fn compact_text(value: Option<&str>, max_chars: usize) -> String {
    let value = clean(value);
    if value.chars().count() <= max_chars {
        return value;
    }
    format!("{}…", value.chars().take(max_chars).collect::<String>())
}

#[allow(dead_code)]
fn format_buy(
    history: &TokenHistory,
    event: &PumpEvent,
    buy: &PumpBuy,
    our_wallet: Pubkey,
) -> String {
    let all_rank = history
        .buys
        .iter()
        .position(|item| same_buy(item, event, buy))
        .map(|index| index + 1)
        .unwrap_or(history.buys.len());
    let external_rank = if buy.wallet == history.creator {
        "DEV".to_owned()
    } else {
        let rank = ordered_external_wallets(history)
            .iter()
            .position(|item| item.buy.wallet == buy.wallet)
            .map(|index| index + 1)
            .unwrap_or(0);
        format!("第{rank}位")
    };
    let price_quote = buy
        .token_amount
        .filter(|amount| *amount > 0)
        .and_then(|amount| {
            raw_amount_as_f64(buy.quote_amount, buy.quote_decimals)
                .map(|quote| quote / (amount as f64 / 1e6))
        });
    let mut line = format!(
        "\n{} [BUY #{:03}] {}={}  Token={}  Price={} {}\n  Wallet={}  身份={}  排名={}（全序号第{}笔）\n  Slot={}  间隔={}  TxIndex={}  Ix={}\n  Tx={}\n",
        timestamp(),
        all_rank,
        quote_label(buy.quote_mint),
        format_raw_amount(buy.quote_amount, buy.quote_decimals),
        buy.token_amount.map(token).unwrap_or_else(|| "未知".into()),
        price_quote
            .map(|price| format!("{price:.12}"))
            .unwrap_or_else(|| "未知".into()),
        quote_label(buy.quote_mint),
        buy.wallet,
        identity(buy.wallet, history.creator, our_wallet),
        external_rank,
        all_rank,
        event.slot,
        event.slot.saturating_sub(history.create_slot),
        event.transaction_index,
        buy.instruction_index,
        event.signature,
    );
    if !buy.exact || event.jito_tip_lamports.is_some() || event.jito_dont_front {
        line.push_str(&format!(
            "  线索: 口径={}  Jito_tip={}  dontfront={}  bundle_id=Geyser不可得\n",
            if buy.exact {
                "实际成交"
            } else {
                "指令参数回退"
            },
            amount(event.jito_tip_lamports),
            yn(event.jito_dont_front),
        ));
    }
    line
}

#[allow(dead_code)]
fn format_sell(
    history: &TokenHistory,
    event: &PumpEvent,
    sell: &PumpSell,
    our_wallet: Pubkey,
) -> String {
    let price_quote = sell
        .quote_amount
        .filter(|_| sell.token_amount > 0)
        .and_then(|amount| {
            raw_amount_as_f64(amount, sell.quote_decimals)
                .map(|quote| quote / (sell.token_amount as f64 / 1e6))
        });
    let mut line = format!(
        "\n{} [SELL] {}={}  Token={}  Price={} {}\n  Wallet={}  身份={}\n  Slot={}  间隔={}  TxIndex={}  Ix={}\n  Tx={}\n",
        timestamp(),
        quote_label(sell.quote_mint),
        sell.quote_amount
            .map(|amount| format_raw_amount(amount, sell.quote_decimals))
            .unwrap_or_else(|| "未知".into()),
        token(sell.token_amount),
        price_quote
            .map(|price| format!("{price:.12}"))
            .unwrap_or_else(|| "未知".into()),
        quote_label(sell.quote_mint),
        sell.wallet,
        identity(sell.wallet, history.creator, our_wallet),
        event.slot,
        event.slot.saturating_sub(history.create_slot),
        event.transaction_index,
        sell.instruction_index,
        event.signature,
    );
    if !sell.exact || event.jito_tip_lamports.is_some() || event.jito_dont_front {
        line.push_str(&format!(
            "  线索: 口径={}  Jito_tip={}  dontfront={}  bundle_id=Geyser不可得\n",
            if sell.exact {
                "实际成交"
            } else {
                "指令参数回退"
            },
            amount(event.jito_tip_lamports),
            yn(event.jito_dont_front),
        ));
    }
    line
}

enum TradeRef<'a> {
    Buy(&'a ObservedBuy),
    Sell(&'a ObservedSell),
}

impl TradeRef<'_> {
    fn order(&self) -> (u64, u64, u32, u16) {
        match self {
            Self::Buy(item) => order_key(item),
            Self::Sell(item) => sell_order_key(item),
        }
    }

    fn wallet(&self) -> Pubkey {
        match self {
            Self::Buy(item) => item.buy.wallet,
            Self::Sell(item) => item.sell.wallet,
        }
    }

    fn signature(&self) -> &str {
        match self {
            Self::Buy(item) => &item.signature,
            Self::Sell(item) => &item.signature,
        }
    }

    fn fee(&self) -> (Pubkey, u64, u64) {
        match self {
            Self::Buy(item) => (item.fee_payer, item.fee_lamports, item.jito_tip_lamports),
            Self::Sell(item) => (item.fee_payer, item.fee_lamports, item.jito_tip_lamports),
        }
    }

    fn reserves(&self) -> Option<(u64, u64)> {
        match self {
            Self::Buy(item) => Some((
                item.buy.virtual_quote_reserves?,
                item.buy.virtual_token_reserves?,
            )),
            Self::Sell(item) => Some((
                item.sell.virtual_quote_reserves?,
                item.sell.virtual_token_reserves?,
            )),
        }
    }
}

fn ordered_trades(history: &TokenHistory) -> Vec<TradeRef<'_>> {
    let mut trades = history
        .buys
        .iter()
        .map(TradeRef::Buy)
        .chain(history.sells.iter().map(TradeRef::Sell))
        .collect::<Vec<_>>();
    trades.sort_by_key(TradeRef::order);
    trades
}

fn format_stats(history: &TokenHistory, our_wallet: Pubkey) -> String {
    const TOKEN_SUPPLY: f64 = 1_000_000_000.0;
    let trades = ordered_trades(history);
    let (quote_mint, quote_decimals) = history_quote(history);
    let quote_name = quote_label(quote_mint);
    let total_buy_lamports = history
        .buys
        .iter()
        .fold(0u64, |sum, item| sum.saturating_add(item.buy.quote_amount));
    let total_sell_lamports = history.sells.iter().fold(0u64, |sum, item| {
        sum.saturating_add(item.sell.quote_amount.unwrap_or(0))
    });
    let unique_buyers = history
        .buys
        .iter()
        .map(|item| item.buy.wallet)
        .collect::<HashSet<_>>()
        .len();
    let fallback_count = history.buys.iter().filter(|item| !item.buy.exact).count()
        + history.sells.iter().filter(|item| !item.sell.exact).count();
    let (price_sol, market_cap_sol) = trades
        .iter()
        .rev()
        .find_map(TradeRef::reserves)
        .filter(|(_, virtual_token)| *virtual_token > 0)
        .map(|(virtual_quote, virtual_token)| {
            let price = raw_amount_as_f64(virtual_quote, quote_decimals).unwrap_or(0.0)
                / (virtual_token as f64 / 1e6);
            (price, price * TOKEN_SUPPLY)
        })
        .unwrap_or((0.0, 0.0));
    let net_flow = total_buy_lamports as i128 - total_sell_lamports as i128;

    let mut holdings_raw = 0.0f64;
    let mut remaining_cost_sol = 0.0f64;
    let mut realized_sol = 0.0f64;
    let mut pnl_complete = true;
    for trade in &trades {
        if trade.wallet() != our_wallet {
            continue;
        }
        match trade {
            TradeRef::Buy(item) => {
                let Some(tokens) = item.buy.token_amount else {
                    pnl_complete = false;
                    continue;
                };
                holdings_raw += tokens as f64;
                let Some(cost) = raw_amount_as_f64(item.buy.quote_amount, item.buy.quote_decimals)
                else {
                    pnl_complete = false;
                    continue;
                };
                remaining_cost_sol += cost;
            }
            TradeRef::Sell(item) => {
                let Some(sell_lamports) = item.sell.quote_amount else {
                    pnl_complete = false;
                    continue;
                };
                if holdings_raw <= 0.0 {
                    pnl_complete = false;
                    continue;
                }
                let sold_raw = (item.sell.token_amount as f64).min(holdings_raw);
                let removed_cost = remaining_cost_sol * sold_raw / holdings_raw;
                let Some(proceeds) = raw_amount_as_f64(sell_lamports, item.sell.quote_decimals)
                else {
                    pnl_complete = false;
                    continue;
                };
                realized_sol += proceeds - removed_cost;
                holdings_raw -= sold_raw;
                remaining_cost_sol -= removed_cost;
                if sold_raw < item.sell.token_amount as f64 {
                    pnl_complete = false;
                }
            }
        }
    }
    let current_value_sol = holdings_raw / 1e6 * price_sol;
    let unrealized_sol = current_value_sol - remaining_cost_sol;
    let gross_pnl_sol = realized_sol + unrealized_sol;
    let mut charged_signatures = HashSet::new();
    let mut fee_lamports = 0u64;
    let mut tip_lamports = 0u64;
    for trade in &trades {
        let (payer, fee, tip) = trade.fee();
        if payer == our_wallet && charged_signatures.insert(trade.signature()) {
            fee_lamports = fee_lamports.saturating_add(fee);
            tip_lamports = tip_lamports.saturating_add(tip);
        }
    }
    let overhead_sol = if is_native_quote(quote_mint) {
        fee_lamports.saturating_add(tip_lamports) as f64 / 1e9
    } else {
        0.0
    };
    let net_pnl_sol = gross_pnl_sol - overhead_sol;
    let pnl_status = if pnl_complete && price_sol > 0.0 {
        "完整"
    } else {
        "部分（缺少实际成交或价格）"
    };

    let mut output = format!(
        "{} [汇总] 买入={}笔 / {} {}  卖出={}笔 / {} {}  净流入={} {}  买家={}\n{} [行情] 当前价格={:.12} {}  估算市值={:.3} {}  回退记录={}\n",
        timestamp(),
        history.buys.len(),
        format_raw_amount(total_buy_lamports, quote_decimals),
        quote_name,
        history.sells.len(),
        format_raw_amount(total_sell_lamports, quote_decimals),
        quote_name,
        unsigned_positive_quote_amount(net_flow, quote_decimals),
        quote_name,
        unique_buyers,
        timestamp(),
        price_sol,
        quote_name,
        market_cap_sol,
        quote_name,
        fallback_count,
    );
    let has_our_trade = trades.iter().any(|trade| trade.wallet() == our_wallet);
    if has_our_trade {
        output.push_str(&format!(
            "{} [我的盈亏] 状态={}  持仓={} token  成本={:.9} {}  当前价值={:.9} {}\n  已实现={:+.9}  未实现={:+.9}  网络费={} SOL  Jito_tip={} SOL  净盈亏={:+.9} {}\n",
            timestamp(),
            pnl_status,
            token(holdings_raw.max(0.0) as u64),
            remaining_cost_sol,
            quote_name,
            current_value_sol,
            quote_name,
            realized_sol,
            unrealized_sol,
            sol(fee_lamports),
            sol(tip_lamports),
            net_pnl_sol,
            quote_name,
        ));
    }
    output
}

fn same_buy(item: &ObservedBuy, event: &PumpEvent, buy: &PumpBuy) -> bool {
    item.signature == event.signature
        && item.buy.instruction_index == buy.instruction_index
        && item.buy.event_index == buy.event_index
        && item.buy.wallet == buy.wallet
}

fn format_our_rank(history: &TokenHistory, our_wallet: Pubkey) -> String {
    let all_total = history.buys.len();
    let external = ordered_external_wallets(history);
    let Some(ours) = history
        .buys
        .iter()
        .find(|item| item.buy.wallet == our_wallet)
    else {
        return String::new();
    };
    let all_rank = history
        .buys
        .iter()
        .position(|item| item.buy.wallet == our_wallet)
        .map(|index| index + 1)
        .unwrap_or(0);
    let external_rank = external
        .iter()
        .position(|item| item.buy.wallet == our_wallet)
        .map(|index| index + 1);
    let same_slot_rank = external
        .iter()
        .filter(|item| item.slot == ours.slot)
        .position(|item| item.buy.wallet == our_wallet)
        .map(|index| index + 1);
    let verdict = match external_rank {
        Some(1) => "第一位",
        Some(_) => "不是第一位",
        None => "DEV钱包，不计外部排名",
    };
    format!(
        "{} [我的排名] 结论={} 外部买家排名={} 外部钱包共={} 全部买入序号=第{}笔/共{}笔 slot={} create_slot_delta={} 同slot外部排名={} tx_index={} ix={} signature={}\n",
        timestamp(),
        verdict,
        external_rank.map(|rank| format!("第{rank}位")).unwrap_or_else(|| "不适用".into()),
        external.len(),
        all_rank,
        all_total,
        ours.slot,
        ours.slot.saturating_sub(history.create_slot),
        same_slot_rank.map(|rank| format!("第{rank}位")).unwrap_or_else(|| "不适用".into()),
        ours.transaction_index,
        ours.buy.instruction_index,
        ours.signature,
    )
}

fn ordered_external_wallets(history: &TokenHistory) -> Vec<&ObservedBuy> {
    let mut wallets = HashSet::new();
    history
        .buys
        .iter()
        .filter(|item| item.buy.wallet != history.creator && wallets.insert(item.buy.wallet))
        .collect()
}

fn identity(wallet: Pubkey, creator: Pubkey, ours: Pubkey) -> &'static str {
    if wallet == ours {
        "我"
    } else if wallet == creator {
        "DEV"
    } else {
        "其他"
    }
}

fn clean(value: Option<&str>) -> String {
    let value = value
        .unwrap_or("")
        .chars()
        .filter(|character| !character.is_control())
        .collect::<String>();
    if value.is_empty() {
        "无".into()
    } else {
        value
    }
}

fn amount(value: Option<u64>) -> String {
    value.map(sol).unwrap_or_else(|| "无".into())
}

fn quote_label(mint: Pubkey) -> String {
    if is_native_quote(mint) {
        "SOL".into()
    } else {
        mint.to_string()
    }
}

fn compact_trade_quote(amount: u64, mint: Pubkey, decimals: Option<u8>) -> String {
    let value = compact_quote_amount(amount, decimals);
    if is_native_quote(mint) {
        format!("{value} SOL")
    } else if mint.to_string() == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" {
        format!("{value} USDC")
    } else {
        format!("{value} QUOTE")
    }
}

fn history_quote(history: &TokenHistory) -> (Pubkey, Option<u8>) {
    history
        .buys
        .first()
        .map(|item| (item.buy.quote_mint, item.buy.quote_decimals))
        .or_else(|| {
            history
                .sells
                .first()
                .map(|item| (item.sell.quote_mint, item.sell.quote_decimals))
        })
        .unwrap_or((spl_token::native_mint::ID, Some(9)))
}

fn signed_quote_amount(amount: i128, decimals: Option<u8>) -> String {
    let magnitude = amount.unsigned_abs().min(u64::MAX as u128) as u64;
    let value = format_raw_amount(magnitude, decimals);
    if amount > 0 {
        format!("+{value}")
    } else if amount < 0 {
        format!("-{value}")
    } else {
        value
    }
}

fn unsigned_positive_quote_amount(amount: i128, decimals: Option<u8>) -> String {
    if amount < 0 {
        signed_quote_amount(amount, decimals)
    } else {
        format_raw_amount(amount.min(u64::MAX as i128) as u64, decimals)
    }
}

fn sol(lamports: u64) -> String {
    let whole = lamports / 1_000_000_000;
    let fraction = lamports % 1_000_000_000;
    if fraction == 0 {
        return whole.to_string();
    }
    let mut result = format!("{whole}.{fraction:09}");
    while result.ends_with('0') {
        result.pop();
    }
    result
}

fn token(raw_amount: u64) -> String {
    let whole = raw_amount / 1_000_000;
    let fraction = raw_amount % 1_000_000;
    if fraction == 0 {
        return whole.to_string();
    }
    let mut result = format!("{whole}.{fraction:06}");
    while result.ends_with('0') {
        result.pop();
    }
    result
}

fn yn(value: bool) -> &'static str {
    if value {
        "是"
    } else {
        "否"
    }
}

fn timestamp() -> String {
    let offset = chrono::FixedOffset::east_opt(8 * 60 * 60).expect("valid UTC+8 offset");
    chrono::Utc::now()
        .with_timezone(&offset)
        .format("%Y-%m-%d %H:%M:%S%.6f CST")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn instruction_only_shred_trade_is_not_published_as_confirmed_buy() {
        let wallet = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let directory = std::env::temp_dir().join(format!(
            "pump-sniper-journal-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let (tx, rx) = mpsc::channel(1);
        let task = tokio::spawn(run(directory.clone(), wallet, rx));
        tx.send(JournalMessage::Chain(Box::new(instruction_only_buy(
            mint, wallet,
        ))))
        .await
        .unwrap();
        drop(tx);
        task.await.unwrap().unwrap();

        let report = tokio::fs::read_to_string(directory.join(format!("{mint}.log")))
            .await
            .unwrap_or_default();
        let _ = tokio::fs::remove_dir_all(directory).await;
        assert!(
            !report.contains("[买入]"),
            "指令级 Shred 事件不能伪装成确认成交: {report}"
        );
    }

    fn instruction_only_buy(mint: Pubkey, wallet: Pubkey) -> PumpEvent {
        PumpEvent {
            source_id: crate::listen::shred::SHRED_SOURCE_ID,
            source_mask: 1u64 << crate::listen::shred::SHRED_SOURCE_ID,
            replayed: false,
            repaired: false,
            slot: 443_949_599,
            transaction_index: 3,
            signature: "instruction-only-signature".into(),
            fee_lamports: 0,
            signer: wallet,
            mint,
            bonding_curve: Pubkey::new_unique(),
            creator: wallet,
            token_program: spl_token::ID,
            quote_mint: spl_token::native_mint::ID,
            quote_decimals: Some(9),
            name: None,
            symbol: None,
            uri: None,
            kind: crate::pump::PumpIxKind::BuyExactQuoteInV2,
            buy_quote_amount: Some(1_000_000_000),
            buy_instruction_count: 1,
            buys: vec![PumpBuy {
                wallet,
                quote_amount: 1_000_000_000,
                quote_mint: spl_token::native_mint::ID,
                quote_decimals: Some(9),
                token_amount: None,
                instruction_index: 3,
                event_index: 0,
                virtual_quote_reserves: None,
                virtual_token_reserves: None,
                exact: false,
            }],
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
    fn submission_wording_distinguishes_udp_dispatch_from_acceptance() {
        assert_eq!(
            submission_acceptance("landx/udp-dispatched"),
            "UDP已发送，未获服务端确认"
        );
        assert_eq!(submission_acceptance("temporal"), "落地节点已接受");
    }

    fn observed(wallet: Pubkey, slot: u64, tx: u64, ix: u32) -> ObservedBuy {
        ObservedBuy {
            slot,
            transaction_index: tx,
            signature: format!("sig-{tx}"),
            buy: PumpBuy {
                wallet,
                quote_amount: 50_000_000,
                quote_mint: spl_token::native_mint::ID,
                quote_decimals: Some(9),
                token_amount: Some(10),
                instruction_index: ix,
                event_index: 0,
                virtual_quote_reserves: Some(31_000_000_000),
                virtual_token_reserves: Some(1_000_000_000_000_000),
                exact: true,
            },
            fee_payer: wallet,
            fee_lamports: 5_000,
            jito_tip_lamports: 0,
        }
    }

    fn observed_sell(wallet: Pubkey, slot: u64, tx: u64, ix: u32) -> ObservedSell {
        ObservedSell {
            slot,
            transaction_index: tx,
            signature: format!("sell-{tx}"),
            sell: PumpSell {
                wallet,
                quote_amount: Some(750_000_000),
                quote_mint: spl_token::native_mint::ID,
                quote_decimals: Some(9),
                token_amount: 50_000_000,
                instruction_index: ix,
                event_index: 0,
                virtual_quote_reserves: Some(20_000_000_000),
                virtual_token_reserves: Some(1_000_000_000),
                exact: true,
            },
            fee_payer: wallet,
            fee_lamports: 5_000,
            jito_tip_lamports: 0,
        }
    }

    #[test]
    fn our_rank_excludes_dev_initial_buy() {
        let dev = Pubkey::new_unique();
        let ours = Pubkey::new_unique();
        let history = TokenHistory {
            creator: dev,
            create_slot: 100,
            buys: vec![observed(dev, 100, 2, 1), observed(ours, 100, 3, 2)],
            sells: vec![],
            seen: HashSet::new(),
        };

        let line = format_our_rank(&history, ours);
        assert!(line.contains("结论=第一位"));
        assert!(line.contains("外部买家排名=第1位"));
        assert!(line.contains("全部买入序号=第2笔/共2笔"));
        assert!(line.contains("slot=100"));
    }

    #[test]
    fn rank_uses_slot_transaction_and_instruction_order() {
        let dev = Pubkey::new_unique();
        let ours = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let mut buys = vec![
            observed(ours, 101, 9, 2),
            observed(other, 100, 8, 3),
            observed(dev, 100, 8, 1),
        ];
        buys.sort_by_key(order_key);
        let history = TokenHistory {
            creator: dev,
            create_slot: 100,
            buys,
            sells: vec![],
            seen: HashSet::new(),
        };

        let line = format_our_rank(&history, ours);
        assert!(line.contains("结论=不是第一位"));
        assert!(line.contains("外部买家排名=第2位"));
        assert!(line.contains("create_slot_delta=1"));
    }

    #[test]
    fn repeated_buys_by_one_wallet_only_take_one_wallet_rank() {
        let dev = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let ours = Pubkey::new_unique();
        let history = TokenHistory {
            creator: dev,
            create_slot: 100,
            buys: vec![
                observed(dev, 100, 1, 1),
                observed(other, 100, 2, 1),
                observed(other, 100, 3, 1),
                observed(ours, 100, 4, 1),
            ],
            sells: vec![],
            seen: HashSet::new(),
        };

        let line = format_our_rank(&history, ours);
        assert!(line.contains("外部买家排名=第2位"));
        assert!(line.contains("外部钱包共=2"));
        assert!(line.contains("全部买入序号=第4笔/共4笔"));
    }

    #[test]
    fn stats_include_market_flow_and_net_pnl_after_sell() {
        let dev = Pubkey::new_unique();
        let ours = Pubkey::new_unique();
        let mut buy = observed(ours, 100, 2, 1);
        buy.buy.quote_amount = 1_000_000_000;
        buy.buy.token_amount = Some(100_000_000);
        buy.buy.virtual_quote_reserves = Some(10_000_000_000);
        buy.buy.virtual_token_reserves = Some(1_000_000_000);
        let history = TokenHistory {
            creator: dev,
            create_slot: 100,
            buys: vec![buy],
            sells: vec![observed_sell(ours, 101, 3, 1)],
            seen: HashSet::new(),
        };

        let lines = format_stats(&history, ours);
        assert!(lines.contains("买入=1笔 / 1 SOL"));
        assert!(lines.contains("卖出=1笔 / 0.75 SOL"));
        assert!(lines.contains("净流入=0.25 SOL"));
        assert!(lines.contains("已实现=+0.250000000"));
        assert!(lines.contains("未实现=+0.500000000"));
        assert!(lines.contains("网络费=0.00001"));
        assert!(lines.contains("净盈亏=+0.749990000 SOL"));
    }

    #[test]
    fn geyser_observation_turns_submission_into_chain_confirmation() {
        let dev = Pubkey::new_unique();
        let ours = Pubkey::new_unique();
        let buy = observed(ours, 101, 2, 1);
        let signature = buy.signature.clone();
        let history = TokenHistory {
            creator: dev,
            create_slot: 100,
            buys: vec![buy],
            sells: vec![],
            seen: HashSet::new(),
        };
        let mint = Pubkey::new_unique();
        let mut pending = HashMap::from([(
            (mint, signature.clone()),
            PendingSubmission {
                side: "买入",
                submitted: Instant::now(),
            },
        )]);

        let line =
            take_confirmation(&mut pending, &signature, 101, mint, history.create_slot).unwrap();
        assert!(line.contains("[确认交易]"));
        assert!(line.contains("买入上链"));
        assert!(line.contains("[ 间隔区块=1 ]"));
        assert!(!pending.contains_key(&(mint, signature)));
    }

    #[test]
    fn monitor_stats_hide_zero_pnl_when_our_wallet_never_traded() {
        let dev = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let ours = Pubkey::new_unique();
        let history = TokenHistory {
            creator: dev,
            create_slot: 100,
            buys: vec![observed(other, 100, 2, 1)],
            sells: vec![],
            seen: HashSet::new(),
        };

        let output = format_stats(&history, ours);
        assert!(output.contains("[汇总]"));
        assert!(output.contains("[行情]"));
        assert!(!output.contains("[我的盈亏]"));
    }

    #[test]
    fn compact_monitor_create_keeps_only_identity_and_chain_position() {
        let dev = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let event = PumpEvent {
            source_id: 0,
            source_mask: 1,
            replayed: false,
            repaired: false,
            slot: 123,
            transaction_index: 0,
            signature: "create-signature".into(),
            fee_lamports: 0,
            signer: dev,
            mint,
            bonding_curve: Pubkey::new_unique(),
            creator: dev,
            token_program: spl_token::ID,
            quote_mint: spl_token::native_mint::ID,
            quote_decimals: Some(9),
            name: Some("Clear Coin".into()),
            symbol: Some("CLEAR".into()),
            uri: None,
            kind: crate::pump::PumpIxKind::Create,
            buy_quote_amount: None,
            buy_instruction_count: 0,
            buys: vec![],
            sells: vec![],
            jito_tip_lamports: None,
            jito_dont_front: false,
            is_create: true,
            is_buy: false,
            is_sell: false,
            seen_ns: 0,
        };

        let output = format_compact_create(&event);
        assert_eq!(output.trim_end().lines().count(), 1);
        assert!(output.contains("[创建"));
        assert!(output.contains("[ CLEAR/Clear Coin"));
        assert!(output.contains(&format!("[ {mint}")));
        assert!(output.contains(&format!("[ {dev}")));
        assert!(output.contains("[ slot=123"));
        assert!(output.contains("[ tx=0"));
        assert!(output.contains("[ create-signature"));
        assert!(!output.contains("curve="));
        assert!(!output.contains("token_program="));
        assert!(!output.contains("uri="));
        assert!(!output.contains("bundle"));
    }

    #[test]
    fn compact_monitor_buy_renders_every_field_on_one_line() {
        let dev = Pubkey::new_unique();
        let ours = Pubkey::new_unique();
        let mut buy = observed(ours, 101, 2, 1);
        buy.buy.quote_amount = 1_000_000_000;
        buy.buy.token_amount = Some(100_000_000);
        buy.buy.virtual_quote_reserves = Some(2_000_000_000);
        buy.buy.virtual_token_reserves = Some(100_000_000);
        let history = TokenHistory {
            creator: dev,
            create_slot: 100,
            buys: vec![buy],
            sells: vec![],
            seen: HashSet::new(),
        };

        let output = format_compact_buy(&history, &history.buys[0], ours);
        assert_eq!(output.trim_end().lines().count(), 1);
        assert!(output.contains("[买入"));
        assert!(output.contains(&ours.to_string()));
        assert!(output.contains("[ 1 SOL"));
        assert!(output.contains("[ 100"));
        assert!(output.contains("[ MCAP"));
        assert!(output.contains("[ 0.02000000000/0.01000000000 ]"));
        assert!(output.contains("[ PNL 100%"));
        assert!(output.contains("[ 101"));
        assert!(output.contains("[ 我"));
        assert!(output.contains("[ sig-2 ]"));
        assert!(!output.contains('='));
        assert!(!output.contains("排名"));
        assert!(!output.contains("累计"));
    }

    #[test]
    fn compact_monitor_sell_without_our_position_uses_zero_cost_and_pnl() {
        let dev = Pubkey::new_unique();
        let seller = Pubkey::new_unique();
        let ours = Pubkey::new_unique();
        let mut sell = observed_sell(seller, 102, 3, 1);
        sell.sell.virtual_quote_reserves = Some(1_500_000_000);
        sell.sell.virtual_token_reserves = Some(100_000_000);
        let history = TokenHistory {
            creator: dev,
            create_slot: 100,
            buys: vec![observed(dev, 100, 1, 1)],
            sells: vec![sell],
            seen: HashSet::new(),
        };

        let output = format_compact_sell(&history, &history.sells[0], ours);
        assert_eq!(output.trim_end().lines().count(), 1);
        assert!(output.contains("[卖出"));
        assert!(output.contains(&seller.to_string()));
        assert!(output.contains("[ 0.01500000000/0"));
        assert!(output.contains("[ PNL 0%"));
        assert!(output.contains("[ 102"));
        assert!(output.contains("[ 其他"));
        assert!(output.contains("[ sell-3 ]"));
        assert!(!output.contains('='));
    }

    #[test]
    fn compact_cost_uses_remaining_average_after_partial_sell() {
        let dev = Pubkey::new_unique();
        let ours = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let mut buy = observed(ours, 100, 1, 1);
        buy.buy.quote_amount = 1_000_000_000;
        buy.buy.token_amount = Some(100_000_000);
        let mut our_sell = observed_sell(ours, 101, 2, 1);
        our_sell.sell.token_amount = 50_000_000;
        let mut current = observed_sell(other, 102, 3, 1);
        current.sell.virtual_quote_reserves = Some(1_500_000_000);
        current.sell.virtual_token_reserves = Some(100_000_000);
        let history = TokenHistory {
            creator: dev,
            create_slot: 100,
            buys: vec![buy],
            sells: vec![our_sell, current],
            seen: HashSet::new(),
        };

        let output = format_compact_sell(&history, &history.sells[1], ours);
        assert!(output.contains("[ 0.01500000000/0.01000000000 ]"));
        assert!(output.contains("[ PNL 50%"));
    }

    #[test]
    fn our_full_sell_line_uses_cost_before_the_sell() {
        let dev = Pubkey::new_unique();
        let ours = Pubkey::new_unique();
        let mut buy = observed(ours, 100, 1, 1);
        buy.buy.quote_amount = 1_000_000_000;
        buy.buy.token_amount = Some(100_000_000);
        let mut sell = observed_sell(ours, 101, 2, 1);
        sell.sell.token_amount = 100_000_000;
        sell.sell.virtual_quote_reserves = Some(1_500_000_000);
        sell.sell.virtual_token_reserves = Some(100_000_000);
        let history = TokenHistory {
            creator: dev,
            create_slot: 100,
            buys: vec![buy],
            sells: vec![sell],
            seen: HashSet::new(),
        };

        let output = format_compact_sell(&history, &history.sells[0], ours);
        assert!(output.contains("[ 0.01500000000/0.01000000000 ]"));
        assert!(output.contains("[ PNL 50%"));
    }

    #[test]
    fn late_old_line_does_not_use_future_trade_cost() {
        let dev = Pubkey::new_unique();
        let ours = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let mut first_buy = observed(ours, 100, 1, 1);
        first_buy.buy.quote_amount = 1_000_000_000;
        first_buy.buy.token_amount = Some(100_000_000);
        let mut future_buy = observed(ours, 103, 4, 1);
        future_buy.buy.quote_amount = 9_000_000_000;
        future_buy.buy.token_amount = Some(100_000_000);
        let mut late_line = observed_sell(other, 102, 3, 1);
        late_line.sell.virtual_quote_reserves = Some(1_500_000_000);
        late_line.sell.virtual_token_reserves = Some(100_000_000);
        let history = TokenHistory {
            creator: dev,
            create_slot: 100,
            buys: vec![first_buy, future_buy],
            sells: vec![late_line],
            seen: HashSet::new(),
        };

        let output = format_compact_sell(&history, &history.sells[0], ours);
        assert!(output.contains("[ 0.01500000000/0.01000000000 ]"));
        assert!(output.contains("[ PNL 50%"));
    }

    #[test]
    fn incomplete_our_buy_is_not_reported_as_zero_pnl() {
        let dev = Pubkey::new_unique();
        let ours = Pubkey::new_unique();
        let mut buy = observed(ours, 100, 1, 1);
        buy.buy.token_amount = None;
        buy.buy.virtual_quote_reserves = None;
        buy.buy.virtual_token_reserves = None;
        let history = TokenHistory {
            creator: dev,
            create_slot: 100,
            buys: vec![buy],
            sells: vec![],
            seen: HashSet::new(),
        };

        let output = format_compact_buy(&history, &history.buys[0], ours);
        assert!(output.contains("未知/未知"));
        assert!(output.contains("[ PNL 未知"));
    }

    #[test]
    fn main_market_line_matches_token_line() {
        let line =
            "[ 08-30 23:00:45.742 ] [创建  ] [ MRHOOD/MrHood ] [ abc                    ] [ def                    ] [ slot=1         ] [ tx=0         ] [ sig        ]\n";
        assert_eq!(main_log_content(line), line.trim_end_matches('\n'));
    }
}
