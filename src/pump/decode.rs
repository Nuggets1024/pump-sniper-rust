use crate::constants::*;
use base64::Engine;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use yellowstone_grpc_proto::prelude::{SubscribeUpdateTransaction, TransactionStatusMeta};

struct InstructionRef<'a> {
    program_id_index: u32,
    accounts: &'a [u8],
    data: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PumpIxKind {
    Create,
    CreateV2,
    Buy,
    BuyExactSolIn,
    BuyV2,
    BuyExactQuoteInV2,
    Sell,
    AmmBuy,
    AmmSell,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PumpBuy {
    pub wallet: Pubkey,
    pub quote_amount: u64,
    pub quote_mint: Pubkey,
    pub quote_decimals: Option<u8>,
    pub token_amount: Option<u64>,
    pub instruction_index: u32,
    pub event_index: u16,
    pub virtual_quote_reserves: Option<u64>,
    pub virtual_token_reserves: Option<u64>,
    /// true 表示金额来自 Pump TradeEvent；false 表示仅有买入指令参数。
    pub exact: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PumpSell {
    pub wallet: Pubkey,
    pub quote_amount: Option<u64>,
    pub quote_mint: Pubkey,
    pub quote_decimals: Option<u8>,
    pub token_amount: u64,
    pub instruction_index: u32,
    pub event_index: u16,
    pub virtual_quote_reserves: Option<u64>,
    pub virtual_token_reserves: Option<u64>,
    pub exact: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PumpEvent {
    /// 0..62 为 Geyser 编号，63 保留给 RPC repair。
    pub source_id: u8,
    /// 当前已知来源位图；首个低延迟事件通常只包含一个 bit。
    pub source_mask: u64,
    /// 来自断线后的历史重放；可用于确认/审计，但不得再次触发新订单。
    pub replayed: bool,
    /// 来自 RPC gap repair；只用于确认/审计，不触发策略。
    pub repaired: bool,
    pub slot: u64,
    pub transaction_index: u64,
    pub signature: String,
    pub fee_lamports: u64,
    pub signer: Pubkey,
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub creator: Pubkey,
    pub token_program: Pubkey,
    pub quote_mint: Pubkey,
    pub quote_decimals: Option<u8>,
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub uri: Option<String>,
    pub kind: PumpIxKind,
    /// Buy 的报价币支出上限或 BuyExactQuoteIn 的确切输入合计。
    pub buy_quote_amount: Option<u64>,
    pub buy_instruction_count: u16,
    pub buys: Vec<PumpBuy>,
    pub sells: Vec<PumpSell>,
    /// 交易中存在发往 Jito tip account 的 SystemProgram transfer。
    pub jito_tip_lamports: Option<u64>,
    /// 任意账户以 `jitodontfront` 开头；只是 bundle 线索，不是 bundle 证明。
    pub jito_dont_front: bool,
    pub is_create: bool,
    pub is_buy: bool,
    pub is_sell: bool,
    pub seen_ns: u64,
}

pub fn is_native_quote(mint: Pubkey) -> bool {
    mint == spl_token::native_mint::ID
}

pub fn format_raw_amount(amount: u64, decimals: Option<u8>) -> String {
    let Some(decimals) = decimals else {
        return amount.to_string();
    };
    if decimals == 0 {
        return amount.to_string();
    }
    let scale = 10u128.saturating_pow(decimals.into());
    let amount = amount as u128;
    let whole = amount / scale;
    let fraction = amount % scale;
    if fraction == 0 {
        return whole.to_string();
    }
    let mut value = format!("{whole}.{fraction:0width$}", width = decimals as usize);
    while value.ends_with('0') {
        value.pop();
    }
    value
}

pub fn raw_amount_as_f64(amount: u64, decimals: Option<u8>) -> Option<f64> {
    let decimals = decimals?;
    Some(amount as f64 / 10f64.powi(decimals.into()))
}

/// scan 发现流的廉价预筛选：只让 Pump create/create_v2 进入完整解码。
pub fn contains_create_instruction(upd: &SubscribeUpdateTransaction) -> bool {
    let Some(tx_info) = upd.transaction.as_ref() else {
        return false;
    };
    if tx_info
        .meta
        .as_ref()
        .and_then(|meta| meta.err.as_ref())
        .is_some()
    {
        return false;
    }
    let Some(message) = tx_info
        .transaction
        .as_ref()
        .and_then(|transaction| transaction.message.as_ref())
    else {
        return false;
    };
    let meta = tx_info.meta.as_ref();
    let keys = account_keys(message, meta);
    execution_order_instructions(message, meta)
        .into_iter()
        .any(|instruction| {
            keys.get(instruction.program_id_index as usize) == Some(&*PUMP_PROGRAM_ID)
                && instruction.data.get(..8).is_some_and(|data| {
                    data == IX_CREATE.as_slice() || data == IX_CREATE_V2.as_slice()
                })
        })
}

pub fn decode_transactions(slot: u64, upd: &SubscribeUpdateTransaction) -> Vec<PumpEvent> {
    let Some(tx_info) = upd.transaction.as_ref() else {
        return vec![];
    };
    let Some(tx) = tx_info.transaction.as_ref() else {
        return vec![];
    };
    let Some(msg) = tx.message.as_ref() else {
        return vec![];
    };
    let meta = tx_info.meta.as_ref();
    if meta.and_then(|value| value.err.as_ref()).is_some() {
        return vec![];
    }
    let keys = account_keys(msg, meta);
    let mut mints = Vec::new();
    for instruction in execution_order_instructions(msg, meta) {
        let Some(program) = keys.get(instruction.program_id_index as usize) else {
            continue;
        };
        if *program != *PUMP_PROGRAM_ID || instruction.data.len() < 8 {
            continue;
        }
        let discriminator: [u8; 8] = instruction.data[..8].try_into().expect("8-byte slice");
        if let Some(mint) = instruction_mint(discriminator, instruction.accounts, &keys) {
            if !mints.contains(&mint) {
                mints.push(mint);
            }
        }
    }
    if let Some(meta) = meta {
        for trade in parse_trade_events(&meta.log_messages) {
            if !mints.contains(&trade.mint) {
                mints.push(trade.mint);
            }
        }
    }
    let mut events = mints
        .into_iter()
        .filter_map(|mint| decode_transaction_for_mint(slot, upd, mint))
        .collect::<Vec<_>>();
    events.extend(decode_amm_transactions(slot, upd, &keys));
    events
}

/// Compatibility helper for callers that only expect one mint per transaction.
pub fn decode_transaction(slot: u64, upd: &SubscribeUpdateTransaction) -> Option<PumpEvent> {
    decode_transactions(slot, upd).into_iter().next()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AmmSide {
    Buy,
    Sell,
}

struct AmmCandidate {
    instruction_index: u32,
    side: AmmSide,
    mint: Pubkey,
    pool: Pubkey,
    wallet: Pubkey,
    quote_mint: Pubkey,
    token_program: Pubkey,
    quote_amount: Option<u64>,
    token_amount: Option<u64>,
}

struct AmmTradeEvent {
    side: AmmSide,
    pool: Pubkey,
    wallet: Pubkey,
    quote_amount: u64,
    token_amount: u64,
    pool_quote_reserves: u64,
    pool_token_reserves: u64,
}

const AMM_BUY_EVENT_DISCRIMINATOR: [u8; 8] = [103, 244, 82, 31, 44, 245, 119, 119];
const AMM_SELL_EVENT_DISCRIMINATOR: [u8; 8] = [62, 47, 55, 10, 165, 3, 220, 42];

fn decode_amm_transactions(
    slot: u64,
    upd: &SubscribeUpdateTransaction,
    keys: &[Pubkey],
) -> Vec<PumpEvent> {
    let Some(tx_info) = upd.transaction.as_ref() else {
        return vec![];
    };
    let Some(tx) = tx_info.transaction.as_ref() else {
        return vec![];
    };
    let Some(msg) = tx.message.as_ref() else {
        return vec![];
    };
    let meta = tx_info.meta.as_ref();
    let signature = tx
        .signatures
        .first()
        .map(|value| bs58::encode(value).into_string())
        .unwrap_or_default();
    let mut candidates = Vec::new();
    for (instruction_index, instruction) in execution_order_instructions(msg, meta)
        .into_iter()
        .enumerate()
    {
        if keys.get(instruction.program_id_index as usize) != Some(&*PUMP_AMM_PROGRAM_ID)
            || instruction.data.len() < 24
        {
            continue;
        }
        let discriminator: [u8; 8] = instruction.data[..8].try_into().expect("8-byte slice");
        let side = match discriminator {
            IX_BUY | IX_AMM_BUY_EXACT_QUOTE_IN => AmmSide::Buy,
            IX_SELL => AmmSide::Sell,
            _ => continue,
        };
        let account = |index: usize| {
            instruction
                .accounts
                .get(index)
                .and_then(|account| keys.get(*account as usize))
                .copied()
        };
        let (quote_amount, token_amount) = match discriminator {
            IX_BUY => (
                read_instruction_u64(instruction.data, 16),
                read_instruction_u64(instruction.data, 8),
            ),
            IX_AMM_BUY_EXACT_QUOTE_IN => (
                read_instruction_u64(instruction.data, 8),
                read_instruction_u64(instruction.data, 16),
            ),
            IX_SELL => (None, read_instruction_u64(instruction.data, 8)),
            _ => unreachable!(),
        };
        let (Some(pool), Some(wallet), Some(mint), Some(quote_mint)) =
            (account(0), account(1), account(3), account(4))
        else {
            continue;
        };
        candidates.push(AmmCandidate {
            instruction_index: instruction_index as u32,
            side,
            mint,
            pool,
            wallet,
            quote_mint,
            token_program: account(11).unwrap_or(spl_token::ID),
            quote_amount,
            token_amount,
        });
    }
    if candidates.is_empty() {
        return vec![];
    }

    let mut exact = meta
        .map(|value| parse_amm_trade_events(&value.log_messages))
        .unwrap_or_default();
    let signer = keys.first().copied().unwrap_or_default();
    let jito_tip_lamports = jito_tip_lamports(msg, meta, keys);
    let jito_dont_front = keys
        .iter()
        .any(|key| key.to_string().starts_with("jitodontfront"));
    let mut events = Vec::<PumpEvent>::new();

    for candidate in candidates {
        let exact_index = exact.iter().position(|event| {
            event.side == candidate.side
                && event.pool == candidate.pool
                && event.wallet == candidate.wallet
        });
        let exact_trade = exact_index.map(|index| exact.remove(index));
        let quote_decimals = token_decimals(meta, candidate.quote_mint);
        let event_index = events
            .iter()
            .position(|event| event.mint == candidate.mint)
            .unwrap_or_else(|| {
                events.push(PumpEvent {
                    source_id: 0,
                    source_mask: 0,
                    replayed: false,
                    repaired: false,
                    slot,
                    transaction_index: tx_info.index,
                    signature: signature.clone(),
                    fee_lamports: meta.map(|value| value.fee).unwrap_or(0),
                    signer,
                    mint: candidate.mint,
                    bonding_curve: candidate.pool,
                    creator: signer,
                    token_program: candidate.token_program,
                    quote_mint: candidate.quote_mint,
                    quote_decimals,
                    name: None,
                    symbol: None,
                    uri: None,
                    kind: if candidate.side == AmmSide::Buy {
                        PumpIxKind::AmmBuy
                    } else {
                        PumpIxKind::AmmSell
                    },
                    buy_quote_amount: None,
                    buy_instruction_count: 0,
                    buys: vec![],
                    sells: vec![],
                    jito_tip_lamports,
                    jito_dont_front,
                    is_create: false,
                    is_buy: false,
                    is_sell: false,
                    seen_ns: now_ns(),
                });
                events.len() - 1
            });
        let event = &mut events[event_index];
        match candidate.side {
            AmmSide::Buy => {
                let quote_amount = exact_trade
                    .as_ref()
                    .map(|trade| trade.quote_amount)
                    .or(candidate.quote_amount)
                    .unwrap_or(0);
                event.is_buy = true;
                event.buy_instruction_count = event.buy_instruction_count.saturating_add(1);
                event.buy_quote_amount = Some(
                    event
                        .buy_quote_amount
                        .unwrap_or(0)
                        .saturating_add(quote_amount),
                );
                event.buys.push(PumpBuy {
                    wallet: candidate.wallet,
                    quote_amount,
                    quote_mint: candidate.quote_mint,
                    quote_decimals,
                    token_amount: exact_trade
                        .as_ref()
                        .map(|trade| trade.token_amount)
                        .or(candidate.token_amount),
                    instruction_index: candidate.instruction_index,
                    event_index: event.buys.len().try_into().unwrap_or(u16::MAX),
                    virtual_quote_reserves: exact_trade
                        .as_ref()
                        .map(|trade| trade.pool_quote_reserves),
                    virtual_token_reserves: exact_trade
                        .as_ref()
                        .map(|trade| trade.pool_token_reserves),
                    exact: exact_trade.is_some(),
                });
            }
            AmmSide::Sell => {
                event.is_sell = true;
                event.sells.push(PumpSell {
                    wallet: candidate.wallet,
                    quote_amount: exact_trade.as_ref().map(|trade| trade.quote_amount),
                    quote_mint: candidate.quote_mint,
                    quote_decimals,
                    token_amount: exact_trade
                        .as_ref()
                        .map(|trade| trade.token_amount)
                        .or(candidate.token_amount)
                        .unwrap_or(0),
                    instruction_index: candidate.instruction_index,
                    event_index: event.sells.len().try_into().unwrap_or(u16::MAX),
                    virtual_quote_reserves: exact_trade
                        .as_ref()
                        .map(|trade| trade.pool_quote_reserves),
                    virtual_token_reserves: exact_trade
                        .as_ref()
                        .map(|trade| trade.pool_token_reserves),
                    exact: exact_trade.is_some(),
                });
            }
        }
    }
    events
}

fn parse_amm_trade_events(logs: &[String]) -> Vec<AmmTradeEvent> {
    logs.iter()
        .filter_map(|line| line.strip_prefix("Program data: "))
        .filter_map(|encoded| {
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .ok()
        })
        .filter_map(|data| parse_amm_trade_event(&data))
        .collect()
}

fn parse_amm_trade_event(data: &[u8]) -> Option<AmmTradeEvent> {
    if data.len() < 184 {
        return None;
    }
    let discriminator: [u8; 8] = data[..8].try_into().ok()?;
    let side = match discriminator {
        AMM_BUY_EVENT_DISCRIMINATOR => AmmSide::Buy,
        AMM_SELL_EVENT_DISCRIMINATOR => AmmSide::Sell,
        _ => return None,
    };
    Some(AmmTradeEvent {
        side,
        token_amount: u64::from_le_bytes(data[16..24].try_into().ok()?),
        pool_token_reserves: u64::from_le_bytes(data[48..56].try_into().ok()?),
        pool_quote_reserves: u64::from_le_bytes(data[56..64].try_into().ok()?),
        quote_amount: u64::from_le_bytes(data[64..72].try_into().ok()?),
        pool: Pubkey::try_from(&data[120..152]).ok()?,
        wallet: Pubkey::try_from(&data[152..184]).ok()?,
    })
}

fn read_instruction_u64(data: &[u8], offset: usize) -> Option<u64> {
    u64::from_le_bytes(data.get(offset..offset.checked_add(8)?)?.try_into().ok()?).into()
}

fn decode_transaction_for_mint(
    slot: u64,
    upd: &SubscribeUpdateTransaction,
    expected_mint: Pubkey,
) -> Option<PumpEvent> {
    let tx_info = upd.transaction.as_ref()?;
    let tx = tx_info.transaction.as_ref()?;
    let msg = tx.message.as_ref()?;
    let meta = tx_info.meta.as_ref();
    if meta.and_then(|m| m.err.as_ref()).is_some() {
        return None;
    }

    let keys = account_keys(msg, meta);
    if keys.is_empty() {
        return None;
    }
    let signer = keys[0];

    let sig = tx
        .signatures
        .first()
        .map(|s| bs58::encode(s).into_string())
        .unwrap_or_default();

    let mut kind = None;
    let mut mint = None;
    let mut curve = None;
    let mut creator = None;
    let mut token_program = spl_token::ID;
    let mut quote_mint = spl_token::native_mint::ID;
    let mut quote_decimals = Some(9);
    let mut name = None;
    let mut symbol = None;
    let mut uri = None;
    let mut buy_quote_amount = None;
    let mut buy_instruction_count = 0u16;
    let mut buy_candidates = Vec::new();
    let mut sell_candidates = Vec::new();
    let mut is_create = false;
    let mut is_buy = false;
    let mut is_sell = false;

    for (instruction_index, ix) in execution_order_instructions(msg, meta)
        .into_iter()
        .enumerate()
    {
        let pid_idx = ix.program_id_index as usize;
        if pid_idx >= keys.len() || keys[pid_idx] != *PUMP_PROGRAM_ID {
            continue;
        }
        if ix.data.len() < 8 {
            continue;
        }
        let disc: [u8; 8] = ix.data[..8].try_into().ok()?;
        if instruction_mint(disc, ix.accounts, &keys) != Some(expected_mint) {
            continue;
        }
        let acc = |i: usize| {
            ix.accounts
                .get(i)
                .and_then(|idx| keys.get(*idx as usize))
                .copied()
        };

        match disc {
            IX_CREATE | IX_CREATE_V2 => {
                is_create = true;
                kind = Some(if disc == IX_CREATE_V2 {
                    PumpIxKind::CreateV2
                } else {
                    PumpIxKind::Create
                });
                mint = acc(0);
                curve = acc(2);
                creator = Some(signer);
                if let Some(tp) = acc(ix.accounts.len().saturating_sub(5)) {
                    if tp == spl_token_2022::ID || tp == spl_token::ID {
                        token_program = tp;
                    }
                }
                // 现网 create_v2 走 Token-2022
                if disc == IX_CREATE_V2 {
                    token_program = spl_token_2022::ID;
                }
                if let Some(metadata) = parse_create_data(ix.data) {
                    name = Some(metadata.name);
                    symbol = Some(metadata.symbol);
                    uri = Some(metadata.uri);
                    creator = metadata.creator.or(creator);
                }
            }
            IX_BUY | IX_BUY_EXACT_SOL_IN | IX_BUY_V2 | IX_BUY_EXACT_QUOTE_IN_V2 => {
                is_buy = true;
                buy_instruction_count = buy_instruction_count.saturating_add(1);
                if kind.is_none() {
                    kind = Some(match disc {
                        IX_BUY => PumpIxKind::Buy,
                        IX_BUY_EXACT_SOL_IN => PumpIxKind::BuyExactSolIn,
                        IX_BUY_V2 => PumpIxKind::BuyV2,
                        IX_BUY_EXACT_QUOTE_IN_V2 => PumpIxKind::BuyExactQuoteInV2,
                        _ => unreachable!(),
                    });
                }
                let is_v2 = disc == IX_BUY_V2 || disc == IX_BUY_EXACT_QUOTE_IN_V2;
                mint = mint.or_else(|| acc(if is_v2 { 1 } else { 2 }));
                curve = curve.or_else(|| acc(if is_v2 { 10 } else { 3 }));
                if let Some(tp) = acc(if is_v2 { 3 } else { 8 }) {
                    if tp == spl_token_2022::ID || tp == spl_token::ID {
                        token_program = tp;
                    }
                }
                if is_v2 {
                    quote_mint = acc(2).unwrap_or(quote_mint);
                    quote_decimals = token_decimals(meta, quote_mint);
                }
                if let Some(amount) = parse_buy_quote_amount(&disc, ix.data) {
                    buy_quote_amount =
                        Some(buy_quote_amount.unwrap_or(0u64).saturating_add(amount));
                    if let Some(wallet) = acc(if is_v2 { 13 } else { 6 }) {
                        buy_candidates.push(PumpBuy {
                            wallet,
                            quote_amount: amount,
                            quote_mint,
                            quote_decimals,
                            token_amount: None,
                            instruction_index: instruction_index as u32,
                            event_index: buy_instruction_count.saturating_sub(1),
                            virtual_quote_reserves: None,
                            virtual_token_reserves: None,
                            exact: false,
                        });
                    }
                }
            }
            IX_SELL | IX_SELL_V2 => {
                is_sell = true;
                if kind.is_none() {
                    kind = Some(PumpIxKind::Sell);
                }
                let is_v2 = disc == IX_SELL_V2;
                mint = mint.or_else(|| acc(if is_v2 { 1 } else { 2 }));
                if !is_v2 {
                    curve = curve.or_else(|| acc(3));
                } else if let Some(tp) = acc(3) {
                    if tp == spl_token_2022::ID || tp == spl_token::ID {
                        token_program = tp;
                    }
                }
                if is_v2 {
                    curve = curve.or_else(|| acc(10));
                    quote_mint = acc(2).unwrap_or(quote_mint);
                    quote_decimals = token_decimals(meta, quote_mint);
                }
                if ix.data.len() >= 16 {
                    let wallet = acc(if is_v2 { 13 } else { 6 });
                    if let (Some(wallet), Ok(bytes)) = (wallet, ix.data[8..16].try_into()) {
                        sell_candidates.push(PumpSell {
                            wallet,
                            quote_amount: None,
                            quote_mint,
                            quote_decimals,
                            token_amount: u64::from_le_bytes(bytes),
                            instruction_index: instruction_index as u32,
                            event_index: sell_candidates.len().try_into().unwrap_or(u16::MAX),
                            virtual_quote_reserves: None,
                            virtual_token_reserves: None,
                            exact: false,
                        });
                    }
                }
            }
            _ => {}
        }
    }

    let exact_trades = meta
        .map(|value| {
            parse_trade_events(&value.log_messages)
                .into_iter()
                .filter(|trade| trade.mint == expected_mint)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if let Some(trade) = exact_trades.first() {
        quote_mint = trade.quote_mint;
        quote_decimals = token_decimals(meta, quote_mint);
        mint = Some(expected_mint);
        if kind.is_none() {
            kind = Some(if trade.is_buy {
                PumpIxKind::BuyV2
            } else {
                PumpIxKind::Sell
            });
        }
        is_buy |= exact_trades.iter().any(|value| value.is_buy);
        is_sell |= exact_trades.iter().any(|value| !value.is_buy);
    }
    let kind = kind?;
    let mint = mint?;
    let bonding_curve = curve.unwrap_or_else(|| crate::pda::bonding_curve(&mint));
    let creator = creator.unwrap_or(signer);
    let exact_buys = exact_trades.iter().filter(|trade| trade.is_buy);
    let buys = if exact_buys.clone().next().is_none() {
        buy_candidates
    } else {
        exact_buys
            .enumerate()
            .map(|(i, trade)| PumpBuy {
                wallet: trade.wallet,
                quote_amount: trade.quote_amount,
                quote_mint: trade.quote_mint,
                quote_decimals: token_decimals(meta, trade.quote_mint),
                token_amount: Some(trade.token_amount),
                instruction_index: buy_candidates
                    .get(i)
                    .map(|buy| buy.instruction_index)
                    .unwrap_or(u32::MAX),
                event_index: i.try_into().unwrap_or(u16::MAX),
                virtual_quote_reserves: Some(trade.virtual_quote_reserves),
                virtual_token_reserves: Some(trade.virtual_token_reserves),
                exact: true,
            })
            .collect::<Vec<_>>()
    };
    let exact_sells = exact_trades.iter().filter(|trade| !trade.is_buy);
    let sells = if exact_sells.clone().next().is_none() {
        sell_candidates
    } else {
        exact_sells
            .enumerate()
            .map(|(i, trade)| PumpSell {
                wallet: trade.wallet,
                quote_amount: Some(trade.quote_amount),
                quote_mint: trade.quote_mint,
                quote_decimals: token_decimals(meta, trade.quote_mint),
                token_amount: trade.token_amount,
                instruction_index: sell_candidates
                    .get(i)
                    .map(|sell| sell.instruction_index)
                    .unwrap_or(u32::MAX),
                event_index: i.try_into().unwrap_or(u16::MAX),
                virtual_quote_reserves: Some(trade.virtual_quote_reserves),
                virtual_token_reserves: Some(trade.virtual_token_reserves),
                exact: true,
            })
            .collect::<Vec<_>>()
    };
    if !buys.is_empty() {
        buy_quote_amount = Some(
            buys.iter()
                .fold(0u64, |sum, buy| sum.saturating_add(buy.quote_amount)),
        );
        buy_instruction_count = buys.len().try_into().unwrap_or(u16::MAX);
    }
    let jito_tip_lamports = jito_tip_lamports(msg, meta, &keys);
    let jito_dont_front = keys
        .iter()
        .any(|key| key.to_string().starts_with("jitodontfront"));

    Some(PumpEvent {
        source_id: 0,
        source_mask: 0,
        replayed: false,
        repaired: false,
        slot,
        transaction_index: tx_info.index,
        signature: sig,
        fee_lamports: meta.map(|value| value.fee).unwrap_or(0),
        signer,
        mint,
        bonding_curve,
        creator,
        token_program,
        quote_mint,
        quote_decimals,
        name,
        symbol,
        uri,
        kind,
        buy_quote_amount,
        buy_instruction_count,
        buys,
        sells,
        jito_tip_lamports,
        jito_dont_front,
        is_create,
        is_buy,
        is_sell,
        seen_ns: now_ns(),
    })
}

/// Return outer instructions and their CPI instructions in execution order.
/// Aggregators commonly invoke Pump through CPI, so only scanning the message's
/// top-level instructions silently drops otherwise valid Pump trades.
fn execution_order_instructions<'a>(
    msg: &'a yellowstone_grpc_proto::prelude::Message,
    meta: Option<&'a TransactionStatusMeta>,
) -> Vec<InstructionRef<'a>> {
    let mut result = Vec::with_capacity(
        msg.instructions.len()
            + meta
                .map(|value| {
                    value
                        .inner_instructions
                        .iter()
                        .map(|group| group.instructions.len())
                        .sum::<usize>()
                })
                .unwrap_or(0),
    );

    for (outer_index, instruction) in msg.instructions.iter().enumerate() {
        result.push(InstructionRef {
            program_id_index: instruction.program_id_index,
            accounts: &instruction.accounts,
            data: &instruction.data,
        });
        if let Some(group) = meta.and_then(|value| {
            value
                .inner_instructions
                .iter()
                .find(|group| group.index as usize == outer_index)
        }) {
            result.extend(group.instructions.iter().map(|inner| InstructionRef {
                program_id_index: inner.program_id_index,
                accounts: &inner.accounts,
                data: &inner.data,
            }));
        }
    }
    result
}

struct TradeEvent {
    mint: Pubkey,
    wallet: Pubkey,
    quote_amount: u64,
    quote_mint: Pubkey,
    token_amount: u64,
    is_buy: bool,
    virtual_quote_reserves: u64,
    virtual_token_reserves: u64,
}

const TRADE_EVENT_DISCRIMINATOR: [u8; 8] = [0xbd, 0xdb, 0x7f, 0xd3, 0x4e, 0xe6, 0x61, 0xee];

fn parse_trade_events(logs: &[String]) -> Vec<TradeEvent> {
    logs.iter()
        .filter_map(|line| line.strip_prefix("Program data: "))
        .filter_map(|encoded| {
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .ok()
        })
        .filter_map(|data| parse_trade_event(&data))
        .map(|trade| {
            let quote_mint = trade
                .quote_mint
                .filter(|mint| *mint != Pubkey::default())
                .unwrap_or(spl_token::native_mint::ID);
            TradeEvent {
                mint: trade.mint,
                wallet: trade.wallet,
                quote_amount: trade.quote_amount.unwrap_or(trade.legacy_sol_amount),
                quote_mint,
                token_amount: trade.token_amount,
                is_buy: trade.is_buy,
                virtual_quote_reserves: trade
                    .quote_virtual_reserves
                    .unwrap_or(trade.legacy_virtual_sol_reserves),
                virtual_token_reserves: trade.virtual_token_reserves,
            }
        })
        .collect()
}

struct RawTradeEvent {
    mint: Pubkey,
    legacy_sol_amount: u64,
    token_amount: u64,
    is_buy: bool,
    wallet: Pubkey,
    legacy_virtual_sol_reserves: u64,
    virtual_token_reserves: u64,
    quote_mint: Option<Pubkey>,
    quote_amount: Option<u64>,
    quote_virtual_reserves: Option<u64>,
}

fn parse_trade_event(data: &[u8]) -> Option<RawTradeEvent> {
    // discriminator + mint + sol + token + is_buy + user
    if data.len() < 113 || data[..8] != TRADE_EVENT_DISCRIMINATOR {
        return None;
    }
    let (quote_mint, quote_amount, quote_virtual_reserves) =
        parse_trade_event_quote_fields(data).unwrap_or((None, None, None));
    Some(RawTradeEvent {
        mint: Pubkey::try_from(&data[8..40]).ok()?,
        legacy_sol_amount: u64::from_le_bytes(data[40..48].try_into().ok()?),
        token_amount: u64::from_le_bytes(data[48..56].try_into().ok()?),
        is_buy: data[56] != 0,
        wallet: Pubkey::try_from(&data[57..89]).ok()?,
        legacy_virtual_sol_reserves: u64::from_le_bytes(data[97..105].try_into().ok()?),
        virtual_token_reserves: u64::from_le_bytes(data[105..113].try_into().ok()?),
        quote_mint,
        quote_amount,
        quote_virtual_reserves,
    })
}

fn parse_trade_event_quote_fields(
    data: &[u8],
) -> Option<(Option<Pubkey>, Option<u64>, Option<u64>)> {
    // Skip the fixed and variable fields preceding the quote extension in the
    // current official TradeEvent Borsh layout. Older events legitimately end
    // before this extension and fall back to native SOL fields.
    let mut cursor = 113usize;
    cursor = cursor.checked_add(16 + 32 + 8 + 8 + 32 + 8 + 8 + 1 + 8 * 4)?;
    let name_len = read_u32(data, &mut cursor)? as usize;
    cursor = cursor.checked_add(name_len + 1 + 8 * 4)?;
    let shareholders = read_u32(data, &mut cursor)? as usize;
    cursor = cursor.checked_add(shareholders.checked_mul(34)?)?;
    let quote_mint = Pubkey::try_from(data.get(cursor..cursor.checked_add(32)?)?).ok()?;
    cursor += 32;
    let quote_amount = read_u64(data, &mut cursor)?;
    let virtual_quote_reserves = read_u64(data, &mut cursor)?;
    Some((
        Some(quote_mint),
        Some(quote_amount),
        Some(virtual_quote_reserves),
    ))
}

fn read_u32(data: &[u8], cursor: &mut usize) -> Option<u32> {
    let end = cursor.checked_add(4)?;
    let value = u32::from_le_bytes(data.get(*cursor..end)?.try_into().ok()?);
    *cursor = end;
    Some(value)
}

fn read_u64(data: &[u8], cursor: &mut usize) -> Option<u64> {
    let end = cursor.checked_add(8)?;
    let value = u64::from_le_bytes(data.get(*cursor..end)?.try_into().ok()?);
    *cursor = end;
    Some(value)
}

struct CreateMetadata {
    name: String,
    symbol: String,
    uri: String,
    creator: Option<Pubkey>,
}

fn account_keys(
    msg: &yellowstone_grpc_proto::prelude::Message,
    meta: Option<&TransactionStatusMeta>,
) -> Vec<Pubkey> {
    let mut keys = msg
        .account_keys
        .iter()
        .filter_map(|key| Pubkey::try_from(key.as_slice()).ok())
        .collect::<Vec<_>>();
    if let Some(meta) = meta {
        append_loaded(&mut keys, meta);
    }
    keys
}

fn instruction_mint(discriminator: [u8; 8], accounts: &[u8], keys: &[Pubkey]) -> Option<Pubkey> {
    let index = match discriminator {
        IX_CREATE | IX_CREATE_V2 => 0,
        IX_BUY | IX_BUY_EXACT_SOL_IN | IX_SELL => 2,
        IX_BUY_V2 | IX_BUY_EXACT_QUOTE_IN_V2 | IX_SELL_V2 => 1,
        _ => return None,
    };
    accounts
        .get(index)
        .and_then(|account| keys.get(*account as usize))
        .copied()
}

fn token_decimals(meta: Option<&TransactionStatusMeta>, mint: Pubkey) -> Option<u8> {
    if mint == spl_token::native_mint::ID {
        return Some(9);
    }
    let mint = mint.to_string();
    meta.into_iter()
        .flat_map(|value| {
            value
                .pre_token_balances
                .iter()
                .chain(value.post_token_balances.iter())
        })
        .find(|balance| balance.mint == mint)
        .and_then(|balance| balance.ui_token_amount.as_ref())
        .and_then(|amount| amount.decimals.try_into().ok())
}

fn append_loaded(keys: &mut Vec<Pubkey>, meta: &TransactionStatusMeta) {
    for k in meta
        .loaded_writable_addresses
        .iter()
        .chain(meta.loaded_readonly_addresses.iter())
    {
        if let Ok(pk) = Pubkey::try_from(k.as_slice()) {
            keys.push(pk);
        }
    }
}

fn parse_buy_quote_amount(disc: &[u8; 8], data: &[u8]) -> Option<u64> {
    if data.len() < 24 {
        return None;
    }
    // Buy：amount + max_sol_cost
    // BuyExactSolIn：spendable_sol_in + min_tokens_out
    if disc == &IX_BUY || disc == &IX_BUY_V2 {
        Some(u64::from_le_bytes(data[16..24].try_into().ok()?))
    } else {
        Some(u64::from_le_bytes(data[8..16].try_into().ok()?))
    }
}

/// 解码 discriminator 后的 Borsh `name` / `symbol` / `uri` / `creator`。
fn parse_create_data(data: &[u8]) -> Option<CreateMetadata> {
    if data.len() < 8 + 12 + 32 {
        return None;
    }
    let mut i = 8usize;
    let mut read_string = || {
        if i + 4 > data.len() {
            return None;
        }
        let n = u32::from_le_bytes(data[i..i + 4].try_into().ok()?) as usize;
        i = i.checked_add(4)?;
        let end = i.checked_add(n)?;
        let value = std::str::from_utf8(data.get(i..end)?).ok()?.to_owned();
        i = end;
        Some(value)
    };
    let name = read_string()?;
    let symbol = read_string()?;
    let uri = read_string()?;
    let creator = data
        .get(i..i.checked_add(32)?)
        .and_then(|bytes| Pubkey::try_from(bytes).ok());
    Some(CreateMetadata {
        name,
        symbol,
        uri,
        creator,
    })
}

fn jito_tip_lamports(
    msg: &yellowstone_grpc_proto::prelude::Message,
    meta: Option<&TransactionStatusMeta>,
    keys: &[Pubkey],
) -> Option<u64> {
    let outer = msg.instructions.iter().fold(0u64, |total, ix| {
        total.saturating_add(jito_tip_for_instruction(
            ix.program_id_index,
            &ix.accounts,
            &ix.data,
            keys,
        ))
    });
    let inner = meta
        .into_iter()
        .flat_map(|m| m.inner_instructions.iter())
        .flat_map(|group| group.instructions.iter())
        .fold(0u64, |total, ix| {
            total.saturating_add(jito_tip_for_instruction(
                ix.program_id_index,
                &ix.accounts,
                &ix.data,
                keys,
            ))
        });
    let total = outer.saturating_add(inner);
    (total > 0).then_some(total)
}

fn jito_tip_for_instruction(
    program_id_index: u32,
    accounts: &[u8],
    data: &[u8],
    keys: &[Pubkey],
) -> u64 {
    let Some(program) = keys.get(program_id_index as usize) else {
        return 0;
    };
    if *program != solana_sdk::system_program::ID {
        return 0;
    }
    let Some(destination) = accounts.get(1).and_then(|index| keys.get(*index as usize)) else {
        return 0;
    };
    if !JITO_TIP_ACCOUNTS.contains(destination) {
        return 0;
    }
    parse_system_transfer(data).unwrap_or(0)
}

fn parse_system_transfer(data: &[u8]) -> Option<u64> {
    if data.len() < 12 || u32::from_le_bytes(data[..4].try_into().ok()?) != 2 {
        return None;
    }
    Some(u64::from_le_bytes(data[4..12].try_into().ok()?))
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use yellowstone_grpc_proto::prelude::{
        CompiledInstruction, InnerInstruction, InnerInstructions, Message,
        SubscribeUpdateTransactionInfo, TokenBalance, Transaction, UiTokenAmount,
    };

    #[allow(clippy::too_many_arguments)]
    fn trade_event_log(
        mint: Pubkey,
        wallet: Pubkey,
        quote_mint: Pubkey,
        quote_amount: u64,
        token_amount: u64,
        is_buy: bool,
        virtual_quote_reserves: u64,
        virtual_token_reserves: u64,
    ) -> String {
        let mut data = TRADE_EVENT_DISCRIMINATOR.to_vec();
        data.extend_from_slice(mint.as_ref());
        data.extend_from_slice(&quote_amount.to_le_bytes()); // legacy sol_amount
        data.extend_from_slice(&token_amount.to_le_bytes());
        data.push(is_buy.into());
        data.extend_from_slice(wallet.as_ref());
        data.extend_from_slice(&0i64.to_le_bytes());
        data.extend_from_slice(&virtual_quote_reserves.to_le_bytes());
        data.extend_from_slice(&virtual_token_reserves.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes()); // real sol reserves
        data.extend_from_slice(&0u64.to_le_bytes()); // real token reserves
        data.extend_from_slice(Pubkey::default().as_ref()); // fee recipient
        data.extend_from_slice(&0u64.to_le_bytes()); // fee bps
        data.extend_from_slice(&0u64.to_le_bytes()); // fee
        data.extend_from_slice(Pubkey::default().as_ref()); // creator
        data.extend_from_slice(&0u64.to_le_bytes()); // creator fee bps
        data.extend_from_slice(&0u64.to_le_bytes()); // creator fee
        data.push(0); // track volume
        data.extend_from_slice(&[0; 32]); // volume fields
        data.extend_from_slice(&0u32.to_le_bytes()); // ix_name
        data.push(0); // mayhem
        data.extend_from_slice(&[0; 32]); // cashback and buyback fields
        data.extend_from_slice(&0u32.to_le_bytes()); // shareholders
        data.extend_from_slice(quote_mint.as_ref());
        data.extend_from_slice(&quote_amount.to_le_bytes());
        data.extend_from_slice(&virtual_quote_reserves.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        format!(
            "Program data: {}",
            base64::engine::general_purpose::STANDARD.encode(data)
        )
    }

    fn amm_buy_event_log(
        pool: Pubkey,
        wallet: Pubkey,
        token_amount: u64,
        quote_amount: u64,
        pool_token_reserves: u64,
        pool_quote_reserves: u64,
    ) -> String {
        let mut data = AMM_BUY_EVENT_DISCRIMINATOR.to_vec();
        data.extend_from_slice(&0i64.to_le_bytes());
        data.extend_from_slice(&token_amount.to_le_bytes());
        data.extend_from_slice(&quote_amount.to_le_bytes()); // max quote
        data.extend_from_slice(&0u64.to_le_bytes()); // user base reserves
        data.extend_from_slice(&0u64.to_le_bytes()); // user quote reserves
        data.extend_from_slice(&pool_token_reserves.to_le_bytes());
        data.extend_from_slice(&pool_quote_reserves.to_le_bytes());
        data.extend_from_slice(&quote_amount.to_le_bytes());
        data.extend_from_slice(&[0; 48]); // fees and quote totals
        data.extend_from_slice(pool.as_ref());
        data.extend_from_slice(wallet.as_ref());
        format!(
            "Program data: {}",
            base64::engine::general_purpose::STANDARD.encode(data)
        )
    }

    #[test]
    fn decodes_pumpswap_buy_event() {
        let wallet = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let keys = [
            wallet,
            pool,
            mint,
            spl_token::native_mint::ID,
            spl_token_2022::ID,
            *PUMP_AMM_PROGRAM_ID,
            Pubkey::new_unique(),
        ];
        let mut accounts = vec![6; 23];
        accounts[0] = 1;
        accounts[1] = 0;
        accounts[3] = 2;
        accounts[4] = 3;
        accounts[11] = 4;
        let mut data = IX_BUY.to_vec();
        data.extend_from_slice(&50_000_000u64.to_le_bytes());
        data.extend_from_slice(&200_000_000u64.to_le_bytes());
        let update = SubscribeUpdateTransaction {
            slot: 600,
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: vec![11; 64],
                transaction: Some(Transaction {
                    signatures: vec![vec![11; 64]],
                    message: Some(Message {
                        account_keys: keys.iter().map(|key| key.to_bytes().to_vec()).collect(),
                        instructions: vec![CompiledInstruction {
                            program_id_index: 5,
                            accounts,
                            data,
                        }],
                        ..Default::default()
                    }),
                }),
                meta: Some(TransactionStatusMeta {
                    log_messages: vec![amm_buy_event_log(
                        pool,
                        wallet,
                        49_000_000,
                        190_000_000,
                        900_000_000,
                        10_000_000_000,
                    )],
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };

        let event = decode_transaction(600, &update).expect("PumpSwap buy must decode");
        assert_eq!(event.kind, PumpIxKind::AmmBuy);
        assert_eq!(event.mint, mint);
        assert_eq!(event.bonding_curve, pool);
        assert_eq!(event.buys.len(), 1);
        assert_eq!(event.buys[0].wallet, wallet);
        assert_eq!(event.buys[0].quote_amount, 190_000_000);
        assert_eq!(event.buys[0].token_amount, Some(49_000_000));
        assert!(event.buys[0].exact);
    }

    #[test]
    fn decodes_buy_exact_quote_in_v2_with_usdc_decimals() {
        let signature = bs58::encode([7u8; 64]).into_string();
        let wallet = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let curve = Pubkey::new_unique();
        let usdc = Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v").unwrap();
        let keys = [
            wallet,
            mint,
            usdc,
            spl_token_2022::ID,
            spl_token::ID,
            curve,
            *PUMP_PROGRAM_ID,
            Pubkey::new_unique(),
        ];
        let mut accounts = vec![7; 27];
        accounts[1] = 1;
        accounts[2] = 2;
        accounts[3] = 3;
        accounts[4] = 4;
        accounts[10] = 5;
        accounts[13] = 0;
        let mut data = IX_BUY_EXACT_QUOTE_IN_V2.to_vec();
        data.extend_from_slice(&1_250_000u64.to_le_bytes());
        data.extend_from_slice(&5_000_000u64.to_le_bytes());
        let message = Message {
            account_keys: keys.iter().map(|key| key.to_bytes().to_vec()).collect(),
            instructions: vec![CompiledInstruction {
                program_id_index: 6,
                accounts,
                data,
            }],
            ..Default::default()
        };
        let meta = TransactionStatusMeta {
            log_messages: vec![trade_event_log(
                mint,
                wallet,
                usdc,
                1_250_000,
                6_250_000,
                true,
                30_000_000_000,
                1_000_000_000_000_000,
            )],
            pre_token_balances: vec![TokenBalance {
                mint: usdc.to_string(),
                ui_token_amount: Some(UiTokenAmount {
                    decimals: 6,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        let update = SubscribeUpdateTransaction {
            slot: 500,
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: bs58::decode(&signature).into_vec().unwrap(),
                transaction: Some(Transaction {
                    signatures: vec![bs58::decode(&signature).into_vec().unwrap()],
                    message: Some(message),
                }),
                meta: Some(meta),
                ..Default::default()
            }),
        };

        let event = decode_transaction(500, &update).expect("BuyExactQuoteInV2 must decode");
        assert_eq!(event.kind, PumpIxKind::BuyExactQuoteInV2);
        assert_eq!(event.mint, mint);
        assert_eq!(event.bonding_curve, curve);
        assert_eq!(event.quote_mint, usdc);
        assert_eq!(event.quote_decimals, Some(6));
        assert_eq!(event.buy_quote_amount, Some(1_250_000));
        assert_eq!(event.buys[0].quote_amount, 1_250_000);
        assert_eq!(event.buys[0].token_amount, Some(6_250_000));
        assert_eq!(
            format_raw_amount(event.buys[0].quote_amount, Some(6)),
            "1.25"
        );
    }

    #[test]
    fn decodes_buy_v2_instruction_fallback() {
        let wallet = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let curve = Pubkey::new_unique();
        let keys = [
            wallet,
            mint,
            spl_token::native_mint::ID,
            spl_token_2022::ID,
            curve,
            *PUMP_PROGRAM_ID,
            Pubkey::new_unique(),
        ];
        let mut accounts = vec![6; 27];
        accounts[1] = 1;
        accounts[2] = 2;
        accounts[3] = 3;
        accounts[10] = 4;
        accounts[13] = 0;
        let mut data = IX_BUY_V2.to_vec();
        data.extend_from_slice(&5_000_000u64.to_le_bytes());
        data.extend_from_slice(&200_000_000u64.to_le_bytes());
        let update = SubscribeUpdateTransaction {
            slot: 499,
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: vec![3; 64],
                transaction: Some(Transaction {
                    signatures: vec![vec![3; 64]],
                    message: Some(Message {
                        account_keys: keys.iter().map(|key| key.to_bytes().to_vec()).collect(),
                        instructions: vec![CompiledInstruction {
                            program_id_index: 5,
                            accounts,
                            data,
                        }],
                        ..Default::default()
                    }),
                }),
                meta: Some(TransactionStatusMeta::default()),
                ..Default::default()
            }),
        };

        let event = decode_transaction(499, &update).expect("BuyV2 fallback must decode");
        assert_eq!(event.kind, PumpIxKind::BuyV2);
        assert_eq!(event.mint, mint);
        assert_eq!(event.bonding_curve, curve);
        assert_eq!(event.buy_quote_amount, Some(200_000_000));
        assert_eq!(event.buys[0].wallet, wallet);
        assert_eq!(event.buys[0].quote_amount, 200_000_000);
        assert!(!event.buys[0].exact);
    }

    #[test]
    fn one_transaction_decodes_into_one_event_per_mint() {
        let signature = bs58::encode([9u8; 64]).into_string();
        let wallet = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let curve_a = Pubkey::new_unique();
        let curve_b = Pubkey::new_unique();
        let keys = [
            wallet,
            mint_a,
            mint_b,
            curve_a,
            curve_b,
            *PUMP_PROGRAM_ID,
            Pubkey::new_unique(),
        ];
        let make_buy = |mint_index: u8, curve_index: u8, amount: u64| {
            let mut data = IX_BUY_EXACT_SOL_IN.to_vec();
            data.extend_from_slice(&amount.to_le_bytes());
            data.extend_from_slice(&1u64.to_le_bytes());
            CompiledInstruction {
                program_id_index: 5,
                accounts: vec![6, 6, mint_index, curve_index, 6, 6, 0, 6, 6],
                data,
            }
        };
        let message = Message {
            account_keys: keys.iter().map(|key| key.to_bytes().to_vec()).collect(),
            instructions: vec![make_buy(1, 3, 10_000_000), make_buy(2, 4, 20_000_000)],
            ..Default::default()
        };
        let meta = TransactionStatusMeta {
            log_messages: vec![
                trade_event_log(
                    mint_a,
                    wallet,
                    spl_token::native_mint::ID,
                    10_000_000,
                    100_000_000,
                    true,
                    1,
                    1,
                ),
                trade_event_log(
                    mint_b,
                    wallet,
                    spl_token::native_mint::ID,
                    20_000_000,
                    200_000_000,
                    true,
                    1,
                    1,
                ),
            ],
            ..Default::default()
        };
        let update = SubscribeUpdateTransaction {
            slot: 501,
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: bs58::decode(&signature).into_vec().unwrap(),
                transaction: Some(Transaction {
                    signatures: vec![bs58::decode(&signature).into_vec().unwrap()],
                    message: Some(message),
                }),
                meta: Some(meta),
                ..Default::default()
            }),
        };

        let events = decode_transactions(501, &update);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].mint, mint_a);
        assert_eq!(events[0].buys[0].quote_amount, 10_000_000);
        assert_eq!(events[1].mint, mint_b);
        assert_eq!(events[1].buys[0].quote_amount, 20_000_000);
    }

    fn sell_v2_update(
        signature: &str,
        slot: u64,
        wallet: Pubkey,
        instruction_data: &str,
        trade_event: &str,
        through_cpi: bool,
    ) -> SubscribeUpdateTransaction {
        let mint = Pubkey::from_str("7MFDFtdJSAJdiF4nEsJ3fuADnMyb84ARgfZpMUzXpump").unwrap();
        let aggregator = Pubkey::new_unique();
        let placeholder = Pubkey::new_unique();
        let keys = [
            wallet,
            mint,
            spl_token_2022::ID,
            *PUMP_PROGRAM_ID,
            aggregator,
            placeholder,
        ];
        let mut accounts = vec![5; 14];
        accounts[1] = 1; // mint
        accounts[3] = 2; // token program
        accounts[13] = 0; // user
        let data = bs58::decode(instruction_data).into_vec().unwrap();
        let (instructions, inner_instructions) = if through_cpi {
            (
                vec![CompiledInstruction {
                    program_id_index: 4,
                    accounts: vec![0, 1],
                    data: vec![1],
                }],
                vec![InnerInstructions {
                    index: 0,
                    instructions: vec![InnerInstruction {
                        program_id_index: 3,
                        accounts,
                        data,
                        stack_height: Some(2),
                    }],
                }],
            )
        } else {
            (
                vec![CompiledInstruction {
                    program_id_index: 3,
                    accounts,
                    data,
                }],
                vec![],
            )
        };
        let message = Message {
            account_keys: keys.iter().map(|key| key.to_bytes().to_vec()).collect(),
            instructions,
            ..Default::default()
        };
        let meta = TransactionStatusMeta {
            inner_instructions,
            log_messages: vec![format!("Program data: {trade_event}")],
            ..Default::default()
        };
        SubscribeUpdateTransaction {
            slot,
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: bs58::decode(signature).into_vec().unwrap(),
                transaction: Some(Transaction {
                    signatures: vec![bs58::decode(signature).into_vec().unwrap()],
                    message: Some(message),
                }),
                meta: Some(meta),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn decodes_direct_sell_v2() {
        let signature = "21NRv2nrc1zBueSWiuRHhH4ZzDWXDBHUHBZSTS5qB9PbPVBjECYXebxdVdjc4x1FrPPaLKgR4bm55qYHcsyrABUJ";
        let wallet = Pubkey::from_str("DEXGUYMrUaNTAkVgVvuDuMjycdwTcq2qhqhbv1EdNuTS").unwrap();
        let update = sell_v2_update(
            signature,
            442_894_991,
            wallet,
            "9Zq9ZwMbe2bpTDnK3SKQZhS646Ktrs8rj",
            "vdt/007mYe5eVc54/G/YQ0kPVhfMUI4SFMnB2sSz9UH2zmVEDUfVD4t3HQAAAAAAM3ed9w8AAAAAtcMENEYPQVmdCX59nkWkHQMN2GoOhZ7gF/Vv0562JHVoNZRqAAAAAOXHewIHAAAAeQAODHDMAwA=",
            false,
        );

        let event = decode_transaction(update.slot, &update).expect("direct SellV2 must decode");
        assert_eq!(event.signature, signature);
        assert_eq!(
            event.mint.to_string(),
            "7MFDFtdJSAJdiF4nEsJ3fuADnMyb84ARgfZpMUzXpump"
        );
        assert_eq!(event.token_program, spl_token_2022::ID);
        assert_eq!(event.sells.len(), 1);
        assert_eq!(event.sells[0].wallet, wallet);
        assert_eq!(event.sells[0].quote_amount, Some(1_931_147));
        assert_eq!(event.sells[0].token_amount, 68_578_801_459);
    }

    #[test]
    fn decodes_sell_v2_invoked_by_other_bot_cpi() {
        let signature = "5KxDpiMuAeM1BgHYpYF9NEabEyZ5Hqmsxv81PtLb1zHjxtfEtZ25DPfALid9UK5RfjrW7eELNRHTT5AHyre8DdUc";
        let wallet = Pubkey::from_str("BLAMEEhCJyRuChcs6p6ckex6MKUNmLLmtfC6vteCddEg").unwrap();
        let update = sell_v2_update(
            signature,
            442_894_990,
            wallet,
            "9Zq9ZwMbe2bwmexqicBviyLR9jC2imTxK",
            "vdt/007mYe5eVc54/G/YQ0kPVhfMUI4SFMnB2sSz9UH2zmVEDUfVD9HU5w4AAAAAbrp+hAIIAAAAmX15rj/WbtUELduE93xyyok4Zs3MuUm4AQ1i2NgXN9loNZRqAAAAAHA/mQIHAAAARolwFGDMAwA=",
            true,
        );

        let event = decode_transaction(update.slot, &update).expect("CPI SellV2 must decode");
        assert_eq!(event.signature, signature);
        assert_eq!(
            event.mint.to_string(),
            "7MFDFtdJSAJdiF4nEsJ3fuADnMyb84ARgfZpMUzXpump"
        );
        assert_eq!(event.sells.len(), 1);
        assert_eq!(event.sells[0].wallet, wallet);
        assert_eq!(event.sells[0].quote_amount, Some(250_074_321));
        assert_eq!(event.sells[0].token_amount, 8_806_905_854_574);
    }

    #[test]
    fn decodes_pump_sell_invoked_by_aggregator_cpi() {
        // Regression fixture from transaction
        // 5cJv9ggkrjFa7BL6tJDehoyjUERJvMrh5rDhCjoJo1VbfAcuTowkM9695yHbPGRFMTFxiToahkh5uvGoWFvXdTWj.
        let signature = "5cJv9ggkrjFa7BL6tJDehoyjUERJvMrh5rDhCjoJo1VbfAcuTowkM9695yHbPGRFMTFxiToahkh5uvGoWFvXdTWj";
        let wallet = Pubkey::from_str("4BHBSZA96XCaepYLvmteV6wLqKTqJPyfkME941X5PqHP").unwrap();
        let mint = Pubkey::from_str("61aL4Gz9SQkmo43bExYuVeSAcBpR4rwZpRnzxbxNpump").unwrap();
        let curve = Pubkey::from_str("26C9q3S8cFsof3MoKYbBrWM6Vwgkhj68tbvEAYbuum9u").unwrap();
        let aggregator = Pubkey::from_str("XTbotxFEemJLuNiKwi9JBbGGoTfGeiqrboPHmweZ73i").unwrap();
        let placeholder = Pubkey::new_unique();
        let keys = [
            wallet,
            placeholder,
            mint,
            curve,
            *PUMP_PROGRAM_ID,
            aggregator,
        ];
        let outer = CompiledInstruction {
            program_id_index: 5,
            accounts: vec![0, 2, 3],
            data: vec![1],
        };
        let inner_sell = InnerInstruction {
            program_id_index: 4,
            // Pump Sell uses mint=account 2, curve=account 3, user=account 6.
            accounts: vec![1, 1, 2, 3, 1, 1, 0],
            data: bs58::decode("5jRcjdixRUDFKLM4tMVEg7dZQ98t6xBYB")
                .into_vec()
                .unwrap(),
            stack_height: Some(2),
        };
        let message = Message {
            account_keys: keys.iter().map(|key| key.to_bytes().to_vec()).collect(),
            instructions: vec![outer],
            ..Default::default()
        };
        let meta = TransactionStatusMeta {
            inner_instructions: vec![InnerInstructions {
                index: 0,
                instructions: vec![inner_sell],
            }],
            log_messages: vec![
                "Program data: vdt/007mYe5KcB6OBi+ynYwwdPwHhqvIN/sNXp79QReMNXvLn2OTfwCdMV0AAAAAChq+WCMWAAAALzUp5f7xo/mrvVMy9zu6AgB0d4wmu9Fqfnec7aBAGGZcM5RqAAAAAP+AJWgKAAAA2LQBtwCPAgA=".into(),
            ],
            ..Default::default()
        };
        let update = SubscribeUpdateTransaction {
            slot: 442_893_328,
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: bs58::decode(signature).into_vec().unwrap(),
                transaction: Some(Transaction {
                    signatures: vec![bs58::decode(signature).into_vec().unwrap()],
                    message: Some(message),
                }),
                meta: Some(meta),
                ..Default::default()
            }),
        };

        let event =
            decode_transaction(update.slot, &update).expect("CPI Pump sell must be decoded");
        assert_eq!(event.signature, signature);
        assert_eq!(event.mint, mint);
        assert_eq!(event.bonding_curve, curve);
        assert!(event.is_sell);
        assert_eq!(event.sells.len(), 1);
        assert_eq!(event.sells[0].wallet, wallet);
        assert_eq!(event.sells[0].quote_amount, Some(1_563_532_544));
        assert_eq!(event.sells[0].token_amount, 24_341_068_519_946);
        assert!(event.sells[0].exact);
    }

    #[test]
    fn decodes_real_create_v2_metadata() {
        let data = bs58::decode("2MJr4FV2wMPZmw8KbCAAZoi6bZbdnwJ8kgZp9p65YqPJFgDQqHq9dW5qEQ7urQWzAhfcRQH32nM4spEHFrWxCrxNa52a4R2cvSxDXxu8VxhJsHX1YC1PKggFEnY2dfUNApjHVZnCRG2XRaKDMSsYZJn3Z64bHij2hmtx4kRwbRuD5ApbUbLGRycGvyyC4PmAZtGkPJXyNkF")
            .into_vec()
            .unwrap();
        let metadata = parse_create_data(&data).unwrap();
        assert_eq!(metadata.name, "SHIBECOIN");
        assert_eq!(metadata.symbol, "SHIBE");
        assert_eq!(
            metadata.uri,
            "https://ipfs.io/ipfs/bafkreibypyqrmqgxhy23q3xjb3rh74ufq5ksvd6a2bgqib6atsf7hyugua"
        );
        assert_eq!(
            metadata.creator,
            Some(Pubkey::from_str("53XxqvfbhmVs3ZidMrwfsUmeCNtetxxut1XVDXLjjpyV").unwrap())
        );
    }

    #[test]
    fn decodes_system_transfer_lamports() {
        let mut data = 2u32.to_le_bytes().to_vec();
        data.extend_from_slice(&123_456u64.to_le_bytes());
        assert_eq!(parse_system_transfer(&data), Some(123_456));
        data[0] = 3;
        assert_eq!(parse_system_transfer(&data), None);
    }

    #[test]
    fn create_prefilter_rejects_regular_pump_trades() {
        let update = |discriminator: [u8; 8]| SubscribeUpdateTransaction {
            slot: 1,
            transaction: Some(SubscribeUpdateTransactionInfo {
                transaction: Some(Transaction {
                    message: Some(Message {
                        account_keys: vec![PUMP_PROGRAM_ID.to_bytes().to_vec()],
                        instructions: vec![CompiledInstruction {
                            program_id_index: 0,
                            data: discriminator.to_vec(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };

        assert!(contains_create_instruction(&update(IX_CREATE_V2)));
        assert!(!contains_create_instruction(&update(IX_BUY_EXACT_SOL_IN)));
    }

    #[test]
    fn decodes_exact_trade_event_amount_wallet_and_token() {
        let mint = Pubkey::new_unique();
        let wallet = Pubkey::new_unique();
        let mut data = TRADE_EVENT_DISCRIMINATOR.to_vec();
        data.extend_from_slice(mint.as_ref());
        data.extend_from_slice(&75_000_000u64.to_le_bytes());
        data.extend_from_slice(&1_234_567u64.to_le_bytes());
        data.push(1);
        data.extend_from_slice(wallet.as_ref());
        data.extend_from_slice(&1_700_000_000u64.to_le_bytes());
        data.extend_from_slice(&31_000_000_000u64.to_le_bytes());
        data.extend_from_slice(&1_000_000_000_000_000u64.to_le_bytes());

        let trade = parse_trade_event(&data).unwrap();
        assert_eq!(trade.mint, mint);
        assert_eq!(trade.wallet, wallet);
        assert_eq!(trade.legacy_sol_amount, 75_000_000);
        assert_eq!(trade.token_amount, 1_234_567);
        assert!(trade.is_buy);
        assert_eq!(trade.legacy_virtual_sol_reserves, 31_000_000_000);
        assert_eq!(trade.virtual_token_reserves, 1_000_000_000_000_000);

        data[56] = 0;
        let sell = parse_trade_event(&data).unwrap();
        assert!(!sell.is_buy);
        assert_eq!(sell.wallet, wallet);
        assert_eq!(sell.legacy_sol_amount, 75_000_000);
    }

    #[test]
    fn treats_default_quote_mint_in_current_trade_event_as_sol() {
        let mint = Pubkey::new_unique();
        let wallet = Pubkey::new_unique();
        let log = trade_event_log(
            mint,
            wallet,
            Pubkey::default(),
            9_999_999,
            357_547_484_171,
            false,
            30_000_000_001,
            1_073_000_000_000_000,
        );

        let trades = parse_trade_events(&[log]);
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].quote_mint, spl_token::native_mint::ID);
        assert_eq!(trades[0].quote_amount, 9_999_999);
        assert_eq!(trades[0].virtual_quote_reserves, 30_000_000_001);
    }
}
