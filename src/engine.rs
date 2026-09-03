//! 订单 actor：事件判定、原子仓位状态转换、并发执行与链上确认。

use crate::admin::{BotCommand, BotRunState};
use crate::config::AppConfig;
use crate::exec::buy::BuyResult;
use crate::exec::sell::SellResult;
use crate::exec::{execute_buy, execute_sell, land::Lander};
use crate::journal::TradeJournal;
use crate::position::{BuyResolution, Position, PositionBook, ReconcileSide, SellDecision};
use crate::pump::PumpEvent;
use crate::strategy::{Action, StrategyFrame};
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Semaphore};

const CONFIRMATION_CACHE_CAPACITY: usize = 4096;

enum OrderResult {
    Buy {
        event: Box<PumpEvent>,
        result: anyhow::Result<BuyResult>,
    },
    Sell {
        mint: Pubkey,
        result: anyhow::Result<SellResult>,
    },
}

#[derive(Default)]
struct RecentConfirmations {
    buys: HashMap<String, u64>,
    sells: HashSet<String>,
    order: VecDeque<(bool, String)>,
}

impl RecentConfirmations {
    fn remember_buy(&mut self, signature: String, tokens: u64) {
        if self.buys.insert(signature.clone(), tokens).is_none() {
            self.order.push_back((true, signature));
        }
        self.prune();
    }

    fn remember_sell(&mut self, signature: String) {
        if self.sells.insert(signature.clone()) {
            self.order.push_back((false, signature));
        }
        self.prune();
    }

    fn prune(&mut self) {
        while self.order.len() > CONFIRMATION_CACHE_CAPACITY {
            if let Some((is_buy, signature)) = self.order.pop_front() {
                if is_buy {
                    self.buys.remove(&signature);
                } else {
                    self.sells.remove(&signature);
                }
            }
        }
    }
}

pub async fn run(
    cfg: AppConfig,
    payer: Arc<Keypair>,
    mut frames: mpsc::Receiver<StrategyFrame>,
    journal: TradeJournal,
    mut control_rx: Option<mpsc::Receiver<BotCommand>>,
    target_release_tx: Option<mpsc::Sender<Pubkey>>,
) -> anyhow::Result<()> {
    let cfg = Arc::new(cfg);
    let lander = Lander::new(cfg.as_ref());
    lander.warmup().await;
    let initial_hash = lander.latest_blockhash().await?;
    let (blockhash_tx, blockhash_rx) = watch::channel(initial_hash);
    spawn_blockhash_refresher(lander.clone(), blockhash_tx);

    let mut positions = PositionBook::default();
    let mut confirmations = RecentConfirmations::default();
    let (order_tx, mut order_rx) =
        mpsc::channel::<OrderResult>(cfg.landing.max_inflight_orders.saturating_mul(2).max(8));
    let permits = Arc::new(Semaphore::new(cfg.landing.max_inflight_orders));
    let mut execution_allows_new_orders = false;
    let mut admin_stopping = false;
    let mut admin_stopped = true;
    let mut timeout_tick = tokio::time::interval(Duration::from_millis(50));
    timeout_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    crate::telemetry::info(
        "引擎",
        format!(
            "钱包={}  模式={}  监听={}  跟盘={}  最大并发={}  初始状态=等待管理页启动",
            payer.pubkey(),
            cfg.bot.mode.label(),
            if cfg.follow.is_empty() {
                "Pump发币"
            } else {
                "follow钱包"
            },
            cfg.follow.len(),
            cfg.landing.max_inflight_orders,
        ),
    );

    loop {
        tokio::select! {
            biased;

            Some(done) = order_rx.recv() => {
                handle_order_result(
                    done, &cfg, &lander, &payer, &journal, &mut positions,
                    &mut confirmations, &blockhash_rx, &order_tx, &permits,
                    target_release_tx.as_ref(),
                );
                finish_admin_stop_if_flat(&positions, &mut admin_stopping, &mut admin_stopped);
                crate::admin::set_positions(positions.snapshots());
            }
            maybe_command = async {
                match control_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            }, if control_rx.is_some() => {
                let Some(command) = maybe_command else {
                    control_rx = None;
                    continue;
                };
                handle_bot_command(
                    command, &cfg, &lander, &payer, &journal, &mut positions,
                    &blockhash_rx, &order_tx, &permits, &mut execution_allows_new_orders,
                    &mut admin_stopping, &mut admin_stopped, target_release_tx.as_ref(),
                );
                crate::admin::set_positions(positions.snapshots());
            }
            maybe_frame = frames.recv() => {
                let Some(frame) = maybe_frame else { break };
                let allows_new_orders =
                    execution_allows_new_orders && !admin_stopping && !admin_stopped;
                if !allows_new_orders && matches!(frame.intent, Some(Action::Buy)) {
                    release_target_now(frame.event.mint, target_release_tx.as_ref());
                    crate::admin::set_positions(positions.snapshots());
                    continue;
                }
                // scan 的 mint 筛选已在策略路由中完成；journal 只看到目标事件。
                journal.record(&frame.event);
                handle_chain_event(
                    frame, &cfg, &lander, &payer, &journal,
                    &mut positions, &mut confirmations, &blockhash_rx, &order_tx, &permits,
                    allows_new_orders,
                    target_release_tx.as_ref(),
                );
                crate::admin::set_positions(positions.snapshots());
            }
            _ = timeout_tick.tick() => {
                for position in positions.claim_timeouts(Duration::from_millis(cfg.sell.max_hold_ms)) {
                    let mint = position.mint;
                    journal.note(
                        mint,
                        "执行卖出",
                        format!(
                            "[{}] 执行卖出，原因: 固定时间卖出，持仓: {}毫秒",
                            position.mint, cfg.sell.max_hold_ms
                        ),
                    );
                    if !spawn_sell(
                        &cfg, &lander, &payer, position, current_hash(&blockhash_rx),
                        &order_tx, &permits,
                    ) {
                        positions.fail_sell(mint);
                    }
                }
                for stale in positions.claim_stale_submissions(Duration::from_millis(
                    cfg.landing.confirmation_timeout_ms,
                )) {
                    execution_allows_new_orders = false;
                    let side = match stale.side {
                        ReconcileSide::Buy => "买入",
                        ReconcileSide::Sell => "卖出",
                    };
                    crate::telemetry::error_fields(
                        "失败",
                        [
                            "核账熔断".to_owned(),
                            side.to_owned(),
                            stale.mint.to_string(),
                            stale.signature.clone(),
                        ],
                    );
                    journal.note(
                        stale.mint,
                        "等待核账",
                        format!(
                            "[{}] {side}提交后{}毫秒未收到Geyser确认，已熔断新开仓: {}",
                            stale.mint, cfg.landing.confirmation_timeout_ms, stale.signature
                        ),
                    );
                    if stale.side == ReconcileSide::Buy
                        && positions.abandon_unconfirmed_buy(stale.mint)
                    {
                        release_target_now(stale.mint, target_release_tx.as_ref());
                    }
                }
                if admin_stopping {
                    for mint in positions.abandon_unconfirmed_buys() {
                        crate::telemetry::warn(
                            "控制",
                            format!("停止等待超时，丢弃未确认买入并释放监听 mint={mint}"),
                        );
                        release_target_now(mint, target_release_tx.as_ref());
                    }
                    for mint in positions.abandon_stale_submitted_sells(Duration::from_millis(
                        cfg.landing.confirmation_timeout_ms,
                    )) {
                        crate::telemetry::warn(
                            "控制",
                            format!("停止等待超时，丢弃过期卖出核账并释放监听 mint={mint}"),
                        );
                        release_target_now(mint, target_release_tx.as_ref());
                    }
                }
                finish_admin_stop_if_flat(&positions, &mut admin_stopping, &mut admin_stopped);
                crate::admin::set_positions(positions.snapshots());
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn handle_chain_event(
    frame: StrategyFrame,
    cfg: &Arc<AppConfig>,
    lander: &Lander,
    payer: &Arc<Keypair>,
    journal: &TradeJournal,
    positions: &mut PositionBook,
    confirmations: &mut RecentConfirmations,
    blockhash: &watch::Receiver<Hash>,
    order_tx: &mpsc::Sender<OrderResult>,
    permits: &Arc<Semaphore>,
    execution_allows_new_orders: bool,
    target_release_tx: Option<&mpsc::Sender<Pubkey>>,
) {
    let StrategyFrame { event, intent } = frame;
    let wallet = payer.pubkey();
    if let Some(actual_tokens) = confirmed_buy_tokens(&event, wallet) {
        confirmations.remember_buy(event.signature.clone(), actual_tokens);
        if let SellDecision::Start(position) =
            positions.confirm_buy(event.mint, &event.signature, actual_tokens)
        {
            journal.note(
                event.mint,
                "执行卖出",
                format!("[{}] 买入确认后执行此前挂起的退出", event.mint),
            );
            if !spawn_sell(
                cfg,
                lander,
                payer,
                position,
                current_hash(blockhash),
                order_tx,
                permits,
            ) {
                positions.fail_sell(event.mint);
            }
        }
    }
    if confirmed_sell(&event, wallet) {
        confirmations.remember_sell(event.signature.clone());
        if positions.confirm_sell(event.mint, &event.signature) {
            crate::telemetry::info(
                "仓位确认",
                format!("卖出已上链 mint={} sig={}", event.mint, event.signature),
            );
            schedule_target_release(event.mint, target_release_tx);
        }
    }

    let Some(action) = intent else {
        return;
    };
    match action {
        Action::Buy => {
            if !execution_allows_new_orders
                || !journal.allows_new_orders()
                || !crate::listen::integrity_allows_new_orders()
            {
                if !execution_allows_new_orders {
                    crate::telemetry::info(
                        "控制",
                        format!("bot 未启动，忽略 create 并释放监听 mint={}", event.mint),
                    );
                } else {
                    crate::telemetry::error(
                        "安全熔断",
                        format!("执行或审计状态不完整，拒绝新开仓 mint={}", event.mint),
                    );
                }
                release_target_now(event.mint, target_release_tx);
                return;
            }
            if !positions.begin_buy(event.mint, event.creator) {
                return;
            }
            if !journal.note(
                event.mint,
                "执行买入",
                format!(
                    "[{}] 开始买入，触发交易: {}，触发slot: {}，计划SOL: {}",
                    event.mint, event.signature, event.slot, cfg.buy.sol_amount
                ),
            ) {
                positions.fail_buy(event.mint);
                crate::telemetry::error(
                    "安全熔断",
                    format!("关键审计消息无法入队，取消买入 mint={}", event.mint),
                );
                return;
            }
            let mint = event.mint;
            if !spawn_buy(
                cfg,
                lander,
                payer,
                event,
                current_hash(blockhash),
                order_tx,
                permits,
            ) {
                positions.fail_buy(mint);
                journal.note(
                    mint,
                    "买入拥塞",
                    format!("[{mint}] 并发订单已达硬上限，已取消本次买入"),
                );
            }
        }
        Action::SellObserved { mint, seller } => {
            if !cfg.sell.follow_creator_sell {
                return;
            }
            if positions.creator(mint) != Some(seller) {
                return;
            }
            match positions.request_sell(mint) {
                SellDecision::Start(position) => {
                    journal.note(
                        mint,
                        "执行卖出",
                        format!("[{mint}] 执行卖出，原因: DEV卖出，DEV: {seller}"),
                    );
                    if !spawn_sell(
                        cfg,
                        lander,
                        payer,
                        position,
                        current_hash(blockhash),
                        order_tx,
                        permits,
                    ) {
                        positions.fail_sell(mint);
                    }
                }
                SellDecision::Deferred => {
                    journal.note(
                        mint,
                        "挂起卖出",
                        format!("[{mint}] 买入尚未确认，已记录DEV卖出，确认到账后立即退出"),
                    );
                }
                SellDecision::Ignored => {}
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_order_result(
    done: OrderResult,
    cfg: &Arc<AppConfig>,
    lander: &Lander,
    payer: &Arc<Keypair>,
    journal: &TradeJournal,
    positions: &mut PositionBook,
    confirmations: &mut RecentConfirmations,
    blockhash: &watch::Receiver<Hash>,
    order_tx: &mpsc::Sender<OrderResult>,
    permits: &Arc<Semaphore>,
    target_release_tx: Option<&mpsc::Sender<Pubkey>>,
) {
    match done {
        OrderResult::Buy { event, result } => match result {
            Ok(result) => {
                let mint = event.mint;
                let buy_sigs = result.signatures.clone();
                journal.submitted(
                    mint,
                    "买入",
                    result.signature.clone(),
                    buy_sigs.clone(),
                    result.channel,
                );
                let position = Position {
                    mint,
                    bonding_curve: event.bonding_curve,
                    creator: event.creator,
                    token_program: event.token_program,
                    user_token: result.user_token,
                    token_amount: result.tokens_est,
                    opened: std::time::Instant::now(),
                    buy_sig: result.signature.clone(),
                    buy_sigs: buy_sigs.clone(),
                };
                if positions.buy_submitted(position) != BuyResolution::AwaitingConfirmation {
                    return;
                }

                let confirmed = buy_sigs.iter().find_map(|signature| {
                    confirmations
                        .buys
                        .remove(signature)
                        .map(|tokens| (signature, tokens))
                });
                if let Some((signature, tokens)) = confirmed {
                    if let SellDecision::Start(position) =
                        positions.confirm_buy(mint, signature, tokens)
                    {
                        if !spawn_sell(
                            cfg,
                            lander,
                            payer,
                            position,
                            current_hash(blockhash),
                            order_tx,
                            permits,
                        ) {
                            positions.fail_sell(mint);
                        }
                    }
                } else {
                    crate::telemetry::info_fields(
                        "仓位",
                        [
                            "等待买入上链确认".to_owned(),
                            mint.to_string(),
                            result.signature.clone(),
                        ],
                    );
                }
            }
            Err(error) => {
                positions.fail_buy(event.mint);
                crate::telemetry::error_fields(
                    "失败",
                    ["买入".to_owned(), event.mint.to_string(), error.to_string()],
                );
                journal.note(
                    event.mint,
                    "买入失败",
                    format!("[{}] {}", event.mint, error),
                );
            }
        },
        OrderResult::Sell { mint, result } => match result {
            Ok(result) => {
                let sell_sigs = result.signatures.clone();
                journal.submitted(
                    mint,
                    "卖出",
                    result.signature.clone(),
                    sell_sigs.clone(),
                    result.channel,
                );
                if !positions.sell_submitted(mint, sell_sigs.clone()) {
                    return;
                }
                if let Some(signature) = sell_sigs
                    .iter()
                    .find(|signature| confirmations.sells.remove(*signature))
                {
                    if positions.confirm_sell(mint, signature) {
                        schedule_target_release(mint, target_release_tx);
                    }
                } else {
                    crate::telemetry::info_fields(
                        "仓位",
                        [
                            "等待卖出上链确认".to_owned(),
                            mint.to_string(),
                            result.signature.clone(),
                        ],
                    );
                }
            }
            Err(error) => {
                positions.fail_sell(mint);
                crate::telemetry::error_fields(
                    "失败",
                    ["卖出".to_owned(), mint.to_string(), error.to_string()],
                );
                journal.note(mint, "卖出失败", format!("[{mint}] {error}"));
            }
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_bot_command(
    command: BotCommand,
    cfg: &Arc<AppConfig>,
    lander: &Lander,
    payer: &Arc<Keypair>,
    journal: &TradeJournal,
    positions: &mut PositionBook,
    blockhash: &watch::Receiver<Hash>,
    order_tx: &mpsc::Sender<OrderResult>,
    permits: &Arc<Semaphore>,
    execution_allows_new_orders: &mut bool,
    admin_stopping: &mut bool,
    admin_stopped: &mut bool,
    target_release_tx: Option<&mpsc::Sender<Pubkey>>,
) {
    match command {
        BotCommand::Start => {
            *execution_allows_new_orders = true;
            *admin_stopping = false;
            *admin_stopped = false;
            for mint in positions.abandon_unconfirmed_buys() {
                crate::telemetry::warn("控制", format!("丢弃未确认买入状态并释放监听 mint={mint}"));
                if let Some(tx) = target_release_tx {
                    let _ = tx.try_send(mint);
                }
            }
            for mint in positions.abandon_stale_submitted_sells(Duration::from_millis(
                cfg.landing.confirmation_timeout_ms,
            )) {
                crate::telemetry::warn(
                    "控制",
                    format!("丢弃过期卖出核账状态并释放监听 mint={mint}"),
                );
                if let Some(tx) = target_release_tx {
                    let _ = tx.try_send(mint);
                }
            }
            crate::admin::set_bot_state(BotRunState::Running);
            crate::telemetry::info("控制", "bot 已允许继续开仓");
        }
        BotCommand::StopGracefully => {
            *admin_stopping = true;
            *admin_stopped = false;
            crate::admin::set_bot_state(BotRunState::Stopping);
            crate::telemetry::warn("控制", "停止并卖出：已暂停新开仓");
            for position in positions.claim_all_for_shutdown() {
                let mint = position.mint;
                journal.note(
                    mint,
                    "执行卖出",
                    format!("[{mint}] 执行卖出，原因: 管理页停止"),
                );
                if !spawn_sell(
                    cfg,
                    lander,
                    payer,
                    position,
                    current_hash(blockhash),
                    order_tx,
                    permits,
                ) {
                    positions.fail_sell(mint);
                }
            }
            finish_admin_stop_if_flat(positions, admin_stopping, admin_stopped);
        }
        BotCommand::ForceStop => {
            *admin_stopping = false;
            *admin_stopped = true;
            crate::admin::set_bot_state(BotRunState::Stopped);
            crate::telemetry::warn("控制", "bot 已强制停止新开仓；已有后台监听仍保持运行");
        }
    }
}

fn finish_admin_stop_if_flat(
    positions: &PositionBook,
    admin_stopping: &mut bool,
    admin_stopped: &mut bool,
) {
    if *admin_stopping && positions.is_empty() {
        *admin_stopping = false;
        *admin_stopped = true;
        crate::admin::set_bot_state(BotRunState::Stopped);
        crate::telemetry::info("控制", "bot 已停止，当前无持仓");
    }
}

fn schedule_target_release(mint: Pubkey, target_release_tx: Option<&mpsc::Sender<Pubkey>>) {
    let Some(target_release_tx) = target_release_tx.cloned() else {
        return;
    };
    crate::telemetry::info("监听", format!("mint={mint} 卖出确认，30秒后停止监听"));
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        let _ = target_release_tx.send(mint).await;
    });
}

fn release_target_now(mint: Pubkey, target_release_tx: Option<&mpsc::Sender<Pubkey>>) {
    if let Some(target_release_tx) = target_release_tx {
        let _ = target_release_tx.try_send(mint);
    }
}

fn spawn_buy(
    cfg: &Arc<AppConfig>,
    lander: &Lander,
    payer: &Arc<Keypair>,
    event: PumpEvent,
    recent: Hash,
    order_tx: &mpsc::Sender<OrderResult>,
    permits: &Arc<Semaphore>,
) -> bool {
    let Ok(permit) = permits.clone().try_acquire_owned() else {
        crate::telemetry::warn("订单拥塞", format!("买入并发已满 mint={}", event.mint));
        return false;
    };
    let cfg = cfg.clone();
    let lander = lander.clone();
    let payer = payer.clone();
    let order_tx = order_tx.clone();
    tokio::spawn(async move {
        let _permit = permit;
        let result = execute_buy(&cfg, &lander, &payer, &event, recent, event.slot).await;
        let _ = order_tx
            .send(OrderResult::Buy {
                event: Box::new(event),
                result,
            })
            .await;
    });
    true
}

fn spawn_sell(
    cfg: &Arc<AppConfig>,
    lander: &Lander,
    payer: &Arc<Keypair>,
    position: Position,
    recent: Hash,
    order_tx: &mpsc::Sender<OrderResult>,
    permits: &Arc<Semaphore>,
) -> bool {
    let mint = position.mint;
    let Ok(permit) = permits.clone().try_acquire_owned() else {
        crate::telemetry::error("订单拥塞", format!("卖出并发已满 mint={mint}"));
        return false;
    };
    let cfg = cfg.clone();
    let lander = lander.clone();
    let payer = payer.clone();
    let order_tx = order_tx.clone();
    tokio::spawn(async move {
        let _permit = permit;
        let result = execute_sell(&cfg, &lander, &payer, &position, recent).await;
        let _ = order_tx.send(OrderResult::Sell { mint, result }).await;
    });
    true
}

fn spawn_blockhash_refresher(lander: Lander, tx: watch::Sender<Hash>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(400));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Ok(hash) = lander.latest_blockhash().await {
                tx.send_replace(hash);
            }
        }
    });
}

#[inline]
fn current_hash(rx: &watch::Receiver<Hash>) -> Hash {
    *rx.borrow()
}

fn confirmed_buy_tokens(event: &PumpEvent, wallet: Pubkey) -> Option<u64> {
    let mut found = false;
    let mut total = 0u64;
    for buy in &event.buys {
        if buy.wallet != wallet || !buy.exact {
            continue;
        }
        let Some(tokens) = buy.token_amount else {
            continue;
        };
        total = total.checked_add(tokens)?;
        found = true;
    }
    found.then_some(total).filter(|tokens| *tokens > 0)
}

fn confirmed_sell(event: &PumpEvent, wallet: Pubkey) -> bool {
    event.sells.iter().any(|sell| sell.wallet == wallet)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confirmed_buy_uses_only_exact_trades_from_our_wallet() {
        let wallet = Pubkey::new_unique();
        let mut event = test_event();
        event.buys = vec![
            crate::pump::PumpBuy {
                wallet,
                quote_amount: 1,
                quote_mint: spl_token::native_mint::ID,
                quote_decimals: Some(9),
                token_amount: Some(7),
                instruction_index: 0,
                event_index: 0,
                virtual_quote_reserves: None,
                virtual_token_reserves: None,
                exact: true,
            },
            crate::pump::PumpBuy {
                wallet: Pubkey::new_unique(),
                quote_amount: 1,
                quote_mint: spl_token::native_mint::ID,
                quote_decimals: Some(9),
                token_amount: Some(99),
                instruction_index: 1,
                event_index: 0,
                virtual_quote_reserves: None,
                virtual_token_reserves: None,
                exact: true,
            },
        ];

        assert_eq!(confirmed_buy_tokens(&event, wallet), Some(7));
    }

    fn test_event() -> PumpEvent {
        PumpEvent {
            source_id: 0,
            source_mask: 1,
            replayed: false,
            repaired: false,
            slot: 1,
            transaction_index: 0,
            signature: "sig".into(),
            fee_lamports: 0,
            signer: Pubkey::new_unique(),
            mint: Pubkey::new_unique(),
            bonding_curve: Pubkey::new_unique(),
            creator: Pubkey::new_unique(),
            token_program: spl_token::ID,
            quote_mint: spl_token::native_mint::ID,
            quote_decimals: Some(9),
            name: None,
            symbol: None,
            uri: None,
            kind: crate::pump::PumpIxKind::BuyExactSolIn,
            buy_quote_amount: None,
            buy_instruction_count: 0,
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
}
