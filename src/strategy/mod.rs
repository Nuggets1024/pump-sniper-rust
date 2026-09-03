//! 跟盘策略：按发盘钱包和金额区间决定买/卖。

use crate::config::{AppConfig, BotMode};
use crate::pump::{is_native_quote, PumpEvent};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub enum Action {
    /// 跟盘买入
    Buy,
    /// 观察到卖出；seller 身份由引擎与建仓时保存的 creator 最终核对。
    SellObserved { mint: Pubkey, seller: Pubkey },
}

/// 策略输出帧：链上事实原样交给交易模块做确认，Intent 只表达策略意图。
pub struct StrategyFrame {
    pub event: PumpEvent,
    pub intent: Option<Action>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanTarget {
    pub mint: Pubkey,
    pub create_slot: u64,
}

pub struct FollowDev {
    direct_create: bool,
    follows: Vec<(Pubkey, f64, f64)>,
    black_mints: HashSet<Pubkey>,
    black_creators: HashSet<Pubkey>,
}

impl FollowDev {
    pub fn new(cfg: &AppConfig) -> Self {
        Self {
            direct_create: cfg.follow.is_empty(),
            follows: cfg.follow_index(),
            black_mints: cfg.black_mints(),
            black_creators: cfg.black_creators(),
        }
    }

    pub fn on_event(&mut self, ev: &PumpEvent) -> Option<Action> {
        if self.black_mints.contains(&ev.mint) || self.black_creators.contains(&ev.creator) {
            return None;
        }
        if matches!(
            ev.kind,
            crate::pump::PumpIxKind::AmmBuy | crate::pump::PumpIxKind::AmmSell
        ) {
            return None;
        }

        // 全量狙击模式只把发盘指令作为买入信号；普通 buy 不触发下单，
        // sell 只交给引擎匹配已有仓位。
        if self.direct_create && ev.is_create {
            if !is_native_quote(ev.quote_mint) {
                return None;
            }
            return self.claim_buy();
        }

        if ev.is_sell {
            let seller = ev
                .sells
                .first()
                .map(|sell| sell.wallet)
                .unwrap_or(ev.signer);
            if self.direct_create {
                return Some(Action::SellObserved {
                    mint: ev.mint,
                    seller,
                });
            }
            if self.is_follow(&seller) || (seller == ev.creator && self.is_follow(&ev.creator)) {
                return Some(Action::SellObserved {
                    mint: ev.mint,
                    seller,
                });
            }
            return None;
        }

        // follow 只把被关注 dev 的 CREATE 当作买入信号；dev 后续普通买卖
        // 不是跟买信号。目标 mint 的交易仅用于建仓确认、审计和 creator sell。
        if !ev.is_create {
            return None;
        }

        if !self.is_follow(&ev.signer) && !self.is_follow(&ev.creator) {
            return None;
        }
        self.buy_if_size(ev)
    }

    fn buy_if_size(&mut self, ev: &PumpEvent) -> Option<Action> {
        if !is_native_quote(ev.quote_mint) {
            return None;
        }
        let (min, max) = self
            .range_for(&ev.signer)
            .or_else(|| self.range_for(&ev.creator))?;
        if let Some(lamports) = ev.buy_quote_amount {
            let sol = lamports as f64 / 1e9;
            if sol < min || sol > max {
                return None;
            }
        }
        self.claim_buy()
    }

    fn claim_buy(&mut self) -> Option<Action> {
        Some(Action::Buy)
    }

    fn is_follow(&self, pk: &Pubkey) -> bool {
        self.follows.iter().any(|(a, _, _)| a == pk)
    }

    fn range_for(&self, pk: &Pubkey) -> Option<(f64, f64)> {
        self.follows
            .iter()
            .find(|(a, _, _)| a == pk)
            .map(|(_, min, max)| (*min, *max))
    }
}

struct StrategyRouter {
    strategy: FollowDev,
    mode: BotMode,
    scan_target: Option<Pubkey>,
    target_tx: Option<tokio::sync::watch::Sender<Vec<ScanTarget>>>,
}

impl StrategyRouter {
    fn new(
        strategy: FollowDev,
        mode: BotMode,
        target_tx: Option<tokio::sync::watch::Sender<Vec<ScanTarget>>>,
    ) -> Self {
        Self {
            strategy,
            mode,
            scan_target: None,
            target_tx,
        }
    }

    fn route(&mut self, event: PumpEvent) -> Option<StrategyFrame> {
        let live = !event.replayed && !event.repaired;
        if self.mode == BotMode::Scan {
            if let Some(target) = self.scan_target {
                if event.mint != target {
                    return None;
                }
            } else {
                // 历史重放不能抢占本次 scan 的唯一目标。
                if !live || !event.is_create {
                    return None;
                }
                let intent = self.strategy.on_event(&event);
                if !matches!(&intent, Some(Action::Buy)) {
                    return None;
                }
                self.scan_target = Some(event.mint);
                self.publish_target(event.mint, event.slot);
                return Some(StrategyFrame { event, intent });
            }
        }

        let intent = live.then(|| self.strategy.on_event(&event)).flatten();
        if event.is_create && matches!(intent, Some(Action::Buy)) {
            self.publish_target(event.mint, event.slot);
        }
        Some(StrategyFrame { event, intent })
    }

    fn publish_target(&self, mint: Pubkey, create_slot: u64) {
        let Some(target_tx) = &self.target_tx else {
            return;
        };
        let mut targets = target_tx.borrow().clone();
        if targets.iter().any(|target| target.mint == mint) {
            return;
        }
        targets.push(ScanTarget { mint, create_slot });
        crate::admin::set_targets(
            targets
                .iter()
                .map(|target| crate::admin::TargetSnapshot {
                    mint: target.mint.to_string(),
                    create_slot: target.create_slot,
                })
                .collect(),
        );
        target_tx.send_replace(targets);
    }

    fn release_target(&mut self, mint: Pubkey) {
        if self.scan_target == Some(mint) {
            self.scan_target = None;
        }
        let Some(target_tx) = &self.target_tx else {
            return;
        };
        let mut targets = target_tx.borrow().clone();
        let before = targets.len();
        targets.retain(|target| target.mint != mint);
        if targets.len() == before {
            return;
        }
        crate::admin::set_targets(
            targets
                .iter()
                .map(|target| crate::admin::TargetSnapshot {
                    mint: target.mint.to_string(),
                    create_slot: target.create_slot,
                })
                .collect(),
        );
        target_tx.send_replace(targets);
        crate::telemetry::info("监听", format!("停止监听 mint={mint}"));
    }
}

/// 策略 actor 只做确定性判定；scan 锁定后不再转发其他 mint。
pub async fn run(
    strategy: FollowDev,
    mode: BotMode,
    mut events: tokio::sync::mpsc::Receiver<PumpEvent>,
    output: tokio::sync::mpsc::Sender<StrategyFrame>,
    target_tx: Option<tokio::sync::watch::Sender<Vec<ScanTarget>>>,
    mut release_rx: Option<tokio::sync::mpsc::Receiver<Pubkey>>,
) {
    let mut router = StrategyRouter::new(strategy, mode, target_tx);
    loop {
        tokio::select! {
            maybe_mint = async {
                match release_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            }, if release_rx.is_some() => {
                let Some(mint) = maybe_mint else {
                    release_rx = None;
                    continue;
                };
                router.release_target(mint);
            }
            maybe_event = events.recv() => {
                let Some(event) = maybe_event else { break };
                if let Some(frame) = router.route(event) {
                    if output.send(frame).await.is_err() {
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pump::PumpIxKind;

    fn event(is_create: bool) -> PumpEvent {
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
            token_program: spl_token_2022::ID,
            quote_mint: spl_token::native_mint::ID,
            quote_decimals: Some(9),
            name: None,
            symbol: None,
            uri: None,
            kind: if is_create {
                PumpIxKind::CreateV2
            } else {
                PumpIxKind::BuyExactSolIn
            },
            buy_quote_amount: None,
            buy_instruction_count: 0,
            buys: vec![],
            sells: vec![],
            jito_tip_lamports: None,
            jito_dont_front: false,
            is_create,
            is_buy: !is_create,
            is_sell: false,
            seen_ns: 0,
        }
    }

    fn sell_event() -> PumpEvent {
        let mut ev = event(false);
        ev.kind = PumpIxKind::Sell;
        ev.is_buy = false;
        ev.is_sell = true;
        ev.signer = ev.creator;
        ev
    }

    fn direct_strategy() -> FollowDev {
        FollowDev {
            direct_create: true,
            follows: vec![],
            black_mints: HashSet::new(),
            black_creators: HashSet::new(),
        }
    }

    fn follow_strategy(dev: Pubkey) -> FollowDev {
        FollowDev {
            direct_create: false,
            follows: vec![(dev, 0.0, 10.0)],
            black_mints: HashSet::new(),
            black_creators: HashSet::new(),
        }
    }

    #[test]
    fn direct_create_mode_buys_create() {
        assert!(matches!(
            direct_strategy().on_event(&event(true)),
            Some(Action::Buy)
        ));
    }

    #[test]
    fn direct_create_mode_ignores_plain_buy() {
        assert!(direct_strategy().on_event(&event(false)).is_none());
    }

    #[test]
    fn follow_buys_dev_create_but_ignores_dev_plain_buy() {
        let dev = Pubkey::new_unique();
        let mut strategy = follow_strategy(dev);
        let mut create = event(true);
        create.creator = dev;
        create.signer = dev;
        assert!(matches!(strategy.on_event(&create), Some(Action::Buy)));

        let mut plain_buy = event(false);
        plain_buy.creator = dev;
        plain_buy.signer = dev;
        assert!(strategy.on_event(&plain_buy).is_none());
    }

    #[test]
    fn follow_create_publishes_mint_target() {
        let dev = Pubkey::new_unique();
        let (target_tx, target_rx) = tokio::sync::watch::channel(vec![]);
        let mut router =
            StrategyRouter::new(follow_strategy(dev), BotMode::Standard, Some(target_tx));
        let mut create = event(true);
        create.creator = dev;
        create.signer = dev;

        router
            .route(create.clone())
            .expect("follow create forwards");

        assert_eq!(
            target_rx.borrow().as_slice(),
            &[ScanTarget {
                mint: create.mint,
                create_slot: create.slot,
            }]
        );
    }

    #[test]
    fn direct_create_mode_forwards_sells_for_open_position_matching() {
        assert!(matches!(
            direct_strategy().on_event(&sell_event()),
            Some(Action::SellObserved { .. })
        ));
    }

    #[test]
    fn direct_create_mode_forwards_non_creator_sell_for_engine_validation() {
        let mut ev = sell_event();
        ev.signer = Pubkey::new_unique();
        assert_ne!(ev.signer, ev.creator);
        assert!(matches!(
            direct_strategy().on_event(&ev),
            Some(Action::SellObserved { seller, .. }) if seller == ev.signer
        ));
    }

    #[test]
    fn direct_create_mode_honors_mint_blacklist() {
        let ev = event(true);
        let mut strategy = direct_strategy();
        strategy.black_mints.insert(ev.mint);
        assert!(strategy.on_event(&ev).is_none());
    }

    #[tokio::test]
    async fn replayed_event_is_forwarded_without_intent() {
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(1);
        let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(run(
            direct_strategy(),
            BotMode::Standard,
            event_rx,
            frame_tx,
            None,
            None,
        ));
        let mut replayed = event(true);
        replayed.replayed = true;
        event_tx.send(replayed).await.unwrap();
        let frame = frame_rx.recv().await.unwrap();
        assert!(frame.event.replayed);
        assert!(frame.intent.is_none());
    }

    #[test]
    fn scan_locks_first_buyable_create_and_drops_other_mints() {
        let mut router = StrategyRouter::new(direct_strategy(), BotMode::Scan, None);
        let first = event(true);
        let target = first.mint;
        let frame = router.route(first).expect("first create locks scan");
        assert!(matches!(frame.intent, Some(Action::Buy)));

        let other = event(false);
        assert_ne!(other.mint, target);
        assert!(router.route(other).is_none());

        let mut target_trade = event(false);
        target_trade.mint = target;
        let frame = router
            .route(target_trade)
            .expect("target trade is forwarded");
        assert_eq!(frame.event.mint, target);
        assert!(frame.intent.is_none());
    }

    #[test]
    fn scan_publishes_target_mint_and_create_slot() {
        let (target_tx, target_rx) = tokio::sync::watch::channel(vec![]);
        let mut router = StrategyRouter::new(direct_strategy(), BotMode::Scan, Some(target_tx));
        let create = event(true);

        router.route(create.clone()).expect("create locks scan");

        assert_eq!(
            target_rx.borrow().as_slice(),
            &[ScanTarget {
                mint: create.mint,
                create_slot: create.slot,
            }]
        );
    }

    #[test]
    fn scan_ignores_replay_when_selecting_target() {
        let mut router = StrategyRouter::new(direct_strategy(), BotMode::Scan, None);
        let mut replayed = event(true);
        replayed.replayed = true;
        assert!(router.route(replayed).is_none());

        let live = event(true);
        assert!(router.route(live).is_some());
    }
}
