//! 本地持仓。

use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub creator: Pubkey,
    pub token_program: Pubkey,
    pub user_token: Pubkey,
    /// `SubmittedBuy` 阶段为估算值；进入 `Open` 前会被链上实际成交量覆盖。
    pub token_amount: u64,
    pub opened: Instant,
    pub buy_sig: String,
    pub buy_sigs: Vec<String>,
}

#[derive(Debug, Clone)]
enum PositionState {
    Buying {
        creator: Pubkey,
        pending_exit: bool,
    },
    SubmittedBuy {
        position: Position,
        pending_exit: bool,
        submitted: Instant,
        reconciliation_reported: bool,
    },
    Open(Position),
    Selling(Position),
    SubmittedSell {
        position: Position,
        signatures: Vec<String>,
        submitted: Instant,
        reconciliation_reported: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SellDecision {
    Start(Position),
    Deferred,
    Ignored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuyResolution {
    AwaitingConfirmation,
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleSubmission {
    pub mint: Pubkey,
    pub side: ReconcileSide,
    pub signature: String,
    pub token_amount: u64,
}

/// 单线程 actor 内使用的订单/仓位状态机。
///
/// 所有网络 await 都发生在本类型之外；状态转换先完成，再启动网络任务，
/// 因而同一 mint 最多只有一个买单或卖单拥有执行权。
#[derive(Default)]
pub struct PositionBook {
    states: HashMap<Pubkey, PositionState>,
}

impl PositionBook {
    pub fn begin_buy(&mut self, mint: Pubkey, creator: Pubkey) -> bool {
        if self.states.contains_key(&mint) {
            return false;
        }
        self.states.insert(
            mint,
            PositionState::Buying {
                creator,
                pending_exit: false,
            },
        );
        true
    }

    pub fn buy_submitted(&mut self, mut position: Position) -> BuyResolution {
        let mint = position.mint;
        match self.states.remove(&mint) {
            Some(PositionState::Buying {
                creator,
                pending_exit,
            }) => {
                position.creator = creator;
                self.states.insert(
                    mint,
                    PositionState::SubmittedBuy {
                        position,
                        pending_exit,
                        submitted: Instant::now(),
                        reconciliation_reported: false,
                    },
                );
                BuyResolution::AwaitingConfirmation
            }
            Some(state) => {
                self.states.insert(mint, state);
                BuyResolution::Stale
            }
            None => BuyResolution::Stale,
        }
    }

    pub fn fail_buy(&mut self, mint: Pubkey) -> bool {
        match self.states.remove(&mint) {
            Some(PositionState::Buying { .. }) => true,
            Some(state) => {
                self.states.insert(mint, state);
                false
            }
            None => false,
        }
    }

    pub fn abandon_unconfirmed_buys(&mut self) -> Vec<Pubkey> {
        let mints: Vec<Pubkey> = self
            .states
            .iter()
            .filter_map(|(mint, state)| match state {
                PositionState::Buying { .. } | PositionState::SubmittedBuy { .. } => Some(*mint),
                _ => None,
            })
            .collect();
        for mint in &mints {
            self.states.remove(mint);
        }
        mints
    }

    pub fn abandon_unconfirmed_buy(&mut self, mint: Pubkey) -> bool {
        match self.states.remove(&mint) {
            Some(PositionState::Buying { .. } | PositionState::SubmittedBuy { .. }) => true,
            Some(state) => {
                self.states.insert(mint, state);
                false
            }
            None => false,
        }
    }

    pub fn abandon_stale_submitted_sells(&mut self, timeout: std::time::Duration) -> Vec<Pubkey> {
        let mints: Vec<Pubkey> = self
            .states
            .iter()
            .filter_map(|(mint, state)| match state {
                PositionState::SubmittedSell { submitted, .. }
                    if submitted.elapsed() >= timeout =>
                {
                    Some(*mint)
                }
                _ => None,
            })
            .collect();
        for mint in &mints {
            self.states.remove(mint);
        }
        mints
    }

    /// 用链上事件确认买入并写入真实 token 数量。
    pub fn confirm_buy(
        &mut self,
        mint: Pubkey,
        signature: &str,
        actual_tokens: u64,
    ) -> SellDecision {
        let Some(state) = self.states.remove(&mint) else {
            return SellDecision::Ignored;
        };
        match state {
            PositionState::SubmittedBuy {
                mut position,
                pending_exit,
                ..
            } if matches_signature(&position.buy_sig, &position.buy_sigs, signature)
                && actual_tokens > 0 =>
            {
                position.token_amount = actual_tokens;
                position.buy_sig = signature.to_owned();
                position.opened = Instant::now();
                if pending_exit {
                    self.states
                        .insert(mint, PositionState::Selling(position.clone()));
                    SellDecision::Start(position)
                } else {
                    self.states.insert(mint, PositionState::Open(position));
                    SellDecision::Ignored
                }
            }
            state => {
                self.states.insert(mint, state);
                SellDecision::Ignored
            }
        }
    }

    pub fn request_sell(&mut self, mint: Pubkey) -> SellDecision {
        let Some(state) = self.states.remove(&mint) else {
            return SellDecision::Ignored;
        };
        match state {
            PositionState::Buying { creator, .. } => {
                self.states.insert(
                    mint,
                    PositionState::Buying {
                        creator,
                        pending_exit: true,
                    },
                );
                SellDecision::Deferred
            }
            PositionState::SubmittedBuy {
                position,
                submitted,
                reconciliation_reported,
                ..
            } => {
                self.states.insert(
                    mint,
                    PositionState::SubmittedBuy {
                        position,
                        pending_exit: true,
                        submitted,
                        reconciliation_reported,
                    },
                );
                SellDecision::Deferred
            }
            PositionState::Open(position) => {
                self.states
                    .insert(mint, PositionState::Selling(position.clone()));
                SellDecision::Start(position)
            }
            state => {
                self.states.insert(mint, state);
                SellDecision::Ignored
            }
        }
    }

    /// 返回买入触发时记录的 creator；不能使用后续 sell 事件里的退化字段判断身份。
    pub fn creator(&self, mint: Pubkey) -> Option<Pubkey> {
        match self.states.get(&mint)? {
            PositionState::Buying { creator, .. } => Some(*creator),
            PositionState::SubmittedBuy { position, .. }
            | PositionState::Open(position)
            | PositionState::Selling(position)
            | PositionState::SubmittedSell { position, .. } => Some(position.creator),
        }
    }

    pub fn sell_submitted(&mut self, mint: Pubkey, signatures: Vec<String>) -> bool {
        let Some(state) = self.states.remove(&mint) else {
            return false;
        };
        match state {
            PositionState::Selling(position) => {
                self.states.insert(
                    mint,
                    PositionState::SubmittedSell {
                        position,
                        signatures,
                        submitted: Instant::now(),
                        reconciliation_reported: false,
                    },
                );
                true
            }
            state => {
                self.states.insert(mint, state);
                false
            }
        }
    }

    pub fn fail_sell(&mut self, mint: Pubkey) -> bool {
        let Some(state) = self.states.remove(&mint) else {
            return false;
        };
        match state {
            PositionState::Selling(position) => {
                self.states.insert(mint, PositionState::Open(position));
                true
            }
            state => {
                self.states.insert(mint, state);
                false
            }
        }
    }

    pub fn confirm_sell(&mut self, mint: Pubkey, signature: &str) -> bool {
        let Some(state) = self.states.remove(&mint) else {
            return false;
        };
        match state {
            PositionState::SubmittedSell { signatures, .. }
                if signatures.iter().any(|expected| expected == signature) =>
            {
                true
            }
            state => {
                self.states.insert(mint, state);
                false
            }
        }
    }

    pub fn claim_timeouts(&mut self, max_hold: std::time::Duration) -> Vec<Position> {
        let mints: Vec<Pubkey> = self
            .states
            .iter()
            .filter_map(|(mint, state)| match state {
                PositionState::Open(position) if position.opened.elapsed() >= max_hold => {
                    Some(*mint)
                }
                _ => None,
            })
            .collect();

        mints
            .into_iter()
            .filter_map(|mint| match self.request_sell(mint) {
                SellDecision::Start(position) => Some(position),
                SellDecision::Deferred | SellDecision::Ignored => None,
            })
            .collect()
    }

    pub fn claim_all_for_shutdown(&mut self) -> Vec<Position> {
        let mints: Vec<Pubkey> = self.states.keys().copied().collect();
        mints
            .into_iter()
            .filter_map(|mint| match self.request_sell(mint) {
                SellDecision::Start(position) => Some(position),
                SellDecision::Deferred | SellDecision::Ignored => None,
            })
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    pub fn snapshots(&self) -> Vec<crate::admin::PositionSnapshot> {
        self.states
            .iter()
            .map(|(mint, state)| match state {
                PositionState::Buying { .. } => crate::admin::PositionSnapshot {
                    mint: mint.to_string(),
                    state: "buying".into(),
                    token_amount: 0,
                    age_ms: 0,
                    buy_sig: String::new(),
                },
                PositionState::SubmittedBuy {
                    position,
                    submitted,
                    ..
                } => snapshot(position, "submitted_buy", submitted.elapsed().as_millis()),
                PositionState::Open(position) => {
                    snapshot(position, "open", position.opened.elapsed().as_millis())
                }
                PositionState::Selling(position) => {
                    snapshot(position, "selling", position.opened.elapsed().as_millis())
                }
                PositionState::SubmittedSell {
                    position,
                    submitted,
                    ..
                } => snapshot(position, "submitted_sell", submitted.elapsed().as_millis()),
            })
            .collect()
    }

    pub fn active_target_snapshots(&self) -> Vec<crate::admin::TargetSnapshot> {
        self.states
            .iter()
            .filter_map(|(mint, state)| match state {
                PositionState::Buying { .. } => Some(crate::admin::TargetSnapshot {
                    mint: mint.to_string(),
                    create_slot: 0,
                }),
                PositionState::SubmittedBuy { position, .. }
                | PositionState::Open(position)
                | PositionState::Selling(position)
                | PositionState::SubmittedSell { position, .. } => {
                    Some(crate::admin::TargetSnapshot {
                        mint: position.mint.to_string(),
                        create_slot: 0,
                    })
                }
            })
            .collect()
    }

    pub fn contains(&self, mint: Pubkey) -> bool {
        self.states.contains_key(&mint)
    }

    pub fn claim_stale_submissions(
        &mut self,
        timeout: std::time::Duration,
    ) -> Vec<StaleSubmission> {
        self.states
            .iter_mut()
            .filter_map(|(mint, state)| match state {
                PositionState::SubmittedBuy {
                    position,
                    submitted,
                    reconciliation_reported,
                    ..
                } if !*reconciliation_reported && submitted.elapsed() >= timeout => {
                    *reconciliation_reported = true;
                    Some(StaleSubmission {
                        mint: *mint,
                        side: ReconcileSide::Buy,
                        signature: joined_signatures(&position.buy_sig, &position.buy_sigs),
                        token_amount: position.token_amount,
                    })
                }
                PositionState::SubmittedSell {
                    position,
                    signatures,
                    submitted,
                    reconciliation_reported,
                } if !*reconciliation_reported && submitted.elapsed() >= timeout => {
                    *reconciliation_reported = true;
                    Some(StaleSubmission {
                        mint: *mint,
                        side: ReconcileSide::Sell,
                        signature: signatures.join(","),
                        token_amount: position.token_amount,
                    })
                }
                _ => None,
            })
            .collect()
    }
}

fn snapshot(position: &Position, state: &str, age_ms: u128) -> crate::admin::PositionSnapshot {
    crate::admin::PositionSnapshot {
        mint: position.mint.to_string(),
        state: state.into(),
        token_amount: position.token_amount,
        age_ms,
        buy_sig: position.buy_sig.clone(),
    }
}

fn matches_signature(primary: &str, all: &[String], signature: &str) -> bool {
    primary == signature || all.iter().any(|candidate| candidate == signature)
}

fn joined_signatures(primary: &str, all: &[String]) -> String {
    if all.is_empty() {
        primary.to_owned()
    } else {
        all.join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn position(mint: Pubkey) -> Position {
        Position {
            mint,
            bonding_curve: Pubkey::new_unique(),
            creator: Pubkey::new_unique(),
            token_program: spl_token::ID,
            user_token: Pubkey::new_unique(),
            token_amount: 10,
            opened: Instant::now(),
            buy_sig: "buy-sig".into(),
            buy_sigs: vec!["buy-sig".into()],
        }
    }

    #[test]
    fn duplicate_buy_is_rejected_while_pending() {
        let mint = Pubkey::new_unique();
        let mut book = PositionBook::default();

        let creator = Pubkey::new_unique();
        assert!(book.begin_buy(mint, creator));
        assert!(!book.begin_buy(mint, creator));
    }

    #[test]
    fn exit_during_buy_is_deferred_until_buy_confirmation() {
        let mint = Pubkey::new_unique();
        let mut book = PositionBook::default();
        assert!(book.begin_buy(mint, Pubkey::new_unique()));
        assert_eq!(book.request_sell(mint), SellDecision::Deferred);
        assert_eq!(
            book.buy_submitted(position(mint)),
            BuyResolution::AwaitingConfirmation
        );

        let decision = book.confirm_buy(mint, "buy-sig", 42);
        let SellDecision::Start(pos) = decision else {
            panic!("deferred exit must start after confirmation");
        };
        assert_eq!(pos.token_amount, 42);
    }

    #[test]
    fn timeout_and_creator_sell_cannot_both_claim_one_position() {
        let mint = Pubkey::new_unique();
        let mut book = PositionBook::default();
        assert!(book.begin_buy(mint, Pubkey::new_unique()));
        book.buy_submitted(position(mint));
        assert_eq!(book.confirm_buy(mint, "buy-sig", 42), SellDecision::Ignored);

        let claimed = book.claim_timeouts(Duration::ZERO);
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].mint, mint);
        assert_eq!(book.request_sell(mint), SellDecision::Ignored);
    }

    #[test]
    fn submitted_sell_closes_only_after_matching_confirmation() {
        let mint = Pubkey::new_unique();
        let mut book = PositionBook::default();
        assert!(book.begin_buy(mint, Pubkey::new_unique()));
        book.buy_submitted(position(mint));
        book.confirm_buy(mint, "buy-sig", 42);
        let SellDecision::Start(_) = book.request_sell(mint) else {
            panic!("open position must be sellable");
        };
        assert!(book.sell_submitted(mint, vec!["sell-sig".into()]));

        assert!(!book.confirm_sell(mint, "different-sig"));
        assert!(book.contains(mint));
        assert!(book.confirm_sell(mint, "sell-sig"));
        assert!(!book.contains(mint));
    }

    #[test]
    fn submitted_buy_accepts_any_variant_signature() {
        let mint = Pubkey::new_unique();
        let mut book = PositionBook::default();
        let mut position = position(mint);
        position.buy_sig = "primary".into();
        position.buy_sigs = vec!["primary".into(), "variant".into()];

        assert!(book.begin_buy(mint, Pubkey::new_unique()));
        book.buy_submitted(position);

        assert_eq!(book.confirm_buy(mint, "variant", 42), SellDecision::Ignored);
        assert!(book.contains(mint));
    }

    #[test]
    fn submitted_sell_accepts_any_variant_signature() {
        let mint = Pubkey::new_unique();
        let mut book = PositionBook::default();
        assert!(book.begin_buy(mint, Pubkey::new_unique()));
        book.buy_submitted(position(mint));
        book.confirm_buy(mint, "buy-sig", 42);
        let SellDecision::Start(_) = book.request_sell(mint) else {
            panic!("open position must be sellable");
        };
        assert!(book.sell_submitted(mint, vec!["primary-sell".into(), "variant-sell".into()]));

        assert!(book.confirm_sell(mint, "variant-sell"));
        assert!(!book.contains(mint));
    }

    #[test]
    fn stale_submission_is_reported_once_for_reconciliation() {
        let mint = Pubkey::new_unique();
        let mut book = PositionBook::default();
        assert!(book.begin_buy(mint, Pubkey::new_unique()));
        book.buy_submitted(position(mint));

        let stale = book.claim_stale_submissions(Duration::ZERO);
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].mint, mint);
        assert_eq!(stale[0].side, ReconcileSide::Buy);
        assert!(book.claim_stale_submissions(Duration::ZERO).is_empty());
    }

    #[test]
    fn creator_is_available_while_buy_is_still_in_flight() {
        let mint = Pubkey::new_unique();
        let creator = Pubkey::new_unique();
        let mut book = PositionBook::default();

        assert!(book.begin_buy(mint, creator));
        assert_eq!(book.creator(mint), Some(creator));
        assert_ne!(book.creator(mint), Some(Pubkey::new_unique()));

        let submitted = position(mint);
        assert_ne!(submitted.creator, creator);
        assert_eq!(
            book.buy_submitted(submitted),
            BuyResolution::AwaitingConfirmation
        );
        assert_eq!(book.creator(mint), Some(creator));
    }
}
