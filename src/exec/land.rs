use crate::config::{AppConfig, CommitmentCfg};
use crate::error::{Result, SniperError};
use crate::exec::sender::{build_sender, SubmissionStatus, TransactionSender};
use base64::Engine;
use bytes::Bytes;
use rand::seq::SliceRandom;
use rand::Rng;
use solana_rpc_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::system_instruction;
use solana_sdk::transaction::Transaction;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

const MAX_WIRE_TRANSACTION_BYTES: usize = 1232;
const DISPATCH_ACK_GRACE: Duration = Duration::from_millis(25);

#[derive(Clone)]
pub struct Lander {
    read_rpc: Arc<RpcClient>,
    senders: Vec<Arc<dyn TransactionSender>>,
    tip_pools: Vec<TipPool>,
}

#[derive(Clone)]
struct TipPool {
    label: String,
    accounts: Vec<Pubkey>,
    lamports: u64,
}

#[derive(Clone)]
pub struct TipPlan {
    pub instructions: Vec<Instruction>,
    pub label: String,
    pub lamports: u64,
}

impl TipPlan {
    fn allows_route(&self, route: &str) -> bool {
        self.instructions.is_empty() || self.label.split('|').any(|label| label == route)
    }
}

pub struct LandReceipt {
    pub signature: Signature,
    pub signatures: Vec<Signature>,
    pub channel: String,
}

pub struct LandTransaction {
    pub transaction: Transaction,
    pub tip: TipPlan,
}

struct PreparedLandTransaction {
    transaction: Arc<Transaction>,
    tip: TipPlan,
    signature: Signature,
    wire: Bytes,
    base64_wire: Bytes,
    eligible_senders: Vec<Arc<dyn TransactionSender>>,
}

impl Lander {
    pub fn new(cfg: &AppConfig) -> Self {
        let commitment = match cfg.rpc.commitment {
            CommitmentCfg::Finalized => CommitmentConfig::finalized(),
            CommitmentCfg::Confirmed => CommitmentConfig::confirmed(),
            CommitmentCfg::Processed => CommitmentConfig::processed(),
        };
        let read_rpc = Arc::new(crate::rpc::blocking(
            cfg.rpc.url.clone(),
            Duration::from_secs(8),
            commitment,
        ));
        let routes = cfg.landing_routes();
        let senders = routes
            .iter()
            .map(|route| build_sender(route, cfg.landing.skip_preflight))
            .collect::<anyhow::Result<Vec<_>>>()
            .expect("landing routes 已在配置加载时校验");
        Self {
            read_rpc,
            senders,
            tip_pools: build_tip_pools(&routes),
        }
    }

    pub fn rpc(&self) -> &RpcClient {
        &self.read_rpc
    }

    /// 启动期并发建立 DNS/TLS/连接池；失败不代表真实交易提交一定失败。
    pub async fn warmup(&self) {
        futures::future::join_all(self.senders.iter().map(|sender| sender.warmup())).await;
        for sender in &self.senders {
            let sender = Arc::downgrade(sender);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(50));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                interval.tick().await;
                loop {
                    interval.tick().await;
                    let Some(sender) = sender.upgrade() else {
                        break;
                    };
                    let _ = sender.warmup().await;
                }
            });
        }
    }

    /// 每个唯一 tip 钱包池各生成一份 plan；调用方可据此签出不同交易并并发提交。
    pub fn tip_plans(&self, payer: &Pubkey) -> Vec<TipPlan> {
        let mut rng = rand::thread_rng();
        if self.tip_pools.is_empty() {
            return vec![TipPlan {
                instructions: Vec::new(),
                label: "none".to_owned(),
                lamports: 0,
            }];
        };

        self.tip_pools
            .iter()
            .filter_map(|pool| {
                let account = choose_tip_account(&pool.accounts, &mut rng)?;
                Some(TipPlan {
                    instructions: vec![system_instruction::transfer(payer, account, pool.lamports)],
                    label: pool.label.clone(),
                    lamports: pool.lamports,
                })
            })
            .collect()
    }

    pub fn tip_instructions(&self, payer: &Pubkey) -> TipPlan {
        let plans = self.tip_plans(payer);
        combine_tip_plans(plans)
    }

    /// 后台刷新使用；RPC 查询不会进入交易构建/签名热路径。
    pub async fn latest_blockhash(&self) -> Result<Hash> {
        let client = self.read_rpc.clone();
        tokio::task::spawn_blocking(move || {
            client
                .get_latest_blockhash()
                .map_err(|error| error.to_string())
        })
        .await
        .map_err(|e| SniperError::Land(format!("等待 blockhash 任务: {e}")))?
        .map_err(|e| SniperError::Land(format!("读取 blockhash: {e}")))
    }

    pub async fn send(&self, tx: &Transaction, tip: &TipPlan) -> Result<LandReceipt> {
        self.send_any(vec![LandTransaction {
            transaction: tx.clone(),
            tip: tip.clone(),
        }])
        .await
    }

    pub async fn send_any(&self, transactions: Vec<LandTransaction>) -> Result<LandReceipt> {
        let started = std::time::Instant::now();
        let prepared = transactions
            .into_iter()
            .map(|entry| {
                let signature = entry.transaction.signatures[0];
                let wire = Bytes::from(
                    bincode::serialize(&entry.transaction)
                        .map_err(|error| SniperError::Land(format!("序列化交易: {error}")))?,
                );
                validate_wire_size(&wire)?;
                let base64_wire =
                    Bytes::from(base64::engine::general_purpose::STANDARD.encode(&wire));
                let eligible_senders = self
                    .senders
                    .iter()
                    .filter(|sender| entry.tip.allows_route(sender.name()))
                    .cloned()
                    .collect::<Vec<_>>();
                Ok(PreparedLandTransaction {
                    transaction: Arc::new(entry.transaction),
                    tip: entry.tip,
                    signature,
                    wire,
                    base64_wire,
                    eligible_senders,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if prepared.is_empty() {
            return Err(SniperError::Land("没有可提交交易".into()));
        }

        let all_signatures = prepared
            .iter()
            .map(|entry| entry.signature)
            .collect::<Vec<_>>();
        let route_names = prepared
            .iter()
            .flat_map(|entry| entry.eligible_senders.iter().map(|sender| sender.name()))
            .collect::<Vec<_>>()
            .join(",");
        let tip_count = prepared
            .iter()
            .map(|entry| entry.tip.instructions.len())
            .sum::<usize>();
        let tip_lamports = prepared.iter().map(|entry| entry.tip.lamports).sum::<u64>();
        let tip_label = prepared
            .iter()
            .map(|entry| entry.tip.label.as_str())
            .collect::<Vec<_>>()
            .join("|");
        let max_wire_len = prepared
            .iter()
            .map(|entry| entry.wire.len())
            .max()
            .unwrap_or_default();
        crate::telemetry::info_fields(
            "提交",
            [
                route_names.clone(),
                format!(
                    "tip={}:{}:{:.6} SOL",
                    tip_count,
                    tip_label,
                    tip_lamports as f64 / 1e9
                ),
                format!("{} bytes", max_wire_len),
                "sent".to_owned(),
                all_signatures
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            ],
        );
        if prepared
            .iter()
            .all(|entry| entry.eligible_senders.is_empty())
        {
            return Err(SniperError::Land(format!(
                "本次 tip={} 没有匹配的提交 route",
                tip_label
            )));
        }
        let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();
        for entry in &prepared {
            for sender in &entry.eligible_senders {
                let sender = sender.clone();
                let result_tx = result_tx.clone();
                let transaction = entry.transaction.clone();
                let wire = entry.wire.clone();
                let base64_wire = entry.base64_wire.clone();
                let signature = entry.signature;
                tokio::spawn(async move {
                    let route_started = std::time::Instant::now();
                    let channel = sender.name().to_owned();
                    let result = sender
                        .submit(transaction, wire, base64_wire)
                        .await
                        .map(|status| {
                            let status_label = match status {
                                SubmissionStatus::Accepted => "accepted",
                                SubmissionStatus::Dispatched => "dispatched",
                            };
                            crate::telemetry::info_fields(
                                "路由",
                                [
                                    channel.clone(),
                                    status_label.to_owned(),
                                    format_elapsed(route_started.elapsed()),
                                    "-".to_owned(),
                                    signature.to_string(),
                                ],
                            );
                            (channel.clone(), signature, status)
                        })
                        .map_err(|error| {
                            crate::telemetry::error_fields(
                                "失败",
                                [
                                    channel.clone(),
                                    "rejected".to_owned(),
                                    format_elapsed(route_started.elapsed()),
                                    error.to_string(),
                                    signature.to_string(),
                                ],
                            );
                            format!("{channel}: {error}")
                        });
                    let _ = result_tx.send(result);
                });
            }
        }
        drop(result_tx);

        let mut errors = Vec::new();
        let mut dispatched = None;
        let mut dispatch_deadline = None;
        let accepted = loop {
            let next = if let Some(deadline) = dispatch_deadline {
                match tokio::time::timeout_at(deadline, result_rx.recv()).await {
                    Ok(result) => result,
                    Err(_) => {
                        let (channel, signature) = dispatched.unwrap();
                        break Ok((format!("{channel}/udp-dispatched"), signature));
                    }
                }
            } else {
                result_rx.recv().await
            };
            match next {
                Some(Ok((channel, signature, SubmissionStatus::Accepted))) => {
                    break Ok((channel, signature));
                }
                Some(Ok((channel, signature, SubmissionStatus::Dispatched))) => {
                    dispatched.get_or_insert((channel, signature));
                    dispatch_deadline
                        .get_or_insert_with(|| tokio::time::Instant::now() + DISPATCH_ACK_GRACE);
                }
                Some(Err(error)) => errors.push(error),
                None => {
                    break if let Some((channel, signature)) = dispatched {
                        Ok((format!("{channel}/udp-dispatched"), signature))
                    } else {
                        Err(if errors.is_empty() {
                            "没有落地节点".into()
                        } else {
                            errors.join(" | ")
                        })
                    };
                }
            }
        };
        match accepted {
            Ok((channel, signature)) => {
                let result = if channel.ends_with("/udp-dispatched") {
                    "dispatched"
                } else {
                    "accepted"
                };
                crate::telemetry::info_fields(
                    "提交",
                    [
                        channel.clone(),
                        result.to_owned(),
                        format_elapsed(started.elapsed()),
                        "-".to_owned(),
                        signature.to_string(),
                    ],
                );
                Ok(LandReceipt {
                    signature,
                    signatures: all_signatures,
                    channel,
                })
            }
            Err(e) => {
                crate::telemetry::error_fields(
                    "失败",
                    [
                        "All routes failed".to_owned(),
                        "failed".to_owned(),
                        format_elapsed(started.elapsed()),
                        e.to_string(),
                        all_signatures
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(","),
                    ],
                );
                Err(SniperError::Land(e))
            }
        }
    }
}

fn build_tip_pools(routes: &[crate::config::LandingRouteCfg]) -> Vec<TipPool> {
    let mut tip_by_pool = HashMap::<Vec<Pubkey>, (Vec<String>, u64)>::new();
    for route in routes {
        if route.tip_lamports == 0 {
            continue;
        }
        let mut accounts = route
            .tip_account_pool()
            .into_iter()
            .map(|account| {
                Pubkey::from_str(account).expect("landing tip account 已在配置加载时校验")
            })
            .collect::<Vec<_>>();
        accounts.sort_unstable();
        accounts.dedup();
        tip_by_pool
            .entry(accounts)
            .and_modify(|(labels, amount)| {
                labels.push(route.name.clone());
                *amount = (*amount).max(route.tip_lamports);
            })
            .or_insert((vec![route.name.clone()], route.tip_lamports));
    }
    let mut pools = tip_by_pool
        .into_iter()
        .map(|(accounts, (mut labels, lamports))| {
            labels.sort();
            labels.dedup();
            TipPool {
                label: labels.join("|"),
                accounts,
                lamports,
            }
        })
        .collect::<Vec<_>>();
    pools.sort_by(|left, right| left.label.cmp(&right.label));
    pools
}

fn combine_tip_plans(plans: Vec<TipPlan>) -> TipPlan {
    let mut instructions = Vec::new();
    let mut labels = Vec::new();
    let mut lamports = 0u64;
    for plan in plans {
        instructions.extend(plan.instructions);
        labels.push(plan.label);
        lamports = lamports.saturating_add(plan.lamports);
    }
    TipPlan {
        instructions,
        label: labels.join("|"),
        lamports,
    }
}

fn choose_tip_account<'a, R: Rng + ?Sized>(
    accounts: &'a [Pubkey],
    rng: &mut R,
) -> Option<&'a Pubkey> {
    accounts.choose(rng)
}

fn format_elapsed(elapsed: Duration) -> String {
    format!("{:.1} ms", elapsed.as_micros() as f64 / 1_000.0)
}

fn validate_wire_size(wire: &[u8]) -> Result<()> {
    let size = wire.len();
    if size > MAX_WIRE_TRANSACTION_BYTES {
        return Err(SniperError::Land(format!(
            "交易 wire size {size} 超过上限 {MAX_WIRE_TRANSACTION_BYTES}"
        )));
    }
    Ok(())
}

pub fn endpoint_label(url: &str) -> String {
    url.split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(url)
        .split('/')
        .next()
        .and_then(|authority| authority.rsplit('@').next())
        .and_then(|authority| authority.split('?').next())
        .filter(|value| !value.is_empty())
        .unwrap_or("rpc")
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{LandingProvider, LandingRouteCfg, LandingTransport};
    use crate::exec::sender::SubmissionStatus;
    use rand::{rngs::StdRng, SeedableRng};
    use solana_sdk::instruction::Instruction;
    use solana_sdk::message::Message;
    use solana_sdk::pubkey::Pubkey;
    use std::collections::HashSet;
    use std::sync::Mutex as StdMutex;

    struct MockSender {
        name: &'static str,
        status: SubmissionStatus,
        delay: Duration,
    }

    #[async_trait::async_trait]
    impl TransactionSender for MockSender {
        fn name(&self) -> &str {
            self.name
        }

        async fn submit(
            &self,
            _transaction: Arc<Transaction>,
            _wire: Bytes,
            _base64_wire: Bytes,
        ) -> anyhow::Result<SubmissionStatus> {
            tokio::time::sleep(self.delay).await;
            Ok(self.status)
        }
    }

    struct RecordingSender {
        name: &'static str,
        hits: Arc<StdMutex<Vec<&'static str>>>,
    }

    #[async_trait::async_trait]
    impl TransactionSender for RecordingSender {
        fn name(&self) -> &str {
            self.name
        }

        async fn submit(
            &self,
            _transaction: Arc<Transaction>,
            _wire: Bytes,
            _base64_wire: Bytes,
        ) -> anyhow::Result<SubmissionStatus> {
            self.hits.lock().unwrap().push(self.name);
            Ok(SubmissionStatus::Dispatched)
        }
    }

    fn test_lander(senders: Vec<Arc<dyn TransactionSender>>) -> Lander {
        Lander {
            read_rpc: Arc::new(crate::rpc::blocking(
                "http://127.0.0.1:8899",
                Duration::from_secs(8),
                CommitmentConfig::confirmed(),
            )),
            senders,
            tip_pools: Vec::new(),
        }
    }

    fn signed_test_transaction() -> Transaction {
        Transaction {
            signatures: vec![Signature::new_unique()],
            message: Message::default(),
        }
    }

    fn no_tip() -> TipPlan {
        TipPlan {
            instructions: Vec::new(),
            label: "none".to_owned(),
            lamports: 0,
        }
    }

    fn named_tip(label: &str) -> TipPlan {
        TipPlan {
            instructions: vec![Instruction {
                program_id: solana_sdk::system_program::ID,
                accounts: Vec::new(),
                data: Vec::new(),
            }],
            label: label.to_owned(),
            lamports: 1_000_000,
        }
    }

    #[test]
    fn rejects_transaction_larger_than_wire_packet_limit() {
        let instruction = Instruction {
            program_id: Pubkey::new_unique(),
            accounts: vec![],
            data: vec![0; 1400],
        };
        let tx = Transaction::new_unsigned(Message::new(&[instruction], None));

        let wire = bincode::serialize(&tx).unwrap();
        assert!(validate_wire_size(&wire).is_err());
    }

    #[test]
    fn accepts_small_transaction() {
        let instruction = Instruction {
            program_id: Pubkey::new_unique(),
            accounts: vec![],
            data: vec![1, 2, 3],
        };
        let tx = Transaction::new_unsigned(Message::new(&[instruction], None));

        let wire = bincode::serialize(&tx).unwrap();
        assert!(validate_wire_size(&wire).is_ok());
    }

    #[test]
    fn endpoint_label_redacts_credentials_and_path() {
        assert_eq!(
            endpoint_label("https://user:secret@rpc.example.test/path?token=hidden"),
            "rpc.example.test"
        );
    }

    #[test]
    fn tip_account_is_selected_from_the_whole_pool() {
        let accounts = (0..10).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
        let mut rng = StdRng::seed_from_u64(7);
        let selected = (0..100)
            .map(|_| *choose_tip_account(&accounts, &mut rng).unwrap())
            .collect::<HashSet<_>>();
        assert!(selected.len() > 1);
        assert!(selected.iter().all(|account| accounts.contains(account)));
    }

    #[test]
    fn same_provider_pool_across_regions_adds_only_one_tip() {
        let route = |name: &str, amount| LandingRouteCfg {
            name: name.into(),
            provider: LandingProvider::Landx,
            endpoint: "ams1.landx.dev".into(),
            api_key: "123456789012".into(),
            tip_accounts: Vec::new(),
            tip_lamports: amount,
            mev_protect: false,
            transport: LandingTransport::Udp,
        };
        let pools = build_tip_pools(&[route("ams", 1_000_000), route("fra", 1_200_000)]);
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].accounts.len(), 10);
        assert_eq!(pools[0].lamports, 1_200_000);
        assert_eq!(pools[0].label, "ams|fra");
    }

    #[test]
    fn multi_provider_routes_prepare_one_tip_instruction_per_unique_pool() {
        let route = |name: &str, provider| LandingRouteCfg {
            name: name.into(),
            provider,
            endpoint: "https://example.test".into(),
            api_key: "key".into(),
            tip_accounts: vec![Pubkey::new_unique().to_string()],
            tip_lamports: 1_000_000,
            mev_protect: false,
            transport: LandingTransport::Http,
        };
        let lander = Lander {
            read_rpc: Arc::new(crate::rpc::blocking(
                "http://127.0.0.1:8899",
                Duration::from_secs(8),
                CommitmentConfig::confirmed(),
            )),
            senders: Vec::new(),
            tip_pools: build_tip_pools(&[
                route("temporal", LandingProvider::Temporal),
                route("astralane", LandingProvider::Astralane),
            ]),
        };

        let tip = lander.tip_instructions(&Pubkey::new_unique());
        assert_eq!(tip.instructions.len(), 2);
        assert_eq!(tip.lamports, 2_000_000);
        assert_eq!(tip.label, "astralane|temporal");
    }

    #[tokio::test]
    async fn acknowledged_channel_wins_over_faster_udp_dispatch() {
        let lander = test_lander(vec![
            Arc::new(MockSender {
                name: "udp",
                status: SubmissionStatus::Dispatched,
                delay: Duration::ZERO,
            }),
            Arc::new(MockSender {
                name: "http",
                status: SubmissionStatus::Accepted,
                delay: Duration::from_millis(5),
            }),
        ]);
        let receipt = lander
            .send(&signed_test_transaction(), &no_tip())
            .await
            .unwrap();
        assert_eq!(receipt.channel, "http");
    }

    #[tokio::test]
    async fn tipped_transaction_is_submitted_only_to_matching_routes() {
        let lander = test_lander(vec![
            Arc::new(MockSender {
                name: "temporal",
                status: SubmissionStatus::Accepted,
                delay: Duration::ZERO,
            }),
            Arc::new(MockSender {
                name: "landx",
                status: SubmissionStatus::Accepted,
                delay: Duration::ZERO,
            }),
        ]);
        let receipt = lander
            .send(&signed_test_transaction(), &named_tip("landx"))
            .await
            .unwrap();
        assert_eq!(receipt.channel, "landx");
    }

    #[tokio::test]
    async fn multi_route_tip_is_submitted_to_each_matching_route() {
        let hits = Arc::new(StdMutex::new(Vec::new()));
        let lander = test_lander(vec![
            Arc::new(RecordingSender {
                name: "temporal",
                hits: hits.clone(),
            }),
            Arc::new(RecordingSender {
                name: "landx",
                hits: hits.clone(),
            }),
            Arc::new(RecordingSender {
                name: "astralane",
                hits: hits.clone(),
            }),
        ]);

        lander
            .send(&signed_test_transaction(), &named_tip("landx|temporal"))
            .await
            .unwrap();

        let submitted = hits.lock().unwrap().iter().copied().collect::<HashSet<_>>();
        assert_eq!(submitted, HashSet::from(["landx", "temporal"]));
    }

    #[tokio::test]
    async fn udp_only_receipt_is_explicitly_dispatched() {
        let lander = test_lander(vec![Arc::new(MockSender {
            name: "udp",
            status: SubmissionStatus::Dispatched,
            delay: Duration::ZERO,
        })]);
        let receipt = lander
            .send(&signed_test_transaction(), &no_tip())
            .await
            .unwrap();
        assert_eq!(receipt.channel, "udp/udp-dispatched");
    }

    #[tokio::test]
    async fn slow_ack_does_not_hold_udp_dispatch_beyond_grace_window() {
        let lander = test_lander(vec![
            Arc::new(MockSender {
                name: "udp",
                status: SubmissionStatus::Dispatched,
                delay: Duration::ZERO,
            }),
            Arc::new(MockSender {
                name: "slow-http",
                status: SubmissionStatus::Accepted,
                delay: Duration::from_millis(100),
            }),
        ]);
        let receipt = lander
            .send(&signed_test_transaction(), &no_tip())
            .await
            .unwrap();
        assert_eq!(receipt.channel, "udp/udp-dispatched");
    }
}
