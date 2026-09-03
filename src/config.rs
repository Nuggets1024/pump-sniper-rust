//! 配置加载与校验。

use crate::error::{Result, SniperError};
use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub bot: BotCfg,
    #[serde(default)]
    pub admin: AdminCfg,
    pub log: LogCfg,
    pub rpc: RpcCfg,
    pub geyser: GeyserCfg,
    pub wallet: WalletCfg,
    pub landing: LandingCfg,
    pub buy: BuyCfg,
    pub sell: SellCfg,
    #[serde(default)]
    pub follow: Vec<FollowCfg>,
    #[serde(default)]
    pub blacklist: BlacklistCfg,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminCfg {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_admin_bind")]
    pub bind: String,
    #[serde(default)]
    pub auth_token: String,
}

impl Default for AdminCfg {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: default_admin_bind(),
            auth_token: String::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BotCfg {
    #[serde(default)]
    pub mode: BotMode,
    #[serde(default)]
    pub trade: bool,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BotMode {
    #[default]
    Standard,
    /// 一次性测试狙击：锁定首个可买 create，之后只处理该 mint。
    Scan,
}

impl BotMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Scan => "scan",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogCfg {
    #[serde(default = "default_filter")]
    pub filter: String,
    #[serde(default = "default_log_dir")]
    pub directory: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcCfg {
    pub url: String,
    #[serde(default)]
    pub commitment: CommitmentCfg,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeyserCfg {
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub x_token: String,
    #[serde(default)]
    pub sources: Vec<GeyserSourceCfg>,
    #[serde(default)]
    pub commitment: CommitmentCfg,
    #[serde(default = "default_reconnect")]
    pub reconnect_ms: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GeyserSourceCfg {
    /// 稳定来源编号（0..62），避免重排后 cursor 错配。
    pub id: u8,
    pub endpoint: String,
    #[serde(default)]
    pub x_token: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalletCfg {
    pub keypair_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LandingCfg {
    pub routes: Vec<LandingRouteCfg>,
    #[serde(default = "default_max_inflight_orders")]
    pub max_inflight_orders: usize,
    #[serde(default = "default_confirmation_timeout_ms")]
    pub confirmation_timeout_ms: u64,
    /// LandX 官方支持明文 HTTP，但 API key 位于 path；生产必须显式承担风险。
    #[serde(default)]
    pub allow_insecure_http: bool,
    #[serde(default = "default_true")]
    pub skip_preflight: bool,
    #[serde(default = "default_cu_limit")]
    pub cu_limit: u32,
    #[serde(default = "default_cu_price")]
    pub cu_price_micro_lamports: u64,
    #[serde(default = "default_true")]
    pub jito_dont_front: bool,
    #[serde(default = "default_true")]
    pub lighthouse_slot_guard: bool,
    #[serde(default = "default_slot_slack")]
    pub lighthouse_slot_slack: u64,
    /// 单笔交易允许附加的所有供应商 tip 总上限。
    #[serde(default)]
    pub max_total_tip_lamports: u64,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CommitmentCfg {
    #[default]
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum LandingProvider {
    Rpc,
    ZeroSlot,
    Temporal,
    Astralane,
    Landx,
}

impl LandingProvider {
    fn min_tip_lamports(self) -> u64 {
        match self {
            Self::Rpc => 0,
            Self::ZeroSlot => 100_000,
            Self::Temporal | Self::Astralane | Self::Landx => 1_000_000,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LandingTransport {
    #[default]
    Http,
    Udp,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LandingRouteCfg {
    pub name: String,
    pub provider: LandingProvider,
    pub endpoint: String,
    #[serde(default)]
    pub api_key: String,
    /// 自定义/private tip 钱包池；空时使用供应商完整公开池。
    #[serde(default)]
    pub tip_accounts: Vec<String>,
    #[serde(default)]
    pub tip_lamports: u64,
    #[serde(default)]
    pub mev_protect: bool,
    #[serde(default)]
    pub transport: LandingTransport,
}

impl LandingRouteCfg {
    pub fn tip_account_pool(&self) -> Vec<&str> {
        if !self.tip_accounts.is_empty() {
            return self.tip_accounts.iter().map(String::as_str).collect();
        }
        crate::tip_accounts::for_provider(self.provider).to_vec()
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuyCfg {
    pub sol_amount: f64,
    /// 新配置使用整数 bps；7000 表示最多容忍 70% 滑点。
    #[serde(default = "default_max_slippage_bps")]
    pub max_slippage_bps: u16,
    #[serde(default = "default_true")]
    pub use_seed_token_account: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SellCfg {
    #[serde(default = "default_true")]
    pub follow_creator_sell: bool,
    #[serde(default = "default_hold")]
    pub max_hold_ms: u64,
    #[serde(default = "default_one")]
    pub min_sol_out_lamports: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowCfg {
    pub address: String,
    pub min_sol: f64,
    pub max_sol: f64,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlacklistCfg {
    #[serde(default)]
    pub mints: Vec<String>,
    #[serde(default)]
    pub creators: Vec<String>,
}

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .map_err(|e| SniperError::Config(format!("读取 {}: {e}", path.display())))?;
        Self::parse_str(&raw, path.display().to_string())
    }

    pub fn parse_str(raw: &str, label: impl Into<String>) -> Result<Self> {
        let label = label.into();
        let cfg: Self =
            toml::from_str(&raw).map_err(|e| SniperError::Config(format!("解析 {label}: {e}")))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        self.admin
            .bind
            .parse::<std::net::SocketAddr>()
            .map_err(|e| SniperError::Config(format!("admin.bind 无效: {e}")))?;
        let single_endpoint = self.geyser.endpoint.as_deref().unwrap_or("").trim();
        if self.geyser.sources.is_empty() && single_endpoint.is_empty() {
            return Err(SniperError::Config(
                "geyser.endpoint 或 geyser.sources 不能为空".into(),
            ));
        }
        if !single_endpoint.is_empty() && !self.geyser.sources.is_empty() {
            return Err(SniperError::Config(
                "geyser.endpoint 与 geyser.sources 只能配置一个".into(),
            ));
        }
        if self.bot.mode == BotMode::Scan && !self.bot.trade {
            return Err(SniperError::Config(
                "bot.mode=scan 必须开启 bot.trade".into(),
            ));
        }
        if self.bot.mode == BotMode::Scan && !self.follow.is_empty() {
            return Err(SniperError::Config(
                "bot.mode=scan 不能配置 follow；scan 会锁定首个可买 create".into(),
            ));
        }
        let mut source_ids = HashSet::new();
        let mut source_endpoints = HashSet::new();
        for source in self.geyser_sources() {
            let id = source.id;
            if id >= 63 {
                return Err(SniperError::Config(
                    "geyser.sources[].id 必须在 0..62，63 保留给RPC回补".into(),
                ));
            }
            if !source_ids.insert(id) {
                return Err(SniperError::Config(format!(
                    "geyser.sources[].id 与有效 source id 重复: {id}"
                )));
            }
            let endpoint = source.endpoint.trim();
            if endpoint.is_empty() {
                return Err(SniperError::Config(format!(
                    "geyser source {id} endpoint 为空"
                )));
            }
            if !source_endpoints.insert(endpoint.to_owned()) {
                return Err(SniperError::Config(format!(
                    "geyser.sources[].endpoint 重复: {endpoint}"
                )));
            }
        }
        // 关闭交易时只依赖日志与 Geyser；钱包、策略和落地配置不参与启动。
        if !self.bot.trade {
            return Ok(());
        }
        if self.landing.routes.is_empty() {
            return Err(SniperError::Config("landing.routes 为空".into()));
        }
        let mut route_names = HashSet::new();
        let routes = self.landing.routes.clone();
        let mut tip_by_pool = HashMap::<Vec<Pubkey>, u64>::new();
        for route in routes {
            if route.name.trim().is_empty() {
                return Err(SniperError::Config("landing.routes[].name 不能为空".into()));
            }
            if !route_names.insert(route.name.clone()) {
                return Err(SniperError::Config(format!(
                    "landing.routes[].name 重复: {}",
                    route.name
                )));
            }
            if route.endpoint.trim().is_empty() {
                return Err(SniperError::Config(format!(
                    "landing route {} endpoint 为空",
                    route.name
                )));
            }
            if route.provider != LandingProvider::Rpc && route.api_key.trim().is_empty() {
                return Err(SniperError::Config(format!(
                    "landing route {} 缺少 api_key",
                    route.name
                )));
            }
            if route.provider == LandingProvider::Landx && route.api_key.len() != 12 {
                return Err(SniperError::Config(format!(
                    "landing route {} 的 LandX api_key 必须正好 12 字节",
                    route.name
                )));
            }
            if route.provider != LandingProvider::Landx && route.transport != LandingTransport::Http
            {
                return Err(SniperError::Config(format!(
                    "landing route {} 只有 LandX 支持 UDP transport",
                    route.name
                )));
            }
            let min_tip = route.provider.min_tip_lamports();
            if route.tip_lamports < min_tip {
                return Err(SniperError::Config(format!(
                    "landing route {} tip_lamports={} 低于供应商最低值 {}",
                    route.name, route.tip_lamports, min_tip
                )));
            }
            let endpoint = route.endpoint.trim();
            let secure_http = endpoint.starts_with("https://");
            let landx_http = route.provider == LandingProvider::Landx
                && route.transport == LandingTransport::Http
                && (secure_http
                    || (self.landing.allow_insecure_http && endpoint.starts_with("http://")));
            let landx_udp = route.provider == LandingProvider::Landx
                && route.transport == LandingTransport::Udp
                && {
                    let target = endpoint.strip_prefix("udp://").unwrap_or(endpoint);
                    !target.is_empty()
                        && !target.contains('/')
                        && !target.chars().any(char::is_whitespace)
                };
            if route.provider != LandingProvider::Rpc && !secure_http && !landx_http && !landx_udp {
                return Err(SniperError::Config(format!(
                    "landing route {} endpoint 与 transport 不匹配",
                    route.name
                )));
            }
            if route.tip_lamports > 0 {
                let mut pool = route
                    .tip_account_pool()
                    .into_iter()
                    .map(|account| {
                        Pubkey::from_str(account).map_err(|_| {
                            SniperError::Config(format!(
                                "landing route {} tip account 无效: {}",
                                route.name, account
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                if pool.is_empty() {
                    return Err(SniperError::Config(format!(
                        "landing route {} 没有可用 tip account",
                        route.name
                    )));
                }
                pool.sort_unstable();
                pool.dedup();
                tip_by_pool
                    .entry(pool)
                    .and_modify(|amount| *amount = (*amount).max(route.tip_lamports))
                    .or_insert(route.tip_lamports);
            }
        }
        let total_tip_lamports = tip_by_pool.values().try_fold(0u64, |total, tip| {
            total
                .checked_add(*tip)
                .ok_or_else(|| SniperError::Config("landing tip 总额溢出".into()))
        })?;
        if total_tip_lamports > self.landing.max_total_tip_lamports {
            return Err(SniperError::Config(format!(
                "landing routes tip 总额 {total_tip_lamports} 超过 max_total_tip_lamports {}",
                self.landing.max_total_tip_lamports
            )));
        }
        if self.landing.routes.len() > 1 && !self.buy.use_seed_token_account {
            return Err(SniperError::Config(
                "多 landing route 会生成不同 tip 交易 variant；必须开启 buy.use_seed_token_account 防止重复买入"
                    .into(),
            ));
        }
        if self.buy.sol_amount <= 0.0 {
            return Err(SniperError::Config("buy.sol_amount 必须大于 0".into()));
        }
        if !self.buy.sol_amount.is_finite() {
            return Err(SniperError::Config("buy.sol_amount 必须是有限数值".into()));
        }
        if self.buy.max_slippage_bps >= 10_000 {
            return Err(SniperError::Config(
                "buy.max_slippage_bps 必须小于 10000".into(),
            ));
        }
        if self.landing.cu_limit == 0 || self.landing.cu_limit > 1_400_000 {
            return Err(SniperError::Config(
                "landing.cu_limit 必须在 1..=1400000 范围内".into(),
            ));
        }
        if self.landing.max_inflight_orders == 0 {
            return Err(SniperError::Config(
                "landing.max_inflight_orders 必须大于 0".into(),
            ));
        }
        if self.landing.confirmation_timeout_ms == 0 {
            return Err(SniperError::Config(
                "landing.confirmation_timeout_ms 必须大于 0".into(),
            ));
        }
        require_private_keypair(&self.wallet.keypair_path)?;
        for f in &self.follow {
            parse_pk(&f.address)?;
            if !f.min_sol.is_finite()
                || !f.max_sol.is_finite()
                || f.min_sol < 0.0
                || f.max_sol < f.min_sol
            {
                return Err(SniperError::Config(format!(
                    "follow {} 的金额范围无效",
                    f.address
                )));
            }
        }
        for mint in &self.blacklist.mints {
            parse_pk(mint)?;
        }
        for creator in &self.blacklist.creators {
            parse_pk(creator)?;
        }
        Ok(())
    }

    pub fn follow_index(&self) -> Vec<(Pubkey, f64, f64)> {
        self.follow
            .iter()
            .filter_map(|f| {
                Pubkey::from_str(&f.address)
                    .ok()
                    .map(|pk| (pk, f.min_sol, f.max_sol))
            })
            .collect()
    }

    pub fn black_mints(&self) -> HashSet<Pubkey> {
        self.blacklist
            .mints
            .iter()
            .filter_map(|s| Pubkey::from_str(s).ok())
            .collect()
    }

    pub fn black_creators(&self) -> HashSet<Pubkey> {
        self.blacklist
            .creators
            .iter()
            .filter_map(|s| Pubkey::from_str(s).ok())
            .collect()
    }

    pub fn buy_lamports(&self) -> u64 {
        (self.buy.sol_amount * 1_000_000_000.0).round() as u64
    }

    pub fn geyser_sources(&self) -> Vec<GeyserSourceCfg> {
        if let Some(endpoint) = self.geyser.endpoint.as_ref() {
            return vec![GeyserSourceCfg {
                id: 0,
                endpoint: endpoint.trim().to_owned(),
                x_token: self.geyser.x_token.clone(),
            }];
        }
        self.geyser
            .sources
            .iter()
            .map(|source| GeyserSourceCfg {
                id: source.id,
                endpoint: source.endpoint.trim().to_owned(),
                x_token: source.x_token.clone(),
            })
            .collect()
    }

    pub fn buy_max_slippage_bps(&self) -> u16 {
        self.buy.max_slippage_bps
    }

    pub fn landing_routes(&self) -> Vec<LandingRouteCfg> {
        self.landing.routes.clone()
    }
}

fn require_private_keypair(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let mode = fs::metadata(path)
        .map_err(|e| SniperError::Wallet(e.to_string()))?
        .permissions()
        .mode();
    if mode & 0o077 != 0 {
        return Err(SniperError::Wallet(format!(
            "{} 权限过宽（模式 {:o}）；请执行 chmod 600 后重试",
            path.display(),
            mode & 0o777
        )));
    }
    Ok(())
}

fn parse_pk(s: &str) -> Result<Pubkey> {
    Pubkey::from_str(s).map_err(|_| SniperError::Config(format!("无效公钥 {s}")))
}

fn default_true() -> bool {
    true
}
fn default_admin_bind() -> String {
    "127.0.0.1:8787".into()
}
fn default_filter() -> String {
    "pump_sniper=info".into()
}
fn default_log_dir() -> String {
    "logs".into()
}
fn default_reconnect() -> u64 {
    500
}
fn default_cu_limit() -> u32 {
    95_000
}
fn default_cu_price() -> u64 {
    11_578_947
}
fn default_slot_slack() -> u64 {
    12
}
fn default_max_inflight_orders() -> usize {
    8
}
fn default_confirmation_timeout_ms() -> u64 {
    5_000
}
fn default_max_slippage_bps() -> u16 {
    3_000
}
fn default_hold() -> u64 {
    8_000
}
fn default_one() -> u64 {
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(extra: &str) -> AppConfig {
        let raw = format!(
            r#"
[bot]
trade = true

[log]

[rpc]
url = "https://rpc.example"

[geyser]
sources = [{{ id = 0, endpoint = "https://geyser.example" }}]

[wallet]
keypair_path = "/tmp/nonexistent-pump-sniper-test-keypair"

[landing]
routes = [{{ name = "rpc", provider = "rpc", endpoint = "https://rpc.example" }}]

[buy]
sol_amount = 0.05
max_slippage_bps = 7000

[sell]

[[follow]]
address = "11111111111111111111111111111111"
min_sol = 0.1
max_sol = 1.0

{extra}
"#
        );
        toml::from_str(&raw).unwrap()
    }

    #[test]
    fn integer_slippage_bps_is_used_directly() {
        let cfg = parse("");
        cfg.validate().unwrap();
        assert_eq!(cfg.buy_max_slippage_bps(), 7000);
    }

    #[test]
    fn unknown_legacy_fields_are_rejected() {
        let raw = r#"
dry_run = true
name = "legacy"
"#;
        assert!(toml::from_str::<BotCfg>(raw).is_err());
    }

    #[test]
    fn trading_mode_accepts_two_independent_geyser_sources() {
        let mut cfg = parse("");
        cfg.geyser.sources = vec![
            GeyserSourceCfg {
                id: 0,
                endpoint: "https://a.example".into(),
                x_token: String::new(),
            },
            GeyserSourceCfg {
                id: 1,
                endpoint: "https://b.example".into(),
                x_token: String::new(),
            },
        ];
        cfg.validate().unwrap();
        assert_eq!(cfg.geyser_sources().len(), 2);
    }

    #[test]
    fn monitoring_only_accepts_two_independent_geyser_sources() {
        let mut cfg = parse("");
        cfg.bot.trade = false;
        cfg.geyser.sources = vec![
            GeyserSourceCfg {
                id: 0,
                endpoint: "https://a.example".into(),
                x_token: String::new(),
            },
            GeyserSourceCfg {
                id: 1,
                endpoint: "https://b.example".into(),
                x_token: String::new(),
            },
        ];
        cfg.validate().unwrap();
    }

    #[test]
    fn independent_sources_keep_separate_tokens() {
        let mut cfg = parse("");
        cfg.geyser.sources = vec![
            GeyserSourceCfg {
                id: 0,
                endpoint: "https://a.example".into(),
                x_token: "token-a".into(),
            },
            GeyserSourceCfg {
                id: 1,
                endpoint: "https://b.example".into(),
                x_token: "token-b".into(),
            },
        ];
        let sources = cfg.geyser_sources();
        assert_eq!(sources[0].x_token, "token-a");
        assert_eq!(sources[1].x_token, "token-b");
    }

    #[test]
    fn independent_sources_require_stable_unique_ids() {
        let mut cfg = parse("");
        cfg.geyser.sources = vec![
            GeyserSourceCfg {
                id: 0,
                endpoint: "https://a.example".into(),
                x_token: String::new(),
            },
            GeyserSourceCfg {
                id: 0,
                endpoint: "https://b.example".into(),
                x_token: String::new(),
            },
        ];
        assert!(cfg.validate().is_err());

        cfg.geyser.sources[1].id = 1;
        cfg.validate().unwrap();
    }

    #[test]
    fn landing_routes_are_pluggable_and_require_tip_budget() {
        let mut cfg = parse("");
        cfg.landing.routes = vec![LandingRouteCfg {
            name: "temporal".into(),
            provider: LandingProvider::Temporal,
            endpoint: "https://nozomi.temporal.xyz".into(),
            api_key: "secret".into(),
            tip_accounts: vec!["TEMPaMeCRFAS9EKF53Jd6KpHxgL47uWLcpFArU1Fanq".into()],
            tip_lamports: 1_000_000,
            mev_protect: false,
            transport: LandingTransport::Http,
        }];
        assert!(cfg.validate().is_err());
        cfg.landing.max_total_tip_lamports = 1_000_000;
        cfg.validate().unwrap();
        assert_eq!(cfg.landing_routes()[0].provider, LandingProvider::Temporal);
    }

    #[test]
    fn shared_tip_account_counts_once_at_the_highest_amount() {
        let mut cfg = parse("");
        let account = "TEMPaMeCRFAS9EKF53Jd6KpHxgL47uWLcpFArU1Fanq";
        cfg.landing.routes = vec![
            LandingRouteCfg {
                name: "temporal-a".into(),
                provider: LandingProvider::Temporal,
                endpoint: "https://a.example".into(),
                api_key: "secret".into(),
                tip_accounts: vec![account.into()],
                tip_lamports: 1_000_000,
                mev_protect: false,
                transport: LandingTransport::Http,
            },
            LandingRouteCfg {
                name: "temporal-b".into(),
                provider: LandingProvider::Temporal,
                endpoint: "https://b.example".into(),
                api_key: "secret".into(),
                tip_accounts: vec![account.into()],
                tip_lamports: 1_200_000,
                mev_protect: false,
                transport: LandingTransport::Http,
            },
        ];
        cfg.landing.max_total_tip_lamports = 1_200_000;
        cfg.validate().unwrap();
    }

    #[test]
    fn multi_route_trading_requires_seed_token_account_guard() {
        let mut cfg = parse("");
        cfg.buy.use_seed_token_account = false;
        cfg.landing.routes = vec![
            LandingRouteCfg {
                name: "temporal".into(),
                provider: LandingProvider::Temporal,
                endpoint: "https://nozomi.temporal.xyz".into(),
                api_key: "secret".into(),
                tip_accounts: vec!["TEMPaMeCRFAS9EKF53Jd6KpHxgL47uWLcpFArU1Fanq".into()],
                tip_lamports: 1_000_000,
                mev_protect: false,
                transport: LandingTransport::Http,
            },
            LandingRouteCfg {
                name: "astralane".into(),
                provider: LandingProvider::Astralane,
                endpoint: "https://edge.astralane.io".into(),
                api_key: "secret".into(),
                tip_accounts: vec!["astrazznxsGUhWShqgNtAdfrzP2G83DzcWVJDxwV9bF".into()],
                tip_lamports: 1_000_000,
                mev_protect: false,
                transport: LandingTransport::Http,
            },
        ];
        cfg.landing.max_total_tip_lamports = 2_000_000;

        assert!(cfg.validate().is_err());
        cfg.buy.use_seed_token_account = true;
        cfg.validate().unwrap();
    }

    #[test]
    fn authenticated_landing_route_rejects_plain_http() {
        let mut cfg = parse("");
        cfg.landing.routes = vec![LandingRouteCfg {
            name: "0slot".into(),
            provider: LandingProvider::ZeroSlot,
            endpoint: "http://ny.0slot.trade".into(),
            api_key: "secret".into(),
            tip_accounts: Vec::new(),
            tip_lamports: 0,
            mev_protect: false,
            transport: LandingTransport::Http,
        }];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn authenticated_landing_route_requires_tip() {
        let mut cfg = parse("");
        cfg.landing.routes = vec![LandingRouteCfg {
            name: "astralane".into(),
            provider: LandingProvider::Astralane,
            endpoint: "https://edge.astralane.io".into(),
            api_key: "secret".into(),
            tip_accounts: Vec::new(),
            tip_lamports: 0,
            mev_protect: true,
            transport: LandingTransport::Http,
        }];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn monitoring_only_does_not_depend_on_wallet_or_landing_config() {
        let mut cfg = parse("");
        cfg.bot.trade = false;
        cfg.wallet.keypair_path = "/definitely/not/a/wallet".into();
        cfg.landing.routes = vec![LandingRouteCfg {
            name: String::new(),
            provider: LandingProvider::Astralane,
            endpoint: String::new(),
            api_key: String::new(),
            tip_accounts: Vec::new(),
            tip_lamports: 0,
            mev_protect: false,
            transport: LandingTransport::Http,
        }];
        cfg.validate().unwrap();
    }

    #[test]
    fn scan_mode_requires_trading_and_global_create_subscription() {
        let mut cfg = parse("");
        cfg.bot.mode = BotMode::Scan;
        assert!(cfg.validate().is_err());

        cfg.follow.clear();
        cfg.validate().unwrap();

        cfg.bot.trade = false;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn landx_http_and_udp_use_builtin_tip_pool() {
        for (endpoint, transport) in [
            ("http://ams1.landx.dev", LandingTransport::Http),
            ("ams1.landx.dev", LandingTransport::Udp),
        ] {
            let mut cfg = parse("");
            cfg.landing.routes = vec![LandingRouteCfg {
                name: "landx".into(),
                provider: LandingProvider::Landx,
                endpoint: endpoint.into(),
                api_key: "123456789012".into(),
                tip_accounts: Vec::new(),
                tip_lamports: 1_000_000,
                mev_protect: false,
                transport,
            }];
            cfg.landing.max_total_tip_lamports = 1_000_000;
            cfg.landing.allow_insecure_http = transport == LandingTransport::Http;
            cfg.validate().unwrap();
            assert_eq!(cfg.landing_routes()[0].tip_account_pool().len(), 10);
        }
    }

    #[test]
    fn landx_rejects_non_12_byte_api_key() {
        let mut cfg = parse("");
        cfg.landing.routes = vec![LandingRouteCfg {
            name: "landx".into(),
            provider: LandingProvider::Landx,
            endpoint: "ams1.landx.dev".into(),
            api_key: "too-short".into(),
            tip_accounts: Vec::new(),
            tip_lamports: 1_000_000,
            mev_protect: false,
            transport: LandingTransport::Udp,
        }];
        cfg.landing.max_total_tip_lamports = 1_000_000;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn landx_plain_http_requires_explicit_opt_in() {
        let mut cfg = parse("");
        cfg.landing.routes = vec![LandingRouteCfg {
            name: "landx".into(),
            provider: LandingProvider::Landx,
            endpoint: "http://ams1.landx.dev".into(),
            api_key: "123456789012".into(),
            tip_accounts: Vec::new(),
            tip_lamports: 1_000_000,
            mev_protect: false,
            transport: LandingTransport::Http,
        }];
        cfg.landing.max_total_tip_lamports = 1_000_000;
        assert!(cfg.validate().is_err());
        cfg.landing.allow_insecure_http = true;
        cfg.validate().unwrap();
    }

    #[test]
    fn example_config_stays_parseable() {
        toml::from_str::<AppConfig>(include_str!("../config.example.toml")).unwrap();
    }
}
