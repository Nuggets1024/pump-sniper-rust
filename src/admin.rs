//! Built-in local admin page and JSON/SSE API.

use crate::config::AppConfig;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use solana_account_decoder_client_types::{token::TokenAccountType, UiAccountData};
use solana_commitment_config::CommitmentConfig;
use solana_program_pack::Pack;
use solana_rpc_client_api::request::TokenAccountsFilter;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use spl_token::state::{Account as TokenAccountState, Mint as MintState};
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_stream::wrappers::BroadcastStream;

const EVENT_BUFFER: usize = 1024;
const RECENT_LOGS: usize = 300;

static ADMIN: OnceLock<AdminApp> = OnceLock::new();

#[derive(Clone)]
pub struct AdminApp {
    inner: Arc<AdminInner>,
}

struct AdminInner {
    status: RwLock<AdminStatus>,
    tokens: RwLock<TokenStore>,
    events: broadcast::Sender<AdminEvent>,
    commands: mpsc::Sender<BotCommand>,
    config_path: PathBuf,
    auth_token: String,
    rpc_url: String,
}

pub struct AdminRuntime {
    pub commands: mpsc::Receiver<BotCommand>,
}

#[derive(Debug, Clone)]
pub enum BotCommand {
    Start,
    StopGracefully,
    SellAll(Vec<crate::position::Position>),
    ForceStop,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BotRunState {
    Running,
    Stopping,
    Stopped,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdminEvent {
    pub ts: String,
    pub level: String,
    pub tag: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PositionSnapshot {
    pub mint: String,
    pub state: String,
    pub token_amount: u64,
    pub age_ms: u128,
    pub buy_sig: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TargetSnapshot {
    pub mint: String,
    pub create_slot: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TokenSummary {
    pub mint: String,
    pub create_time: Option<String>,
    pub create_slot: Option<u64>,
    pub sniper_slot: Option<u64>,
    pub interval_slot: Option<u64>,
    pub buy_price: Option<f64>,
    pub sell_price: Option<f64>,
    pub dev_hash: Option<String>,
    pub sniper_hash: Option<String>,
    pub candidate_hashes: Vec<String>,
    pub execution_channel: Option<String>,
    pub remark: String,
    pub status: String,
    #[serde(skip)]
    pub pending_buy_sigs: HashSet<String>,
    #[serde(skip)]
    pub pending_sell_sigs: HashSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenLogRow {
    pub ts: String,
    pub kind: String,
    pub slot: Option<u64>,
    pub signature: Option<String>,
    pub message: String,
    pub data: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HoldingsRequest {
    pub rpc_url: Option<String>,
    pub owner: Option<String>,
    #[serde(default)]
    pub mints: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HoldingSnapshot {
    pub mint: String,
    pub token_program: String,
    pub token_account: String,
    pub amount: String,
    pub decimals: u8,
    pub ui_amount: Option<f64>,
    pub ui_amount_string: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SellAllRequest {
    pub holdings: Vec<SellHoldingRequest>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SellHoldingRequest {
    pub mint: String,
    pub token_program: String,
    pub token_account: String,
    pub amount: String,
}

#[derive(Default)]
struct TokenStore {
    order: VecDeque<String>,
    summaries: HashMap<String, TokenSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdminStatus {
    pub bot: BotRunState,
    pub wallet: Option<String>,
    pub balance_lamports: Option<u64>,
    pub latest_slot: Option<u64>,
    pub geyser_endpoint: String,
    pub geyser_connections: usize,
    pub shred_enabled: bool,
    pub shred_endpoint: String,
    pub feed: crate::listen::FeedStats,
    pub active_targets: Vec<TargetSnapshot>,
    pub positions: Vec<PositionSnapshot>,
    pub recent_logs: VecDeque<AdminEvent>,
}

impl AdminStatus {
    fn new(cfg: &AppConfig) -> Self {
        let geyser_endpoint = cfg
            .geyser_sources()
            .first()
            .map(|source| source.endpoint.clone())
            .unwrap_or_default();
        Self {
            bot: BotRunState::Stopped,
            wallet: None,
            balance_lamports: None,
            latest_slot: None,
            geyser_endpoint,
            geyser_connections: 2,
            shred_enabled: cfg.shred.enabled,
            shred_endpoint: cfg.shred.endpoint.clone(),
            feed: crate::listen::FeedStats::default(),
            active_targets: Vec::new(),
            positions: Vec::new(),
            recent_logs: VecDeque::with_capacity(RECENT_LOGS),
        }
    }
}

pub fn init(cfg: &AppConfig, config_path: impl Into<PathBuf>) -> AdminRuntime {
    let (events, _) = broadcast::channel(EVENT_BUFFER);
    let (commands_tx, commands_rx) = mpsc::channel(32);
    let app = AdminApp {
        inner: Arc::new(AdminInner {
            status: RwLock::new(AdminStatus::new(cfg)),
            tokens: RwLock::new(TokenStore::default()),
            events,
            commands: commands_tx,
            config_path: config_path.into(),
            auth_token: cfg.admin.auth_token.clone(),
            rpc_url: cfg.rpc.url.clone(),
        }),
    };
    let _ = ADMIN.set(app);
    AdminRuntime {
        commands: commands_rx,
    }
}

pub fn app() -> Option<AdminApp> {
    ADMIN.get().cloned()
}

pub fn spawn_server(bind: SocketAddr) {
    let Some(app) = app() else {
        return;
    };
    tokio::spawn(async move {
        let router = Router::new()
            .route("/api/status", get(status))
            .route("/api/events", get(events))
            .route("/api/config", get(config_get).post(config_save))
            .route("/api/config/reload", post(config_reload))
            .route("/api/bot/start", post(bot_start))
            .route("/api/bot/stop", post(bot_stop))
            .route("/api/holdings/sell-all", post(holdings_sell_all))
            .route("/api/bot/force-stop", post(bot_force_stop))
            .route("/api/tokens", get(tokens))
            .route("/api/tokens/:mint/logs", get(token_logs))
            .route("/api/holdings", post(holdings))
            .with_state(app.clone());

        match tokio::net::TcpListener::bind(bind).await {
            Ok(listener) => {
                crate::telemetry::info("管理页", format!("http://{bind}"));
                if let Err(error) = axum::serve(listener, router).await {
                    crate::telemetry::error("管理页", error.to_string());
                }
            }
            Err(error) => crate::telemetry::error("管理页", format!("绑定失败 {bind}: {error}")),
        }
    });
}

pub fn emit_log(level: impl Into<String>, tag: impl Into<String>, message: impl Into<String>) {
    let Some(app) = app() else {
        return;
    };
    let tag = tag.into();
    if !show_in_admin_log(&tag) {
        return;
    }
    let event = AdminEvent {
        ts: timestamp(),
        level: level.into(),
        tag,
        message: message.into(),
    };
    let app_for_task = app.clone();
    let event_for_task = event.clone();
    tokio::spawn(async move {
        let mut status = app_for_task.inner.status.write().await;
        if status.recent_logs.len() >= RECENT_LOGS {
            status.recent_logs.pop_front();
        }
        status.recent_logs.push_back(event_for_task);
    });
    let _ = app.inner.events.send(event);
}

fn show_in_admin_log(tag: &str) -> bool {
    !matches!(
        tag,
        "订阅" | "监听" | "创建" | "买入" | "卖出" | "市场" | "过期CREATE"
    )
}

pub fn set_wallet(wallet: impl Into<String>, balance_lamports: Option<u64>) {
    if let Some(app) = app() {
        let wallet = wallet.into();
        tokio::spawn(async move {
            let mut status = app.inner.status.write().await;
            status.wallet = Some(wallet);
            status.balance_lamports = balance_lamports;
        });
    }
}

pub fn set_latest_slot(slot: u64) {
    if let Some(app) = app() {
        tokio::spawn(async move {
            let mut status = app.inner.status.write().await;
            if status.latest_slot.is_none_or(|latest| slot > latest) {
                status.latest_slot = Some(slot);
            }
        });
    }
}

pub fn set_bot_state(bot: BotRunState) {
    if let Some(app) = app() {
        tokio::spawn(async move {
            app.inner.status.write().await.bot = bot;
        });
    }
}

pub fn set_positions(positions: Vec<PositionSnapshot>) {
    if let Some(app) = app() {
        tokio::spawn(async move {
            app.inner.status.write().await.positions = positions;
        });
    }
}

pub fn set_targets(targets: Vec<TargetSnapshot>) {
    if let Some(app) = app() {
        tokio::spawn(async move {
            app.inner.status.write().await.active_targets = targets;
        });
    }
}

pub fn record_token_event(
    mint: impl Into<String>,
    _creator: impl Into<String>,
    _name: Option<String>,
    _symbol: Option<String>,
    create_slot: Option<u64>,
    slot: Option<u64>,
    signature: Option<String>,
    kind: impl Into<String>,
    _message: impl Into<String>,
    data: Value,
) {
    let Some(app) = app() else {
        return;
    };
    let mint = mint.into();
    let kind = kind.into();
    tokio::spawn(async move {
        let mut buy_slot_lookup = None;
        let mut store = app.inner.tokens.write().await;
        if !store.summaries.contains_key(&mint) {
            store.order.push_front(mint.clone());
        }
        let summary = store.summaries.entry(mint.clone()).or_insert(TokenSummary {
            mint: mint.clone(),
            create_time: None,
            create_slot,
            sniper_slot: None,
            interval_slot: None,
            buy_price: None,
            sell_price: None,
            dev_hash: None,
            sniper_hash: None,
            candidate_hashes: Vec::new(),
            execution_channel: None,
            remark: String::new(),
            status: "处理中".into(),
            pending_buy_sigs: HashSet::new(),
            pending_sell_sigs: HashSet::new(),
        });
        if create_slot.is_some() {
            summary.create_slot = create_slot;
        }
        match kind.as_str() {
            "create" => {
                summary.create_time.get_or_insert_with(timestamp);
                summary.dev_hash = signature.clone();
            }
            "buy" => {
                let is_our = data.get("is_our").and_then(Value::as_bool).unwrap_or(false);
                let exact = data
                    .get("buy")
                    .and_then(|buy| buy.get("exact"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if is_our && exact {
                    if let Some(price) = data.get("price").and_then(Value::as_f64) {
                        summary.buy_price = Some(price);
                    }
                    if signature.as_ref().is_some_and(|sig| {
                        summary.pending_buy_sigs.remove(sig)
                            || summary.candidate_hashes.contains(sig)
                    }) {
                        summary.status = "成功".into();
                        summary.sniper_hash = signature.clone();
                        summary.sniper_slot = slot;
                        summary.interval_slot = slot_gap(summary.create_slot, slot);
                        summary.remark = format!("成交核账来源：{}", event_source_label(&data));
                    }
                }
            }
            "sell" => {
                let is_our = data.get("is_our").and_then(Value::as_bool).unwrap_or(false);
                let exact = data
                    .get("sell")
                    .and_then(|sell| sell.get("exact"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if is_our && exact {
                    if let Some(price) = data.get("price").and_then(Value::as_f64) {
                        summary.sell_price = Some(price);
                    }
                    if signature
                        .as_ref()
                        .is_some_and(|sig| summary.pending_sell_sigs.remove(sig))
                    {
                        summary.status = "成功".into();
                        summary.remark = format!("卖出成交核账来源：{}", event_source_label(&data));
                    }
                }
            }
            "submitted" => match data.get("side").and_then(Value::as_str) {
                Some("买入") => {
                    if let Some(signature) = signature.clone() {
                        summary.sniper_hash = Some(signature.clone());
                        summary.pending_buy_sigs.insert(signature);
                    }
                    let candidate_hashes = candidate_hashes(&data);
                    for candidate in &candidate_hashes {
                        summary.pending_buy_sigs.insert(candidate.clone());
                    }
                    if !candidate_hashes.is_empty() {
                        summary.candidate_hashes = candidate_hashes.clone();
                        buy_slot_lookup = Some(candidate_hashes);
                    } else if let Some(signature) = summary.sniper_hash.clone() {
                        buy_slot_lookup = Some(vec![signature]);
                    }
                    summary.execution_channel = data
                        .get("channel")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    summary.status = "处理中".into();
                }
                Some("卖出") => {
                    if let Some(signature) = signature.clone() {
                        summary.pending_sell_sigs.insert(signature);
                    }
                    for candidate in candidate_hashes(&data) {
                        summary.pending_sell_sigs.insert(candidate);
                    }
                }
                _ => {}
            },
            "note" => {
                if let Some(status) = note_status(&data) {
                    if matches!(status.as_str(), "失败" | "待确认") {
                        let has_chain_result = summary.sniper_slot.is_some()
                            && matches!(summary.status.as_str(), "链上成功" | "链上失败" | "成功");
                        if !has_chain_result {
                            if let Some(failed_slot) = slot {
                                summary.sniper_slot.get_or_insert(failed_slot);
                                summary.interval_slot =
                                    slot_gap(summary.create_slot, Some(failed_slot));
                            }
                            summary.remark = note_remark(&data);
                        }
                    }
                    if note_overrides(summary.status.as_str(), status.as_str()) {
                        summary.status = status;
                    }
                }
            }
            _ => {}
        }
        while store.order.len() > 300 {
            if let Some(old) = store.order.pop_back() {
                store.summaries.remove(&old);
            }
        }
        drop(store);
        if let Some(signatures) = buy_slot_lookup {
            spawn_signature_slot_lookup(app, mint, signatures);
        }
    });
}

fn spawn_signature_slot_lookup(app: AdminApp, mint: String, signatures: Vec<String>) {
    tokio::spawn(async move {
        let parsed_signatures = signatures
            .iter()
            .filter_map(|signature| Signature::from_str(signature).ok())
            .collect::<Vec<_>>();
        if parsed_signatures.is_empty() {
            return;
        }
        // 与其它 RPC 读路径一致：固定走 .no_proxy() 工厂，不再顺从系统代理环境变量。
        let client = crate::rpc::nonblocking(
            app.inner.rpc_url.clone(),
            std::time::Duration::from_secs(8),
            CommitmentConfig::processed(),
        );

        let mut last_chain_failure = None;
        let mut last_lookup_error = None;
        for attempt in 0..40 {
            match client
                .get_signature_statuses_with_history(&parsed_signatures)
                .await
            {
                Ok(response) => {
                    let mut any_pending = false;
                    for (index, status) in response.value.into_iter().enumerate() {
                        let Some(status) = status else {
                            any_pending = true;
                            continue;
                        };
                        let Some(signature) = signatures.get(index).cloned() else {
                            continue;
                        };
                        if let Some(error) = status.err {
                            last_chain_failure = Some((signature, status.slot, error.to_string()));
                            continue;
                        }
                        let mut store = app.inner.tokens.write().await;
                        let Some(summary) = store.summaries.get_mut(&mint) else {
                            return;
                        };
                        if !summary.pending_buy_sigs.contains(&signature)
                            && !summary.candidate_hashes.contains(&signature)
                            && summary.sniper_hash.as_deref() != Some(signature.as_str())
                        {
                            return;
                        }
                        summary.pending_buy_sigs.remove(&signature);
                        summary.sniper_hash = Some(signature);
                        summary.sniper_slot = Some(status.slot);
                        summary.interval_slot = slot_gap(summary.create_slot, Some(status.slot));
                        if summary.status != "成功" {
                            summary.status = "链上成功".into();
                            summary.remark = "RPC已确认买入上链，等待Geyser核账".into();
                        }
                        return;
                    }
                    if !any_pending {
                        break;
                    }
                }
                Err(error) => {
                    last_lookup_error = Some(error.to_string());
                    if attempt == 39 {
                        crate::telemetry::warn(
                            "管理页",
                            format!("查询交易slot重试耗尽 mint={mint}: {error}"),
                        );
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }

        if let Some((signature, slot, error)) = last_chain_failure {
            let mut store = app.inner.tokens.write().await;
            let Some(summary) = store.summaries.get_mut(&mint) else {
                return;
            };
            if summary.status != "成功" && summary.status != "链上成功" {
                summary.pending_buy_sigs.remove(&signature);
                summary.sniper_hash = Some(signature);
                summary.sniper_slot = Some(slot);
                summary.interval_slot = slot_gap(summary.create_slot, Some(slot));
                summary.status = "链上失败".into();
                summary.remark = error;
            }
        } else if let Some(error) = last_lookup_error {
            let mut store = app.inner.tokens.write().await;
            let Some(summary) = store.summaries.get_mut(&mint) else {
                return;
            };
            if !matches!(summary.status.as_str(), "成功" | "链上成功" | "链上失败") {
                summary.status = "待确认".into();
                summary.remark = format!("RPC 暂时无法确认交易状态：{error}");
            }
        }
    });
}

async fn status(State(app): State<AdminApp>, headers: HeaderMap) -> Response {
    if let Err(response) = authorize(&app, &headers, None) {
        return response;
    }
    let mut status = app.inner.status.read().await.clone();
    status.feed = crate::listen::feed_stats();
    Json(status).into_response()
}

async fn events(
    State(app): State<AdminApp>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if let Err(response) = authorize(&app, &headers, query.get("token").map(String::as_str)) {
        return response;
    }
    let stream = BroadcastStream::new(app.inner.events.subscribe()).filter_map(|result| async {
        let event = result.ok()?;
        let data = serde_json::to_string(&event).ok()?;
        Some(Ok::<_, Infallible>(Event::default().data(data)))
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::new().text("keep-alive"))
        .into_response()
}

async fn config_get(State(app): State<AdminApp>, headers: HeaderMap) -> Response {
    if let Err(response) = authorize(&app, &headers, None) {
        return response;
    }
    match tokio::fs::read_to_string(&app.inner.config_path).await {
        Ok(raw) => raw_response(StatusCode::OK, "text/plain; charset=utf-8", raw),
        Err(error) => api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

async fn config_save(State(app): State<AdminApp>, headers: HeaderMap, body: String) -> Response {
    if let Err(response) = authorize(&app, &headers, None) {
        return response;
    }
    if let Err(error) = AppConfig::parse_str(&body, "admin payload") {
        return api_error(StatusCode::BAD_REQUEST, error.to_string());
    }
    match tokio::fs::write(&app.inner.config_path, body).await {
        Ok(()) => Json(serde_json::json!({ "ok": true })).into_response(),
        Err(error) => api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

async fn config_reload(State(app): State<AdminApp>, headers: HeaderMap) -> Response {
    if let Err(response) = authorize(&app, &headers, None) {
        return response;
    }
    let raw = match tokio::fs::read_to_string(&app.inner.config_path).await {
        Ok(raw) => raw,
        Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };
    if let Err(error) = AppConfig::parse_str(&raw, app.inner.config_path.display().to_string()) {
        return api_error(StatusCode::BAD_REQUEST, error.to_string());
    }
    emit_log(
        "INFO",
        "配置",
        "配置文件已校验；Geyser/RPC/钱包/落地 route 变更需停止后重启生效",
    );
    Json(serde_json::json!({
        "ok": true,
        "message": "配置文件已校验；运行中 bot 的连接类配置需停止后重启生效"
    }))
    .into_response()
}

async fn bot_start(State(app): State<AdminApp>, headers: HeaderMap) -> Response {
    command(&app, &headers, BotCommand::Start).await
}

async fn bot_stop(State(app): State<AdminApp>, headers: HeaderMap) -> Response {
    command(&app, &headers, BotCommand::StopGracefully).await
}

async fn bot_force_stop(State(app): State<AdminApp>, headers: HeaderMap) -> Response {
    command(&app, &headers, BotCommand::ForceStop).await
}

async fn holdings_sell_all(
    State(app): State<AdminApp>,
    headers: HeaderMap,
    Json(request): Json<SellAllRequest>,
) -> Response {
    if let Err(response) = authorize(&app, &headers, None) {
        return response;
    }
    let mut candidates = Vec::new();
    for holding in request.holdings {
        let (Ok(mint), Ok(token_program), Ok(user_token), Ok(token_amount)) = (
            Pubkey::from_str(&holding.mint),
            Pubkey::from_str(&holding.token_program),
            Pubkey::from_str(&holding.token_account),
            holding.amount.parse::<u64>(),
        ) else {
            continue;
        };
        if token_amount == 0 || !matches!(token_program, spl_token::ID | spl_token_2022::ID) {
            continue;
        }
        candidates.push((
            mint,
            token_program,
            user_token,
            token_amount,
            crate::pda::bonding_curve(&mint),
        ));
    }
    if candidates.is_empty() {
        return api_error(StatusCode::BAD_REQUEST, "没有可提交的非零 Token 持仓");
    }

    let rpc = crate::rpc::nonblocking(
        app.inner.rpc_url.clone(),
        std::time::Duration::from_secs(15),
        CommitmentConfig::processed(),
    );
    let curves = candidates
        .iter()
        .map(|candidate| candidate.4)
        .collect::<Vec<_>>();
    let accounts = match rpc.get_multiple_accounts(&curves).await {
        Ok(accounts) => accounts,
        Err(error) => {
            return api_error(
                StatusCode::BAD_GATEWAY,
                format!("读取 Pump 曲线账户失败: {error}"),
            )
        }
    };
    let mut positions = Vec::new();
    for ((mint, token_program, user_token, token_amount, bonding_curve), account) in
        candidates.into_iter().zip(accounts)
    {
        let Some(account) = account else { continue };
        if account.owner != *crate::constants::PUMP_PROGRAM_ID {
            continue;
        }
        let Some(creator) = bonding_curve_creator(&account.data) else {
            continue;
        };
        positions.push(crate::position::Position {
            mint,
            bonding_curve,
            creator,
            token_program,
            user_token,
            token_amount,
            opened: std::time::Instant::now(),
            buy_sig: String::new(),
            buy_sigs: Vec::new(),
        });
    }
    if positions.is_empty() {
        return api_error(
            StatusCode::BAD_REQUEST,
            "当前非零余额中没有仍可通过 Pump bonding curve 卖出的 Token",
        );
    }
    let count = positions.len();
    match app
        .inner
        .commands
        .send(BotCommand::SellAll(positions))
        .await
    {
        Ok(()) => Json(serde_json::json!({ "ok": true, "count": count })).into_response(),
        Err(_) => api_error(StatusCode::SERVICE_UNAVAILABLE, "bot 控制通道已关闭"),
    }
}

fn bonding_curve_creator(data: &[u8]) -> Option<Pubkey> {
    const DISCRIMINATOR: [u8; 8] = [23, 183, 248, 55, 96, 216, 172, 96];
    const COMPLETE_OFFSET: usize = 8 + 5 * 8;
    const CREATOR_OFFSET: usize = 8 + 5 * 8 + 1;
    if data.get(..8)? != DISCRIMINATOR
        || data.get(COMPLETE_OFFSET).copied()? != 0
        || data.len() < CREATOR_OFFSET + 32
    {
        return None;
    }
    Some(Pubkey::new_from_array(
        data[CREATOR_OFFSET..CREATOR_OFFSET + 32].try_into().ok()?,
    ))
}

async fn tokens(State(app): State<AdminApp>, headers: HeaderMap) -> Response {
    if let Err(response) = authorize(&app, &headers, None) {
        return response;
    }
    let store = app.inner.tokens.read().await;
    let tokens = store
        .order
        .iter()
        .filter_map(|mint| store.summaries.get(mint))
        // 狙击列表只展示确实提交过买单的记录；仅监听到 CREATE、但因熔断或
        // 并发限制没有发单的 mint 不应长期显示为“处理中”。
        .filter(|summary| is_submitted_sniper(summary))
        .cloned()
        .collect::<Vec<_>>();
    Json(tokens).into_response()
}

async fn holdings(
    State(app): State<AdminApp>,
    headers: HeaderMap,
    Json(request): Json<HoldingsRequest>,
) -> Response {
    if let Err(response) = authorize(&app, &headers, None) {
        return response;
    }
    let owner = match request.owner {
        Some(owner) => owner,
        None => match app.inner.status.read().await.wallet.clone() {
            Some(wallet) => wallet,
            None => return api_error(StatusCode::BAD_REQUEST, "owner 为空，且当前钱包未知"),
        },
    };
    let owner = match Pubkey::from_str(&owner) {
        Ok(owner) => owner,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, format!("owner 无效: {error}")),
    };
    let rpc_url = request.rpc_url.unwrap_or_else(|| app.inner.rpc_url.clone());
    let rpc = crate::rpc::nonblocking(
        rpc_url,
        std::time::Duration::from_secs(30),
        CommitmentConfig::processed(),
    );

    // 指定 mints 时，沿用旧的按 ATA 余额查询逻辑
    if !request.mints.is_empty() {
        let mut snapshots = Vec::new();
        for mint in &request.mints {
            let parsed_mint = match Pubkey::from_str(mint) {
                Ok(mint) => mint,
                Err(error) => {
                    snapshots.push(HoldingSnapshot {
                        mint: mint.clone(),
                        token_program: String::new(),
                        token_account: String::new(),
                        amount: "0".into(),
                        decimals: 0,
                        ui_amount: None,
                        ui_amount_string: "0".into(),
                        error: Some(format!("mint 无效: {error}")),
                    });
                    continue;
                }
            };
            for token_program in [spl_token::ID, spl_token_2022::ID] {
                let token_account =
                    crate::pda::associated_user(&owner, &parsed_mint, &token_program);
                match rpc.get_token_account_balance(&token_account).await {
                    Ok(balance) => snapshots.push(HoldingSnapshot {
                        mint: mint.clone(),
                        token_program: token_program.to_string(),
                        token_account: token_account.to_string(),
                        amount: balance.amount,
                        decimals: balance.decimals,
                        ui_amount: balance.ui_amount,
                        ui_amount_string: balance.ui_amount_string,
                        error: None,
                    }),
                    Err(error) => snapshots.push(HoldingSnapshot {
                        mint: mint.clone(),
                        token_program: token_program.to_string(),
                        token_account: token_account.to_string(),
                        amount: "0".into(),
                        decimals: 0,
                        ui_amount: None,
                        ui_amount_string: "0".into(),
                        error: Some(error.to_string()),
                    }),
                }
            }
        }
        return Json(snapshots).into_response();
    }

    // 查询钱包真实持仓：列出全部 token 账户，仅保留非零余额
    struct RawAccount {
        mint: String,
        token_program: String,
        token_account: String,
        amount: u64,
        decimals: Option<u8>,
        ui_amount: Option<f64>,
        ui_amount_string: Option<String>,
    }
    let mut raw_accounts: Vec<RawAccount> = Vec::new();
    let mut unique_mints: HashSet<String> = HashSet::new();
    for token_program in [spl_token::ID, spl_token_2022::ID] {
        let filter = TokenAccountsFilter::ProgramId(token_program);
        let accounts = match rpc.get_token_accounts_by_owner(&owner, filter).await {
            Ok(accounts) => accounts,
            Err(_) => continue,
        };
        for keyed in accounts {
            let Some(account) = decode_token_holding(&keyed.account.data) else {
                continue;
            };
            if account.amount == 0 {
                continue;
            }
            if account.decimals.is_none() {
                unique_mints.insert(account.mint.clone());
            }
            raw_accounts.push(RawAccount {
                mint: account.mint,
                token_program: token_program.to_string(),
                token_account: keyed.pubkey.clone(),
                amount: account.amount,
                decimals: account.decimals,
                ui_amount: account.ui_amount,
                ui_amount_string: account.ui_amount_string,
            });
        }
    }

    // 批量拉取 mint 账户以读取 decimals
    let mint_pubkeys: Vec<Pubkey> = unique_mints
        .iter()
        .filter_map(|mint| Pubkey::from_str(mint).ok())
        .collect();
    let mut mint_decimals: HashMap<String, u8> = HashMap::new();
    if !mint_pubkeys.is_empty() {
        if let Ok(accounts) = rpc.get_multiple_accounts(&mint_pubkeys).await {
            for (pubkey, account) in mint_pubkeys.iter().zip(accounts.iter()) {
                let Some(account) = account else { continue };
                let data = &account.data;
                if let Ok(mint_state) = MintState::unpack(data) {
                    mint_decimals.insert(pubkey.to_string(), mint_state.decimals);
                }
            }
        }
    }

    let mut snapshots = Vec::new();
    for raw in raw_accounts {
        let decimals = raw
            .decimals
            .or_else(|| mint_decimals.get(&raw.mint).copied())
            .unwrap_or(0);
        let ui_amount = raw
            .ui_amount
            .unwrap_or_else(|| ui_amount_value(raw.amount, decimals));
        snapshots.push(HoldingSnapshot {
            mint: raw.mint,
            token_program: raw.token_program,
            token_account: raw.token_account,
            amount: raw.amount.to_string(),
            decimals,
            ui_amount: Some(ui_amount),
            ui_amount_string: raw
                .ui_amount_string
                .unwrap_or_else(|| format_ui_amount(raw.amount, decimals)),
            error: None,
        });
    }
    Json(snapshots).into_response()
}

struct DecodedTokenHolding {
    mint: String,
    amount: u64,
    decimals: Option<u8>,
    ui_amount: Option<f64>,
    ui_amount_string: Option<String>,
}

fn decode_token_holding(data: &UiAccountData) -> Option<DecodedTokenHolding> {
    if let UiAccountData::Json(parsed) = data {
        let TokenAccountType::Account(account) =
            serde_json::from_value::<TokenAccountType>(parsed.parsed.clone()).ok()?
        else {
            return None;
        };
        return Some(DecodedTokenHolding {
            mint: account.mint,
            amount: account.token_amount.amount.parse().ok()?,
            decimals: Some(account.token_amount.decimals),
            ui_amount: account.token_amount.ui_amount,
            ui_amount_string: Some(account.token_amount.ui_amount_string),
        });
    }

    let raw = data.decode()?;
    let account = TokenAccountState::unpack(&raw).ok()?;
    Some(DecodedTokenHolding {
        mint: account.mint.to_string(),
        amount: account.amount,
        decimals: None,
        ui_amount: None,
        ui_amount_string: None,
    })
}

fn ui_amount_value(amount: u64, decimals: u8) -> f64 {
    amount as f64 / 10f64.powi(decimals as i32)
}

fn format_ui_amount(amount: u64, decimals: u8) -> String {
    let mut formatted = format!(
        "{:.*}",
        decimals as usize,
        ui_amount_value(amount, decimals)
    );
    if formatted.contains('.') {
        while formatted.ends_with('0') {
            formatted.pop();
        }
        if formatted.ends_with('.') {
            formatted.pop();
        }
    }
    formatted
}

async fn command(app: &AdminApp, headers: &HeaderMap, command: BotCommand) -> Response {
    if let Err(response) = authorize(app, headers, None) {
        return response;
    }
    match app.inner.commands.send(command).await {
        Ok(()) => Json(serde_json::json!({ "ok": true })).into_response(),
        Err(_) => api_error(StatusCode::SERVICE_UNAVAILABLE, "bot 控制通道已关闭"),
    }
}

async fn token_logs(
    State(app): State<AdminApp>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    Path(mint): Path<String>,
) -> Response {
    if let Err(response) = authorize(&app, &headers, query.get("token").map(String::as_str)) {
        return response;
    }
    if !mint
        .chars()
        .all(|character| character.is_ascii_alphanumeric())
    {
        return api_error(StatusCode::BAD_REQUEST, "mint 无效");
    }
    let log_dir = app
        .inner
        .config_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("logs");
    let path = log_dir.join(format!("{mint}.log"));
    match tokio::fs::read_to_string(path).await {
        Ok(raw) => raw_response(StatusCode::OK, "text/plain; charset=utf-8", raw),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            api_error(StatusCode::NOT_FOUND, "token 原始日志不存在")
        }
        Err(error) => api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

fn authorize(
    app: &AdminApp,
    headers: &HeaderMap,
    query_token: Option<&str>,
) -> Result<(), Response> {
    if app.inner.auth_token.is_empty() {
        return Ok(());
    }
    let expected = format!("Bearer {}", app.inner.auth_token);
    let actual = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    if actual == Some(expected.as_str()) || query_token == Some(app.inner.auth_token.as_str()) {
        Ok(())
    } else {
        Err(api_error(StatusCode::UNAUTHORIZED, "unauthorized"))
    }
}

fn api_error(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(serde_json::json!({ "ok": false, "error": message.into() })),
    )
        .into_response()
}

fn raw_response(status: StatusCode, content_type: &'static str, body: String) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .expect("valid response")
}

fn slot_gap(create_slot: Option<u64>, latest_slot: Option<u64>) -> Option<u64> {
    latest_slot
        .zip(create_slot)
        .map(|(latest, create)| latest.saturating_sub(create))
}

fn is_submitted_sniper(summary: &TokenSummary) -> bool {
    summary.sniper_hash.is_some() || !summary.candidate_hashes.is_empty()
}

fn event_source_label(data: &Value) -> &'static str {
    match data
        .get("event")
        .and_then(|event| event.get("source_id"))
        .and_then(Value::as_u64)
    {
        Some(62) => "Shred",
        Some(63) => "RPC repair",
        Some(_) => "gRPC",
        None => "未知",
    }
}

fn candidate_hashes(data: &Value) -> Vec<String> {
    data.get("signatures")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn note_status(data: &Value) -> Option<String> {
    let tag = data.get("tag").and_then(Value::as_str)?;
    match tag {
        "执行买入" | "执行卖出" | "挂起卖出" => Some("处理中".into()),
        "买入拥塞" | "买入失败" | "卖出失败" => Some("失败".into()),
        "等待核账" => Some("待确认".into()),
        _ => None,
    }
}

/// 是否为已定型的交易结果（终态）。
fn is_terminal_status(status: &str) -> bool {
    matches!(status, "成功" | "链上成功" | "失败" | "链上失败")
}

/// 是否为等待/进行中的非终态。
fn is_pending_status(status: &str) -> bool {
    matches!(status, "待确认" | "处理中")
}

/// 是否为成功类终态（这类结果一旦确立，不被「失败」倒退覆盖）。
fn is_success_status(status: &str) -> bool {
    matches!(status, "成功" | "链上成功" | "链上失败")
}

/// 判断新的 note 状态是否允许覆盖当前状态（返回 true 表示允许用 incoming 覆盖）。
///
/// 规则（含原代码意图，修复了「失败被待确认倒退」的缺陷）：
/// 1. 已定型结果（成功/失败）不可被待确认/处理中倒退覆盖 —— 修复核心 bug：
///    「买入失败」后紧接着的「等待核账超时」不得把状态倒退成待确认。
/// 2. 成功类结果不被「失败」覆盖 —— 已锁定的成功/链上结果不因本地失败误判而倒退。
/// 3. 其它情况允许覆盖（进行中→终态推进、链上结果后到优先、本地失败被链上成功纠正）。
fn note_overrides(current: &str, incoming: &str) -> bool {
    if is_terminal_status(current) && is_pending_status(incoming) {
        return false;
    }
    if is_success_status(current) && incoming == "失败" {
        return false;
    }
    true
}

fn note_remark(data: &Value) -> String {
    let tag = data.get("tag").and_then(Value::as_str).unwrap_or_default();
    let body = data.get("body").and_then(Value::as_str).unwrap_or_default();
    if tag == "等待核账" {
        if let Some((_, reason)) = body.split_once("] ") {
            return reason
                .split_once("，已熔断")
                .map(|(summary, _)| summary)
                .unwrap_or(reason)
                .to_owned();
        }
    }
    body.to_owned()
}

fn timestamp() -> String {
    let offset = chrono::FixedOffset::east_opt(8 * 60 * 60).expect("valid UTC+8 offset");
    chrono::Utc::now()
        .with_timezone(&offset)
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json_parsed_token_2022_holding_from_owner_scan() {
        let data: UiAccountData = serde_json::from_value(serde_json::json!({
            "program": "spl-token-2022",
            "parsed": {
                "type": "account",
                "info": {
                    "mint": "DLz1imBbiPWN7bwss4WnRnbcYzJNHH4w7A7ks1Whpump",
                    "owner": "FGkyCsQMxQiFEvEqW8dTfgvs3aAPruNpAYg7HfbmXsPs",
                    "tokenAmount": {
                        "amount": "176595210438",
                        "decimals": 6,
                        "uiAmount": 176595.210438,
                        "uiAmountString": "176595.210438"
                    },
                    "state": "initialized",
                    "isNative": false,
                    "extensions": []
                }
            },
            "space": 170
        }))
        .unwrap();

        let holding = decode_token_holding(&data).expect("应解析 jsonParsed Token-2022 账户");
        assert_eq!(holding.mint, "DLz1imBbiPWN7bwss4WnRnbcYzJNHH4w7A7ks1Whpump");
        assert_eq!(holding.amount, 176_595_210_438);
        assert_eq!(holding.decimals, Some(6));
        assert_eq!(holding.ui_amount_string.as_deref(), Some("176595.210438"));
    }

    #[test]
    fn parses_creator_from_current_bonding_curve_layout() {
        let creator = Pubkey::new_unique();
        let mut data = vec![0u8; 115];
        data[..8].copy_from_slice(&[23, 183, 248, 55, 96, 216, 172, 96]);
        data[49..81].copy_from_slice(creator.as_ref());
        assert_eq!(bonding_curve_creator(&data), Some(creator));
        data[48] = 1;
        assert_eq!(bonding_curve_creator(&data), None);
        data[48] = 0;
        data[0] = 0;
        assert_eq!(bonding_curve_creator(&data), None);
    }

    fn summary_with_submission(sniper_hash: Option<&str>, candidates: &[&str]) -> TokenSummary {
        TokenSummary {
            mint: "mint".into(),
            create_time: None,
            create_slot: None,
            sniper_slot: None,
            interval_slot: None,
            buy_price: None,
            sell_price: None,
            dev_hash: None,
            sniper_hash: sniper_hash.map(str::to_owned),
            candidate_hashes: candidates.iter().map(|value| (*value).to_owned()).collect(),
            execution_channel: None,
            remark: String::new(),
            status: "处理中".into(),
            pending_buy_sigs: HashSet::new(),
            pending_sell_sigs: HashSet::new(),
        }
    }

    #[test]
    fn sniper_list_excludes_observed_mint_without_local_submission() {
        assert!(!is_submitted_sniper(&summary_with_submission(None, &[])));
        assert!(is_submitted_sniper(&summary_with_submission(
            Some("sig"),
            &[]
        )));
        assert!(is_submitted_sniper(&summary_with_submission(
            None,
            &["candidate"]
        )));
    }

    #[test]
    fn slot_gap_uses_observed_slot_delta() {
        assert_eq!(slot_gap(Some(443_870_619), Some(443_870_622)), Some(3));
    }

    #[test]
    fn terminal_status_not_reverted_by_pending_notice() {
        // 已定型结果不应被后续「等待核账」/「处理中」倒退覆盖（核心 bug：失败→待确认）
        assert!(!note_overrides("失败", "待确认"));
        assert!(!note_overrides("失败", "处理中"));
        assert!(!note_overrides("成功", "待确认"));
        assert!(!note_overrides("成功", "处理中"));
        assert!(!note_overrides("链上失败", "待确认"));
        assert!(!note_overrides("链上成功", "处理中"));
    }

    #[test]
    fn success_terminal_not_overridden_by_failure() {
        // 已锁定的成功/链上结果不被本地「买入失败」倒退（原代码意图）
        assert!(!note_overrides("成功", "失败"));
        assert!(!note_overrides("链上成功", "失败"));
        assert!(!note_overrides("链上失败", "失败"));
    }

    #[test]
    fn progress_towards_terminal_allowed() {
        // 进行中→待确认/终态推进
        assert!(note_overrides("处理中", "待确认"));
        assert!(note_overrides("处理中", "失败"));
        assert!(note_overrides("处理中", "成功"));
        assert!(note_overrides("待确认", "失败"));
        assert!(note_overrides("待确认", "成功"));
        // 本地「失败」被链上结果纠正（后到终态覆盖）
        assert!(note_overrides("失败", "链上失败"));
        assert!(note_overrides("失败", "成功"));
        assert!(note_overrides("失败", "链上成功"));
        assert!(note_overrides("成功", "链上成功"));
    }

    #[test]
    fn waiting_reconciliation_remark_keeps_concrete_reason() {
        let remark = note_remark(&serde_json::json!({
            "tag": "等待核账",
            "body": "[4cZY] 买入提交后5000毫秒未收到Geyser确认，已熔断新开仓: sig-a,sig-b"
        }));

        assert_eq!(remark, "买入提交后5000毫秒未收到Geyser确认");
        assert_eq!(
            note_status(&serde_json::json!({ "tag": "等待核账" })),
            Some("待确认".into())
        );
    }
}
