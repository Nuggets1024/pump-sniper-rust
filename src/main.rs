use clap::Parser;
use pump_sniper::config::{AppConfig, BotMode};
use pump_sniper::journal::TradeJournal;
use pump_sniper::listen::{geyser, merge, repair, shred};
use pump_sniper::strategy::{self, FollowDev, ScanTarget};
use pump_sniper::telemetry::{self, yn};
use pump_sniper::trading;
use pump_sniper::wallet::load_keypair;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::signature::Signer;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

#[derive(Parser, Debug)]
#[command(name = "pump-sniper", about = "Pump.fun 跟盘狙击")]
struct Args {
    #[arg(short, long, default_value = "config.toml", help = "配置文件路径")]
    config: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let cfg = AppConfig::load(&args.config)?;
    let admin_runtime = pump_sniper::admin::init(&cfg, &args.config);
    let _log_guard = telemetry::init(&cfg.log);
    if cfg.admin.enabled {
        let bind = cfg.admin.bind.parse()?;
        pump_sniper::admin::spawn_server(bind);
    }
    pump_sniper::price::start().await;

    telemetry::info(
        "启动",
        format!(
            "模式={}  交易={}  监听={}",
            cfg.bot.mode.label(),
            yn(cfg.bot.trade),
            if cfg.follow.is_empty() {
                "Pump发币"
            } else {
                "follow钱包"
            },
        ),
    );

    let kp = if cfg.bot.trade {
        let keypair = load_keypair(&cfg.wallet.keypair_path)?;
        let wallet = keypair.pubkey();
        let rpc_url = cfg.rpc.url.clone();
        let balance = tokio::task::spawn_blocking(move || {
            pump_sniper::rpc::blocking(
                rpc_url,
                Duration::from_secs(5),
                CommitmentConfig::confirmed(),
            )
            .get_balance(&wallet)
        })
        .await;
        match balance {
            Ok(Ok(lamports)) => telemetry::info_fields(
                "钱包",
                [
                    keypair.pubkey().to_string(),
                    format!("{} SOL", format_sol(lamports)),
                ],
            ),
            Ok(Err(_)) | Err(_) => telemetry::error_fields(
                "钱包",
                [keypair.pubkey().to_string(), "查询失败".to_owned()],
            ),
        }
        let balance_lamports = match balance {
            Ok(Ok(lamports)) => Some(lamports),
            Ok(Err(_)) | Err(_) => None,
        };
        pump_sniper::admin::set_wallet(keypair.pubkey().to_string(), balance_lamports);
        Some(keypair)
    } else {
        None
    };
    let watched_wallet = kp.as_ref().map(Signer::pubkey).unwrap_or_default();
    let journal_directory = std::path::PathBuf::from(&cfg.log.directory);
    let journal = TradeJournal::start(&journal_directory, watched_wallet);

    let (feed_tx, feed_rx) = mpsc::channel(4096);
    let (market_tx, market_rx) = mpsc::channel(4096);
    let (scan_target_tx, scan_target_rx) = watch::channel::<Vec<ScanTarget>>(vec![]);
    let (target_release_tx, target_release_rx) = mpsc::channel(1024);
    let publish_targets = cfg.bot.mode == BotMode::Scan || !cfg.follow.is_empty();
    let (gap_tx, gap_rx) = mpsc::unbounded_channel();
    let gap_path = journal_directory.join("cursors/pending-gaps.json");
    let gap_store = if cfg.bot.mode == BotMode::Scan {
        telemetry::info("会话重置", "scan 已清空 cursor 与 pending gaps");
        repair::GapStore::fresh(gap_path)?
    } else {
        repair::GapStore::open(gap_path)
    };
    tokio::spawn(merge::run(feed_rx, market_tx));
    tokio::spawn(repair::run(
        cfg.rpc.url.clone(),
        gap_rx,
        gap_tx.clone(),
        feed_tx.clone(),
        gap_store.clone(),
    ));
    if cfg.shred.enabled {
        let shred_cfg = cfg.shred.clone();
        let shred_tx = feed_tx.clone();
        tokio::spawn(async move {
            if let Err(error) = shred::run(shred_cfg, shred_tx).await {
                telemetry::error("ShredStream", error.to_string());
            }
        });
    }
    for gap in gap_store.pending() {
        pump_sniper::listen::begin_gap_repair();
        gap_tx
            .send(gap)
            .map_err(|_| anyhow::anyhow!("RPC 回补任务已停止"))?;
    }

    let geyser_sources = cfg.geyser_sources();
    let Some(source) = geyser_sources.first().cloned() else {
        anyhow::bail!("geyser.sources 为空");
    };
    if geyser_sources.len() > 1 {
        telemetry::warn(
            "Geyser",
            format!(
                "已配置 {} 个 endpoint；当前版本只使用第一个 endpoint={}",
                geyser_sources.len(),
                source.endpoint
            ),
        );
    }
    if cfg.bot.mode == BotMode::Scan {
        telemetry::info(
            "Geyser",
            "scan 单 endpoint 双连接：主连接监听 CREATE，目标连接监听 mint 交易",
        );
    }
    let endpoint = source.endpoint.clone();
    let cfg_listen =
        geyser::RuntimeConfig::new(&cfg, source.endpoint.clone(), source.x_token.clone());
    let tx = feed_tx.clone();
    let gaps = gap_tx.clone();
    let gap_store = gap_store.clone();
    tokio::spawn(async move {
        if let Err(e) = geyser::run(
            cfg_listen,
            source.id,
            watched_wallet,
            tx,
            gaps,
            gap_store,
            None,
        )
        .await
        {
            telemetry::error_fields(
                "失败",
                [
                    "主监听".to_owned(),
                    pump_sniper::exec::land::endpoint_label(&endpoint),
                    e.to_string(),
                ],
            );
        }
    });
    let target_cfg = geyser::RuntimeConfig::new(&cfg, source.endpoint, source.x_token);
    let target_rx = scan_target_rx.clone();
    let target_tx = feed_tx.clone();
    tokio::spawn(async move {
        let _ = geyser::run_scan_target(target_cfg, source.id, target_rx, target_tx).await;
    });
    drop(feed_tx);
    drop(gap_tx);

    if !cfg.bot.trade {
        telemetry::info("交易", "已关闭，仅监控市场事件");
        let mut market_rx = market_rx;
        while let Some(event) = market_rx.recv().await {
            journal.record(&event);
        }
        return Ok(());
    }

    let (frame_tx, frame_rx) = mpsc::channel(4096);
    tokio::spawn(strategy::run(
        FollowDev::new(&cfg),
        cfg.bot.mode,
        market_rx,
        frame_tx,
        publish_targets.then_some(scan_target_tx),
        Some(target_release_rx),
    ));
    let kp = kp.expect("交易开关已加载钱包");
    trading::run(
        cfg,
        Arc::new(kp),
        frame_rx,
        journal,
        Some(admin_runtime.commands),
        Some(target_release_tx),
    )
    .await?;
    Ok(())
}

fn format_sol(lamports: u64) -> String {
    format!(
        "{}.{:09}",
        lamports / 1_000_000_000,
        lamports % 1_000_000_000
    )
}

#[cfg(test)]
mod tests {
    use super::format_sol;

    #[test]
    fn formats_wallet_balance_without_floating_point_loss() {
        assert_eq!(format_sol(1_234_567_890), "1.234567890");
        assert_eq!(format_sol(42), "0.000000042");
    }
}
