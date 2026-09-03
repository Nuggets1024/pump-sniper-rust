//! 从 getClusterNodes 拉全网节点 IP，先 TCP 筛 :10000，再 GetSlot。
//! 同时对照 Helius processed slot，只把贴尖端的节点当「最快」。
//!
//!   cargo run --example probe-geyser
//!   RPC_URL=https://... cargo run --example probe-geyser

use futures::stream::{self, StreamExt};
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_response::RpcContactInfo;
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::CommitmentLevel;

const GEYSER_PORT: u16 = 10000;
const TCP_TIMEOUT: Duration = Duration::from_millis(800);
const GRPC_TIMEOUT: Duration = Duration::from_secs(2);
const TCP_CONCURRENCY: usize = 256;
const GRPC_CONCURRENCY: usize = 64;
/// 落后不超过这么多 slot 视为尖端（约 0.8s）。
const TIP_MAX_LAG: i64 = 2;
const DEFAULT_RPC: &str =
    "https://mainnet.helius-rpc.com/?api-key=050fbd9b-4b5e-4c14-a3f4-2193df488549";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let rpc_url = std::env::var("RPC_URL").unwrap_or_else(|_| DEFAULT_RPC.to_string());
    let t_all = Instant::now();

    eprintln!("拉取 getClusterNodes …");
    let url_ips = rpc_url.clone();
    let ips = tokio::task::spawn_blocking(move || fetch_ips(&url_ips)).await??;
    eprintln!("去重 {} 个 IP", ips.len());

    eprintln!(
        "① TCP :{GEYSER_PORT}  超时 {}ms  并发 {TCP_CONCURRENCY}",
        TCP_TIMEOUT.as_millis()
    );
    let t_tcp = Instant::now();
    let open = tcp_filter(ips).await;
    eprintln!(
        "   TCP 开放 {}  耗时 {:.1}s",
        open.len(),
        t_tcp.elapsed().as_secs_f64()
    );

    let clock = Arc::new(SlotClock::default());
    let sampler = {
        let clock = clock.clone();
        let url = rpc_url.clone();
        tokio::spawn(async move { sample_rpc_slots(url, clock).await })
    };

    eprintln!(
        "② GetSlot  超时 {}s  并发 {GRPC_CONCURRENCY}，对照 RPC processed",
        GRPC_TIMEOUT.as_secs()
    );
    let t_grpc = Instant::now();
    let mut ok_rows = grpc_filter(open).await;
    eprintln!(
        "   gRPC 可连 {}  耗时 {:.1}s",
        ok_rows.len(),
        t_grpc.elapsed().as_secs_f64()
    );
    sampler.abort();

    let samples = clock.samples.lock().await.clone();
    for r in &mut ok_rows {
        r.lag = match expected_slot(&samples, r.at) {
            Some(exp) => exp as i64 - r.slot as i64,
            None => 0,
        };
        r.tip = r.lag <= TIP_MAX_LAG;
    }

    // 尖端优先，同档按延迟，再按落后（负数=比 RPC 还新）
    ok_rows.sort_by(|a, b| {
        b.tip
            .cmp(&a.tip)
            .then(a.ms.partial_cmp(&b.ms).unwrap())
            .then(a.lag.cmp(&b.lag))
    });

    println!();
    println!(
        "{:<18} {:>6}  {:>8}  {:<4}  {:<4}  {}",
        "IP", "耗时ms", "落后slot", "尖端", "结果", "说明"
    );
    println!("{}", "-".repeat(88));
    for r in &ok_rows {
        println!(
            "{:<18} {:>6.0}  {:>8}  {:<4}  {:<4}  GetSlot={}  http://{}:{}",
            r.ip,
            r.ms,
            r.lag,
            if r.tip { "是" } else { "否" },
            "通",
            r.slot,
            r.ip,
            GEYSER_PORT
        );
    }
    println!("{}", "-".repeat(88));
    let tip_n = ok_rows.iter().filter(|r| r.tip).count();
    println!(
        "可连 {}    尖端 {}    全程 {:.1}s    落后≤{} slot 算尖端",
        ok_rows.len(),
        tip_n,
        t_all.elapsed().as_secs_f64(),
        TIP_MAX_LAG
    );
    if let Some(ahead) = ok_rows.iter().filter(|r| r.tip).min_by_key(|r| r.lag) {
        println!(
            "slot 最快: http://{}:{}  落后{}  {}ms",
            ahead.ip, GEYSER_PORT, ahead.lag, ahead.ms as u64
        );
    }
    if let Some(best) = ok_rows.iter().find(|r| r.tip) {
        println!(
            "推荐（尖端里延迟最低）: http://{}:{}  落后{}  {}ms",
            best.ip, GEYSER_PORT, best.lag, best.ms as u64
        );
    } else if let Some(best) = ok_rows.first() {
        println!(
            "无尖端节点，slot 最接近: http://{}:{}  落后{}",
            best.ip, GEYSER_PORT, best.lag
        );
    }
    Ok(())
}

fn fetch_ips(rpc_url: &str) -> anyhow::Result<Vec<String>> {
    let client = RpcClient::new(rpc_url.to_string());
    let nodes: Vec<RpcContactInfo> = client.get_cluster_nodes()?;
    let mut set = BTreeSet::new();
    for n in &nodes {
        for addr in [n.gossip, n.rpc, n.tpu, n.pubsub].into_iter().flatten() {
            match addr.ip() {
                IpAddr::V4(ip) if !ip.is_loopback() && !ip.is_unspecified() && !ip.is_private() => {
                    set.insert(ip.to_string());
                }
                _ => {}
            }
        }
    }
    Ok(set.into_iter().collect())
}

#[derive(Default)]
struct SlotClock {
    samples: Mutex<Vec<(Instant, u64)>>,
}

async fn sample_rpc_slots(rpc_url: String, clock: Arc<SlotClock>) {
    loop {
        let url = rpc_url.clone();
        let slot = tokio::task::spawn_blocking(move || {
            RpcClient::new_with_commitment(url, CommitmentConfig::processed()).get_slot()
        })
        .await
        .ok()
        .and_then(Result::ok);
        if let Some(slot) = slot {
            clock.samples.lock().await.push((Instant::now(), slot));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn expected_slot(samples: &[(Instant, u64)], at: Instant) -> Option<u64> {
    samples
        .iter()
        .rev()
        .find(|(t, _)| *t <= at)
        .or_else(|| samples.first())
        .map(|(_, s)| *s)
}

async fn tcp_filter(ips: Vec<String>) -> Vec<String> {
    let total = ips.len();
    let mut open = Vec::new();
    let mut done = 0usize;
    let mut stream = stream::iter(ips.into_iter().map(tcp_open)).buffer_unordered(TCP_CONCURRENCY);
    while let Some((ip, ok)) = stream.next().await {
        done += 1;
        if ok {
            open.push(ip);
        }
        if done % 500 == 0 {
            eprintln!("   TCP 进度 {done}/{total}  开放 {}", open.len());
        }
    }
    open
}

async fn tcp_open(ip: String) -> (String, bool) {
    let addr = format!("{ip}:{GEYSER_PORT}");
    let ok = tokio::time::timeout(TCP_TIMEOUT, TcpStream::connect(&addr))
        .await
        .ok()
        .and_then(Result::ok)
        .is_some();
    (ip, ok)
}

async fn grpc_filter(ips: Vec<String>) -> Vec<Row> {
    let total = ips.len();
    let mut ok_rows = Vec::new();
    let mut done = 0usize;
    let mut stream =
        stream::iter(ips.into_iter().map(probe_grpc)).buffer_unordered(GRPC_CONCURRENCY);
    while let Some(row) = stream.next().await {
        done += 1;
        if row.ok {
            eprintln!("   通  {:<16} {:>5.0}ms  slot={}", row.ip, row.ms, row.slot);
            ok_rows.push(row);
        }
        if done % 20 == 0 {
            eprintln!("   gRPC 进度 {done}/{total}  已通 {}", ok_rows.len());
        }
    }
    ok_rows
}

struct Row {
    ip: String,
    ok: bool,
    ms: f64,
    slot: u64,
    at: Instant,
    lag: i64,
    tip: bool,
}

async fn probe_grpc(ip: String) -> Row {
    let url = format!("http://{ip}:{GEYSER_PORT}");
    let t0 = Instant::now();
    let res = tokio::time::timeout(GRPC_TIMEOUT, try_slot(&url)).await;
    let at = Instant::now();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    match res {
        Ok(Ok(slot)) => Row {
            ip,
            ok: true,
            ms,
            slot,
            at,
            lag: 0,
            tip: false,
        },
        _ => Row {
            ip,
            ok: false,
            ms,
            slot: 0,
            at,
            lag: 0,
            tip: false,
        },
    }
}

async fn try_slot(url: &str) -> anyhow::Result<u64> {
    let mut client = GeyserGrpcClient::build_from_shared(url.to_string())?
        .connect()
        .await?;
    let resp = client.get_slot(Some(CommitmentLevel::Processed)).await?;
    Ok(resp.slot)
}
