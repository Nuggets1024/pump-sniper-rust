//! 可插拔的已签交易提交 adapter。
//!
//! Adapter 只能发送调用方给出的同一份 wire transaction，不能修改指令、
//! blockhash 或签名；这样并发供应商不会产生可重复成交的不同交易签名。

use crate::config::{LandingProvider, LandingRouteCfg, LandingTransport};
use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use reqwest::Client;
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::transaction::Transaction;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

const LANDX_UDP_SOCKET_TTL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmissionStatus {
    Accepted,
    Dispatched,
}

#[async_trait]
pub trait TransactionSender: Send + Sync {
    fn name(&self) -> &str;
    async fn warmup(&self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn submit(
        &self,
        transaction: Arc<Transaction>,
        wire: Bytes,
        base64_wire: Bytes,
    ) -> anyhow::Result<SubmissionStatus>;
}

pub fn build_sender(
    route: &LandingRouteCfg,
    skip_preflight: bool,
) -> anyhow::Result<Arc<dyn TransactionSender>> {
    match route.provider {
        LandingProvider::Rpc => Ok(Arc::new(RpcSender::new(route, skip_preflight))),
        LandingProvider::ZeroSlot => Ok(Arc::new(HttpSender::new(route, HttpProtocol::ZeroSlot)?)),
        LandingProvider::Temporal => Ok(Arc::new(HttpSender::new(route, HttpProtocol::Temporal)?)),
        LandingProvider::Astralane => {
            Ok(Arc::new(HttpSender::new(route, HttpProtocol::Astralane)?))
        }
        LandingProvider::Landx if route.transport == LandingTransport::Udp => {
            Ok(Arc::new(LandxUdpSender::new(route)?))
        }
        LandingProvider::Landx => Ok(Arc::new(HttpSender::new(route, HttpProtocol::Landx)?)),
    }
}

struct RpcSender {
    name: String,
    client: Arc<solana_client::rpc_client::RpcClient>,
    config: RpcSendTransactionConfig,
}

impl RpcSender {
    fn new(route: &LandingRouteCfg, skip_preflight: bool) -> Self {
        Self {
            name: route.name.clone(),
            client: Arc::new(crate::rpc::blocking(
                route.endpoint.clone(),
                Duration::from_secs(8),
                solana_sdk::commitment_config::CommitmentConfig::confirmed(),
            )),
            config: RpcSendTransactionConfig {
                skip_preflight,
                preflight_commitment: None,
                encoding: None,
                max_retries: Some(0),
                min_context_slot: None,
            },
        }
    }
}

#[async_trait]
impl TransactionSender for RpcSender {
    fn name(&self) -> &str {
        &self.name
    }

    async fn submit(
        &self,
        transaction: Arc<Transaction>,
        _wire: Bytes,
        _base64_wire: Bytes,
    ) -> anyhow::Result<SubmissionStatus> {
        let client = self.client.clone();
        let config = self.config;
        tokio::task::spawn_blocking(move || {
            client
                .send_transaction_with_config(transaction.as_ref(), config)
                .map(|_| SubmissionStatus::Accepted)
                .map_err(|error| error.to_string())
        })
        .await
        .context("等待 RPC 提交任务")?
        .map_err(anyhow::Error::msg)
    }
}

#[derive(Clone, Copy)]
enum HttpProtocol {
    ZeroSlot,
    Temporal,
    Astralane,
    Landx,
}

struct HttpSender {
    name: String,
    endpoint: String,
    api_key: String,
    mev_protect: bool,
    protocol: HttpProtocol,
    client: Client,
}

impl HttpSender {
    fn new(route: &LandingRouteCfg, protocol: HttpProtocol) -> anyhow::Result<Self> {
        let client = Client::builder()
            .pool_idle_timeout(Duration::from_secs(55))
            .tcp_keepalive(Duration::from_secs(30))
            .timeout(Duration::from_secs(3))
            .build()?;
        Ok(Self {
            name: route.name.clone(),
            endpoint: route.endpoint.trim_end_matches('/').to_owned(),
            api_key: route.api_key.clone(),
            mev_protect: route.mev_protect,
            protocol,
            client,
        })
    }

    fn url(&self) -> String {
        match self.protocol {
            HttpProtocol::ZeroSlot if self.endpoint.ends_with("/txb") => self.endpoint.clone(),
            HttpProtocol::ZeroSlot => format!("{}/txb", self.endpoint),
            HttpProtocol::Temporal if self.endpoint.ends_with("/api/sendTransaction2") => {
                self.endpoint.clone()
            }
            HttpProtocol::Temporal => format!("{}/api/sendTransaction2", self.endpoint),
            HttpProtocol::Astralane
                if self.endpoint.ends_with("/iris") || self.endpoint.ends_with("/iris2") =>
            {
                self.endpoint.clone()
            }
            HttpProtocol::Astralane => format!("{}/iris2", self.endpoint),
            HttpProtocol::Landx => landx_http_url(&self.endpoint, &self.api_key),
        }
    }

    async fn response_error(response: reqwest::Response) -> anyhow::Error {
        let status = response.status();
        let bytes = response.bytes().await.unwrap_or_default();
        let limit = bytes.len().min(512);
        let body = String::from_utf8_lossy(&bytes[..limit]);
        anyhow::anyhow!("HTTP {status}: {body}")
    }
}

#[async_trait]
impl TransactionSender for HttpSender {
    fn name(&self) -> &str {
        &self.name
    }

    async fn warmup(&self) -> anyhow::Result<()> {
        let response = match self.protocol {
            HttpProtocol::ZeroSlot => {
                self.client
                    .post(format!("{}/", self.endpoint))
                    .query(&[("api-key", self.api_key.as_str())])
                    .header("Content-Type", "application/json")
                    .body(r#"{"jsonrpc":"2.0","id":1,"method":"getHealth"}"#)
                    .send()
                    .await
            }
            HttpProtocol::Temporal => {
                self.client
                    .get(format!("{}/ping", self.endpoint))
                    .send()
                    .await
            }
            HttpProtocol::Astralane => {
                self.client
                    .post(self.url())
                    .query(&[("api-key", self.api_key.as_str()), ("method", "getHealth")])
                    .body(Bytes::new())
                    .send()
                    .await
            }
            HttpProtocol::Landx => {
                self.client
                    .get(format!("{}/ping", self.endpoint))
                    .send()
                    .await
            }
        }
        .map_err(|error| anyhow::anyhow!(error.without_url().to_string()))?;
        if !response.status().is_success() {
            return Err(Self::response_error(response).await);
        }
        let _ = response.bytes().await;
        Ok(())
    }

    async fn submit(
        &self,
        _transaction: Arc<Transaction>,
        wire: Bytes,
        base64_wire: Bytes,
    ) -> anyhow::Result<SubmissionStatus> {
        let url = self.url();
        let mut request = self.client.post(url);
        request = match self.protocol {
            HttpProtocol::ZeroSlot => request
                .query(&[("api-key", self.api_key.as_str())])
                .header("Content-Type", "application/octet-stream")
                .header("User-Agent", "")
                .body(wire),
            HttpProtocol::Temporal => request
                .query(&[("c", self.api_key.as_str())])
                .header("Content-Type", "text/plain")
                .body(base64_wire),
            HttpProtocol::Astralane => {
                let mut query = vec![
                    ("api-key", self.api_key.as_str()),
                    ("method", "sendTransaction"),
                ];
                if self.mev_protect {
                    query.push(("mev-protect", "true"));
                }
                request
                    .query(&query)
                    .header("Content-Type", "text/plain")
                    .body(base64_wire)
            }
            HttpProtocol::Landx => request
                .header("Content-Type", "application/octet-stream")
                .body(wire),
        };
        let response = request
            .send()
            .await
            .map_err(|error| anyhow::anyhow!(error.without_url().to_string()))?;
        if !response.status().is_success() {
            return Err(Self::response_error(response).await);
        }

        let body = response.bytes().await.unwrap_or_default();
        validate_success_body(self.protocol, &body)?;
        Ok(SubmissionStatus::Accepted)
    }
}

fn validate_success_body(protocol: HttpProtocol, body: &[u8]) -> anyhow::Result<()> {
    match protocol {
        HttpProtocol::ZeroSlot => anyhow::ensure!(
            String::from_utf8_lossy(body).trim() == "ok",
            "0slot Binary-Tx 响应不是 ok"
        ),
        HttpProtocol::Landx => anyhow::ensure!(
            String::from_utf8_lossy(body).trim() == "ok",
            "LandX HTTP 响应不是 ok"
        ),
        HttpProtocol::Temporal | HttpProtocol::Astralane => {}
    }
    Ok(())
}

struct LandxUdpSender {
    name: String,
    endpoint: String,
    api_key: [u8; 12],
    socket: Mutex<Option<(Arc<UdpSocket>, std::time::Instant)>>,
}

impl LandxUdpSender {
    fn new(route: &LandingRouteCfg) -> anyhow::Result<Self> {
        let api_key: [u8; 12] = route
            .api_key
            .as_bytes()
            .try_into()
            .context("LandX api_key 必须正好 12 字节")?;
        Ok(Self {
            name: route.name.clone(),
            endpoint: landx_udp_endpoint(&route.endpoint),
            api_key,
            socket: Mutex::new(None),
        })
    }

    async fn socket(&self) -> anyhow::Result<Arc<UdpSocket>> {
        let mut cached = self.socket.lock().await;
        if let Some((socket, created)) = cached.as_ref() {
            if created.elapsed() < LANDX_UDP_SOCKET_TTL {
                return Ok(socket.clone());
            }
        }
        let address = tokio::net::lookup_host(&self.endpoint)
            .await
            .context("解析 LandX UDP endpoint")?
            .next()
            .context("LandX UDP endpoint 没有可用地址")?;
        let bind = if address.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let socket = UdpSocket::bind(bind)
            .await
            .context("绑定 LandX UDP socket")?;
        socket
            .connect(address)
            .await
            .context("连接 LandX UDP endpoint")?;
        let socket = Arc::new(socket);
        *cached = Some((socket.clone(), std::time::Instant::now()));
        Ok(socket)
    }

    async fn invalidate_socket(&self) {
        *self.socket.lock().await = None;
    }
}

#[async_trait]
impl TransactionSender for LandxUdpSender {
    fn name(&self) -> &str {
        &self.name
    }

    async fn warmup(&self) -> anyhow::Result<()> {
        self.socket().await.map(|_| ())
    }

    async fn submit(
        &self,
        _transaction: Arc<Transaction>,
        wire: Bytes,
        _base64_wire: Bytes,
    ) -> anyhow::Result<SubmissionStatus> {
        let packet = landx_udp_packet(&self.api_key, &wire);
        let socket = self.socket().await?;
        let sent = match socket.send(&packet).await {
            Ok(sent) => sent,
            Err(error) => {
                self.invalidate_socket().await;
                return Err(error).context("发送 LandX UDP transaction");
            }
        };
        anyhow::ensure!(sent == packet.len(), "LandX UDP packet 未完整发送");
        Ok(SubmissionStatus::Dispatched)
    }
}

fn landx_udp_packet(api_key: &[u8; 12], wire: &[u8]) -> Vec<u8> {
    let mut packet = Vec::with_capacity(12 + wire.len());
    packet.extend_from_slice(api_key);
    packet.extend_from_slice(wire);
    packet
}

fn landx_udp_endpoint(endpoint: &str) -> String {
    let endpoint = endpoint.trim().trim_start_matches("udp://");
    if endpoint.rsplit_once(':').is_some() {
        endpoint.to_owned()
    } else {
        format!("{endpoint}:10000")
    }
}

fn landx_http_url(endpoint: &str, api_key: &str) -> String {
    let mut url = reqwest::Url::parse(endpoint).expect("LandX HTTP endpoint 已在配置阶段校验");
    {
        let mut segments = url
            .path_segments_mut()
            .expect("LandX HTTP endpoint 必须是 base URL");
        segments.pop_if_empty();
        segments.push("txb");
        segments.push(api_key);
    }
    url.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(provider: LandingProvider, endpoint: &str) -> LandingRouteCfg {
        LandingRouteCfg {
            name: "test".into(),
            provider,
            endpoint: endpoint.into(),
            api_key: "secret".into(),
            tip_accounts: Vec::new(),
            tip_lamports: 0,
            mev_protect: true,
            transport: crate::config::LandingTransport::Http,
        }
    }

    #[test]
    fn provider_paths_are_normalized_once() {
        let zero = HttpSender::new(
            &route(LandingProvider::ZeroSlot, "https://ny.0slot.trade"),
            HttpProtocol::ZeroSlot,
        )
        .unwrap();
        assert_eq!(zero.url(), "https://ny.0slot.trade/txb");

        let temporal = HttpSender::new(
            &route(LandingProvider::Temporal, "https://nozomi.temporal.xyz"),
            HttpProtocol::Temporal,
        )
        .unwrap();
        assert_eq!(
            temporal.url(),
            "https://nozomi.temporal.xyz/api/sendTransaction2"
        );

        let astralane = HttpSender::new(
            &route(LandingProvider::Astralane, "https://edge.astralane.io"),
            HttpProtocol::Astralane,
        )
        .unwrap();
        assert_eq!(astralane.url(), "https://edge.astralane.io/iris2");

        let landx = HttpSender::new(
            &route(LandingProvider::Landx, "http://ams1.landx.dev"),
            HttpProtocol::Landx,
        )
        .unwrap();
        assert_eq!(landx.url(), "http://ams1.landx.dev/txb/secret");
    }

    #[test]
    fn zeroslot_binary_response_is_plain_ok() {
        assert!(validate_success_body(HttpProtocol::ZeroSlot, b"ok\n").is_ok());
        assert!(validate_success_body(HttpProtocol::ZeroSlot, br#"{"result":"sig"}"#).is_err());
    }

    #[test]
    fn temporal_accepts_documented_empty_success_body() {
        assert!(validate_success_body(HttpProtocol::Temporal, b"").is_ok());
    }

    #[test]
    fn landx_udp_uses_default_port_and_prefixes_api_key() {
        assert_eq!(
            landx_udp_endpoint("udp://ams1.landx.dev"),
            "ams1.landx.dev:10000"
        );
        assert_eq!(
            landx_udp_endpoint("ams1.landx.dev:10001"),
            "ams1.landx.dev:10001"
        );
        let packet = landx_udp_packet(b"123456789012", &[1, 2, 3]);
        assert_eq!(&packet[..12], b"123456789012");
        assert_eq!(&packet[12..], &[1, 2, 3]);
        assert_eq!(
            landx_http_url("http://ams1.landx.dev", "abc/def?xy12"),
            "http://ams1.landx.dev/txb/abc%2Fdef%3Fxy12"
        );
    }

    #[tokio::test]
    async fn landx_udp_sends_authenticated_wire_packet() {
        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut cfg = route(
            LandingProvider::Landx,
            &receiver.local_addr().unwrap().to_string(),
        );
        cfg.api_key = "123456789012".into();
        cfg.transport = LandingTransport::Udp;
        let sender = LandxUdpSender::new(&cfg).unwrap();
        sender
            .submit(
                Arc::new(Transaction::default()),
                Bytes::from_static(&[7, 8, 9]),
                Bytes::new(),
            )
            .await
            .unwrap();

        let mut packet = [0u8; 32];
        let size = tokio::time::timeout(Duration::from_secs(1), receiver.recv(&mut packet))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&packet[..size], b"123456789012\x07\x08\x09");
    }
}
