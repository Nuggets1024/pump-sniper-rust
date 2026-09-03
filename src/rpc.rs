//! RPC clients used by the bot's read and standard RPC submission paths.
//!
//! Low-latency blockchain RPC must not inherit desktop HTTP proxy variables:
//! an intermittent local proxy TLS failure would otherwise block transaction
//! construction before any landing provider is reached.

use reqwest::Client;
use solana_rpc_client::http_sender::HttpSender;
use solana_rpc_client::nonblocking::rpc_client::RpcClient as AsyncRpcClient;
use solana_rpc_client::rpc_client::{RpcClient, RpcClientConfig};
use solana_sdk::commitment_config::CommitmentConfig;
use std::time::Duration;

fn http_client(timeout: Duration) -> Client {
    Client::builder()
        .no_proxy()
        .default_headers(HttpSender::default_headers())
        .timeout(timeout)
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(30))
        .build()
        .expect("构建 RPC HTTP client")
}

pub fn blocking(url: impl ToString, timeout: Duration, commitment: CommitmentConfig) -> RpcClient {
    RpcClient::new_sender(
        HttpSender::new_with_client(url, http_client(timeout)),
        RpcClientConfig::with_commitment(commitment),
    )
}

pub fn nonblocking(
    url: impl ToString,
    timeout: Duration,
    commitment: CommitmentConfig,
) -> AsyncRpcClient {
    AsyncRpcClient::new_sender(
        HttpSender::new_with_client(url, http_client(timeout)),
        RpcClientConfig::with_commitment(commitment),
    )
}
