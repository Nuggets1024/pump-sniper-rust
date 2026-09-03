//! 统一错误类型。

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SniperError {
    #[error("配置: {0}")]
    Config(String),
    #[error("钱包: {0}")]
    Wallet(String),
    #[error("解码: {0}")]
    Decode(String),
    #[error("策略跳过: {0}")]
    Skip(String),
    #[error("RPC: {0}")]
    Rpc(String),
    #[error("上链: {0}")]
    Land(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, SniperError>;
