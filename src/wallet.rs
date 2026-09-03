//! 从文件加载密钥，拒绝不安全权限。

use crate::error::{Result, SniperError};
use solana_sdk::signature::{Keypair, Signer};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

pub fn load_keypair(path: &Path) -> Result<Keypair> {
    let data = fs::read(path).map_err(|e| SniperError::Wallet(format!("{e}")))?;
    let mode = fs::metadata(path)
        .map_err(|e| SniperError::Wallet(e.to_string()))?
        .permissions()
        .mode();
    if mode & 0o077 != 0 {
        return Err(SniperError::Wallet(format!(
            "{} 权限 {:o}，必须是 600",
            path.display(),
            mode & 0o777
        )));
    }
    if let Ok(bytes) = serde_json::from_slice::<Vec<u8>>(&data) {
        return Keypair::try_from(bytes.as_slice())
            .map_err(|e| SniperError::Wallet(format!("JSON 密钥: {e}")));
    }
    let s = String::from_utf8_lossy(&data);
    let trimmed = s.trim();
    if let Ok(bytes) = bs58::decode(trimmed).into_vec() {
        return Keypair::try_from(bytes.as_slice())
            .map_err(|e| SniperError::Wallet(format!("base58 密钥: {e}")));
    }
    Err(SniperError::Wallet(
        "密钥须为 JSON 字节数组或 base58".into(),
    ))
}

pub fn pubkey_short(kp: &Keypair) -> String {
    let s = kp.pubkey().to_string();
    format!("{}…{}", &s[..4], &s[s.len() - 4..])
}
