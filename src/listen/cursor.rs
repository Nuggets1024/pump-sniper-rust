//! 每个 Geyser feed 的持久 slot cursor。

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct CursorFile {
    last_slot: Option<u64>,
}

pub fn start(path: PathBuf, resume: bool) -> (Option<u64>, watch::Sender<Option<u64>>) {
    if !resume {
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => crate::listen::fail_integrity(format!(
                "cursor 重置失败 path={} error={error}",
                path.display()
            )),
        }
    }
    let initial = resume.then(|| load(&path)).flatten();
    let (tx, mut rx) = watch::channel(initial);
    tokio::spawn(async move {
        while rx.changed().await.is_ok() {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let snapshot = CursorFile {
                last_slot: *rx.borrow_and_update(),
            };
            if let Err(error) = persist(&path, snapshot).await {
                crate::listen::fail_integrity(format!(
                    "cursor 持久化失败 path={} error={error}",
                    path.display()
                ));
                return;
            }
        }
    });
    (initial, tx)
}

fn load(path: &PathBuf) -> Option<u64> {
    let raw = match std::fs::read(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => {
            crate::listen::fail_integrity(format!(
                "cursor 读取失败 path={} error={error}",
                path.display()
            ));
            return None;
        }
    };
    match serde_json::from_slice::<CursorFile>(&raw) {
        Ok(cursor) => cursor.last_slot,
        Err(error) => {
            crate::listen::fail_integrity(format!(
                "cursor 损坏 path={} error={error}",
                path.display()
            ));
            None
        }
    }
}

async fn persist(path: &PathBuf, cursor: CursorFile) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("cursor 路径没有父目录"))?;
    tokio::fs::create_dir_all(parent).await?;
    let temp = unique_temp_path(path);
    let payload = serde_json::to_vec(&cursor)?;
    durable_replace(parent, path, &temp, &payload).await?;
    Ok(())
}

fn unique_temp_path(path: &std::path::Path) -> PathBuf {
    path.with_extension(format!("json.tmp.{}", std::process::id()))
}

async fn durable_replace(
    parent: &std::path::Path,
    path: &std::path::Path,
    temp: &std::path::Path,
    payload: &[u8],
) -> anyhow::Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(temp)
        .await?;
    file.write_all(payload).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(temp, path).await?;
    tokio::fs::File::open(parent).await?.sync_all().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cursor_is_atomically_persisted_and_loaded() {
        let path = std::env::temp_dir().join(format!(
            "pump-sniper-cursor-{}-{}.json",
            std::process::id(),
            rand::random::<u64>()
        ));
        persist(
            &path,
            CursorFile {
                last_slot: Some(42),
            },
        )
        .await
        .unwrap();
        assert_eq!(load(&path), Some(42));
        let _ = tokio::fs::remove_file(path).await;
    }
}
