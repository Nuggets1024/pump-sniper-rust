//! 链上监听（Geyser）。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static INTEGRITY_OK: AtomicBool = AtomicBool::new(true);
static PENDING_GAPS: AtomicU64 = AtomicU64::new(0);

pub mod cursor;
pub mod geyser;
pub mod merge;
pub mod repair;

pub fn integrity_allows_new_orders() -> bool {
    INTEGRITY_OK.load(Ordering::Acquire) && PENDING_GAPS.load(Ordering::Acquire) == 0
}

#[doc(hidden)]
pub fn begin_gap_repair() {
    PENDING_GAPS.fetch_add(1, Ordering::AcqRel);
}

pub(crate) fn finish_gap_repair() {
    let _ = PENDING_GAPS.fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
        Some(pending.saturating_sub(1))
    });
}

pub(crate) fn fail_integrity(reason: impl std::fmt::Display) {
    INTEGRITY_OK.store(false, Ordering::Release);
    crate::telemetry::error("完整性熔断", reason.to_string());
}
