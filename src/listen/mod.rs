//! 链上监听（Geyser + Jito ShredStream）。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static INTEGRITY_OK: AtomicBool = AtomicBool::new(true);
static PENDING_GAPS: AtomicU64 = AtomicU64::new(0);
static SHRED_BATCHES: AtomicU64 = AtomicU64::new(0);
static SHRED_TRANSACTIONS: AtomicU64 = AtomicU64::new(0);
static SHRED_CANDIDATES: AtomicU64 = AtomicU64::new(0);
static SHRED_EVENTS: AtomicU64 = AtomicU64::new(0);
static SHRED_FIRST: AtomicU64 = AtomicU64::new(0);
static SHRED_DUPLICATES: AtomicU64 = AtomicU64::new(0);
static GEYSER_FIRST: AtomicU64 = AtomicU64::new(0);
static GEYSER_DUPLICATES: AtomicU64 = AtomicU64::new(0);
static LAST_SHRED_SLOT: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct FeedStats {
    pub shred_batches: u64,
    pub shred_transactions: u64,
    pub shred_candidates: u64,
    pub shred_events: u64,
    pub shred_first: u64,
    pub shred_duplicates: u64,
    pub geyser_first: u64,
    /// Geyser arrived after a matching Shred event; this is a Shred race win.
    pub geyser_duplicates: u64,
    pub last_shred_slot: Option<u64>,
}

pub fn feed_stats() -> FeedStats {
    let last_shred_slot = LAST_SHRED_SLOT.load(Ordering::Relaxed);
    FeedStats {
        shred_batches: SHRED_BATCHES.load(Ordering::Relaxed),
        shred_transactions: SHRED_TRANSACTIONS.load(Ordering::Relaxed),
        shred_candidates: SHRED_CANDIDATES.load(Ordering::Relaxed),
        shred_events: SHRED_EVENTS.load(Ordering::Relaxed),
        shred_first: SHRED_FIRST.load(Ordering::Relaxed),
        shred_duplicates: SHRED_DUPLICATES.load(Ordering::Relaxed),
        geyser_first: GEYSER_FIRST.load(Ordering::Relaxed),
        geyser_duplicates: GEYSER_DUPLICATES.load(Ordering::Relaxed),
        last_shred_slot: (last_shred_slot != 0).then_some(last_shred_slot),
    }
}

pub(crate) fn record_shred_batch(slot: u64, transactions: u64, candidates: u64, events: u64) {
    SHRED_BATCHES.fetch_add(1, Ordering::Relaxed);
    SHRED_TRANSACTIONS.fetch_add(transactions, Ordering::Relaxed);
    SHRED_CANDIDATES.fetch_add(candidates, Ordering::Relaxed);
    SHRED_EVENTS.fetch_add(events, Ordering::Relaxed);
    LAST_SHRED_SLOT.fetch_max(slot, Ordering::Relaxed);
}

pub(crate) fn record_feed_result(source_id: u8, duplicate: bool) {
    if source_id == shred::SHRED_SOURCE_ID {
        if duplicate {
            SHRED_DUPLICATES.fetch_add(1, Ordering::Relaxed);
        } else {
            SHRED_FIRST.fetch_add(1, Ordering::Relaxed);
        }
    } else if duplicate {
        GEYSER_DUPLICATES.fetch_add(1, Ordering::Relaxed);
    } else {
        GEYSER_FIRST.fetch_add(1, Ordering::Relaxed);
    }
}

pub mod cursor;
pub mod geyser;
pub mod merge;
pub mod repair;
pub mod shred;

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
