//! Per-action engine routing between worker pools.
//!
//! Every worker's bidi stream names its pool in the `osprey-engine-pool` header (`rust` or `shadow`; missing or
//! unknown means `python`). At enqueue each action goes to exactly one of `python` and `rust`, chosen by
//! `routing_bucket(action_id) < rust_bps`, and a sample of the actions bound for `python` is also copied into the
//! bounded, read-only `shadow` pool. The rates are basis points (0..=10000) read live from etcd and default to 0.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::etcd_config::{self, KeyHandler};
use crate::priority_queue::Priority;

/// The bidi stream metadata header a worker sets to choose its pool.
pub const ENGINE_POOL_HEADER: &str = "osprey-engine-pool";

/// Rates are in basis points: 10000 means every action.
pub const BPS_SCALE: u32 = 10_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnginePool {
    Python,
    Rust,
    Shadow,
}

impl EnginePool {
    /// The pool a stream's header names: no header, or `python`, is python. `None` for a value naming no pool,
    /// which the caller also treats as python.
    pub fn from_header(value: Option<&str>) -> Option<EnginePool> {
        let Some(value) = value.map(str::trim) else {
            return Some(EnginePool::Python);
        };
        [EnginePool::Python, EnginePool::Rust, EnginePool::Shadow]
            .into_iter()
            .find(|pool| value.eq_ignore_ascii_case(pool.as_str()))
    }

    pub fn as_str(self) -> &'static str {
        match self {
            EnginePool::Python => "python",
            EnginePool::Rust => "rust",
            EnginePool::Shadow => "shadow",
        }
    }
}

/// SplitMix64's finalizer (Steele, Lea and Flood, 2014) over the action id. The constants are fixed, so an
/// action id has the same bucket in every process, restart and release, and a redelivered or retried action
/// lands on the same pool. Snowflake ids carry worker and sequence bits low, which is why the id is mixed first.
pub fn stable_hash(action_id: u64) -> u64 {
    let mut z = action_id.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Bucket in `0..BPS_SCALE` deciding `rust` vs `python`.
pub fn routing_bucket(action_id: u64) -> u32 {
    (stable_hash(action_id) % BPS_SCALE as u64) as u32
}

/// Bucket deciding the shadow sample; rehashed so the sample is independent of the routing split.
pub fn shadow_bucket(action_id: u64) -> u32 {
    (stable_hash(stable_hash(action_id)) % BPS_SCALE as u64) as u32
}

/// Live routing rates. Every field defaults to 0: all actions go to `python` and no shadow copies are made.
#[derive(Debug, Default)]
pub struct EngineRoutingFlags {
    rust_bps_async: AtomicU32,
    rust_bps_sync: AtomicU32,
    shadow_bps_async: AtomicU32,
    shadow_bps_sync: AtomicU32,
}

impl EngineRoutingFlags {
    pub fn rust_bps(&self, priority: Priority) -> u32 {
        match priority {
            Priority::Sync => self.rust_bps_sync.load(Ordering::Relaxed),
            Priority::Async => self.rust_bps_async.load(Ordering::Relaxed),
        }
    }

    pub fn shadow_bps(&self, priority: Priority) -> u32 {
        match priority {
            Priority::Sync => self.shadow_bps_sync.load(Ordering::Relaxed),
            Priority::Async => self.shadow_bps_async.load(Ordering::Relaxed),
        }
    }

    /// Sets the flag named `name` (the last etcd key segment). A missing or unparsable value is 0; values above
    /// `BPS_SCALE` are clamped. Returns false for an unknown name.
    pub fn set(&self, name: &str, value: Option<&str>) -> bool {
        let field = match name {
            "rust_bps_async" => &self.rust_bps_async,
            "rust_bps_sync" => &self.rust_bps_sync,
            "shadow_bps_async" => &self.shadow_bps_async,
            "shadow_bps_sync" => &self.shadow_bps_sync,
            _ => return false,
        };
        let bps = etcd_config::convert_to_u32(value).min(BPS_SCALE);
        field.store(bps, Ordering::Relaxed);
        tracing::info!({flag = name, bps = bps}, "engine routing flag updated");
        true
    }
}

struct EngineRoutingKeys {
    root: String,
    flags: Arc<EngineRoutingFlags>,
}

impl KeyHandler for EngineRoutingKeys {
    fn handle_key_updated(&self, key: &str, value: Option<&str>) {
        let name = key
            .strip_prefix(self.root.as_str())
            .and_then(|rest| rest.strip_prefix('/'));
        match name {
            Some(name) if self.flags.set(name, value) => {}
            _ => tracing::warn!({ key = key }, "unknown engine routing key"),
        }
    }
}

/// Watches `root` in etcd and keeps `flags` current. Until the watch starts (no etcd, or `ETCD_PEERS` unset)
/// the flags keep their values, which start at 0; failures retry in the background and never block startup.
pub fn spawn_engine_routing_watcher(root: String, flags: Arc<EngineRoutingFlags>) {
    let root = root.trim_end_matches('/').to_string();
    tokio::spawn(async move {
        loop {
            let handler = Arc::new(EngineRoutingKeys {
                root: root.clone(),
                flags: flags.clone(),
            });
            match etcd_config::run_etcd_watcher(root.clone(), handler).await {
                Ok(()) => {
                    tracing::info!({root = %root}, "watching engine routing flags");
                    return;
                }
                Err(error) => {
                    tracing::warn!({root = %root, error = %error}, "engine routing watch failed, retrying in 30s");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_hash_values_are_pinned() {
        // Changing these moves live traffic between pools; never update them.
        assert_eq!(stable_hash(0), 0xE220_A839_7B1D_CDAF);
        assert_eq!(
            routing_bucket(0),
            (0xE220_A839_7B1D_CDAFu64 % 10_000) as u32
        );
        assert_eq!(
            routing_bucket(1_227_039_953_469_571_133),
            routing_bucket(1_227_039_953_469_571_133)
        );
    }

    #[test]
    fn buckets_are_roughly_uniform_on_snowflake_ids() {
        // Consecutive snowflakes from one worker differ only in the low sequence bits.
        let base: u64 = 1_227_039_953_469_571_072;
        let n = 100_000u64;
        let under_half = (0..n)
            .filter(|i| routing_bucket(base + i) < BPS_SCALE / 2)
            .count() as f64;
        assert!((under_half / n as f64 - 0.5).abs() < 0.01, "{under_half}");
        let shadow_and_rust = (0..n)
            .filter(|i| routing_bucket(base + i) < 5_000 && shadow_bucket(base + i) < 1_000)
            .count() as f64;
        assert!(
            (shadow_and_rust / n as f64 - 0.05).abs() < 0.005,
            "{shadow_and_rust}"
        );
    }

    #[test]
    fn missing_or_unknown_header_means_python() {
        assert_eq!(EnginePool::from_header(None), Some(EnginePool::Python));
        assert_eq!(EnginePool::from_header(Some("")), None);
        assert_eq!(EnginePool::from_header(Some("java")), None);
        assert_eq!(
            EnginePool::from_header(Some("python")),
            Some(EnginePool::Python)
        );
        assert_eq!(
            EnginePool::from_header(Some("Rust")),
            Some(EnginePool::Rust)
        );
        assert_eq!(
            EnginePool::from_header(Some(" shadow ")),
            Some(EnginePool::Shadow)
        );
    }

    #[test]
    fn flags_default_to_zero_and_parse_from_etcd_keys() {
        let flags = Arc::new(EngineRoutingFlags::default());
        for p in [Priority::Sync, Priority::Async] {
            assert_eq!(flags.rust_bps(p), 0);
            assert_eq!(flags.shadow_bps(p), 0);
        }
        let keys = EngineRoutingKeys {
            root: "/config/osprey_coordinator/engine_routing".into(),
            flags: flags.clone(),
        };
        keys.handle_key_updated(
            "/config/osprey_coordinator/engine_routing/rust_bps_async",
            Some("\"250\""),
        );
        keys.handle_key_updated(
            "/config/osprey_coordinator/engine_routing/rust_bps_sync",
            Some("99999"),
        );
        keys.handle_key_updated(
            "/config/osprey_coordinator/engine_routing/shadow_bps_sync",
            Some("garbage"),
        );
        keys.handle_key_updated("/config/other/rust_bps_async", Some("10000"));
        assert_eq!(flags.rust_bps(Priority::Async), 250);
        assert_eq!(flags.rust_bps(Priority::Sync), BPS_SCALE);
        assert_eq!(flags.shadow_bps(Priority::Sync), 0);
        keys.handle_key_updated(
            "/config/osprey_coordinator/engine_routing/rust_bps_async",
            None,
        );
        assert_eq!(flags.rust_bps(Priority::Async), 0);
    }
}
