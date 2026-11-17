pub mod metered_io;
pub mod tcp;
pub mod udp;

use std::collections::VecDeque;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use dashmap::DashMap;
use hickory_resolver::TokioResolver;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Proto {
    Tcp,
    Udp,
}

impl Proto {
    pub fn as_str(&self) -> &'static str {
        match self {
            Proto::Tcp => "tcp",
            Proto::Udp => "udp",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeviceKey {
    pub ip: IpAddr,
    pub proto: Proto,
}

pub struct DeviceEntry {
    pub ip: IpAddr,
    pub proto: Proto,
    pub active_conns: AtomicU64,
    pub total_conns: AtomicU64,
    pub rx_bytes: Arc<AtomicU64>,
    pub tx_bytes: Arc<AtomicU64>,
    pub first_seen_ts: i64,
    pub last_active_ts: AtomicI64,
    pub recent_ports: Mutex<VecDeque<u16>>,
    // Sampler scratch fields for 1s rate calculation
    pub prev_rx_bytes: AtomicU64,
    pub prev_tx_bytes: AtomicU64,
    pub rx_bps: AtomicU64,
    pub tx_bps: AtomicU64,
}

impl DeviceEntry {
    pub fn new(ip: IpAddr, proto: Proto, port: u16) -> Self {
        let now = Utc::now().timestamp();
        let mut ports = VecDeque::with_capacity(8);
        ports.push_back(port);
        Self {
            ip,
            proto,
            active_conns: AtomicU64::new(0),
            total_conns: AtomicU64::new(0),
            rx_bytes: Arc::new(AtomicU64::new(0)),
            tx_bytes: Arc::new(AtomicU64::new(0)),
            first_seen_ts: now,
            last_active_ts: AtomicI64::new(now),
            recent_ports: Mutex::new(ports),
            prev_rx_bytes: AtomicU64::new(0),
            prev_tx_bytes: AtomicU64::new(0),
            rx_bps: AtomicU64::new(0),
            tx_bps: AtomicU64::new(0),
        }
    }

    pub fn record_port(&self, port: u16) {
        self.last_active_ts.store(Utc::now().timestamp(), Ordering::Relaxed);
        let mut q = self.recent_ports.lock().unwrap();
        if q.back() != Some(&port) {
            if q.len() >= 8 {
                q.pop_front();
            }
            q.push_back(port);
        }
    }

    pub fn snapshot(&self, endpoint_id: &str) -> LiveDeviceSnapshot {
        let ports: Vec<u16> = self.recent_ports.lock().unwrap().iter().copied().collect();
        LiveDeviceSnapshot {
            endpoint_id: endpoint_id.to_string(),
            ip: self.ip.to_string(),
            proto: self.proto.as_str().to_string(),
            active_conns: self.active_conns.load(Ordering::Relaxed),
            total_conns: self.total_conns.load(Ordering::Relaxed),
            rx_bytes: self.rx_bytes.load(Ordering::Relaxed),
            tx_bytes: self.tx_bytes.load(Ordering::Relaxed),
            rx_bps: self.rx_bps.load(Ordering::Relaxed),
            tx_bps: self.tx_bps.load(Ordering::Relaxed),
            first_seen_ts: self.first_seen_ts,
            last_active_ts: self.last_active_ts.load(Ordering::Relaxed),
            recent_ports: ports,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveDeviceSnapshot {
    pub endpoint_id: String,
    pub ip: String,
    pub proto: String,
    pub active_conns: u64,
    pub total_conns: u64,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub rx_bps: u64,
    pub tx_bps: u64,
    pub first_seen_ts: i64,
    pub last_active_ts: i64,
    pub recent_ports: Vec<u16>,
}

#[derive(Default)]
pub struct DeviceTracker {
    devices: DashMap<DeviceKey, Arc<DeviceEntry>>,
}

impl DeviceTracker {
    pub fn on_connect(&self, peer: SocketAddr, proto: Proto) -> Arc<DeviceEntry> {
        let key = DeviceKey {
            ip: peer.ip(),
            proto,
        };
        let entry = self
            .devices
            .entry(key)
            .or_insert_with(|| Arc::new(DeviceEntry::new(peer.ip(), proto, peer.port())))
            .clone();
        entry.active_conns.fetch_add(1, Ordering::Relaxed);
        entry.total_conns.fetch_add(1, Ordering::Relaxed);
        entry.record_port(peer.port());
        entry
    }

    pub fn get_or_touch_udp(&self, peer: SocketAddr) -> Arc<DeviceEntry> {
        let key = DeviceKey {
            ip: peer.ip(),
            proto: Proto::Udp,
        };
        let entry = self
            .devices
            .entry(key)
            .or_insert_with(|| Arc::new(DeviceEntry::new(peer.ip(), Proto::Udp, peer.port())))
            .clone();
        entry.record_port(peer.port());
        entry
    }

    pub fn on_disconnect(entry: &DeviceEntry) {
        entry.last_active_ts.store(Utc::now().timestamp(), Ordering::Relaxed);
        let _ = entry
            .active_conns
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(1)));
    }

    /// Update 1s speeds and prune idle disconnected devices older than `idle_ttl_secs`.
    pub fn tick_1s(&self, idle_ttl_secs: i64) {
        let now = Utc::now().timestamp();
        let mut to_remove = Vec::new();
        for item in self.devices.iter() {
            let entry = item.value();
            let rx = entry.rx_bytes.load(Ordering::Relaxed);
            let tx = entry.tx_bytes.load(Ordering::Relaxed);
            let prev_rx = entry.prev_rx_bytes.swap(rx, Ordering::Relaxed);
            let prev_tx = entry.prev_tx_bytes.swap(tx, Ordering::Relaxed);
            entry.rx_bps.store(rx.saturating_sub(prev_rx), Ordering::Relaxed);
            entry.tx_bps.store(tx.saturating_sub(prev_tx), Ordering::Relaxed);

            if entry.active_conns.load(Ordering::Relaxed) == 0
                && now - entry.last_active_ts.load(Ordering::Relaxed) > idle_ttl_secs
            {
                to_remove.push(item.key().clone());
            }
        }
        for k in to_remove {
            self.devices.remove(&k);
        }
    }

    pub fn list_snapshots(&self, endpoint_id: &str) -> Vec<LiveDeviceSnapshot> {
        let mut list: Vec<LiveDeviceSnapshot> = self
            .devices
            .iter()
            .map(|r| r.value().snapshot(endpoint_id))
            .collect();
        list.sort_by(|a, b| {
            b.active_conns
                .cmp(&a.active_conns)
                .then_with(|| (b.rx_bps + b.tx_bps).cmp(&(a.rx_bps + a.tx_bps)))
                .then_with(|| b.last_active_ts.cmp(&a.last_active_ts))
        });
        list
    }
}

#[derive(Default)]
pub struct EndpointStats {
    pub rx_bytes: Arc<AtomicU64>,
    pub tx_bytes: Arc<AtomicU64>,
    pub active_tcp_conns: AtomicU64,
    pub total_tcp_conns: AtomicU64,
    pub active_udp_assocs: AtomicU64,
    pub zero_copy_conns: AtomicU64,
    pub connect_errors: AtomicU64,
    pub last_rtt_us: AtomicU64,
    // Sampler scratch for 1s rate
    pub prev_rx_bytes: AtomicU64,
    pub prev_tx_bytes: AtomicU64,
    pub rx_bps: AtomicU64,
    pub tx_bps: AtomicU64,
}

pub struct EndpointRuntime {
    pub id: String,
    pub stats: Arc<EndpointStats>,
    pub devices: Arc<DeviceTracker>,
    pub cancel: CancellationToken,
    pub last_error: Mutex<Option<String>>,
    pub tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl EndpointRuntime {
    pub fn new(id: String) -> Arc<Self> {
        Arc::new(Self {
            id,
            stats: Arc::new(EndpointStats::default()),
            devices: Arc::new(DeviceTracker::default()),
            cancel: CancellationToken::new(),
            last_error: Mutex::new(None),
            tasks: Mutex::new(Vec::new()),
        })
    }

    pub fn set_error(&self, err: Option<String>) {
        *self.last_error.lock().unwrap() = err;
    }

    pub fn get_error(&self) -> Option<String> {
        self.last_error.lock().unwrap().clone()
    }

    /// Force-stop listener and all active connections/associations immediately.
    pub async fn shutdown_force(&self) {
        self.cancel.cancel();
        let handles: Vec<JoinHandle<()>> = {
            let mut guard = self.tasks.lock().unwrap();
            std::mem::take(&mut *guard)
        };
        for h in handles {
            let _ = h.await;
        }
        self.stats.active_tcp_conns.store(0, Ordering::Relaxed);
        self.stats.active_udp_assocs.store(0, Ordering::Relaxed);
    }
}

pub type SharedResolver = Arc<arc_swap::ArcSwap<TokioResolver>>;
