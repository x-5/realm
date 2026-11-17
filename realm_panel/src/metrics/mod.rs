use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sysinfo::{
    CpuRefreshKind, MemoryRefreshKind, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::config::reconciler::Reconciler;
use crate::probe::ProbeManager;
use crate::relay::LiveDeviceSnapshot;
use crate::storage::{PerfSample, Storage};

const RING_CAPACITY: usize = 3600;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointLiveMetric {
    pub endpoint_id: String,
    pub name: String,
    pub listen: String,
    pub remote: String,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub rx_bps: u64,
    pub tx_bps: u64,
    pub active_tcp_conns: u64,
    pub active_udp_assocs: u64,
    pub total_tcp_conns: u64,
    pub zero_copy_conns: u64,
    pub connect_errors: u64,
    pub last_rtt_us: u64,
    pub device_count: usize,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealtimeSnapshot {
    pub ts: i64,
    pub total_rx_bps: u64,
    pub total_tx_bps: u64,
    pub total_rx_bytes: u64,
    pub total_tx_bytes: u64,
    pub total_active_conns: u64,
    pub endpoints: Vec<EndpointLiveMetric>,
    pub devices: Vec<LiveDeviceSnapshot>,
    pub perf: PerfSample,
}

struct PendingBucket {
    rx_delta: u64,
    tx_delta: u64,
    peak_rx_bps: u64,
    peak_tx_bps: u64,
    active_conns_max: u64,
    errors_delta: u64,
}

#[derive(Clone)]
pub struct MetricsManager {
    ring: Arc<Mutex<VecDeque<RealtimeSnapshot>>>,
    latest: Arc<Mutex<Option<RealtimeSnapshot>>>,
    tx: broadcast::Sender<RealtimeSnapshot>,
}

impl MetricsManager {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(64);
        Self {
            ring: Arc::new(Mutex::new(VecDeque::with_capacity(RING_CAPACITY))),
            latest: Arc::new(Mutex::new(None)),
            tx,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RealtimeSnapshot> {
        self.tx.subscribe()
    }

    pub fn latest(&self) -> Option<RealtimeSnapshot> {
        self.latest.lock().unwrap().clone()
    }

    pub fn recent_series(&self, limit: usize) -> Vec<RealtimeSnapshot> {
        let ring = self.ring.lock().unwrap();
        let skip = ring.len().saturating_sub(limit);
        ring.iter().skip(skip).cloned().collect()
    }

    pub fn start_sampler(
        self,
        reconciler: Arc<Reconciler>,
        storage: Storage,
        _probe_mgr: ProbeManager,
        cancel: CancellationToken,
    ) {
        tokio::spawn(async move {
            let pid = sysinfo::get_current_pid().ok();
            let mut sys = System::new_with_specifics(
                RefreshKind::nothing()
                    .with_cpu(CpuRefreshKind::everything())
                    .with_memory(MemoryRefreshKind::everything()),
            );
            // Warm up CPU baseline
            sys.refresh_cpu_usage();
            if let Some(p) = pid {
                sys.refresh_processes_specifics(
                    ProcessesToUpdate::Some(&[p]),
                    true,
                    ProcessRefreshKind::nothing().with_cpu().with_memory(),
                );
            }

            let mut prev_errors: HashMap<String, u64> = HashMap::new();
            let mut pending: HashMap<String, PendingBucket> = HashMap::new();
            let mut sec_count: u64 = 0;

            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        let now = Utc::now().timestamp();
                        flush_pending(&storage, now, &mut pending);
                        break;
                    }
                    _ = ticker.tick() => {}
                }

                sec_count += 1;
                let now = Utc::now().timestamp();

                // Sample process & system performance
                let perf = collect_perf(&mut sys, pid, now);

                let mut ep_metrics = Vec::new();
                let mut all_devices = Vec::new();
                let mut total_rx_bps = 0u64;
                let mut total_tx_bps = 0u64;
                let mut total_rx_bytes = 0u64;
                let mut total_tx_bytes = 0u64;
                let mut total_active_conns = 0u64;

                {
                    let running = reconciler.running.lock().await;
                    for (id, entry) in running.iter() {
                        let stats = &entry.runtime.stats;
                        let rx = stats.rx_bytes.load(Ordering::Relaxed);
                        let tx = stats.tx_bytes.load(Ordering::Relaxed);
                        let prev_rx = stats.prev_rx_bytes.swap(rx, Ordering::Relaxed);
                        let prev_tx = stats.prev_tx_bytes.swap(tx, Ordering::Relaxed);
                        let rx_bps = rx.saturating_sub(prev_rx);
                        let tx_bps = tx.saturating_sub(prev_tx);
                        stats.rx_bps.store(rx_bps, Ordering::Relaxed);
                        stats.tx_bps.store(tx_bps, Ordering::Relaxed);

                        let active_tcp = stats.active_tcp_conns.load(Ordering::Relaxed);
                        let active_udp = stats.active_udp_assocs.load(Ordering::Relaxed);
                        let active = active_tcp + active_udp;
                        let total_tcp = stats.total_tcp_conns.load(Ordering::Relaxed);
                        let zero_copy = stats.zero_copy_conns.load(Ordering::Relaxed);
                        let errs = stats.connect_errors.load(Ordering::Relaxed);
                        let last_err_prev = prev_errors.insert(id.clone(), errs).unwrap_or(0);
                        let err_delta = errs.saturating_sub(last_err_prev);
                        let last_rtt_us = stats.last_rtt_us.load(Ordering::Relaxed);

                        // Tick live entry devices (prune disconnected older than 60s)
                        entry.runtime.devices.tick_1s(60);
                        let ep_devices = entry.runtime.devices.list_snapshots(id);
                        let device_count = ep_devices.iter().filter(|d| d.active_conns > 0).count();
                        all_devices.extend(ep_devices);

                        total_rx_bps += rx_bps;
                        total_tx_bps += tx_bps;
                        total_rx_bytes += rx;
                        total_tx_bytes += tx;
                        total_active_conns += active;

                        let bucket = pending.entry(id.clone()).or_insert(PendingBucket {
                            rx_delta: 0,
                            tx_delta: 0,
                            peak_rx_bps: 0,
                            peak_tx_bps: 0,
                            active_conns_max: 0,
                            errors_delta: 0,
                        });
                        bucket.rx_delta += rx_bps;
                        bucket.tx_delta += tx_bps;
                        bucket.peak_rx_bps = bucket.peak_rx_bps.max(rx_bps);
                        bucket.peak_tx_bps = bucket.peak_tx_bps.max(tx_bps);
                        bucket.active_conns_max = bucket.active_conns_max.max(active);
                        bucket.errors_delta += err_delta;

                        ep_metrics.push(EndpointLiveMetric {
                            endpoint_id: id.clone(),
                            name: entry.managed.meta.name.clone(),
                            listen: entry.managed.raw_conf.listen.clone(),
                            remote: entry.managed.raw_conf.remote.clone(),
                            rx_bytes: rx,
                            tx_bytes: tx,
                            rx_bps,
                            tx_bps,
                            active_tcp_conns: active_tcp,
                            active_udp_assocs: active_udp,
                            total_tcp_conns: total_tcp,
                            zero_copy_conns: zero_copy,
                            connect_errors: errs,
                            last_rtt_us,
                            device_count,
                            last_error: entry.runtime.get_error(),
                        });
                    }
                }

                ep_metrics.sort_by(|a, b| a.endpoint_id.cmp(&b.endpoint_id));
                all_devices.sort_by(|a, b| {
                    b.active_conns
                        .cmp(&a.active_conns)
                        .then_with(|| (b.rx_bps + b.tx_bps).cmp(&(a.rx_bps + a.tx_bps)))
                });

                let snap = RealtimeSnapshot {
                    ts: now,
                    total_rx_bps,
                    total_tx_bps,
                    total_rx_bytes,
                    total_tx_bytes,
                    total_active_conns,
                    endpoints: ep_metrics,
                    devices: all_devices,
                    perf: perf.clone(),
                };

                {
                    let mut ring = self.ring.lock().unwrap();
                    if ring.len() >= RING_CAPACITY {
                        ring.pop_front();
                    }
                    ring.push_back(snap.clone());
                }
                *self.latest.lock().unwrap() = Some(snap.clone());
                let _ = self.tx.send(snap);

                // Flush traffic buckets every 15s and perf samples every 60s
                if sec_count % 15 == 0 {
                    flush_pending(&storage, now, &mut pending);
                }
                if sec_count % 60 == 0 {
                    let _ = storage.insert_perf_sample(&perf);
                }
                if sec_count % 3600 == 0 {
                    let _ = storage.cleanup_retention();
                }
            }
        });
    }
}

fn flush_pending(storage: &Storage, ts: i64, pending: &mut HashMap<String, PendingBucket>) {
    for (ep_id, b) in pending.drain() {
        let _ = storage.record_traffic_delta(
            ts,
            &ep_id,
            b.rx_delta,
            b.tx_delta,
            b.peak_rx_bps,
            b.peak_tx_bps,
            b.active_conns_max,
            b.errors_delta,
        );
    }
}

fn collect_perf(sys: &mut System, pid: Option<sysinfo::Pid>, ts: i64) -> PerfSample {
    sys.refresh_cpu_usage();
    sys.refresh_memory();
    if let Some(p) = pid {
        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[p]),
            true,
            ProcessRefreshKind::nothing().with_cpu().with_memory(),
        );
    }

    let (proc_cpu_pct, proc_mem_rss_kb, proc_mem_vsz_kb) = pid
        .and_then(|p| sys.process(p))
        .map(|proc_info| {
            (
                proc_info.cpu_usage() as f64,
                proc_info.memory() / 1024,
                proc_info.virtual_memory() / 1024,
            )
        })
        .unwrap_or((0.0, 0, 0));

    let (open_fds, threads) = read_proc_self_counts();
    let fd_limit = read_fd_limit();

    let rt_metrics = tokio::runtime::Handle::current().metrics();
    let tokio_workers = rt_metrics.num_workers() as u64;
    let tokio_alive_tasks = rt_metrics.num_alive_tasks() as u64;

    PerfSample {
        ts,
        proc_cpu_pct,
        proc_mem_rss_kb,
        proc_mem_vsz_kb,
        open_fds,
        fd_limit,
        threads,
        tokio_workers,
        tokio_alive_tasks,
        sys_cpu_pct: sys.global_cpu_usage() as f64,
        sys_mem_used_kb: sys.used_memory() / 1024,
        sys_mem_total_kb: sys.total_memory() / 1024,
    }
}

fn read_proc_self_counts() -> (u64, u64) {
    #[cfg(target_os = "linux")]
    {
        let open_fds = std::fs::read_dir("/proc/self/fd")
            .map(|it| it.count() as u64)
            .unwrap_or(0);
        let threads = std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines().find_map(|line| {
                    line.strip_prefix("Threads:")
                        .and_then(|v| v.trim().parse::<u64>().ok())
                })
            })
            .unwrap_or(1);
        (open_fds, threads)
    }
    #[cfg(not(target_os = "linux"))]
    {
        (0, 1)
    }
}

fn read_fd_limit() -> u64 {
    #[cfg(all(unix, not(target_os = "android")))]
    {
        realm_syscall::get_nofile_limit()
            .map(|(soft, _)| soft)
            .unwrap_or(0)
    }
    #[cfg(not(all(unix, not(target_os = "android"))))]
    {
        0
    }
}
