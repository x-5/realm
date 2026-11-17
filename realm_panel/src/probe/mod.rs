use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use dashmap::DashMap;
use realm_core::endpoint::RemoteAddr;
use realm_syscall::new_tcp_socket;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::config::reconciler::Reconciler;
use crate::relay::tcp::resolve_remote;
use crate::relay::SharedResolver;
use crate::storage::{ProbeRecord, Storage};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetProbeStatus {
    pub endpoint_id: String,
    pub target: String,
    pub protocol: String,
    pub reachable: bool,
    pub dns_ms: Option<u64>,
    pub rtt_ms: Option<u64>,
    pub resolved_ip: Option<String>,
    pub consecutive_failures: u32,
    pub last_checked_ts: i64,
    pub error_msg: Option<String>,
}

#[derive(Clone)]
pub struct ProbeManager {
    latest: Arc<DashMap<(String, String), TargetProbeStatus>>,
    last_run: Arc<DashMap<String, i64>>,
}

impl ProbeManager {
    pub fn new() -> Self {
        Self {
            latest: Arc::new(DashMap::new()),
            last_run: Arc::new(DashMap::new()),
        }
    }

    pub fn list_latest(&self) -> Vec<TargetProbeStatus> {
        let mut out: Vec<TargetProbeStatus> =
            self.latest.iter().map(|r| r.value().clone()).collect();
        out.sort_by(|a, b| a.endpoint_id.cmp(&b.endpoint_id).then(a.target.cmp(&b.target)));
        out
    }

    pub fn list_for_endpoint(&self, endpoint_id: &str) -> Vec<TargetProbeStatus> {
        self.latest
            .iter()
            .filter(|r| r.key().0 == endpoint_id)
            .map(|r| r.value().clone())
            .collect()
    }

    pub fn start_scheduler(self, reconciler: Arc<Reconciler>, storage: Storage, cancel: CancellationToken) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(3));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    _ = ticker.tick() => {}
                }

                let now = Utc::now().timestamp();
                let managed = reconciler.managed.lock().await.clone();

                // Clean up probe entries for deleted endpoints
                let active_ids: std::collections::HashSet<&str> =
                    managed.iter().map(|m| m.meta.id.as_str()).collect();
                let stale_keys: Vec<(String, String)> = self
                    .latest
                    .iter()
                    .filter(|r| !active_ids.contains(r.key().0.as_str()))
                    .map(|r| r.key().clone())
                    .collect();
                for k in stale_keys {
                    self.latest.remove(&k);
                }

                for ep in managed {
                    if !ep.meta.enabled || !ep.meta.probe_enabled {
                        continue;
                    }
                    let interval = ep.meta.probe_interval_sec.max(3) as i64;
                    let prev = self.last_run.get(&ep.meta.id).map(|v| *v).unwrap_or(0);
                    if now - prev < interval {
                        continue;
                    }
                    self.last_run.insert(ep.meta.id.clone(), now);

                    let mgr = self.clone();
                    let st = storage.clone();
                    let resv = reconciler.resolver.clone();
                    tokio::spawn(async move {
                        mgr.probe_endpoint(&ep, &resv, &st).await;
                    });
                }
            }
        });
    }

    pub async fn probe_endpoint_now(
        &self,
        reconciler: &Reconciler,
        storage: &Storage,
        endpoint_id: &str,
    ) -> anyhow::Result<Vec<TargetProbeStatus>> {
        let managed = reconciler.managed.lock().await.clone();
        let ep = managed
            .into_iter()
            .find(|m| m.meta.id == endpoint_id)
            .ok_or_else(|| anyhow::anyhow!("endpoint '{}' not found", endpoint_id))?;
        Ok(self.probe_endpoint(&ep, &reconciler.resolver, storage).await)
    }

    async fn probe_endpoint(
        &self,
        ep: &crate::config::ManagedEndpoint,
        resolver: &SharedResolver,
        storage: &Storage,
    ) -> Vec<TargetProbeStatus> {
        let (core_ep, no_tcp, use_udp) = ep.build_core_endpoint();
        let timeout_dur = Duration::from_millis(ep.meta.probe_timeout_ms.max(200));
        let mut targets: Vec<(&RemoteAddr, String)> = vec![(&core_ep.raddr, ep.raw_conf.remote.clone())];
        for (extra, raw_s) in core_ep.extra_raddrs.iter().zip(ep.raw_conf.extra_remotes.iter()) {
            targets.push((extra, raw_s.clone()));
        }

        let mut results = Vec::with_capacity(targets.len());
        let proto = if !no_tcp {
            "tcp"
        } else if use_udp {
            "udp"
        } else {
            "tcp"
        };

        for (raddr, target_label) in targets {
            let status = probe_single_target(
                &ep.meta.id,
                &target_label,
                raddr,
                proto,
                core_ep.conn_opts.bind_address,
                core_ep.conn_opts.bind_interface.as_deref(),
                timeout_dur,
                resolver,
                &self.latest,
            )
            .await;

            let _ = storage.insert_probe_result(&ProbeRecord {
                ts: status.last_checked_ts,
                endpoint_id: status.endpoint_id.clone(),
                target: status.target.clone(),
                protocol: status.protocol.clone(),
                reachable: status.reachable,
                dns_ms: status.dns_ms,
                rtt_ms: status.rtt_ms,
                error_msg: status.error_msg.clone(),
            });

            results.push(status);
        }
        results
    }
}

#[allow(clippy::too_many_arguments)]
async fn probe_single_target(
    endpoint_id: &str,
    target_label: &str,
    raddr: &RemoteAddr,
    proto: &str,
    bind_addr: Option<SocketAddr>,
    bind_iface: Option<&str>,
    timeout_dur: Duration,
    resolver: &SharedResolver,
    latest: &DashMap<(String, String), TargetProbeStatus>,
) -> TargetProbeStatus {
    let now = Utc::now().timestamp();
    let dns_start = Instant::now();
    let resolved = match timeout(timeout_dur, resolve_remote(resolver, raddr)).await {
        Ok(Ok(addrs)) => addrs,
        Ok(Err(e)) => {
            return record_status(
                latest,
                endpoint_id,
                target_label,
                proto,
                false,
                Some(dns_start.elapsed().as_millis() as u64),
                None,
                None,
                Some(format!("dns error: {e}")),
                now,
            );
        }
        Err(_) => {
            return record_status(
                latest,
                endpoint_id,
                target_label,
                proto,
                false,
                Some(dns_start.elapsed().as_millis() as u64),
                None,
                None,
                Some("dns timeout".to_string()),
                now,
            );
        }
    };
    let dns_ms = dns_start.elapsed().as_millis() as u64;

    if proto == "udp" {
        // UDP is connectionless: verify DNS resolution & socket creation/connect (route check)
        let addr = resolved[0];
        let rtt_start = Instant::now();
        let udp_res = tokio::net::UdpSocket::bind(match addr {
            SocketAddr::V4(_) => "0.0.0.0:0",
            SocketAddr::V6(_) => "[::]:0",
        })
        .await;
        match udp_res {
            Ok(sock) => match sock.connect(addr).await {
                Ok(()) => record_status(
                    latest,
                    endpoint_id,
                    target_label,
                    "udp",
                    true,
                    Some(dns_ms),
                    Some(rtt_start.elapsed().as_millis() as u64),
                    Some(addr.to_string()),
                    None,
                    now,
                ),
                Err(e) => record_status(
                    latest,
                    endpoint_id,
                    target_label,
                    "udp",
                    false,
                    Some(dns_ms),
                    None,
                    Some(addr.to_string()),
                    Some(format!("udp route error: {e}")),
                    now,
                ),
            },
            Err(e) => record_status(
                latest,
                endpoint_id,
                target_label,
                "udp",
                false,
                Some(dns_ms),
                None,
                Some(addr.to_string()),
                Some(format!("udp socket error: {e}")),
                now,
            ),
        }
    } else {
        let mut last_err = None;
        let mut last_ip = None;
        for addr in resolved {
            last_ip = Some(addr.to_string());
            let rtt_start = Instant::now();
            let sock = match new_tcp_socket(&addr) {
                Ok(s) => s,
                Err(e) => {
                    last_err = Some(e.to_string());
                    continue;
                }
            };
            if let Some(ba) = bind_addr {
                if let Err(e) = sock.bind(&ba.into()) {
                    last_err = Some(format!("bind {ba}: {e}"));
                    continue;
                }
            }
            #[cfg(target_os = "linux")]
            if let Some(iface) = bind_iface {
                if let Err(e) = realm_syscall::bind_to_device(&sock, iface) {
                    last_err = Some(format!("bind_to_device {iface}: {e}"));
                    continue;
                }
            }
            let std_stream: std::net::TcpStream = sock.into();
            let tokio_sock = match tokio::net::TcpSocket::from_std_stream(std_stream) {
                s => s,
            };
            match timeout(timeout_dur, tokio_sock.connect(addr)).await {
                Ok(Ok(stream)) => {
                    let rtt_ms = rtt_start.elapsed().as_millis() as u64;
                    drop(stream);
                    return record_status(
                        latest,
                        endpoint_id,
                        target_label,
                        "tcp",
                        true,
                        Some(dns_ms),
                        Some(rtt_ms),
                        Some(addr.to_string()),
                        None,
                        now,
                    );
                }
                Ok(Err(e)) => last_err = Some(e.to_string()),
                Err(_) => last_err = Some(format!("tcp connect timeout ({}ms)", timeout_dur.as_millis())),
            }
        }

        record_status(
            latest,
            endpoint_id,
            target_label,
            "tcp",
            false,
            Some(dns_ms),
            None,
            last_ip,
            last_err.or_else(|| Some("unreachable".to_string())),
            now,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn record_status(
    latest: &DashMap<(String, String), TargetProbeStatus>,
    endpoint_id: &str,
    target: &str,
    protocol: &str,
    reachable: bool,
    dns_ms: Option<u64>,
    rtt_ms: Option<u64>,
    resolved_ip: Option<String>,
    error_msg: Option<String>,
    now: i64,
) -> TargetProbeStatus {
    let key = (endpoint_id.to_string(), target.to_string());
    let prev = latest.get(&key).map(|r| r.clone());
    let consecutive_failures = if reachable {
        0
    } else {
        prev.as_ref().map(|p| p.consecutive_failures + 1).unwrap_or(1)
    };

    if let Some(ref prev_s) = prev {
        if prev_s.reachable && !reachable {
            tracing::warn!(
                endpoint_id = %endpoint_id,
                target = %target,
                error = ?error_msg,
                "target port became unreachable"
            );
        } else if !prev_s.reachable && reachable {
            tracing::info!(
                endpoint_id = %endpoint_id,
                target = %target,
                rtt_ms = ?rtt_ms,
                "target port recovered"
            );
        }
    } else if !reachable {
        tracing::warn!(
            endpoint_id = %endpoint_id,
            target = %target,
            error = ?error_msg,
            "initial target probe failed"
        );
    }

    let st = TargetProbeStatus {
        endpoint_id: endpoint_id.to_string(),
        target: target.to_string(),
        protocol: protocol.to_string(),
        reachable,
        dns_ms,
        rtt_ms,
        resolved_ip,
        consecutive_failures,
        last_checked_ts: now,
        error_msg,
    };
    latest.insert(key, st.clone());
    st
}

#[allow(dead_code)]
fn _unused_types(_: HashMap<String, ()>, _: TcpStream) {}
