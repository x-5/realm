use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use arc_swap::ArcSwap;
use chrono::Utc;
use hickory_resolver::config::{ResolverConfig, ResolverOpts};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use hickory_resolver::TokioResolver;
use realm::conf::{Config, DnsConf, FullConf};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::config::{
    fingerprint_endpoint, load_full_conf, sync_endpoints_with_storage, validate_endpoint_conf,
    validate_full_conf, write_conf_atomic, ConfigFormat, EndpointConfExt, EndpointPayload,
    ManagedEndpoint,
};
use crate::relay::{tcp, udp, EndpointRuntime, SharedResolver};
use crate::storage::{EndpointMeta, Storage};

pub struct RunningEntry {
    pub managed: ManagedEndpoint,
    pub runtime: Arc<EndpointRuntime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReloadReport {
    pub added: usize,
    pub updated: usize,
    pub removed: usize,
    pub unchanged: usize,
    pub disabled: usize,
    pub errors: Vec<String>,
}

pub struct Reconciler {
    pub config_path: PathBuf,
    pub storage: Storage,
    pub resolver: SharedResolver,
    pub lock: Mutex<()>,
    pub current_conf: Mutex<FullConf>,
    pub managed: Mutex<Vec<ManagedEndpoint>>,
    pub running: Mutex<HashMap<String, RunningEntry>>,
}

impl Reconciler {
    pub fn new(config_path: PathBuf, storage: Storage) -> Result<Arc<Self>> {
        let conf = load_full_conf(&config_path)?;
        let resolver = build_resolver(&conf.dns)?;
        let shared_resolver: SharedResolver = Arc::new(ArcSwap::from_pointee(resolver));

        Ok(Arc::new(Self {
            config_path,
            storage,
            resolver: shared_resolver,
            lock: Mutex::new(()),
            current_conf: Mutex::new(conf),
            managed: Mutex::new(Vec::new()),
            running: Mutex::new(HashMap::new()),
        }))
    }

    pub async fn initial_apply(&self) -> Result<ReloadReport> {
        let _guard = self.lock.lock().await;
        let conf = self.current_conf.lock().await.clone_via_json();
        self.apply_conf_locked(conf, "startup", false).await
    }

    /// Reload configuration from disk (triggered by file watcher, SIGHUP, or API).
    pub async fn reload_from_disk(&self, source: &str) -> Result<ReloadReport> {
        let _guard = self.lock.lock().await;
        let new_conf = load_full_conf(&self.config_path)?;
        validate_full_conf(&new_conf, None)?;
        self.apply_conf_locked(new_conf, source, true).await
    }

    pub async fn apply_raw_config_str(&self, raw: &str, source: &str) -> Result<ReloadReport> {
        let _guard = self.lock.lock().await;
        let new_conf = FullConf::from_conf_str(raw).map_err(|e| anyhow!("{}", e))?;
        validate_full_conf(&new_conf, None)?;
        let fmt = ConfigFormat::from_path(&self.config_path);
        write_conf_atomic(&self.config_path, &new_conf, fmt)?;
        self.apply_conf_locked(new_conf, source, true).await
    }

    pub async fn add_endpoint(&self, payload: EndpointPayload) -> Result<(ManagedEndpoint, ReloadReport)> {
        let _guard = self.lock.lock().await;
        validate_endpoint_conf(&payload.conf)?;

        let mut new_conf = self.current_conf.lock().await.clone_via_json();
        let idx = new_conf.endpoints.len() as i64;
        new_conf.endpoints.push(payload.conf.clone_via_json());
        validate_full_conf(&new_conf, None)?;

        let now = Utc::now().timestamp();
        let id = payload
            .id
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| format!("ep-{}", &uuid::Uuid::new_v4().to_string()[..8]));
        let name = payload
            .name
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| format!("{} -> {}", payload.conf.listen, payload.conf.remote));
        let meta = EndpointMeta {
            id: id.clone(),
            name,
            fingerprint: fingerprint_endpoint(&payload.conf),
            enabled: payload.enabled,
            probe_enabled: payload.probe_enabled,
            probe_interval_sec: payload.probe_interval_sec.max(3),
            probe_timeout_ms: payload.probe_timeout_ms.max(200),
            sort_order: idx,
            created_at: now,
            updated_at: now,
        };
        self.storage.upsert_endpoint_meta(&meta)?;

        let fmt = ConfigFormat::from_path(&self.config_path);
        write_conf_atomic(&self.config_path, &new_conf, fmt)?;
        let report = self.apply_conf_locked(new_conf, "web_add", true).await?;

        let created = self
            .managed
            .lock()
            .await
            .iter()
            .find(|m| m.meta.id == id)
            .cloned()
            .ok_or_else(|| anyhow!("newly added endpoint not found"))?;
        Ok((created, report))
    }

    pub async fn update_endpoint(
        &self,
        id: &str,
        payload: EndpointPayload,
    ) -> Result<(ManagedEndpoint, ReloadReport)> {
        let _guard = self.lock.lock().await;
        validate_endpoint_conf(&payload.conf)?;

        let current_managed = self.managed.lock().await.clone();
        let pos = current_managed
            .iter()
            .position(|m| m.meta.id == id)
            .ok_or_else(|| anyhow!("endpoint '{}' not found", id))?;

        let mut new_conf = self.current_conf.lock().await.clone_via_json();
        if pos >= new_conf.endpoints.len() {
            bail!("endpoint index out of sync");
        }
        new_conf.endpoints[pos] = payload.conf.clone_via_json();
        validate_full_conf(&new_conf, None)?;

        let mut meta = current_managed[pos].meta.clone();
        meta.name = payload
            .name
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(meta.name);
        meta.fingerprint = fingerprint_endpoint(&payload.conf);
        meta.enabled = payload.enabled;
        meta.probe_enabled = payload.probe_enabled;
        meta.probe_interval_sec = payload.probe_interval_sec.max(3);
        meta.probe_timeout_ms = payload.probe_timeout_ms.max(200);
        meta.updated_at = Utc::now().timestamp();
        self.storage.upsert_endpoint_meta(&meta)?;

        let fmt = ConfigFormat::from_path(&self.config_path);
        write_conf_atomic(&self.config_path, &new_conf, fmt)?;
        let report = self.apply_conf_locked(new_conf, "web_update", true).await?;

        let updated = self
            .managed
            .lock()
            .await
            .iter()
            .find(|m| m.meta.id == id)
            .cloned()
            .ok_or_else(|| anyhow!("updated endpoint not found"))?;
        Ok((updated, report))
    }

    pub async fn toggle_endpoint(&self, id: &str, enabled: bool) -> Result<ReloadReport> {
        let _guard = self.lock.lock().await;
        let current_managed = self.managed.lock().await.clone();
        let mut meta = current_managed
            .iter()
            .find(|m| m.meta.id == id)
            .map(|m| m.meta.clone())
            .ok_or_else(|| anyhow!("endpoint '{}' not found", id))?;
        meta.enabled = enabled;
        meta.updated_at = Utc::now().timestamp();
        self.storage.upsert_endpoint_meta(&meta)?;

        let conf = self.current_conf.lock().await.clone_via_json();
        self.apply_conf_locked(conf, "web_toggle", false).await
    }

    pub async fn delete_endpoint(&self, id: &str) -> Result<ReloadReport> {
        let _guard = self.lock.lock().await;
        let current_managed = self.managed.lock().await.clone();
        let pos = current_managed
            .iter()
            .position(|m| m.meta.id == id)
            .ok_or_else(|| anyhow!("endpoint '{}' not found", id))?;

        let mut new_conf = self.current_conf.lock().await.clone_via_json();
        new_conf.endpoints.remove(pos);
        self.storage.delete_endpoint_meta(id)?;

        let fmt = ConfigFormat::from_path(&self.config_path);
        write_conf_atomic(&self.config_path, &new_conf, fmt)?;
        self.apply_conf_locked(new_conf, "web_delete", true).await
    }

    pub async fn restore_snapshot(&self, snapshot_id: i64) -> Result<ReloadReport> {
        let snap = self
            .storage
            .get_snapshot(snapshot_id)?
            .ok_or_else(|| anyhow!("snapshot {} not found", snapshot_id))?;
        self.apply_raw_config_str(&snap.content, "rollback").await
    }

    pub async fn export_current_config(&self, fmt: ConfigFormat) -> Result<String> {
        let conf = self.current_conf.lock().await.clone_via_json();
        crate::config::serialize_full_conf(&conf, fmt)
    }

    async fn apply_conf_locked(
        &self,
        new_conf: FullConf,
        source: &str,
        save_snap: bool,
    ) -> Result<ReloadReport> {
        // Update DNS resolver dynamically if configured
        if let Ok(new_resv) = build_resolver(&new_conf.dns) {
            self.resolver.store(Arc::new(new_resv));
        }

        let desired_list = sync_endpoints_with_storage(&new_conf, &self.storage)?;
        let desired_map: HashMap<String, ManagedEndpoint> = desired_list
            .iter()
            .map(|m| (m.meta.id.clone(), m.clone()))
            .collect();

        let mut running = self.running.lock().await;
        let mut report = ReloadReport {
            added: 0,
            updated: 0,
            removed: 0,
            unchanged: 0,
            disabled: 0,
            errors: Vec::new(),
        };

        // 1. Identify removals, disables, and changes -> force-shutdown them first so ports are released
        let existing_ids: Vec<String> = running.keys().cloned().collect();
        let mut updated_ids = HashSet::new();

        for id in existing_ids {
            match desired_map.get(&id) {
                None => {
                    if let Some(old) = running.remove(&id) {
                        tracing::info!(endpoint_id = %id, "removing endpoint (force closing active connections)");
                        old.runtime.shutdown_force().await;
                        report.removed += 1;
                    }
                }
                Some(desired) if !desired.meta.enabled => {
                    if let Some(old) = running.remove(&id) {
                        tracing::info!(endpoint_id = %id, "disabling endpoint (force closing active connections)");
                        old.runtime.shutdown_force().await;
                    }
                }
                Some(desired) => {
                    let existing = running.get(&id).unwrap();
                    if existing.managed.effective_fingerprint != desired.effective_fingerprint {
                        if let Some(old) = running.remove(&id) {
                            tracing::info!(endpoint_id = %id, "updating endpoint (force closing active connections)");
                            old.runtime.shutdown_force().await;
                            updated_ids.insert(id);
                            report.updated += 1;
                        }
                    } else {
                        // Metadata (e.g. name, probe settings) might have changed without changing relay config
                        running.get_mut(&id).unwrap().managed = desired.clone();
                        report.unchanged += 1;
                    }
                }
            }
        }

        // Brief yield so OS kernel finishes closing unbound sockets before rebinding updated ports
        if report.updated > 0 || report.removed > 0 {
            tokio::task::yield_now().await;
        }

        // 2. Start added or updated enabled endpoints
        for desired in &desired_list {
            if !desired.meta.enabled {
                report.disabled += 1;
                continue;
            }
            if running.contains_key(&desired.meta.id) {
                continue;
            }
            let rt = start_endpoint(desired.clone(), self.resolver.clone());
            if let Some(err) = rt.get_error() {
                report.errors.push(format!("{}: {}", desired.meta.id, err));
            }
            if !updated_ids.contains(&desired.meta.id) {
                report.added += 1;
            }
            running.insert(
                desired.meta.id.clone(),
                RunningEntry {
                    managed: desired.clone(),
                    runtime: rt,
                },
            );
        }

        if save_snap {
            let fmt = ConfigFormat::from_path(&self.config_path);
            if let Ok(text) = crate::config::serialize_full_conf(&new_conf, fmt) {
                let _ = self.storage.save_snapshot(source, fmt.as_str(), &text);
            }
        }

        *self.current_conf.lock().await = new_conf;
        *self.managed.lock().await = desired_list;

        tracing::info!(
            source = %source,
            added = report.added,
            updated = report.updated,
            removed = report.removed,
            unchanged = report.unchanged,
            disabled = report.disabled,
            "configuration reconciled"
        );

        Ok(report)
    }

    pub async fn shutdown_all(&self) {
        let mut running = self.running.lock().await;
        for (_, entry) in running.drain() {
            entry.runtime.shutdown_force().await;
        }
    }
}

fn start_endpoint(managed: ManagedEndpoint, resolver: SharedResolver) -> Arc<EndpointRuntime> {
    let runtime = EndpointRuntime::new(managed.meta.id.clone());
    let (core_ep, no_tcp, use_udp) = managed.build_core_endpoint();

    let mut handles = Vec::new();
    if !no_tcp {
        let rt = runtime.clone();
        let ep = core_ep.clone();
        let resv = resolver.clone();
        handles.push(tokio::spawn(async move {
            tcp::run_tcp(rt, ep, resv).await;
        }));
    }
    if use_udp {
        let rt = runtime.clone();
        let ep = core_ep;
        let resv = resolver;
        handles.push(tokio::spawn(async move {
            udp::run_udp(rt, ep, resv).await;
        }));
    }

    *runtime.tasks.lock().unwrap() = handles;
    runtime
}

pub fn build_resolver(dns_conf: &DnsConf) -> Result<TokioResolver> {
    let (conf_opt, opts_opt) = dns_conf.clone().build();
    let default_conf = realm_core::dns::DnsConf::default();
    let conf: ResolverConfig = conf_opt.unwrap_or(default_conf.conf);
    let opts: ResolverOpts = opts_opt.unwrap_or(default_conf.opts);
    Ok(
        TokioResolver::builder_with_config(conf, TokioRuntimeProvider::default())
            .with_options(opts)
            .build()?,
    )
}

pub trait FullConfCloneExt {
    fn clone_via_json(&self) -> FullConf;
}

impl FullConfCloneExt for FullConf {
    fn clone_via_json(&self) -> FullConf {
        let v = serde_json::to_value(self).expect("serialize FullConf");
        serde_json::from_value(v).expect("deserialize FullConf")
    }
}

#[allow(dead_code)]
pub fn is_single_file(path: &Path) -> bool {
    !path.is_dir()
}
