pub mod reconciler;

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::Write;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use realm::conf::{Config, DnsConf, EndpointConf, FullConf, LogConf, NetConf};
use realm_core::endpoint::Endpoint;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::storage::{EndpointMeta, Storage};

#[derive(Debug, Serialize, Deserialize)]
pub struct EndpointPayload {
    pub id: Option<String>,
    pub name: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub probe_enabled: bool,
    #[serde(default = "default_probe_interval")]
    pub probe_interval_sec: u64,
    #[serde(default = "default_probe_timeout")]
    pub probe_timeout_ms: u64,
    #[serde(flatten)]
    pub conf: EndpointConf,
}

impl Clone for EndpointPayload {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            name: self.name.clone(),
            enabled: self.enabled,
            probe_enabled: self.probe_enabled,
            probe_interval_sec: self.probe_interval_sec,
            probe_timeout_ms: self.probe_timeout_ms,
            conf: self.conf.clone_via_json(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_probe_interval() -> u64 {
    15
}
fn default_probe_timeout() -> u64 {
    3000
}

#[derive(Debug)]
pub struct ManagedEndpoint {
    pub meta: EndpointMeta,
    pub raw_conf: EndpointConf,
    pub effective_conf: EndpointConf,
    pub effective_fingerprint: String,
}

impl Clone for ManagedEndpoint {
    fn clone(&self) -> Self {
        Self {
            meta: self.meta.clone(),
            raw_conf: self.raw_conf.clone_via_json(),
            effective_conf: self.effective_conf.clone_via_json(),
            effective_fingerprint: self.effective_fingerprint.clone(),
        }
    }
}

impl ManagedEndpoint {
    pub fn build_core_endpoint(&self) -> (Endpoint, bool, bool) {
        let info = self.effective_conf.clone_via_json().build();
        (info.endpoint, info.no_tcp, info.use_udp)
    }
}

pub trait EndpointConfExt {
    fn clone_via_json(&self) -> EndpointConf;
}

impl EndpointConfExt for EndpointConf {
    fn clone_via_json(&self) -> EndpointConf {
        let v = serde_json::to_value(self).expect("serialize EndpointConf");
        serde_json::from_value(v).expect("deserialize EndpointConf")
    }
}

pub fn fingerprint_endpoint(ep: &EndpointConf) -> String {
    let json = serde_json::to_string(ep).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(json.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn validate_remote_str(remote: &str) -> Result<()> {
    if remote.parse::<SocketAddr>().is_ok() {
        return Ok(());
    }
    let mut iter = remote.rsplitn(2, ':');
    let port_str = iter
        .next()
        .ok_or_else(|| anyhow!("remote '{remote}' missing port"))?;
    let host = iter
        .next()
        .ok_or_else(|| anyhow!("remote '{remote}' missing host"))?;
    if host.trim().is_empty() {
        bail!("remote '{remote}' has empty host");
    }
    let port: u16 = port_str
        .parse()
        .with_context(|| format!("invalid port in remote '{remote}'"))?;
    if port == 0 {
        bail!("remote '{remote}' port cannot be 0");
    }
    Ok(())
}

pub fn validate_endpoint_conf(ep: &EndpointConf) -> Result<SocketAddr> {
    let laddr = ep
        .listen
        .to_socket_addrs()
        .with_context(|| format!("invalid listen address '{}'", ep.listen))?
        .next()
        .ok_or_else(|| anyhow!("listen address '{}' resolved to nothing", ep.listen))?;

    validate_remote_str(&ep.remote)?;
    for extra in &ep.extra_remotes {
        validate_remote_str(extra)?;
    }

    if let Some(ref bal) = ep.balance {
        let (strategy, weights_str) = bal
            .split_once(':')
            .ok_or_else(|| anyhow!("invalid balance format '{}', expected 'strategy: w1, w2'", bal))?;
        let s = strategy.trim();
        if !matches!(s, "off" | "iphash" | "roundrobin") {
            bail!("unsupported balance strategy '{}'", s);
        }
        let weights: Vec<u8> = weights_str
            .trim()
            .split(',')
            .filter(|x| !x.trim().is_empty())
            .map(|x| x.trim().parse::<u8>())
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| format!("invalid balance weights in '{}'", bal))?;
        let total_remotes = 1 + ep.extra_remotes.len();
        if s != "off" && !weights.is_empty() && weights.len() > total_remotes {
            bail!(
                "balance has {} weights but only {} remote targets",
                weights.len(),
                total_remotes
            );
        }
    }

    if let Some(ref through) = ep.through {
        if through.to_socket_addrs().is_err() {
            let mut ipstr = through.clone();
            ipstr.retain(|c| c != '[' && c != ']');
            ipstr
                .parse::<IpAddr>()
                .with_context(|| format!("invalid through address '{}'", through))?;
        }
    }

    if let Some(ref lt) = ep.listen_transport {
        use realm_core::kaminari::opt::{get_tls_server_conf, get_ws_conf};
        if !lt.trim().is_empty() && get_ws_conf(lt).is_none() && get_tls_server_conf(lt).is_none() {
            bail!("invalid listen_transport '{}'", lt);
        }
    }
    if let Some(ref rt) = ep.remote_transport {
        use realm_core::kaminari::opt::{get_tls_client_conf, get_ws_conf};
        if !rt.trim().is_empty() && get_ws_conf(rt).is_none() && get_tls_client_conf(rt).is_none() {
            bail!("invalid remote_transport '{}'", rt);
        }
    }

    Ok(laddr)
}

pub fn validate_full_conf(conf: &FullConf, disabled_ids: Option<&HashSet<String>>) -> Result<()> {
    let _ = disabled_ids;
    let mut tcp_listeners = HashSet::new();
    let mut udp_listeners = HashSet::new();

    for (idx, ep) in conf.endpoints.iter().enumerate() {
        let laddr = validate_endpoint_conf(ep)
            .with_context(|| format!("endpoint #{} ({}) invalid", idx + 1, ep.listen))?;
        let mut net = ep.network;
        net.take_field(&conf.network);
        let no_tcp = net.no_tcp.unwrap_or(false);
        let use_udp = net.use_udp.unwrap_or(false);
        if !no_tcp && !tcp_listeners.insert(laddr) {
            bail!("duplicate TCP listen address {}", laddr);
        }
        if use_udp && !udp_listeners.insert(laddr) {
            bail!("duplicate UDP listen address {}", laddr);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigFormat {
    Toml,
    Json,
}

impl ConfigFormat {
    pub fn from_path(path: &Path) -> Self {
        match path.extension().and_then(|s| s.to_str()) {
            Some("json") => ConfigFormat::Json,
            _ => ConfigFormat::Toml,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ConfigFormat::Toml => "toml",
            ConfigFormat::Json => "json",
        }
    }
}

pub fn ensure_config_file(path: &Path) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let default_conf = FullConf::new(
        LogConf::default(),
        DnsConf::default(),
        NetConf::default(),
        Vec::new(),
    );
    let fmt = ConfigFormat::from_path(path);
    write_conf_atomic(path, &default_conf, fmt)?;
    Ok(())
}

pub fn load_full_conf(path: &Path) -> Result<FullConf> {
    if path.is_dir() {
        return Ok(FullConf::from_conf_file(path.to_string_lossy().as_ref()));
    }
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    FullConf::from_conf_str(&raw).map_err(|e| anyhow!("parse error: {}", e))
}

pub fn serialize_full_conf(conf: &FullConf, fmt: ConfigFormat) -> Result<String> {
    match fmt {
        ConfigFormat::Toml => Ok(toml::to_string_pretty(conf)?),
        ConfigFormat::Json => Ok(serde_json::to_string_pretty(conf)?),
    }
}

pub fn write_conf_atomic(path: &Path, conf: &FullConf, fmt: ConfigFormat) -> Result<String> {
    if path.is_dir() {
        bail!("cannot overwrite directory config path {} from Web UI", path.display());
    }
    let content = serialize_full_conf(conf, fmt)?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let tmp_name = format!(
        ".{}.tmp.{}",
        path.file_name().and_then(|s| s.to_str()).unwrap_or("realm"),
        uuid::Uuid::new_v4()
    );
    let tmp_path: PathBuf = parent.join(tmp_name);
    {
        let mut f = File::create(&tmp_path)?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;
    }
    fs::rename(&tmp_path, path)?;
    Ok(content)
}

/// Synchronize parsed FullConf endpoints with persisted metadata in SQLite.
pub fn sync_endpoints_with_storage(
    conf: &FullConf,
    storage: &Storage,
) -> Result<Vec<ManagedEndpoint>> {
    let existing_metas = storage.list_endpoint_meta()?;
    let mut used_ids = HashSet::new();
    let now = Utc::now().timestamp();
    let mut out = Vec::with_capacity(conf.endpoints.len());

    for (idx, raw_ep) in conf.endpoints.iter().enumerate() {
        let mut effective = raw_ep.clone_via_json();
        effective.network.take_field(&conf.network);

        let raw_fp = fingerprint_endpoint(raw_ep);
        let eff_fp = fingerprint_endpoint(&effective);

        // Match existing metadata by raw fingerprint first, then by listen+remote fallback
        let matched = existing_metas
            .iter()
            .find(|m| !used_ids.contains(&m.id) && m.fingerprint == raw_fp)
            .or_else(|| {
                existing_metas
                    .iter()
                    .find(|m| !used_ids.contains(&m.id) && m.sort_order == idx as i64)
            });

        let meta = if let Some(m) = matched {
            used_ids.insert(m.id.clone());
            let mut updated = m.clone();
            updated.fingerprint = raw_fp;
            updated.sort_order = idx as i64;
            updated.updated_at = now;
            storage.upsert_endpoint_meta(&updated)?;
            updated
        } else {
            let id = format!("ep-{}", &uuid::Uuid::new_v4().to_string()[..8]);
            used_ids.insert(id.clone());
            let new_meta = EndpointMeta {
                id,
                name: format!("{} -> {}", raw_ep.listen, raw_ep.remote),
                fingerprint: raw_fp,
                enabled: true,
                probe_enabled: true,
                probe_interval_sec: 15,
                probe_timeout_ms: 3000,
                sort_order: idx as i64,
                created_at: now,
                updated_at: now,
            };
            storage.upsert_endpoint_meta(&new_meta)?;
            new_meta
        };

        out.push(ManagedEndpoint {
            meta,
            raw_conf: raw_ep.clone_via_json(),
            effective_conf: effective,
            effective_fingerprint: eff_fp,
        });
    }

    // Remove orphaned metadata rows whose endpoints were removed externally
    for old in existing_metas {
        if !used_ids.contains(&old.id) {
            storage.delete_endpoint_meta(&old.id)?;
        }
    }

    Ok(out)
}
