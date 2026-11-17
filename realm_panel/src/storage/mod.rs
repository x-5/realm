use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointMeta {
    pub id: String,
    pub name: String,
    pub fingerprint: String,
    pub enabled: bool,
    pub probe_enabled: bool,
    pub probe_interval_sec: u64,
    pub probe_timeout_ms: u64,
    pub sort_order: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrafficPoint {
    pub ts: i64,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub peak_rx_bps: u64,
    pub peak_tx_bps: u64,
    pub active_conns_max: u64,
    pub errors: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointTrafficSummary {
    pub endpoint_id: String,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerfSample {
    pub ts: i64,
    pub proc_cpu_pct: f64,
    pub proc_mem_rss_kb: u64,
    pub proc_mem_vsz_kb: u64,
    pub open_fds: u64,
    pub fd_limit: u64,
    pub threads: u64,
    pub tokio_workers: u64,
    pub tokio_alive_tasks: u64,
    pub sys_cpu_pct: f64,
    pub sys_mem_used_kb: u64,
    pub sys_mem_total_kb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeRecord {
    pub ts: i64,
    pub endpoint_id: String,
    pub target: String,
    pub protocol: String,
    pub reachable: bool,
    pub dns_ms: Option<u64>,
    pub rtt_ms: Option<u64>,
    pub error_msg: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigSnapshot {
    pub id: i64,
    pub created_at: i64,
    pub source: String,
    pub format: String,
    pub content: String,
}

#[derive(Clone)]
pub struct Storage {
    conn: Arc<Mutex<Connection>>,
}

impl Storage {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS endpoint_meta (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                probe_enabled INTEGER NOT NULL DEFAULT 1,
                probe_interval_sec INTEGER NOT NULL DEFAULT 15,
                probe_timeout_ms INTEGER NOT NULL DEFAULT 3000,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS traffic_1m (
                ts_minute INTEGER NOT NULL,
                endpoint_id TEXT NOT NULL,
                rx_bytes INTEGER NOT NULL,
                tx_bytes INTEGER NOT NULL,
                peak_rx_bps INTEGER NOT NULL,
                peak_tx_bps INTEGER NOT NULL,
                active_conns_max INTEGER NOT NULL,
                errors INTEGER NOT NULL,
                PRIMARY KEY (ts_minute, endpoint_id)
            );
            CREATE INDEX IF NOT EXISTS idx_traffic_1m_ts ON traffic_1m(ts_minute);

            CREATE TABLE IF NOT EXISTS traffic_1h (
                ts_hour INTEGER NOT NULL,
                endpoint_id TEXT NOT NULL,
                rx_bytes INTEGER NOT NULL,
                tx_bytes INTEGER NOT NULL,
                peak_rx_bps INTEGER NOT NULL,
                peak_tx_bps INTEGER NOT NULL,
                active_conns_max INTEGER NOT NULL,
                errors INTEGER NOT NULL,
                PRIMARY KEY (ts_hour, endpoint_id)
            );
            CREATE INDEX IF NOT EXISTS idx_traffic_1h_ts ON traffic_1h(ts_hour);

            CREATE TABLE IF NOT EXISTS perf_samples (
                ts_minute INTEGER PRIMARY KEY,
                proc_cpu_pct REAL NOT NULL,
                proc_mem_rss_kb INTEGER NOT NULL,
                proc_mem_vsz_kb INTEGER NOT NULL,
                open_fds INTEGER NOT NULL,
                fd_limit INTEGER NOT NULL,
                threads INTEGER NOT NULL,
                tokio_workers INTEGER NOT NULL,
                tokio_alive_tasks INTEGER NOT NULL,
                sys_cpu_pct REAL NOT NULL,
                sys_mem_used_kb INTEGER NOT NULL,
                sys_mem_total_kb INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS probe_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                endpoint_id TEXT NOT NULL,
                target TEXT NOT NULL,
                protocol TEXT NOT NULL,
                reachable INTEGER NOT NULL,
                dns_ms INTEGER,
                rtt_ms INTEGER,
                error_msg TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_probe_ep_ts ON probe_results(endpoint_id, ts);

            CREATE TABLE IF NOT EXISTS config_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER NOT NULL,
                source TEXT NOT NULL,
                format TEXT NOT NULL,
                content TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS auth_users (
                username TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            ",
        )?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn get_setting(&self, key: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let v = conn
            .query_row("SELECT value FROM settings WHERE key = ?1", params![key], |r| r.get(0))
            .optional()?;
        Ok(v)
    }

    pub fn set_setting(&self, key: &str, value: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO settings(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn get_user_hash(&self, username: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let h = conn
            .query_row(
                "SELECT password_hash FROM auth_users WHERE username = ?1",
                params![username],
                |r| r.get(0),
            )
            .optional()?;
        Ok(h)
    }

    pub fn upsert_user(&self, username: &str, password_hash: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().timestamp();
        conn.execute(
            "INSERT INTO auth_users(username, password_hash, updated_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(username) DO UPDATE SET password_hash = excluded.password_hash, updated_at = excluded.updated_at",
            params![username, password_hash, now],
        )?;
        Ok(())
    }

    pub fn has_any_user(&self) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM auth_users", [], |r| r.get(0))?;
        Ok(count > 0)
    }

    pub fn list_endpoint_meta(&self) -> Result<Vec<EndpointMeta>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, fingerprint, enabled, probe_enabled, probe_interval_sec, probe_timeout_ms, sort_order, created_at, updated_at
             FROM endpoint_meta ORDER BY sort_order ASC, created_at ASC",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok(EndpointMeta {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    fingerprint: r.get(2)?,
                    enabled: r.get::<_, i64>(3)? != 0,
                    probe_enabled: r.get::<_, i64>(4)? != 0,
                    probe_interval_sec: r.get::<_, i64>(5)? as u64,
                    probe_timeout_ms: r.get::<_, i64>(6)? as u64,
                    sort_order: r.get(7)?,
                    created_at: r.get(8)?,
                    updated_at: r.get(9)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn upsert_endpoint_meta(&self, meta: &EndpointMeta) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO endpoint_meta(id, name, fingerprint, enabled, probe_enabled, probe_interval_sec, probe_timeout_ms, sort_order, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                fingerprint = excluded.fingerprint,
                enabled = excluded.enabled,
                probe_enabled = excluded.probe_enabled,
                probe_interval_sec = excluded.probe_interval_sec,
                probe_timeout_ms = excluded.probe_timeout_ms,
                sort_order = excluded.sort_order,
                updated_at = excluded.updated_at",
            params![
                meta.id,
                meta.name,
                meta.fingerprint,
                if meta.enabled { 1 } else { 0 },
                if meta.probe_enabled { 1 } else { 0 },
                meta.probe_interval_sec as i64,
                meta.probe_timeout_ms as i64,
                meta.sort_order,
                meta.created_at,
                meta.updated_at
            ],
        )?;
        Ok(())
    }

    pub fn delete_endpoint_meta(&self, id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM endpoint_meta WHERE id = ?1", params![id])?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_traffic_delta(
        &self,
        ts_sec: i64,
        endpoint_id: &str,
        rx_delta: u64,
        tx_delta: u64,
        peak_rx_bps: u64,
        peak_tx_bps: u64,
        active_conns: u64,
        errors_delta: u64,
    ) -> Result<()> {
        if rx_delta == 0 && tx_delta == 0 && active_conns == 0 && errors_delta == 0 {
            return Ok(());
        }
        let ts_minute = ts_sec - (ts_sec.rem_euclid(60));
        let ts_hour = ts_sec - (ts_sec.rem_euclid(3600));
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO traffic_1m(ts_minute, endpoint_id, rx_bytes, tx_bytes, peak_rx_bps, peak_tx_bps, active_conns_max, errors)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(ts_minute, endpoint_id) DO UPDATE SET
                rx_bytes = rx_bytes + excluded.rx_bytes,
                tx_bytes = tx_bytes + excluded.tx_bytes,
                peak_rx_bps = MAX(peak_rx_bps, excluded.peak_rx_bps),
                peak_tx_bps = MAX(peak_tx_bps, excluded.peak_tx_bps),
                active_conns_max = MAX(active_conns_max, excluded.active_conns_max),
                errors = errors + excluded.errors",
            params![
                ts_minute,
                endpoint_id,
                rx_delta as i64,
                tx_delta as i64,
                peak_rx_bps as i64,
                peak_tx_bps as i64,
                active_conns as i64,
                errors_delta as i64
            ],
        )?;
        tx.execute(
            "INSERT INTO traffic_1h(ts_hour, endpoint_id, rx_bytes, tx_bytes, peak_rx_bps, peak_tx_bps, active_conns_max, errors)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(ts_hour, endpoint_id) DO UPDATE SET
                rx_bytes = rx_bytes + excluded.rx_bytes,
                tx_bytes = tx_bytes + excluded.tx_bytes,
                peak_rx_bps = MAX(peak_rx_bps, excluded.peak_rx_bps),
                peak_tx_bps = MAX(peak_tx_bps, excluded.peak_tx_bps),
                active_conns_max = MAX(active_conns_max, excluded.active_conns_max),
                errors = errors + excluded.errors",
            params![
                ts_hour,
                endpoint_id,
                rx_delta as i64,
                tx_delta as i64,
                peak_rx_bps as i64,
                peak_tx_bps as i64,
                active_conns as i64,
                errors_delta as i64
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn query_traffic_series(
        &self,
        range: &str,
        endpoint_id: Option<&str>,
    ) -> Result<Vec<TrafficPoint>> {
        let now = Utc::now().timestamp();
        let (table, col, since) = match range {
            "24h" => ("traffic_1m", "ts_minute", now - 24 * 3600),
            "7d" => ("traffic_1m", "ts_minute", now - 7 * 86400),
            "30d" => ("traffic_1h", "ts_hour", now - 30 * 86400),
            _ => ("traffic_1m", "ts_minute", now - 3600),
        };
        let conn = self.conn.lock().unwrap();
        if let Some(ep) = endpoint_id.filter(|s| !s.is_empty()) {
            let sql = format!(
                "SELECT {col}, rx_bytes, tx_bytes, peak_rx_bps, peak_tx_bps, active_conns_max, errors
                 FROM {table} WHERE {col} >= ?1 AND endpoint_id = ?2 ORDER BY {col} ASC"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt
                .query_map(params![since, ep], |r| {
                    Ok(TrafficPoint {
                        ts: r.get(0)?,
                        rx_bytes: r.get::<_, i64>(1)? as u64,
                        tx_bytes: r.get::<_, i64>(2)? as u64,
                        peak_rx_bps: r.get::<_, i64>(3)? as u64,
                        peak_tx_bps: r.get::<_, i64>(4)? as u64,
                        active_conns_max: r.get::<_, i64>(5)? as u64,
                        errors: r.get::<_, i64>(6)? as u64,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        } else {
            let sql = format!(
                "SELECT {col}, SUM(rx_bytes), SUM(tx_bytes), MAX(peak_rx_bps), MAX(peak_tx_bps), SUM(active_conns_max), SUM(errors)
                 FROM {table} WHERE {col} >= ?1 GROUP BY {col} ORDER BY {col} ASC"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt
                .query_map(params![since], |r| {
                    Ok(TrafficPoint {
                        ts: r.get(0)?,
                        rx_bytes: r.get::<_, i64>(1)? as u64,
                        tx_bytes: r.get::<_, i64>(2)? as u64,
                        peak_rx_bps: r.get::<_, i64>(3)? as u64,
                        peak_tx_bps: r.get::<_, i64>(4)? as u64,
                        active_conns_max: r.get::<_, i64>(5)? as u64,
                        errors: r.get::<_, i64>(6)? as u64,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        }
    }

    pub fn query_traffic_by_endpoint(&self, range: &str) -> Result<Vec<EndpointTrafficSummary>> {
        let now = Utc::now().timestamp();
        let (table, col, since) = match range {
            "24h" => ("traffic_1m", "ts_minute", now - 24 * 3600),
            "7d" => ("traffic_1m", "ts_minute", now - 7 * 86400),
            "30d" => ("traffic_1h", "ts_hour", now - 30 * 86400),
            _ => ("traffic_1m", "ts_minute", now - 3600),
        };
        let conn = self.conn.lock().unwrap();
        let sql = format!(
            "SELECT endpoint_id, SUM(rx_bytes), SUM(tx_bytes) FROM {table} WHERE {col} >= ?1 GROUP BY endpoint_id ORDER BY (SUM(rx_bytes)+SUM(tx_bytes)) DESC"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map(params![since], |r| {
                Ok(EndpointTrafficSummary {
                    endpoint_id: r.get(0)?,
                    rx_bytes: r.get::<_, i64>(1)? as u64,
                    tx_bytes: r.get::<_, i64>(2)? as u64,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn query_traffic_series_between(
        &self,
        from_ts: i64,
        to_ts: i64,
        endpoint_id: Option<&str>,
    ) -> Result<Vec<TrafficPoint>> {
        let now = Utc::now().timestamp();
        let span = (to_ts - from_ts).max(0);
        let (table, col) = if span <= 7 * 86400 && from_ts >= now - 7 * 86400 {
            ("traffic_1m", "ts_minute")
        } else {
            ("traffic_1h", "ts_hour")
        };
        let conn = self.conn.lock().unwrap();
        if let Some(ep) = endpoint_id.filter(|s| !s.is_empty()) {
            let sql = format!(
                "SELECT {col}, rx_bytes, tx_bytes, peak_rx_bps, peak_tx_bps, active_conns_max, errors
                 FROM {table} WHERE {col} >= ?1 AND {col} <= ?2 AND endpoint_id = ?3 ORDER BY {col} ASC"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt
                .query_map(params![from_ts, to_ts, ep], |r| {
                    Ok(TrafficPoint {
                        ts: r.get(0)?,
                        rx_bytes: r.get::<_, i64>(1)? as u64,
                        tx_bytes: r.get::<_, i64>(2)? as u64,
                        peak_rx_bps: r.get::<_, i64>(3)? as u64,
                        peak_tx_bps: r.get::<_, i64>(4)? as u64,
                        active_conns_max: r.get::<_, i64>(5)? as u64,
                        errors: r.get::<_, i64>(6)? as u64,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        } else {
            let sql = format!(
                "SELECT {col}, SUM(rx_bytes), SUM(tx_bytes), MAX(peak_rx_bps), MAX(peak_tx_bps), SUM(active_conns_max), SUM(errors)
                 FROM {table} WHERE {col} >= ?1 AND {col} <= ?2 GROUP BY {col} ORDER BY {col} ASC"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt
                .query_map(params![from_ts, to_ts], |r| {
                    Ok(TrafficPoint {
                        ts: r.get(0)?,
                        rx_bytes: r.get::<_, i64>(1)? as u64,
                        tx_bytes: r.get::<_, i64>(2)? as u64,
                        peak_rx_bps: r.get::<_, i64>(3)? as u64,
                        peak_tx_bps: r.get::<_, i64>(4)? as u64,
                        active_conns_max: r.get::<_, i64>(5)? as u64,
                        errors: r.get::<_, i64>(6)? as u64,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        }
    }

    pub fn query_traffic_by_endpoint_between(
        &self,
        from_ts: i64,
        to_ts: i64,
    ) -> Result<Vec<EndpointTrafficSummary>> {
        let now = Utc::now().timestamp();
        let span = (to_ts - from_ts).max(0);
        let (table, col) = if span <= 7 * 86400 && from_ts >= now - 7 * 86400 {
            ("traffic_1m", "ts_minute")
        } else {
            ("traffic_1h", "ts_hour")
        };
        let conn = self.conn.lock().unwrap();
        let sql = format!(
            "SELECT endpoint_id, SUM(rx_bytes), SUM(tx_bytes)
             FROM {table} WHERE {col} >= ?1 AND {col} <= ?2
             GROUP BY endpoint_id ORDER BY (SUM(rx_bytes)+SUM(tx_bytes)) DESC"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map(params![from_ts, to_ts], |r| {
                Ok(EndpointTrafficSummary {
                    endpoint_id: r.get(0)?,
                    rx_bytes: r.get::<_, i64>(1)? as u64,
                    tx_bytes: r.get::<_, i64>(2)? as u64,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn insert_perf_sample(&self, s: &PerfSample) -> Result<()> {
        let ts_minute = s.ts - s.ts.rem_euclid(60);
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO perf_samples(ts_minute, proc_cpu_pct, proc_mem_rss_kb, proc_mem_vsz_kb, open_fds, fd_limit, threads, tokio_workers, tokio_alive_tasks, sys_cpu_pct, sys_mem_used_kb, sys_mem_total_kb)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
             ON CONFLICT(ts_minute) DO UPDATE SET
                proc_cpu_pct = excluded.proc_cpu_pct,
                proc_mem_rss_kb = excluded.proc_mem_rss_kb,
                proc_mem_vsz_kb = excluded.proc_mem_vsz_kb,
                open_fds = excluded.open_fds,
                fd_limit = excluded.fd_limit,
                threads = excluded.threads,
                tokio_workers = excluded.tokio_workers,
                tokio_alive_tasks = excluded.tokio_alive_tasks,
                sys_cpu_pct = excluded.sys_cpu_pct,
                sys_mem_used_kb = excluded.sys_mem_used_kb,
                sys_mem_total_kb = excluded.sys_mem_total_kb",
            params![
                ts_minute,
                s.proc_cpu_pct,
                s.proc_mem_rss_kb as i64,
                s.proc_mem_vsz_kb as i64,
                s.open_fds as i64,
                s.fd_limit as i64,
                s.threads as i64,
                s.tokio_workers as i64,
                s.tokio_alive_tasks as i64,
                s.sys_cpu_pct,
                s.sys_mem_used_kb as i64,
                s.sys_mem_total_kb as i64
            ],
        )?;
        Ok(())
    }

    pub fn query_perf_samples(&self, range: &str) -> Result<Vec<PerfSample>> {
        let now = Utc::now().timestamp();
        let since = match range {
            "24h" => now - 24 * 3600,
            "7d" => now - 7 * 86400,
            "30d" => now - 30 * 86400,
            _ => now - 3600,
        };
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT ts_minute, proc_cpu_pct, proc_mem_rss_kb, proc_mem_vsz_kb, open_fds, fd_limit, threads, tokio_workers, tokio_alive_tasks, sys_cpu_pct, sys_mem_used_kb, sys_mem_total_kb
             FROM perf_samples WHERE ts_minute >= ?1 ORDER BY ts_minute ASC",
        )?;
        let rows = stmt
            .query_map(params![since], |r| {
                Ok(PerfSample {
                    ts: r.get(0)?,
                    proc_cpu_pct: r.get(1)?,
                    proc_mem_rss_kb: r.get::<_, i64>(2)? as u64,
                    proc_mem_vsz_kb: r.get::<_, i64>(3)? as u64,
                    open_fds: r.get::<_, i64>(4)? as u64,
                    fd_limit: r.get::<_, i64>(5)? as u64,
                    threads: r.get::<_, i64>(6)? as u64,
                    tokio_workers: r.get::<_, i64>(7)? as u64,
                    tokio_alive_tasks: r.get::<_, i64>(8)? as u64,
                    sys_cpu_pct: r.get(9)?,
                    sys_mem_used_kb: r.get::<_, i64>(10)? as u64,
                    sys_mem_total_kb: r.get::<_, i64>(11)? as u64,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn insert_probe_result(&self, rec: &ProbeRecord) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO probe_results(ts, endpoint_id, target, protocol, reachable, dns_ms, rtt_ms, error_msg)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                rec.ts,
                rec.endpoint_id,
                rec.target,
                rec.protocol,
                if rec.reachable { 1 } else { 0 },
                rec.dns_ms.map(|v| v as i64),
                rec.rtt_ms.map(|v| v as i64),
                rec.error_msg
            ],
        )?;
        Ok(())
    }

    pub fn query_probe_history(&self, endpoint_id: &str, limit: usize) -> Result<Vec<ProbeRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT ts, endpoint_id, target, protocol, reachable, dns_ms, rtt_ms, error_msg
             FROM probe_results WHERE endpoint_id = ?1 ORDER BY ts DESC LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![endpoint_id, limit as i64], |r| {
                Ok(ProbeRecord {
                    ts: r.get(0)?,
                    endpoint_id: r.get(1)?,
                    target: r.get(2)?,
                    protocol: r.get(3)?,
                    reachable: r.get::<_, i64>(4)? != 0,
                    dns_ms: r.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                    rtt_ms: r.get::<_, Option<i64>>(6)?.map(|v| v as u64),
                    error_msg: r.get(7)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn save_snapshot(&self, source: &str, format: &str, content: &str) -> Result<i64> {
        let mut conn = self.conn.lock().unwrap();
        let now = Utc::now().timestamp();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO config_snapshots(created_at, source, format, content) VALUES (?1, ?2, ?3, ?4)",
            params![now, source, format, content],
        )?;
        let id = tx.last_insert_rowid();
        tx.execute(
            "DELETE FROM config_snapshots WHERE id NOT IN (SELECT id FROM config_snapshots ORDER BY id DESC LIMIT 20)",
            [],
        )?;
        tx.commit()?;
        Ok(id)
    }

    pub fn list_snapshots(&self) -> Result<Vec<ConfigSnapshot>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, created_at, source, format, content FROM config_snapshots ORDER BY id DESC LIMIT 20",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok(ConfigSnapshot {
                    id: r.get(0)?,
                    created_at: r.get(1)?,
                    source: r.get(2)?,
                    format: r.get(3)?,
                    content: r.get(4)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_snapshot(&self, id: i64) -> Result<Option<ConfigSnapshot>> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT id, created_at, source, format, content FROM config_snapshots WHERE id = ?1",
                params![id],
                |r| {
                    Ok(ConfigSnapshot {
                        id: r.get(0)?,
                        created_at: r.get(1)?,
                        source: r.get(2)?,
                        format: r.get(3)?,
                        content: r.get(4)?,
                    })
                },
            )
            .optional()?;
        Ok(row)
    }

    pub fn cleanup_retention(&self) -> Result<()> {
        let now = Utc::now().timestamp();
        let cutoff_7d = now - 7 * 86400;
        let cutoff_365d = now - 365 * 86400;
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM traffic_1m WHERE ts_minute < ?1", params![cutoff_7d])?;
        conn.execute("DELETE FROM traffic_1h WHERE ts_hour < ?1", params![cutoff_365d])?;
        conn.execute("DELETE FROM perf_samples WHERE ts_minute < ?1", params![cutoff_365d])?;
        conn.execute("DELETE FROM probe_results WHERE ts < ?1", params![cutoff_7d])?;
        Ok(())
    }
}
