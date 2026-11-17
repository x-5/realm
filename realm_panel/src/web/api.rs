use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::config::{ConfigFormat, EndpointPayload};
use crate::web::AppState;

pub type ApiError = (StatusCode, Json<serde_json::Value>);

fn err_resp(code: StatusCode, msg: impl ToString) -> ApiError {
    (
        code,
        Json(serde_json::json!({
            "error": msg.to_string(),
        })),
    )
}

#[derive(Deserialize)]
pub struct LoginReq {
    pub username: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct LoginResp {
    pub token: String,
    pub username: String,
}

pub async fn login(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginReq>,
) -> Result<Json<LoginResp>, ApiError> {
    match state.auth.verify_login(&req.username, &req.password) {
        Ok(Some(token)) => Ok(Json(LoginResp {
            token,
            username: req.username,
        })),
        Ok(None) => Err(err_resp(StatusCode::UNAUTHORIZED, "invalid username or password")),
        Err(e) => Err(err_resp(StatusCode::INTERNAL_SERVER_ERROR, e)),
    }
}

#[derive(Deserialize)]
pub struct ChangePasswordReq {
    pub current_password: String,
    pub new_password: String,
}

pub async fn change_password(
    State(state): State<Arc<AppState>>,
    Extension(username): Extension<String>,
    Json(req): Json<ChangePasswordReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if req.new_password.len() < 6 {
        return Err(err_resp(
            StatusCode::BAD_REQUEST,
            "new password must be at least 6 characters",
        ));
    }
    match state.auth.verify_login(&username, &req.current_password) {
        Ok(Some(_)) => {
            state
                .auth
                .change_password(&username, &req.new_password)
                .map_err(|e| err_resp(StatusCode::INTERNAL_SERVER_ERROR, e))?;
            Ok(Json(serde_json::json!({ "ok": true })))
        }
        Ok(None) => Err(err_resp(StatusCode::BAD_REQUEST, "current password incorrect")),
        Err(e) => Err(err_resp(StatusCode::INTERNAL_SERVER_ERROR, e)),
    }
}

pub async fn me(Extension(username): Extension<String>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "username": username }))
}

#[derive(Serialize)]
pub struct EndpointView {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub probe_enabled: bool,
    pub probe_interval_sec: u64,
    pub probe_timeout_ms: u64,
    #[serde(flatten)]
    pub conf: realm::conf::EndpointConf,
    pub running: bool,
    pub last_error: Option<String>,
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
    pub devices: Vec<crate::relay::LiveDeviceSnapshot>,
    pub probes: Vec<crate::probe::TargetProbeStatus>,
}

pub async fn list_endpoints(State(state): State<Arc<AppState>>) -> Json<Vec<EndpointView>> {
    let managed = state.reconciler.managed.lock().await.clone();
    let running = state.reconciler.running.lock().await;

    let mut list = Vec::with_capacity(managed.len());
    for m in managed {
        let id = m.meta.id.clone();
        let probes = state.probes.list_for_endpoint(&id);
        if let Some(r) = running.get(&id) {
            let st = &r.runtime.stats;
            let devices = r.runtime.devices.list_snapshots(&id);
            list.push(EndpointView {
                id,
                name: m.meta.name,
                enabled: m.meta.enabled,
                probe_enabled: m.meta.probe_enabled,
                probe_interval_sec: m.meta.probe_interval_sec,
                probe_timeout_ms: m.meta.probe_timeout_ms,
                conf: m.raw_conf,
                running: r.runtime.get_error().is_none(),
                last_error: r.runtime.get_error(),
                rx_bytes: st.rx_bytes.load(Ordering::Relaxed),
                tx_bytes: st.tx_bytes.load(Ordering::Relaxed),
                rx_bps: st.rx_bps.load(Ordering::Relaxed),
                tx_bps: st.tx_bps.load(Ordering::Relaxed),
                active_tcp_conns: st.active_tcp_conns.load(Ordering::Relaxed),
                active_udp_assocs: st.active_udp_assocs.load(Ordering::Relaxed),
                total_tcp_conns: st.total_tcp_conns.load(Ordering::Relaxed),
                zero_copy_conns: st.zero_copy_conns.load(Ordering::Relaxed),
                connect_errors: st.connect_errors.load(Ordering::Relaxed),
                last_rtt_us: st.last_rtt_us.load(Ordering::Relaxed),
                devices,
                probes,
            });
        } else {
            list.push(EndpointView {
                id,
                name: m.meta.name,
                enabled: m.meta.enabled,
                probe_enabled: m.meta.probe_enabled,
                probe_interval_sec: m.meta.probe_interval_sec,
                probe_timeout_ms: m.meta.probe_timeout_ms,
                conf: m.raw_conf,
                running: false,
                last_error: None,
                rx_bytes: 0,
                tx_bytes: 0,
                rx_bps: 0,
                tx_bps: 0,
                active_tcp_conns: 0,
                active_udp_assocs: 0,
                total_tcp_conns: 0,
                zero_copy_conns: 0,
                connect_errors: 0,
                last_rtt_us: 0,
                devices: Vec::new(),
                probes,
            });
        }
    }
    Json(list)
}

pub async fn create_endpoint(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<EndpointPayload>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let (ep, report) = state
        .reconciler
        .add_endpoint(payload)
        .await
        .map_err(|e| err_resp(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(serde_json::json!({
        "id": ep.meta.id,
        "report": report,
    })))
}

pub async fn update_endpoint(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(payload): Json<EndpointPayload>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let (ep, report) = state
        .reconciler
        .update_endpoint(&id, payload)
        .await
        .map_err(|e| err_resp(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(serde_json::json!({
        "id": ep.meta.id,
        "report": report,
    })))
}

#[derive(Deserialize)]
pub struct ToggleReq {
    pub enabled: bool,
}

pub async fn toggle_endpoint(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(req): Json<ToggleReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let report = state
        .reconciler
        .toggle_endpoint(&id, req.enabled)
        .await
        .map_err(|e| err_resp(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(serde_json::json!({ "report": report })))
}

pub async fn delete_endpoint(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let report = state
        .reconciler
        .delete_endpoint(&id)
        .await
        .map_err(|e| err_resp(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(serde_json::json!({ "report": report })))
}

pub async fn reload_config(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let report = state
        .reconciler
        .reload_from_disk("manual_api")
        .await
        .map_err(|e| err_resp(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(serde_json::json!({ "report": report })))
}

#[derive(Deserialize)]
pub struct FormatQuery {
    pub format: Option<String>,
}

pub async fn export_config(
    State(state): State<Arc<AppState>>,
    Query(q): Query<FormatQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let fmt = match q.format.as_deref() {
        Some("json") => ConfigFormat::Json,
        Some("toml") => ConfigFormat::Toml,
        _ => ConfigFormat::from_path(&state.reconciler.config_path),
    };
    let content = state
        .reconciler
        .export_current_config(fmt)
        .await
        .map_err(|e| err_resp(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(Json(serde_json::json!({
        "format": fmt.as_str(),
        "content": content,
    })))
}

#[derive(Deserialize)]
pub struct ImportConfigReq {
    pub content: String,
}

pub async fn import_config(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ImportConfigReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let report = state
        .reconciler
        .apply_raw_config_str(&req.content, "web_import")
        .await
        .map_err(|e| err_resp(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(serde_json::json!({ "report": report })))
}

pub async fn list_snapshots(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<crate::storage::ConfigSnapshot>>, ApiError> {
    let list = state
        .storage
        .list_snapshots()
        .map_err(|e| err_resp(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(Json(list))
}

pub async fn restore_snapshot(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let report = state
        .reconciler
        .restore_snapshot(id)
        .await
        .map_err(|e| err_resp(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(serde_json::json!({ "report": report })))
}

pub async fn stats_overview(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let latest = state.metrics.latest();
    let recent = state.metrics.recent_series(120);
    let probes = state.probes.list_latest();
    Json(serde_json::json!({
        "latest": latest,
        "recent": recent,
        "probes": probes,
    }))
}

#[derive(Deserialize)]
pub struct TrafficQuery {
    pub range: Option<String>,
    pub endpoint_id: Option<String>,
    pub from: Option<i64>,
    pub to: Option<i64>,
}

pub async fn stats_traffic(
    State(state): State<Arc<AppState>>,
    Query(q): Query<TrafficQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if q.from.is_some() || q.to.is_some() {
        let now = chrono::Utc::now().timestamp();
        let from_ts = q.from.unwrap_or(now - 7 * 86400);
        let to_ts = q.to.unwrap_or(now);
        if from_ts > to_ts {
            return Err(err_resp(
                StatusCode::BAD_REQUEST,
                "invalid time range: 'from' must be <= 'to'",
            ));
        }
        let series = state
            .storage
            .query_traffic_series_between(from_ts, to_ts, q.endpoint_id.as_deref())
            .map_err(|e| err_resp(StatusCode::INTERNAL_SERVER_ERROR, e))?;
        let summary = state
            .storage
            .query_traffic_by_endpoint_between(from_ts, to_ts)
            .map_err(|e| err_resp(StatusCode::INTERNAL_SERVER_ERROR, e))?;
        return Ok(Json(serde_json::json!({
            "range": "custom",
            "unit": "bytes",
            "from": from_ts,
            "to": to_ts,
            "series": series,
            "by_endpoint": summary,
        })));
    }

    let range = q.range.as_deref().unwrap_or("24h");
    if range == "1h" {
        let recent = state.metrics.recent_series(3600);
        let ep_filter = q.endpoint_id.as_deref().filter(|s| !s.is_empty());
        let series: Vec<crate::storage::TrafficPoint> = recent
            .into_iter()
            .map(|snap| {
                if let Some(ep) = ep_filter {
                    let found = snap.endpoints.iter().find(|e| e.endpoint_id == ep);
                    let rx = found.map(|e| e.rx_bps).unwrap_or(0);
                    let tx = found.map(|e| e.tx_bps).unwrap_or(0);
                    let conns = found
                        .map(|e| e.active_tcp_conns + e.active_udp_assocs)
                        .unwrap_or(0);
                    crate::storage::TrafficPoint {
                        ts: snap.ts,
                        rx_bytes: rx,
                        tx_bytes: tx,
                        peak_rx_bps: rx,
                        peak_tx_bps: tx,
                        active_conns_max: conns,
                        errors: 0,
                    }
                } else {
                    crate::storage::TrafficPoint {
                        ts: snap.ts,
                        rx_bytes: snap.total_rx_bps,
                        tx_bytes: snap.total_tx_bps,
                        peak_rx_bps: snap.total_rx_bps,
                        peak_tx_bps: snap.total_tx_bps,
                        active_conns_max: snap.total_active_conns,
                        errors: 0,
                    }
                }
            })
            .collect();
        let summary = state
            .storage
            .query_traffic_by_endpoint("1h")
            .unwrap_or_default();
        return Ok(Json(serde_json::json!({
            "range": range,
            "unit": "bps",
            "series": series,
            "by_endpoint": summary,
        })));
    }

    let series = state
        .storage
        .query_traffic_series(range, q.endpoint_id.as_deref())
        .map_err(|e| err_resp(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    let summary = state
        .storage
        .query_traffic_by_endpoint(range)
        .map_err(|e| err_resp(StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(serde_json::json!({
        "range": range,
        "unit": "bytes",
        "series": series,
        "by_endpoint": summary,
    })))
}

#[derive(Deserialize)]
pub struct PerfQuery {
    pub range: Option<String>,
}

pub async fn stats_performance(
    State(state): State<Arc<AppState>>,
    Query(q): Query<PerfQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let range = q.range.as_deref().unwrap_or("1h");
    if range == "1h" {
        let series: Vec<crate::storage::PerfSample> = state
            .metrics
            .recent_series(600)
            .into_iter()
            .map(|s| s.perf)
            .collect();
        return Ok(Json(serde_json::json!({
            "range": range,
            "latest": state.metrics.latest().map(|s| s.perf),
            "series": series,
        })));
    }
    let series = state
        .storage
        .query_perf_samples(range)
        .map_err(|e| err_resp(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(Json(serde_json::json!({
        "range": range,
        "latest": state.metrics.latest().map(|s| s.perf),
        "series": series,
    })))
}

#[derive(Deserialize)]
pub struct DeviceQuery {
    pub endpoint_id: Option<String>,
}

pub async fn live_devices(
    State(state): State<Arc<AppState>>,
    Query(q): Query<DeviceQuery>,
) -> Json<Vec<crate::relay::LiveDeviceSnapshot>> {
    let running = state.reconciler.running.lock().await;
    let mut out = Vec::new();
    for (id, entry) in running.iter() {
        if let Some(ref filter) = q.endpoint_id {
            if !filter.is_empty() && filter != id {
                continue;
            }
        }
        out.extend(entry.runtime.devices.list_snapshots(id));
    }
    out.sort_by(|a, b| {
        b.active_conns
            .cmp(&a.active_conns)
            .then_with(|| (b.rx_bps + b.tx_bps).cmp(&(a.rx_bps + a.tx_bps)))
            .then_with(|| b.last_active_ts.cmp(&a.last_active_ts))
    });
    Json(out)
}

pub async fn list_probes(State(state): State<Arc<AppState>>) -> Json<Vec<crate::probe::TargetProbeStatus>> {
    Json(state.probes.list_latest())
}

pub async fn run_probe_now(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<crate::probe::TargetProbeStatus>>, ApiError> {
    let res = state
        .probes
        .probe_endpoint_now(&state.reconciler, &state.storage, &id)
        .await
        .map_err(|e| err_resp(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(res))
}

pub async fn probe_history(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<crate::storage::ProbeRecord>>, ApiError> {
    let res = state
        .storage
        .query_probe_history(&id, 100)
        .map_err(|e| err_resp(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(Json(res))
}

#[derive(Deserialize)]
pub struct LogQuery {
    pub level: Option<String>,
    pub endpoint_id: Option<String>,
    pub keyword: Option<String>,
    pub limit: Option<usize>,
}

pub async fn query_logs(
    State(state): State<Arc<AppState>>,
    Query(q): Query<LogQuery>,
) -> Json<serde_json::Value> {
    let logs = state.telemetry.query_logs(
        q.level.as_deref(),
        q.endpoint_id.as_deref(),
        q.keyword.as_deref(),
        q.limit.unwrap_or(500),
    );
    Json(serde_json::json!({
        "level": state.telemetry.current_level(),
        "logs": logs,
    }))
}

#[derive(Deserialize)]
pub struct SetLogLevelReq {
    pub level: String,
}

pub async fn set_log_level(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SetLogLevelReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state
        .telemetry
        .set_level(&req.level)
        .map_err(|e| err_resp(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(serde_json::json!({ "level": req.level })))
}
