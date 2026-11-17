pub mod api;
pub mod auth;
pub mod ws;

use std::sync::Arc;

use axum::http::{header, StatusCode, Uri};
use axum::middleware;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::Router;
use rust_embed::Embed;
use tower_http::cors::CorsLayer;

use crate::config::reconciler::Reconciler;
use crate::metrics::MetricsManager;
use crate::probe::ProbeManager;
use crate::storage::Storage;
use crate::telemetry::TelemetryHandle;
use auth::AuthService;

#[derive(Embed)]
#[folder = "frontend/dist"]
struct Assets;

pub struct AppState {
    pub reconciler: Arc<Reconciler>,
    pub storage: Storage,
    pub metrics: MetricsManager,
    pub probes: ProbeManager,
    pub telemetry: TelemetryHandle,
    pub auth: AuthService,
}

pub fn build_router(state: Arc<AppState>) -> Router {
    let protected = Router::new()
        .route("/auth/me", get(api::me))
        .route("/auth/password", post(api::change_password))
        .route("/endpoints", get(api::list_endpoints).post(api::create_endpoint))
        .route(
            "/endpoints/{id}",
            put(api::update_endpoint).delete(api::delete_endpoint),
        )
        .route("/endpoints/{id}/toggle", post(api::toggle_endpoint))
        .route("/config/reload", post(api::reload_config))
        .route("/config/export", get(api::export_config))
        .route("/config/import", post(api::import_config))
        .route("/config/snapshots", get(api::list_snapshots))
        .route("/config/snapshots/{id}/restore", post(api::restore_snapshot))
        .route("/stats/overview", get(api::stats_overview))
        .route("/stats/traffic", get(api::stats_traffic))
        .route("/stats/performance", get(api::stats_performance))
        .route("/devices", get(api::live_devices))
        .route("/probes", get(api::list_probes))
        .route("/probes/{id}/run", post(api::run_probe_now))
        .route("/probes/{id}/history", get(api::probe_history))
        .route("/logs", get(api::query_logs))
        .route("/logs/level", put(api::set_log_level))
        .route("/ws/realtime", get(ws::ws_realtime))
        .route("/ws/logs", get(ws::ws_logs))
        .layer(middleware::from_fn_with_state(state.clone(), auth::require_auth));

    let api_router = Router::new()
        .route("/auth/login", post(api::login))
        .route("/health", get(|| async { "ok" }))
        .merge(protected);

    Router::new()
        .nest("/api", api_router)
        .fallback(static_handler)
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn static_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let target = if path.is_empty() { "index.html" } else { path };

    if let Some(content) = Assets::get(target) {
        let mime = mime_guess::from_path(target).first_or_octet_stream();
        ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response()
    } else if let Some(index) = Assets::get("index.html") {
        Html(index.data).into_response()
    } else {
        (StatusCode::NOT_FOUND, "UI assets not built").into_response()
    }
}
