use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use serde::Deserialize;

use crate::web::AppState;

pub async fn ws_realtime(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_realtime_ws(socket, state))
}

async fn handle_realtime_ws(mut socket: WebSocket, state: Arc<AppState>) {
    if let Some(latest) = state.metrics.latest() {
        let payload = serde_json::json!({
            "type": "realtime",
            "snapshot": latest,
            "probes": state.probes.list_latest(),
        });
        if socket
            .send(Message::Text(payload.to_string().into()))
            .await
            .is_err()
        {
            return;
        }
    }

    let mut rx = state.metrics.subscribe();
    loop {
        match rx.recv().await {
            Ok(snap) => {
                let payload = serde_json::json!({
                    "type": "realtime",
                    "snapshot": snap,
                    "probes": state.probes.list_latest(),
                });
                if socket
                    .send(Message::Text(payload.to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(_) => break,
        }
    }
}

#[derive(Deserialize)]
pub struct WsLogParams {
    pub level: Option<String>,
    pub endpoint_id: Option<String>,
}

pub async fn ws_logs(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    Query(params): Query<WsLogParams>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_logs_ws(socket, state, params))
}

async fn handle_logs_ws(mut socket: WebSocket, state: Arc<AppState>, params: WsLogParams) {
    let initial = state.telemetry.query_logs(
        params.level.as_deref(),
        params.endpoint_id.as_deref(),
        None,
        200,
    );
    for entry in initial {
        if let Ok(txt) = serde_json::to_string(&entry) {
            if socket.send(Message::Text(txt.into())).await.is_err() {
                return;
            }
        }
    }

    let mut rx = state.telemetry.subscribe();
    let min_rank = params.level.as_deref().map(rank).unwrap_or(0);

    loop {
        match rx.recv().await {
            Ok(entry) => {
                if rank(&entry.level) < min_rank {
                    continue;
                }
                if let Some(ref ep) = params.endpoint_id {
                    if !ep.is_empty() && entry.endpoint_id.as_deref() != Some(ep.as_str()) {
                        continue;
                    }
                }
                if let Ok(txt) = serde_json::to_string(&entry) {
                    if socket.send(Message::Text(txt.into())).await.is_err() {
                        break;
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(_) => break,
        }
    }
}

fn rank(l: &str) -> u8 {
    match l.to_ascii_uppercase().as_str() {
        "ERROR" => 5,
        "WARN" => 4,
        "INFO" => 3,
        "DEBUG" => 2,
        "TRACE" => 1,
        _ => 0,
    }
}
