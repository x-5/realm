use std::collections::VecDeque;
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::broadcast;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_log::LogTracer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::reload::{self, Handle};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry};

const MAX_MEMORY_LOGS: usize = 3000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub id: u64,
    pub ts: i64,
    pub level: String,
    pub target: String,
    pub endpoint_id: Option<String>,
    pub conn_id: Option<String>,
    pub message: String,
    pub fields: Value,
}

#[derive(Clone)]
pub struct TelemetryHandle {
    buffer: Arc<Mutex<VecDeque<LogEntry>>>,
    tx: broadcast::Sender<LogEntry>,
    reload_handle: Arc<Handle<EnvFilter, Registry>>,
    current_level: Arc<Mutex<String>>,
}

impl TelemetryHandle {
    pub fn query_logs(
        &self,
        level: Option<&str>,
        endpoint_id: Option<&str>,
        keyword: Option<&str>,
        limit: usize,
    ) -> Vec<LogEntry> {
        let min_rank = level.map(level_rank).unwrap_or(0);
        let kw = keyword.map(|k| k.to_ascii_lowercase());
        let buf = self.buffer.lock().unwrap();
        let mut out: Vec<LogEntry> = buf
            .iter()
            .rev()
            .filter(|e| level_rank(&e.level) >= min_rank)
            .filter(|e| match endpoint_id {
                Some(id) if !id.is_empty() => e.endpoint_id.as_deref() == Some(id),
                _ => true,
            })
            .filter(|e| match &kw {
                Some(k) if !k.is_empty() => {
                    e.message.to_ascii_lowercase().contains(k)
                        || e.target.to_ascii_lowercase().contains(k)
                        || e.fields.to_string().to_ascii_lowercase().contains(k)
                }
                _ => true,
            })
            .take(limit.min(MAX_MEMORY_LOGS))
            .cloned()
            .collect();
        out.reverse();
        out
    }

    pub fn subscribe(&self) -> broadcast::Receiver<LogEntry> {
        self.tx.subscribe()
    }

    pub fn current_level(&self) -> String {
        self.current_level.lock().unwrap().clone()
    }

    pub fn set_level(&self, filter_str: &str) -> Result<(), String> {
        let filter = EnvFilter::try_new(filter_str).map_err(|e| format!("invalid filter: {e}"))?;
        self.reload_handle
            .reload(filter)
            .map_err(|e| format!("reload failed: {e}"))?;
        *self.current_level.lock().unwrap() = filter_str.to_string();
        tracing::info!(level = %filter_str, "log filter updated dynamically");
        Ok(())
    }
}

fn level_rank(l: &str) -> u8 {
    match l.to_ascii_uppercase().as_str() {
        "ERROR" => 5,
        "WARN" => 4,
        "INFO" => 3,
        "DEBUG" => 2,
        "TRACE" => 1,
        _ => 0,
    }
}

struct JsonFieldVisitor {
    message: String,
    endpoint_id: Option<String>,
    conn_id: Option<String>,
    fields: Map<String, Value>,
}

impl JsonFieldVisitor {
    fn new() -> Self {
        Self {
            message: String::new(),
            endpoint_id: None,
            conn_id: None,
            fields: Map::new(),
        }
    }
}

impl Visit for JsonFieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let name = field.name();
        let val = format!("{:?}", value);
        let cleaned = val.trim_matches('"').to_string();
        if name == "message" {
            self.message = cleaned;
        } else if name == "endpoint_id" {
            self.endpoint_id = Some(cleaned);
        } else if name == "conn_id" {
            self.conn_id = Some(cleaned);
        } else {
            self.fields.insert(name.to_string(), Value::String(cleaned));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        let name = field.name();
        if name == "message" {
            self.message = value.to_string();
        } else if name == "endpoint_id" {
            self.endpoint_id = Some(value.to_string());
        } else if name == "conn_id" {
            self.conn_id = Some(value.to_string());
        } else {
            self.fields
                .insert(name.to_string(), Value::String(value.to_string()));
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), Value::Number(value.into()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), Value::Bool(value));
    }
}

struct MemoryBroadcastLayer {
    seq: AtomicU64,
    buffer: Arc<Mutex<VecDeque<LogEntry>>>,
    tx: broadcast::Sender<LogEntry>,
}

impl<S> Layer<S> for MemoryBroadcastLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        let mut visitor = JsonFieldVisitor::new();
        event.record(&mut visitor);

        // Extract bridged log fields if present
        let target = visitor
            .fields
            .get("log.target")
            .and_then(|v| v.as_str())
            .map(ToString::to_string)
            .unwrap_or_else(|| meta.target().to_string());

        let entry = LogEntry {
            id: self.seq.fetch_add(1, Ordering::Relaxed) + 1,
            ts: Utc::now().timestamp_millis(),
            level: meta.level().as_str().to_string(),
            target,
            endpoint_id: visitor.endpoint_id,
            conn_id: visitor.conn_id,
            message: visitor.message,
            fields: Value::Object(visitor.fields),
        };

        {
            let mut buf = self.buffer.lock().unwrap();
            if buf.len() >= MAX_MEMORY_LOGS {
                buf.pop_front();
            }
            buf.push_back(entry.clone());
        }

        let _ = self.tx.send(entry);
    }
}

pub fn init_telemetry(
    log_dir: &Path,
    default_filter: &str,
) -> anyhow::Result<(TelemetryHandle, Option<WorkerGuard>)> {
    let _ = LogTracer::init();
    std::fs::create_dir_all(log_dir)?;

    let env_filter = EnvFilter::try_new(default_filter)
        .unwrap_or_else(|_| EnvFilter::new("info,hickory_proto=warn,hickory_resolver=warn"));
    let (filter_layer, reload_handle) = reload::Layer::new(env_filter);

    let buffer = Arc::new(Mutex::new(VecDeque::with_capacity(MAX_MEMORY_LOGS)));
    let (tx, _) = broadcast::channel(1024);

    let mem_layer = MemoryBroadcastLayer {
        seq: AtomicU64::new(0),
        buffer: buffer.clone(),
        tx: tx.clone(),
    };

    let file_appender = tracing_appender::rolling::daily(log_dir, "realm-panel.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let file_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_writer(non_blocking)
        .with_target(true)
        .with_current_span(false);

    let stdout_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_target(true)
        .with_ansi(true);

    let _ = tracing_subscriber::registry()
        .with(filter_layer)
        .with(mem_layer)
        .with(file_layer)
        .with(stdout_layer)
        .try_init();

    let handle = TelemetryHandle {
        buffer,
        tx,
        reload_handle: Arc::new(reload_handle),
        current_level: Arc::new(Mutex::new(default_filter.to_string())),
    };

    Ok((handle, Some(guard)))
}
