use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "realm-panel",
    version,
    about = "Realm Pro: Hot-reload, Web UI, live device/traffic analytics, performance, probing & observability built on realm"
)]
pub struct CliArgs {
    /// Path to realm configuration file (.toml / .json) or directory
    #[arg(short = 'c', long = "config", default_value = "./realm.toml", env = "REALM_CONFIG")]
    pub config: PathBuf,

    /// Web UI / API listen address
    #[arg(short = 'w', long = "web-listen", default_value = "127.0.0.1:18080", env = "REALM_WEB_LISTEN")]
    pub web_listen: String,

    /// Path to SQLite database file for stats, metadata & snapshots
    #[arg(long = "db", default_value = "./realm-panel.db", env = "REALM_PANEL_DB")]
    pub db: PathBuf,

    /// Directory for rotating structured JSON logs
    #[arg(long = "log-dir", default_value = "./logs", env = "REALM_LOG_DIR")]
    pub log_dir: PathBuf,

    /// Initial log filter (e.g. info, debug, warn, trace)
    #[arg(long = "log-level", default_value = "info", env = "REALM_LOG_LEVEL")]
    pub log_level: String,

    /// Initial admin username (used on first launch)
    #[arg(long = "admin-user", default_value = "admin", env = "REALM_ADMIN_USER")]
    pub admin_user: String,

    /// Initial admin password (auto-generated if omitted on first launch)
    #[arg(long = "admin-pass", env = "REALM_ADMIN_PASS")]
    pub admin_pass: Option<String>,

    /// Set Linux splice pipe capacity in 4K pages
    #[arg(short = 'p', long = "pipe-page")]
    pub pipe_page: Option<usize>,

    /// Set open file descriptor limit
    #[arg(short = 'n', long = "nofile")]
    pub nofile: Option<u64>,

    /// Path to pre-connect hook dynamic library (.so)
    #[arg(short = 'j', long = "pre-conn-hook")]
    pub pre_conn_hook: Option<String>,

    /// Run as a unix daemon
    #[arg(short = 'd', long = "daemon")]
    pub daemon: bool,
}
