use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use realm_panel::cli::CliArgs;
use realm_panel::config::ensure_config_file;
use realm_panel::config::reconciler::Reconciler;
use realm_panel::metrics::MetricsManager;
use realm_panel::probe::ProbeManager;
use realm_panel::storage::Storage;
use realm_panel::telemetry::init_telemetry;
use realm_panel::web::auth::AuthService;
use realm_panel::web::{build_router, AppState};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = CliArgs::parse();

    #[cfg(unix)]
    if args.daemon {
        realm_syscall::daemonize("realm-panel is running in the background");
    }

    #[cfg(all(unix, not(target_os = "android")))]
    {
        if let Some(n) = args.nofile {
            let _ = realm_syscall::set_nofile_limit(n);
        } else {
            let _ = realm_syscall::bump_nofile_limit();
        }
    }

    #[cfg(target_os = "linux")]
    if let Some(page) = args.pipe_page {
        realm_io::set_pipe_size(page * 0x1000);
    }

    if let Some(ref hook_path) = args.pre_conn_hook {
        realm_hook::pre_conn::load_dylib(hook_path);
    }

    realm_core::kaminari::install_tls_provider();

    let (telemetry, _log_guard) = init_telemetry(&args.log_dir, &args.log_level)?;
    tracing::info!(
        config = %args.config.display(),
        web_listen = %args.web_listen,
        db = %args.db.display(),
        "starting realm-panel"
    );

    ensure_config_file(&args.config)?;

    let storage = Storage::open(&args.db)?;
    let auth = AuthService::init(storage.clone(), &args.admin_user, args.admin_pass.as_deref())?;
    let reconciler = Reconciler::new(args.config.clone(), storage.clone())?;
    let report = reconciler.initial_apply().await?;
    tracing::info!(?report, "initial endpoints applied");

    let cancel = CancellationToken::new();
    let probes = ProbeManager::new();
    let metrics = MetricsManager::new();

    probes
        .clone()
        .start_scheduler(reconciler.clone(), storage.clone(), cancel.clone());
    metrics.clone().start_sampler(
        reconciler.clone(),
        storage.clone(),
        probes.clone(),
        cancel.clone(),
    );

    // File watcher for external config hot-reload (with 350ms debounce)
    let _watcher = spawn_config_watcher(&args.config, reconciler.clone(), cancel.clone())?;

    // SIGHUP hot-reload
    #[cfg(unix)]
    spawn_sighup_handler(reconciler.clone(), cancel.clone());

    let state = Arc::new(AppState {
        reconciler: reconciler.clone(),
        storage,
        metrics,
        probes,
        telemetry,
        auth,
    });

    let app = build_router(state);
    let web_addr: SocketAddr = args.web_listen.parse()?;
    let listener = tokio::net::TcpListener::bind(web_addr).await?;
    tracing::info!(addr = %web_addr, "web UI & API server listening");

    let server_cancel = cancel.clone();
    let server_task = tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                server_cancel.cancelled().await;
            })
            .await;
    });

    wait_shutdown_signal().await;
    tracing::info!("shutdown signal received, stopping services");
    cancel.cancel();
    reconciler.shutdown_all().await;
    let _ = server_task.await;

    Ok(())
}

async fn wait_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = sigint.recv() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(unix)]
fn spawn_sighup_handler(reconciler: Arc<Reconciler>, cancel: CancellationToken) {
    use tokio::signal::unix::{signal, SignalKind};
    tokio::spawn(async move {
        let Ok(mut hup) = signal(SignalKind::hangup()) else {
            return;
        };
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = hup.recv() => {
                    tracing::info!("SIGHUP received, reloading configuration from disk");
                    if let Err(e) = reconciler.reload_from_disk("sighup").await {
                        tracing::error!(error = %e, "SIGHUP reload failed (keeping existing state)");
                    }
                }
            }
        }
    });
}

fn spawn_config_watcher(
    config_path: &Path,
    reconciler: Arc<Reconciler>,
    cancel: CancellationToken,
) -> anyhow::Result<RecommendedWatcher> {
    let (tx, mut rx) = mpsc::channel::<()>(16);
    let watch_target = if config_path.is_dir() {
        config_path.to_path_buf()
    } else {
        config_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf()
    };

    let target_file_name = config_path.file_name().map(|s| s.to_os_string());
    let is_dir = config_path.is_dir();

    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if let Ok(event) = res {
            let relevant = matches!(
                event.kind,
                EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_)
            );
            if !relevant {
                return;
            }
            let matches_target = event.paths.iter().any(|p| {
                let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
                if name.starts_with('.') && name.contains(".tmp.") {
                    return false;
                }
                if is_dir {
                    name.ends_with(".toml") || name.ends_with(".json")
                } else {
                    target_file_name.as_deref() == p.file_name()
                }
            });
            if matches_target {
                let _ = tx.try_send(());
            }
        }
    })?;

    let mode = if is_dir {
        RecursiveMode::Recursive
    } else {
        RecursiveMode::NonRecursive
    };
    watcher.watch(&watch_target, mode)?;

    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                msg = rx.recv() => {
                    if msg.is_none() {
                        break;
                    }
                    // Debounce 350ms to coalesce rapid editor / atomic rename events
                    tokio::time::sleep(Duration::from_millis(350)).await;
                    while rx.try_recv().is_ok() {}
                    if let Err(e) = reconciler.reload_from_disk("file_watcher").await {
                        tracing::error!(error = %e, "hot-reload from file change failed (keeping existing state)");
                    }
                }
            }
        }
    });

    Ok(watcher)
}
