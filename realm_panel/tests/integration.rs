use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use chrono::Utc;
use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio_util::sync::CancellationToken;

use realm::conf::EndpointConf;
use realm_panel::config::reconciler::Reconciler;
use realm_panel::config::{ensure_config_file, ConfigFormat, EndpointPayload};
use realm_panel::metrics::MetricsManager;
use realm_panel::probe::ProbeManager;
use realm_panel::relay::Proto;
use realm_panel::storage::Storage;
use realm_panel::telemetry::{init_telemetry, TelemetryHandle};
use realm_panel::web::auth::AuthService;
use realm_panel::web::{build_router, AppState};

static TEST_TELEMETRY: OnceLock<(TelemetryHandle, TempDir)> = OnceLock::new();

fn shared_telemetry() -> TelemetryHandle {
    TEST_TELEMETRY
        .get_or_init(|| {
            realm_core::kaminari::install_tls_provider();
            let dir = tempfile::tempdir().expect("temp log dir");
            let (handle, guard) = init_telemetry(dir.path(), "info").expect("init telemetry");
            if let Some(g) = guard {
                std::mem::forget(g);
            }
            (handle, dir)
        })
        .0
        .clone()
}

fn free_tcp_port() -> u16 {
    let lis = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    lis.local_addr().unwrap().port()
}

fn free_udp_port() -> u16 {
    let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    sock.local_addr().unwrap().port()
}

async fn spawn_tcp_echo_with_prefix(prefix: &'static [u8]) -> (SocketAddr, CancellationToken) {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = c.cancelled() => break,
                res = lis.accept() => {
                    let Ok((mut sock, _)) = res else { break; };
                    let c2 = c.clone();
                    tokio::spawn(async move {
                        let mut buf = [0u8; 4096];
                        loop {
                            tokio::select! {
                                _ = c2.cancelled() => break,
                                r = sock.read(&mut buf) => {
                                    match r {
                                        Ok(0) | Err(_) => break,
                                        Ok(n) => {
                                            if !prefix.is_empty() {
                                                let _ = sock.write_all(prefix).await;
                                            }
                                            if sock.write_all(&buf[..n]).await.is_err() {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    });
                }
            }
        }
    });
    (addr, cancel)
}

async fn spawn_udp_echo_with_prefix(prefix: &'static [u8]) -> (SocketAddr, CancellationToken) {
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = sock.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        loop {
            tokio::select! {
                _ = c.cancelled() => break,
                res = sock.recv_from(&mut buf) => {
                    if let Ok((n, peer)) = res {
                        let mut out = Vec::with_capacity(prefix.len() + n);
                        out.extend_from_slice(prefix);
                        out.extend_from_slice(&buf[..n]);
                        let _ = sock.send_to(&out, peer).await;
                    }
                }
            }
        }
    });
    (addr, cancel)
}

fn make_endpoint_payload(
    name: &str,
    listen: String,
    remote: String,
    use_udp: bool,
) -> EndpointPayload {
    let conf: EndpointConf = serde_json::from_value(json!({
        "listen": listen,
        "remote": remote,
        "network": {
            "no_tcp": false,
            "use_udp": use_udp
        }
    }))
    .unwrap();

    EndpointPayload {
        id: None,
        name: Some(name.to_string()),
        enabled: true,
        probe_enabled: true,
        probe_interval_sec: 15,
        probe_timeout_ms: 1500,
        conf,
    }
}

#[tokio::test]
async fn test_hot_reload_and_force_disconnect() {
    let _ = shared_telemetry();
    let tmp = tempfile::tempdir().unwrap();
    let conf_path = tmp.path().join("realm.toml");
    let db_path = tmp.path().join("panel.db");
    ensure_config_file(&conf_path).unwrap();
    let storage = Storage::open(&db_path).unwrap();
    let reconciler = Reconciler::new(conf_path, storage).unwrap();
    reconciler.initial_apply().await.unwrap();

    let (b1_addr, b1_cancel) = spawn_tcp_echo_with_prefix(b"").await;
    let (b2_addr, b2_cancel) = spawn_tcp_echo_with_prefix(b"").await;
    let (b3_addr, b3_cancel) = spawn_tcp_echo_with_prefix(b"NEW:").await;

    let l1 = format!("127.0.0.1:{}", free_tcp_port());
    let l2 = format!("127.0.0.1:{}", free_tcp_port());

    let (ep1, _) = reconciler
        .add_endpoint(make_endpoint_payload("ep1", l1.clone(), b1_addr.to_string(), false))
        .await
        .unwrap();
    let (ep2, _) = reconciler
        .add_endpoint(make_endpoint_payload("ep2", l2.clone(), b2_addr.to_string(), false))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(80)).await;

    // Open active connections on both ep1 and ep2
    let mut c1 = TcpStream::connect(&l1).await.unwrap();
    let mut c2 = TcpStream::connect(&l2).await.unwrap();

    c1.write_all(b"hello1").await.unwrap();
    let mut buf1 = [0u8; 6];
    c1.read_exact(&mut buf1).await.unwrap();
    assert_eq!(&buf1, b"hello1");

    c2.write_all(b"hello2").await.unwrap();
    let mut buf2 = [0u8; 6];
    c2.read_exact(&mut buf2).await.unwrap();
    assert_eq!(&buf2, b"hello2");

    // Update ep2 to point to b3_addr -> must force-disconnect c2 while leaving c1 untouched
    let (_, report) = reconciler
        .update_endpoint(
            &ep2.meta.id,
            make_endpoint_payload("ep2-updated", l2.clone(), b3_addr.to_string(), false),
        )
        .await
        .unwrap();

    assert_eq!(report.updated, 1);
    assert_eq!(report.unchanged, 1);

    // c2 should be closed immediately
    let mut drain = [0u8; 32];
    let c2_res = tokio::time::timeout(Duration::from_secs(2), c2.read(&mut drain))
        .await
        .expect("c2 did not close in time");
    match c2_res {
        Ok(0) | Err(_) => {}
        Ok(n) => panic!("expected c2 to be closed, got {n} bytes"),
    }

    // c1 must still be alive and relaying!
    c1.write_all(b"still-alive").await.unwrap();
    let mut alive_buf = [0u8; 11];
    c1.read_exact(&mut alive_buf).await.unwrap();
    assert_eq!(&alive_buf, b"still-alive");

    // New connections to l2 should now reach b3_addr
    tokio::time::sleep(Duration::from_millis(80)).await;
    let mut c2_new = TcpStream::connect(&l2).await.unwrap();
    c2_new.write_all(b"ping").await.unwrap();
    let mut new_buf = [0u8; 8];
    c2_new.read_exact(&mut new_buf).await.unwrap();
    assert_eq!(&new_buf, b"NEW:ping");

    // Now test disable -> force disconnect c1 too
    reconciler.toggle_endpoint(&ep1.meta.id, false).await.unwrap();
    let c1_res = tokio::time::timeout(Duration::from_secs(2), c1.read(&mut drain))
        .await
        .expect("c1 did not close on disable");
    match c1_res {
        Ok(0) | Err(_) => {}
        Ok(n) => panic!("expected c1 to be closed, got {n} bytes"),
    }

    reconciler.shutdown_all().await;
    b1_cancel.cancel();
    b2_cancel.cancel();
    b3_cancel.cancel();
}

#[tokio::test]
async fn test_tcp_and_udp_metering_and_live_devices() {
    let _ = shared_telemetry();
    let tmp = tempfile::tempdir().unwrap();
    let conf_path = tmp.path().join("realm.toml");
    let db_path = tmp.path().join("panel.db");
    ensure_config_file(&conf_path).unwrap();
    let storage = Storage::open(&db_path).unwrap();
    let reconciler = Reconciler::new(conf_path, storage).unwrap();
    reconciler.initial_apply().await.unwrap();

    // Backend prefixes "RESP:" (5 bytes) so rx != tx and we verify directional metering accuracy
    let (tcp_backend, tcp_cancel) = spawn_tcp_echo_with_prefix(b"RESP:").await;
    let (udp_backend, udp_cancel) = spawn_udp_echo_with_prefix(b"U:").await;

    let l_tcp = format!("127.0.0.1:{}", free_tcp_port());
    let l_udp = format!("127.0.0.1:{}", free_udp_port());

    let (ep_tcp, _) = reconciler
        .add_endpoint(make_endpoint_payload(
            "tcp-metered",
            l_tcp.clone(),
            tcp_backend.to_string(),
            false,
        ))
        .await
        .unwrap();
    let (ep_udp, _) = reconciler
        .add_endpoint(make_endpoint_payload(
            "udp-metered",
            l_udp.clone(),
            udp_backend.to_string(),
            true,
        ))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(80)).await;

    // 1. TCP transfer: 100 bytes sent, 105 bytes received
    let payload = vec![b'A'; 100];
    let mut client = TcpStream::connect(&l_tcp).await.unwrap();
    let client_port = client.local_addr().unwrap().port();
    client.write_all(&payload).await.unwrap();
    let mut reply = vec![0u8; 105];
    client.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply[..5], b"RESP:");
    assert_eq!(&reply[5..], payload.as_slice());

    tokio::time::sleep(Duration::from_millis(50)).await;
    {
        let running = reconciler.running.lock().await;
        let rt = &running.get(&ep_tcp.meta.id).unwrap().runtime;
        assert_eq!(rt.stats.rx_bytes.load(Ordering::Relaxed), 100);
        assert_eq!(rt.stats.tx_bytes.load(Ordering::Relaxed), 105);
        assert_eq!(rt.stats.active_tcp_conns.load(Ordering::Relaxed), 1);

        // Compute 1s live device speed
        rt.devices.tick_1s(60);
        let devs = rt.devices.list_snapshots(&ep_tcp.meta.id);
        assert_eq!(devs.len(), 1);
        assert_eq!(devs[0].ip, "127.0.0.1");
        assert_eq!(devs[0].proto, "tcp");
        assert_eq!(devs[0].active_conns, 1);
        assert_eq!(devs[0].rx_bytes, 100);
        assert_eq!(devs[0].tx_bytes, 105);
        assert_eq!(devs[0].rx_bps, 100);
        assert_eq!(devs[0].tx_bps, 105);
        assert!(devs[0].recent_ports.contains(&client_port));
    }

    drop(client);
    tokio::time::sleep(Duration::from_millis(50)).await;
    {
        let running = reconciler.running.lock().await;
        let rt = &running.get(&ep_tcp.meta.id).unwrap().runtime;
        assert_eq!(rt.stats.active_tcp_conns.load(Ordering::Relaxed), 0);
        // Should still be visible during 60s grace window
        rt.devices.tick_1s(60);
        assert_eq!(rt.devices.list_snapshots(&ep_tcp.meta.id).len(), 1);
        // If TTL = -1, idle disconnected device should be pruned immediately
        rt.devices.tick_1s(-1);
        assert!(rt.devices.list_snapshots(&ep_tcp.meta.id).is_empty());
    }

    // 2. UDP transfer: 10 bytes sent ("hello-udp!"), 12 bytes received ("U:hello-udp!")
    let uclient = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let uport = uclient.local_addr().unwrap().port();
    uclient.send_to(b"hello-udp!", &l_udp).await.unwrap();
    let mut ubuf = [0u8; 64];
    let (un, _) = tokio::time::timeout(Duration::from_secs(2), uclient.recv_from(&mut ubuf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&ubuf[..un], b"U:hello-udp!");

    tokio::time::sleep(Duration::from_millis(50)).await;
    {
        let running = reconciler.running.lock().await;
        let rt = &running.get(&ep_udp.meta.id).unwrap().runtime;
        assert_eq!(rt.stats.rx_bytes.load(Ordering::Relaxed), 10);
        assert_eq!(rt.stats.tx_bytes.load(Ordering::Relaxed), 12);
        rt.devices.tick_1s(60);
        let devs = rt.devices.list_snapshots(&ep_udp.meta.id);
        let udp_dev = devs.iter().find(|d| d.proto == Proto::Udp.as_str()).unwrap();
        assert_eq!(udp_dev.ip, "127.0.0.1");
        assert_eq!(udp_dev.rx_bytes, 10);
        assert_eq!(udp_dev.tx_bytes, 12);
        assert!(udp_dev.recent_ports.contains(&uport));
    }

    reconciler.shutdown_all().await;
    tcp_cancel.cancel();
    udp_cancel.cancel();
}

#[test]
fn test_storage_7d_30d_and_snapshots() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::open(&tmp.path().join("panel.db")).unwrap();

    let now = Utc::now().timestamp();
    let ts_3d = now - 3 * 86400;
    let ts_15d = now - 15 * 86400;

    storage.record_traffic_delta(ts_15d, "ep-a", 1000, 2000, 100, 200, 2, 0).unwrap();
    storage.record_traffic_delta(ts_3d, "ep-a", 300, 400, 50, 60, 1, 0).unwrap();
    storage.record_traffic_delta(now, "ep-a", 100, 200, 20, 40, 3, 1).unwrap();

    // 7d query uses traffic_1m and excludes ts_15d
    let series_7d = storage.query_traffic_series("7d", Some("ep-a")).unwrap();
    let sum_7d_rx: u64 = series_7d.iter().map(|p| p.rx_bytes).sum();
    assert_eq!(sum_7d_rx, 400);

    // 30d query uses traffic_1h and includes ts_15d + ts_3d + now
    let series_30d = storage.query_traffic_series("30d", Some("ep-a")).unwrap();
    let sum_30d_rx: u64 = series_30d.iter().map(|p| p.rx_bytes).sum();
    let sum_30d_tx: u64 = series_30d.iter().map(|p| p.tx_bytes).sum();
    assert_eq!(sum_30d_rx, 1400);
    assert_eq!(sum_30d_tx, 2600);

    let by_ep = storage.query_traffic_by_endpoint("30d").unwrap();
    assert_eq!(by_ep.len(), 1);
    assert_eq!(by_ep[0].endpoint_id, "ep-a");
    assert_eq!(by_ep[0].rx_bytes, 1400);

    let snap_id = storage.save_snapshot("test", "toml", "[[endpoints]]\n").unwrap();
    let snaps = storage.list_snapshots().unwrap();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0].id, snap_id);
    assert_eq!(storage.get_snapshot(snap_id).unwrap().unwrap().source, "test");
}

#[tokio::test]
async fn test_probe_reachability_transitions() {
    let _ = shared_telemetry();
    let tmp = tempfile::tempdir().unwrap();
    let conf_path = tmp.path().join("realm.toml");
    let db_path = tmp.path().join("panel.db");
    ensure_config_file(&conf_path).unwrap();
    let storage = Storage::open(&db_path).unwrap();
    let reconciler = Reconciler::new(conf_path, storage.clone()).unwrap();
    reconciler.initial_apply().await.unwrap();

    let target_lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_addr = target_lis.local_addr().unwrap();
    let listen_addr = format!("127.0.0.1:{}", free_tcp_port());

    let (ep, _) = reconciler
        .add_endpoint(make_endpoint_payload(
            "probe-ep",
            listen_addr,
            target_addr.to_string(),
            false,
        ))
        .await
        .unwrap();

    let probes = ProbeManager::new();

    // 1. Target up -> reachable = true
    let r1 = probes
        .probe_endpoint_now(&reconciler, &storage, &ep.meta.id)
        .await
        .unwrap();
    assert_eq!(r1.len(), 1);
    assert!(r1[0].reachable);
    assert!(r1[0].rtt_ms.is_some());
    assert_eq!(r1[0].consecutive_failures, 0);

    // 2. Target down -> reachable = false, failures increment
    drop(target_lis);
    let r2 = probes
        .probe_endpoint_now(&reconciler, &storage, &ep.meta.id)
        .await
        .unwrap();
    assert!(!r2[0].reachable);
    assert_eq!(r2[0].consecutive_failures, 1);

    let r3 = probes
        .probe_endpoint_now(&reconciler, &storage, &ep.meta.id)
        .await
        .unwrap();
    assert!(!r3[0].reachable);
    assert_eq!(r3[0].consecutive_failures, 2);
    assert!(r3[0].error_msg.is_some());

    // 3. Target recovered -> reachable = true, consecutive_failures reset to 0
    let _rebound = TcpListener::bind(target_addr).await.unwrap();
    let r4 = probes
        .probe_endpoint_now(&reconciler, &storage, &ep.meta.id)
        .await
        .unwrap();
    assert!(r4[0].reachable);
    assert_eq!(r4[0].consecutive_failures, 0);

    let hist = storage.query_probe_history(&ep.meta.id, 10).unwrap();
    assert_eq!(hist.len(), 4);

    reconciler.shutdown_all().await;
}

#[tokio::test]
async fn test_web_api_auth_crud_and_static_ui() {
    let telemetry = shared_telemetry();
    let tmp = tempfile::tempdir().unwrap();
    let conf_path = tmp.path().join("realm.toml");
    let db_path = tmp.path().join("panel.db");
    ensure_config_file(&conf_path).unwrap();

    let storage = Storage::open(&db_path).unwrap();
    let auth = AuthService::init(storage.clone(), "admin", Some("secret-pass")).unwrap();
    let reconciler = Reconciler::new(conf_path.clone(), storage.clone()).unwrap();
    reconciler.initial_apply().await.unwrap();

    let probes = ProbeManager::new();
    let metrics = MetricsManager::new();
    let cancel = CancellationToken::new();

    probes
        .clone()
        .start_scheduler(reconciler.clone(), storage.clone(), cancel.clone());
    metrics.clone().start_sampler(
        reconciler.clone(),
        storage.clone(),
        probes.clone(),
        cancel.clone(),
    );

    let state = Arc::new(AppState {
        reconciler: reconciler.clone(),
        storage: storage.clone(),
        metrics,
        probes,
        telemetry,
        auth,
    });

    let app = build_router(state);
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let web_addr = lis.local_addr().unwrap();
    let srv_cancel = cancel.clone();
    let srv = tokio::spawn(async move {
        let _ = axum::serve(lis, app)
            .with_graceful_shutdown(async move {
                srv_cancel.cancelled().await;
            })
            .await;
    });

    let base = format!("http://{web_addr}");
    let http = reqwest::Client::new();

    // 1. Health check and embedded frontend
    let health = http.get(format!("{base}/api/health")).send().await.unwrap();
    assert_eq!(health.status(), 200);
    assert_eq!(health.text().await.unwrap(), "ok");

    let index = http.get(format!("{base}/")).send().await.unwrap();
    assert_eq!(index.status(), 200);
    let html = index.text().await.unwrap();
    assert!(html.contains("<div id=\"root\">"), "embedded index.html should be served");

    // 2. Auth enforcement
    let unauth = http.get(format!("{base}/api/endpoints")).send().await.unwrap();
    assert_eq!(unauth.status(), 401);

    let bad_login = http
        .post(format!("{base}/api/auth/login"))
        .json(&json!({"username": "admin", "password": "wrong"}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_login.status(), 401);

    let login_res: serde_json::Value = http
        .post(format!("{base}/api/auth/login"))
        .json(&json!({"username": "admin", "password": "secret-pass"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = login_res["token"].as_str().unwrap().to_string();

    // 3. Invalid config rejected with 400
    let invalid = http
        .post(format!("{base}/api/endpoints"))
        .bearer_auth(&token)
        .json(&json!({
            "name": "bad",
            "listen": "not-an-ip",
            "remote": "127.0.0.1:80"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), 400);

    // 4. Create endpoint via API -> persisted to realm.toml & running
    let (target_addr, target_cancel) = spawn_tcp_echo_with_prefix(b"").await;
    let listen_port = free_tcp_port();
    let created: serde_json::Value = http
        .post(format!("{base}/api/endpoints"))
        .bearer_auth(&token)
        .json(&json!({
            "name": "api-rule",
            "enabled": true,
            "probe_enabled": true,
            "probe_interval_sec": 10,
            "probe_timeout_ms": 1000,
            "listen": format!("127.0.0.1:{listen_port}"),
            "remote": target_addr.to_string(),
            "network": { "use_udp": true }
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ep_id = created["id"].as_str().unwrap().to_string();
    let toml_text = std::fs::read_to_string(&conf_path).unwrap();
    assert!(toml_text.contains(&format!("127.0.0.1:{listen_port}")));

    // Wait for 1s metrics sampler to produce at least 1 snapshot
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // 5. Probe now, update, toggle, export, stats, devices, logs
    let probe_res = http
        .post(format!("{base}/api/probes/{ep_id}/run"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(probe_res.status(), 200);

    let overview: serde_json::Value = http
        .get(format!("{base}/api/stats/overview"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(overview["latest"]["perf"]["sys_mem_total_kb"].as_u64().unwrap() > 0);
    assert_eq!(overview["latest"]["endpoints"].as_array().unwrap().len(), 1);

    for path in [
        "/api/stats/traffic?range=7d",
        "/api/stats/traffic?range=30d",
        "/api/stats/performance?range=1h",
        "/api/devices",
        "/api/probes",
        "/api/logs?limit=50",
        "/api/config/snapshots",
    ] {
        let resp = http
            .get(format!("{base}{path}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "GET {path} failed");
    }

    let export_txt = reconciler.export_current_config(ConfigFormat::Toml).await.unwrap();
    assert!(export_txt.contains("[[endpoints]]"));

    let lvl_resp = http
        .put(format!("{base}/api/logs/level"))
        .bearer_auth(&token)
        .json(&json!({"level": "debug"}))
        .send()
        .await
        .unwrap();
    assert_eq!(lvl_resp.status(), 200);

    let del_resp = http
        .delete(format!("{base}/api/endpoints/{ep_id}"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(del_resp.status(), 200);

    let list_after: Vec<serde_json::Value> = http
        .get(format!("{base}/api/endpoints"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(list_after.is_empty());

    cancel.cancel();
    reconciler.shutdown_all().await;
    target_cancel.cancel();
    let _ = srv.await;
}
