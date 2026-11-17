use std::cell::OnceCell;
use std::io::{Error, ErrorKind, Result};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Buf, BytesMut};
use futures::try_join;
use realm_core::kaminari::mix::{MixAccept, MixConnect};
use realm_core::kaminari::{AsyncAccept, AsyncConnect, IOStream};
use proxy_protocol::{encode, parse, version1 as v1, version2 as v2, ProxyHeader};
use realm_core::endpoint::{BindOpts, ConnectOpts, Endpoint, ProxyOpts, RemoteAddr};
use realm_core::time::timeoutfut;
use realm_io::buf_size;
use realm_lb::{BalanceCtx, Token};
use realm_syscall::socket2::{SockRef, Socket, TcpKeepalive};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio_util::sync::CancellationToken;

use super::metered_io::{
    bidi_mem_copy_metered, bidi_mem_copy_with_bufs, bidi_zero_copy_metered, ByteCounter,
};
use super::{DeviceEntry, DeviceTracker, EndpointRuntime, EndpointStats, Proto, SharedResolver};

struct TcpConnGuard {
    stats: Arc<EndpointStats>,
    device: Arc<DeviceEntry>,
}

impl TcpConnGuard {
    fn new(stats: Arc<EndpointStats>, device: Arc<DeviceEntry>) -> Self {
        stats.active_tcp_conns.fetch_add(1, Ordering::Relaxed);
        stats.total_tcp_conns.fetch_add(1, Ordering::Relaxed);
        Self { stats, device }
    }
}

impl Drop for TcpConnGuard {
    fn drop(&mut self) {
        let _ = self
            .stats
            .active_tcp_conns
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(1)));
        DeviceTracker::on_disconnect(&self.device);
    }
}

pub async fn resolve_remote(
    resolver: &SharedResolver,
    raddr: &RemoteAddr,
) -> Result<Vec<SocketAddr>> {
    match raddr {
        RemoteAddr::SocketAddr(addr) => Ok(vec![*addr]),
        RemoteAddr::DomainName(host, port) => {
            let guard = resolver.load();
            let lookup = guard.lookup_ip(host.as_str()).await.map_err(Error::other)?;
            let addrs: Vec<SocketAddr> = lookup.iter().map(|ip| SocketAddr::new(ip, *port)).collect();
            if addrs.is_empty() {
                Err(Error::new(
                    ErrorKind::NotFound,
                    format!("dns returned no addresses for {}:{}", host, port),
                ))
            } else {
                Ok(addrs)
            }
        }
    }
}

fn new_socket(addr: &SocketAddr, mptcp: bool) -> Result<Socket> {
    #[cfg(target_os = "linux")]
    {
        if mptcp {
            realm_syscall::new_mptcp_socket(addr)
        } else {
            realm_syscall::new_tcp_socket(addr)
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        realm_syscall::new_tcp_socket(addr)
    }
}

fn build_keepalive(conn_opts: &ConnectOpts) -> Option<TcpKeepalive> {
    if conn_opts.tcp_keepalive == 0 {
        return None;
    }
    let secs = Duration::from_secs(conn_opts.tcp_keepalive as u64);
    let mut kpa = TcpKeepalive::new().with_time(secs);
    #[cfg(not(target_os = "openbsd"))]
    {
        kpa = TcpKeepalive::with_interval(kpa, secs);
        kpa = TcpKeepalive::with_retries(kpa, conn_opts.tcp_keepalive_probe as u32);
    }
    Some(kpa)
}

pub fn bind_listener(laddr: &SocketAddr, bind_opts: &BindOpts) -> Result<TcpListener> {
    let socket = new_socket(laddr, bind_opts.accept_mptcp)?;
    if let SocketAddr::V6(_) = laddr {
        socket.set_only_v6(bind_opts.ipv6_only)?;
    }
    #[cfg(target_os = "linux")]
    if let Some(ref iface) = bind_opts.bind_interface {
        realm_syscall::bind_to_device(&socket, iface)?;
    }
    let _ = socket.set_reuse_address(true);
    socket.bind(&(*laddr).into())?;
    socket.listen(1024)?;
    TcpListener::from_std(socket.into())
}

pub async fn run_tcp(
    runtime: Arc<EndpointRuntime>,
    endpoint: Endpoint,
    resolver: SharedResolver,
) {
    let ep_id = runtime.id.clone();
    let cancel = runtime.cancel.clone();

    let lis = match bind_listener(&endpoint.laddr, &endpoint.bind_opts) {
        Ok(l) => {
            runtime.set_error(None);
            l
        }
        Err(e) => {
            let msg = format!("tcp bind {} failed: {}", endpoint.laddr, e);
            tracing::error!(endpoint_id = %ep_id, listen = %endpoint.laddr, error = %e, "{}", msg);
            runtime.set_error(Some(msg));
            return;
        }
    };

    tracing::info!(
        endpoint_id = %ep_id,
        listen = %endpoint.laddr,
        remote = %endpoint.raddr,
        "tcp relay started"
    );

    let endpoint = Arc::new(endpoint);
    let keepalive = build_keepalive(&endpoint.conn_opts);

    loop {
        let accepted = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::info!(endpoint_id = %ep_id, listen = %endpoint.laddr, "tcp listener cancelled");
                break;
            }
            res = lis.accept() => res,
        };

        let (local, client_addr) = match accepted {
            Ok(x) => x,
            Err(e) if e.kind() == ErrorKind::ConnectionAborted => {
                tracing::warn!(endpoint_id = %ep_id, error = %e, "tcp accept aborted");
                continue;
            }
            Err(e) => {
                tracing::error!(endpoint_id = %ep_id, error = %e, "tcp accept fatal error");
                runtime.set_error(Some(format!("tcp accept error: {e}")));
                break;
            }
        };

        let _ = local.set_nodelay(true);
        if let Some(ref kpa) = keepalive {
            let _ = SockRef::from(&local).set_tcp_keepalive(kpa);
        }

        let rt = runtime.clone();
        let ep = endpoint.clone();
        let resv = resolver.clone();
        let conn_cancel = cancel.clone();

        tokio::spawn(async move {
            handle_connection(rt, ep, resv, local, client_addr, conn_cancel).await;
        });
    }
}

async fn handle_connection(
    runtime: Arc<EndpointRuntime>,
    endpoint: Arc<Endpoint>,
    resolver: SharedResolver,
    local: TcpStream,
    client_addr: SocketAddr,
    cancel: CancellationToken,
) {
    let ep_id = runtime.id.clone();
    let conn_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let started = Instant::now();

    let device = runtime.devices.on_connect(client_addr, Proto::Tcp);
    let _guard = TcpConnGuard::new(runtime.stats.clone(), device.clone());

    let rx_counter = ByteCounter::new(runtime.stats.rx_bytes.clone(), device.rx_bytes.clone());
    let tx_counter = ByteCounter::new(runtime.stats.tx_bytes.clone(), device.tx_bytes.clone());

    let res = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            tracing::debug!(
                endpoint_id = %ep_id,
                conn_id = %conn_id,
                client_addr = %client_addr,
                "tcp connection force-closed by reload/shutdown"
            );
            return;
        }
        r = connect_and_relay(
            &ep_id,
            &conn_id,
            local,
            client_addr,
            &endpoint,
            &resolver,
            &runtime.stats,
            rx_counter,
            tx_counter,
        ) => r,
    };

    let duration_ms = started.elapsed().as_millis() as u64;
    match res {
        Ok((rx, tx)) => {
            tracing::info!(
                endpoint_id = %ep_id,
                conn_id = %conn_id,
                client_addr = %client_addr,
                rx_bytes = rx,
                tx_bytes = tx,
                duration_ms = duration_ms,
                "tcp connection closed"
            );
        }
        Err(e) => {
            runtime.stats.connect_errors.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                endpoint_id = %ep_id,
                conn_id = %conn_id,
                client_addr = %client_addr,
                duration_ms = duration_ms,
                error = %e,
                "tcp relay error"
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn connect_and_relay(
    ep_id: &str,
    conn_id: &str,
    mut local: TcpStream,
    client_addr: SocketAddr,
    endpoint: &Endpoint,
    resolver: &SharedResolver,
    stats: &EndpointStats,
    rx_counter: ByteCounter,
    tx_counter: ByteCounter,
) -> Result<(u64, u64)> {
    // 1. Pre-connect hook (if loaded)
    let mut selected = if realm_hook::pre_conn::is_loaded() {
        let len = realm_hook::pre_conn::first_pkt_len() as usize;
        let mut buf = Vec::<u8>::new();
        if len != 0 {
            buf.resize(len, 0);
            while local.peek(&mut buf).await? < len {}
        }
        let idx = realm_hook::pre_conn::decide_remote_idx(
            endpoint.extra_raddrs.len() as i32,
            buf.as_ptr(),
        );
        match idx {
            0 => Some(&endpoint.raddr),
            i if i >= 1 && (i as usize) <= endpoint.extra_raddrs.len() => {
                Some(&endpoint.extra_raddrs[i as usize - 1])
            }
            _ => return Err(Error::other("rejected by pre-connect hook")),
        }
    } else {
        None
    };

    // 2. Load balancer
    if selected.is_none() {
        let token = endpoint.conn_opts.balancer.next(BalanceCtx {
            src_ip: &client_addr.ip(),
        });
        selected = Some(match token {
            None | Some(Token(0)) => &endpoint.raddr,
            Some(Token(idx)) => {
                let i = idx as usize - 1;
                if i < endpoint.extra_raddrs.len() {
                    &endpoint.extra_raddrs[i]
                } else {
                    &endpoint.raddr
                }
            }
        });
    }

    let raddr = selected.unwrap();

    // 3. Connect to target
    let conn_start = Instant::now();
    let mut remote = connect_target(raddr, &endpoint.conn_opts, resolver).await?;
    let rtt_us = conn_start.elapsed().as_micros() as u64;
    stats.last_rtt_us.store(rtt_us, Ordering::Relaxed);

    let remote_peer = remote.peer_addr().ok();
    tracing::debug!(
        endpoint_id = %ep_id,
        conn_id = %conn_id,
        client_addr = %client_addr,
        remote = %raddr,
        remote_peer = ?remote_peer,
        connect_rtt_us = rtt_us,
        "tcp connected"
    );

    // 4. PROXY protocol
    if endpoint.conn_opts.proxy_opts.accept_proxy || endpoint.conn_opts.proxy_opts.send_proxy {
        handle_proxy(&mut local, &mut remote, endpoint.conn_opts.proxy_opts).await?;
    }

    // 5. Relay (Transport WS/TLS/WSS or Plain zero-copy splice + fallback)
    if let Some((ref ac, ref cc)) = endpoint.conn_opts.transport {
        run_transport_relay(local, remote, ac, cc, rx_counter, tx_counter).await
    } else {
        run_plain_relay(&mut local, &mut remote, stats, rx_counter, tx_counter).await
    }
}

async fn connect_target(
    raddr: &RemoteAddr,
    conn_opts: &ConnectOpts,
    resolver: &SharedResolver,
) -> Result<TcpStream> {
    let mut last_err = None;
    let keepalive = build_keepalive(conn_opts);

    for addr in resolve_remote(resolver, raddr).await? {
        let socket = new_socket(&addr, conn_opts.send_mptcp)?;
        let _ = socket.set_tcp_nodelay(true);
        let _ = socket.set_reuse_address(true);

        if let Some(baddr) = conn_opts.bind_address {
            socket.bind(&baddr.into())?;
        }
        #[cfg(target_os = "linux")]
        if let Some(ref iface) = conn_opts.bind_interface {
            realm_syscall::bind_to_device(&socket, iface)?;
        }
        if let Some(ref kpa) = keepalive {
            socket.set_tcp_keepalive(kpa)?;
        }

        let socket = TcpSocket::from_std_stream(socket.into());
        match timeoutfut(socket.connect(addr), conn_opts.connect_timeout).await {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(e)) => last_err = Some(e),
            Err(_) => {
                last_err = Some(Error::new(
                    ErrorKind::TimedOut,
                    format!("connect to {} timeout", addr),
                ))
            }
        }
    }

    Err(last_err
        .unwrap_or_else(|| Error::new(ErrorKind::InvalidInput, "could not connect to any address")))
}

async fn run_plain_relay(
    local: &mut TcpStream,
    remote: &mut TcpStream,
    stats: &EndpointStats,
    rx: ByteCounter,
    tx: ByteCounter,
) -> Result<(u64, u64)> {
    #[cfg(target_os = "linux")]
    {
        stats.zero_copy_conns.fetch_add(1, Ordering::Relaxed);
        match bidi_zero_copy_metered(local, remote, rx.clone(), tx.clone()).await {
            Ok(counts) => Ok(counts),
            Err(ref e) if e.kind() == ErrorKind::InvalidInput => {
                bidi_mem_copy_metered(local, remote, rx, tx).await
            }
            Err(e) => Err(e),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = stats;
        bidi_mem_copy_metered(local, remote, rx, tx).await
    }
}

async fn run_transport_relay<S: IOStream>(
    src: S,
    dst: S,
    ac: &MixAccept,
    cc: &MixConnect,
    rx: ByteCounter,
    tx: ByteCounter,
) -> Result<(u64, u64)> {
    macro_rules! hs_relay {
        ($ac: expr, $cc: expr) => {
            handshake_and_relay(src, dst, $ac, $cc, rx, tx).await
        };
    }

    if let Some(plain_ac) = ac.as_plain() {
        return match cc {
            MixConnect::Plain(c) => hs_relay!(plain_ac, c),
            MixConnect::Ws(c) => hs_relay!(plain_ac, c),
            MixConnect::Tls(c) => hs_relay!(plain_ac, c),
            MixConnect::Wss(c) => hs_relay!(plain_ac, c),
        };
    }
    if let Some(plain_cc) = cc.as_plain() {
        return match ac {
            MixAccept::Plain(a) => hs_relay!(a, plain_cc),
            MixAccept::Ws(a) => hs_relay!(a, plain_cc),
            MixAccept::Tls(a) => hs_relay!(a, plain_cc),
            MixAccept::Wss(a) => hs_relay!(a, plain_cc),
        };
    }

    hs_relay!(ac, cc)
}

async fn handshake_and_relay<S, AC, CC>(
    src: S,
    dst: S,
    ac: &AC,
    cc: &CC,
    rx: ByteCounter,
    tx: ByteCounter,
) -> Result<(u64, u64)>
where
    S: IOStream,
    AC: AsyncAccept<S>,
    CC: AsyncConnect<S>,
{
    let mut buf1 = vec![0; buf_size()];
    let mut buf2 = vec![0; buf_size()];

    let (mut src_s, mut dst_s) = try_join!(ac.accept(src, &mut buf1), cc.connect(dst, &mut buf2))?;

    bidi_mem_copy_with_bufs(&mut src_s, &mut dst_s, buf1, buf2, rx, tx).await
}

async fn handle_proxy(
    src: &mut TcpStream,
    dst: &mut TcpStream,
    opts: ProxyOpts,
) -> Result<()> {
    let ProxyOpts {
        send_proxy,
        accept_proxy,
        send_proxy_version,
        accept_proxy_timeout,
    } = opts;

    let client_addr = OnceCell::<SocketAddr>::new();
    let server_addr = OnceCell::<SocketAddr>::new();

    if accept_proxy {
        let mut buf = BytesMut::with_capacity(256);
        buf.resize(256, 0);
        let peek_n = timeoutfut(src.peek(&mut buf), accept_proxy_timeout).await??;
        buf.truncate(peek_n);

        let mut slice = buf.as_ref();
        let header = parse(&mut slice).map_err(Error::other)?;
        let parsed_n = peek_n - slice.remaining();

        if let Some((s, d)) = parse_header_addrs(header) {
            let _ = client_addr.set(s);
            let _ = server_addr.set(d);
        }

        buf.truncate(parsed_n);
        src.read_exact(&mut buf).await?;

        if !send_proxy {
            return Ok(());
        }
    }

    if client_addr.get().is_none() {
        let peer = src.peer_addr()?;
        let _ = client_addr.set(peer);
        let zero = match peer {
            SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
        };
        let _ = server_addr.set(zero);
    }

    let c = client_addr.into_inner().unwrap();
    let s = server_addr.into_inner().unwrap();
    let hdr = make_proxy_header(c, s, send_proxy_version);
    let bytes = encode(hdr).map_err(Error::other)?;
    dst.write_all(&bytes).await?;
    Ok(())
}

fn parse_header_addrs(header: ProxyHeader) -> Option<(SocketAddr, SocketAddr)> {
    match header {
        ProxyHeader::Version1 { addresses } => match addresses {
            v1::ProxyAddresses::Ipv4 {
                source,
                destination,
            } => Some((SocketAddr::V4(source), SocketAddr::V4(destination))),
            v1::ProxyAddresses::Ipv6 {
                source,
                destination,
            } => Some((SocketAddr::V6(source), SocketAddr::V6(destination))),
            _ => None,
        },
        ProxyHeader::Version2 {
            command,
            transport_protocol,
            addresses,
        } => {
            if let v2::ProxyCommand::Local = command {
                return None;
            }
            if !matches!(transport_protocol, v2::ProxyTransportProtocol::Stream) {
                return None;
            }
            match addresses {
                v2::ProxyAddresses::Ipv4 {
                    source,
                    destination,
                } => Some((SocketAddr::V4(source), SocketAddr::V4(destination))),
                v2::ProxyAddresses::Ipv6 {
                    source,
                    destination,
                } => Some((SocketAddr::V6(source), SocketAddr::V6(destination))),
                _ => None,
            }
        }
        _ => None,
    }
}

fn make_proxy_header(client: SocketAddr, server: SocketAddr, ver: usize) -> ProxyHeader {
    match ver {
        1 => ProxyHeader::Version1 {
            addresses: match (client, server) {
                (SocketAddr::V4(s), SocketAddr::V4(d)) => v1::ProxyAddresses::Ipv4 {
                    source: s,
                    destination: d,
                },
                (SocketAddr::V6(s), SocketAddr::V6(d)) => v1::ProxyAddresses::Ipv6 {
                    source: s,
                    destination: d,
                },
                _ => v1::ProxyAddresses::Unknown,
            },
        },
        _ => ProxyHeader::Version2 {
            command: v2::ProxyCommand::Proxy,
            transport_protocol: v2::ProxyTransportProtocol::Stream,
            addresses: match (client, server) {
                (SocketAddr::V4(s), SocketAddr::V4(d)) => v2::ProxyAddresses::Ipv4 {
                    source: s,
                    destination: d,
                },
                (SocketAddr::V6(s), SocketAddr::V6(d)) => v2::ProxyAddresses::Ipv6 {
                    source: s,
                    destination: d,
                },
                _ => v2::ProxyAddresses::Unspec,
            },
        },
    }
}
