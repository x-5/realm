use std::collections::HashMap;
use std::io::{IoSlice, IoSliceMut, Result};
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

use realm_core::endpoint::{BindOpts, ConnectOpts, Endpoint};
use realm_core::time::timeoutfut;
use realm_io::mmsg::{recv_mul_pkts, send_mul_pkts, MmsgHdr, MmsgHdrMut, SockAddrStore};
use realm_syscall::new_udp_socket;
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

use super::tcp::resolve_remote;
use super::{DeviceEntry, DeviceTracker, EndpointRuntime, EndpointStats, SharedResolver};

const PACKET_SIZE: usize = 1500;
const MAX_PACKETS: usize = 128;

#[derive(Clone)]
struct Packet {
    buf: [u8; PACKET_SIZE],
    addr: SockAddrStore,
    cursor: u16,
}

impl Packet {
    fn new() -> Self {
        Self {
            buf: [0u8; PACKET_SIZE],
            addr: SockAddrStore::new(),
            cursor: 0,
        }
    }

    fn bytes(&self) -> &[u8] {
        &self.buf[..self.cursor as usize]
    }
}

struct SockMap(RwLock<HashMap<SocketAddr, Arc<UdpSocket>>>);

impl SockMap {
    fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    fn find(&self, addr: &SocketAddr) -> Option<Arc<UdpSocket>> {
        self.0.read().unwrap().get(addr).cloned()
    }

    fn insert(&self, addr: SocketAddr, sock: Arc<UdpSocket>) {
        self.0.write().unwrap().insert(addr, sock);
    }

    fn remove(&self, addr: &SocketAddr) {
        self.0.write().unwrap().remove(addr);
    }
}

struct UdpAssocGuard {
    stats: Arc<EndpointStats>,
    device: Arc<DeviceEntry>,
    sockmap: Arc<SockMap>,
    laddr: SocketAddr,
}

impl Drop for UdpAssocGuard {
    fn drop(&mut self) {
        self.sockmap.remove(&self.laddr);
        let _ = self
            .stats
            .active_udp_assocs
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(1)));
        DeviceTracker::on_disconnect(&self.device);
    }
}

fn bind_udp(laddr: &SocketAddr, bind_opts: &BindOpts) -> Result<UdpSocket> {
    let socket = new_udp_socket(laddr)?;
    if let SocketAddr::V6(_) = laddr {
        socket.set_only_v6(bind_opts.ipv6_only)?;
    }
    #[cfg(target_os = "linux")]
    if let Some(ref iface) = bind_opts.bind_interface {
        realm_syscall::bind_to_device(&socket, iface)?;
    }
    let _ = socket.set_reuse_address(true);
    socket.bind(&(*laddr).into())?;
    UdpSocket::from_std(socket.into())
}

fn associate_udp(raddr: &SocketAddr, conn_opts: &ConnectOpts) -> Result<UdpSocket> {
    let socket = new_udp_socket(raddr)?;
    let _ = socket.set_reuse_address(true);
    if let Some(addr) = conn_opts.bind_address {
        socket.bind(&addr.into())?;
    }
    #[cfg(target_os = "linux")]
    if let Some(ref iface) = conn_opts.bind_interface {
        realm_syscall::bind_to_device(&socket, iface)?;
    }
    UdpSocket::from_std(socket.into())
}

async fn recv_some(sock: &UdpSocket, pkts: &mut [Packet]) -> Result<usize> {
    #[cfg(target_os = "linux")]
    {
        let pkt_amt = pkts.len().min(MAX_PACKETS);
        let mut iovs: MaybeUninit<[IoSliceMut; MAX_PACKETS]> = MaybeUninit::uninit();
        let mut msgs: MaybeUninit<[MmsgHdrMut; MAX_PACKETS]> = MaybeUninit::uninit();
        let iovs = unsafe { iovs.assume_init_mut() };
        let msgs = unsafe { msgs.assume_init_mut() };

        for ((pkt, iov), msg) in pkts.iter_mut().zip(iovs.iter_mut()).zip(msgs.iter_mut()) {
            *iov = IoSliceMut::new(&mut pkt.buf);
            *msg = MmsgHdrMut::new()
                .with_addr(&mut pkt.addr)
                .with_iovec(std::slice::from_mut(iov));
        }

        let n = recv_mul_pkts(sock, &mut msgs[..pkt_amt]).await?;
        let mut bytes: [u16; MAX_PACKETS] = [0u16; MAX_PACKETS];
        for (msg, byte) in msgs.iter().zip(bytes.iter_mut()).take(n) {
            *byte = msg.get_ref().nbytes() as u16;
        }
        for (pkt, byte) in pkts.iter_mut().zip(bytes).take(n) {
            pkt.cursor = byte;
        }
        Ok(n)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let pkt = &mut pkts[0];
        let (bytes, addr) = sock.recv_from(&mut pkt.buf).await?;
        pkt.addr = addr.into();
        pkt.cursor = bytes as u16;
        Ok(1)
    }
}

async fn send_all_to(sock: &UdpSocket, pkts: &[Packet], target: &SockAddrStore) -> Result<u64> {
    #[cfg(target_os = "linux")]
    {
        let pkt_amt = pkts.len().min(MAX_PACKETS);
        let mut iovs: MaybeUninit<[IoSlice; MAX_PACKETS]> = MaybeUninit::uninit();
        let mut msgs: MaybeUninit<[MmsgHdr; MAX_PACKETS]> = MaybeUninit::uninit();
        let iovs = unsafe { iovs.assume_init_mut() };
        let msgs = unsafe { msgs.assume_init_mut() };

        let mut total_bytes = 0u64;
        for ((pkt, iov), msg) in pkts.iter().take(pkt_amt).zip(iovs.iter_mut()).zip(msgs.iter_mut()) {
            let slice = pkt.bytes();
            total_bytes += slice.len() as u64;
            *iov = IoSlice::new(slice);
            *msg = MmsgHdr::new()
                .with_addr(target)
                .with_iovec(std::slice::from_ref(iov));
        }

        let mut cursor = 0;
        while cursor < pkt_amt {
            let n = send_mul_pkts(sock, &mut msgs[cursor..pkt_amt]).await?;
            cursor += n;
        }
        Ok(total_bytes)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let addr: SocketAddr = target.clone().into();
        let mut total_bytes = 0u64;
        for pkt in pkts {
            let slice = pkt.bytes();
            total_bytes += slice.len() as u64;
            let _ = sock.send_to(slice, addr).await?;
        }
        Ok(total_bytes)
    }
}

pub async fn run_udp(
    runtime: Arc<EndpointRuntime>,
    endpoint: Endpoint,
    resolver: SharedResolver,
) {
    let ep_id = runtime.id.clone();
    let cancel = runtime.cancel.clone();

    let lis = match bind_udp(&endpoint.laddr, &endpoint.bind_opts) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            let msg = format!("udp bind {} failed: {}", endpoint.laddr, e);
            tracing::error!(endpoint_id = %ep_id, listen = %endpoint.laddr, error = %e, "{}", msg);
            runtime.set_error(Some(msg));
            return;
        }
    };

    tracing::info!(
        endpoint_id = %ep_id,
        listen = %endpoint.laddr,
        remote = %endpoint.raddr,
        "udp relay started"
    );

    let sockmap = Arc::new(SockMap::new());
    let endpoint = Arc::new(endpoint);
    let mut pkts = vec![Packet::new(); MAX_PACKETS];

    loop {
        let n = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::info!(endpoint_id = %ep_id, listen = %endpoint.laddr, "udp listener cancelled");
                break;
            }
            res = recv_some(&lis, &mut pkts) => match res {
                Ok(n) => n,
                Err(e) => {
                    runtime.stats.connect_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(endpoint_id = %ep_id, error = %e, "udp recv error");
                    continue;
                }
            }
        };

        let raddr = match resolve_remote(&resolver, &endpoint.raddr).await {
            Ok(mut list) => list.remove(0),
            Err(e) => {
                runtime.stats.connect_errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(endpoint_id = %ep_id, remote = %endpoint.raddr, error = %e, "udp resolve failed");
                continue;
            }
        };

        // Group packets by source client address
        let mut by_client: HashMap<SocketAddr, Vec<Packet>> = HashMap::new();
        for pkt in pkts.iter().take(n) {
            let caddr: SocketAddr = pkt.addr.clone().into();
            by_client.entry(caddr).or_default().push(pkt.clone());
        }

        let raddr_store: SockAddrStore = raddr.into();

        for (caddr, client_pkts) in by_client {
            let device = runtime.devices.get_or_touch_udp(caddr);
            let rsock = match sockmap.find(&caddr) {
                Some(s) => s,
                None => match associate_udp(&raddr, &endpoint.conn_opts) {
                    Ok(s) => {
                        let s = Arc::new(s);
                        sockmap.insert(caddr, s.clone());
                        device.active_conns.fetch_add(1, Ordering::Relaxed);
                        device.total_conns.fetch_add(1, Ordering::Relaxed);
                        runtime.stats.active_udp_assocs.fetch_add(1, Ordering::Relaxed);

                        tracing::info!(
                            endpoint_id = %ep_id,
                            client_addr = %caddr,
                            remote = %raddr,
                            "udp new association"
                        );

                        tokio::spawn(send_back(
                            runtime.clone(),
                            lis.clone(),
                            caddr,
                            s.clone(),
                            sockmap.clone(),
                            device.clone(),
                            endpoint.conn_opts.associate_timeout,
                            cancel.clone(),
                        ));
                        s
                    }
                    Err(e) => {
                        runtime.stats.connect_errors.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(endpoint_id = %ep_id, client_addr = %caddr, error = %e, "udp associate failed");
                        continue;
                    }
                },
            };

            match send_all_to(&rsock, &client_pkts, &raddr_store).await {
                Ok(bytes) => {
                    runtime.stats.rx_bytes.fetch_add(bytes, Ordering::Relaxed);
                    device.rx_bytes.fetch_add(bytes, Ordering::Relaxed);
                }
                Err(e) => {
                    runtime.stats.connect_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(endpoint_id = %ep_id, client_addr = %caddr, error = %e, "udp send to remote failed");
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn send_back(
    runtime: Arc<EndpointRuntime>,
    lsock: Arc<UdpSocket>,
    caddr: SocketAddr,
    rsock: Arc<UdpSocket>,
    sockmap: Arc<SockMap>,
    device: Arc<DeviceEntry>,
    timeout: usize,
    cancel: CancellationToken,
) {
    let _guard = UdpAssocGuard {
        stats: runtime.stats.clone(),
        device: device.clone(),
        sockmap,
        laddr: caddr,
    };

    let caddr_store: SockAddrStore = caddr.into();
    let mut pkts = vec![Packet::new(); MAX_PACKETS];

    loop {
        let n = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!(endpoint_id = %runtime.id, client_addr = %caddr, "udp association force-closed");
                break;
            }
            res = timeoutfut(recv_some(&rsock, &mut pkts), timeout) => match res {
                Ok(Ok(n)) => n,
                Ok(Err(e)) => {
                    tracing::warn!(endpoint_id = %runtime.id, client_addr = %caddr, error = %e, "udp rear recv failed");
                    break;
                }
                Err(_) => {
                    tracing::debug!(endpoint_id = %runtime.id, client_addr = %caddr, "udp association timeout");
                    break;
                }
            }
        };

        match send_all_to(&lsock, &pkts[..n], &caddr_store).await {
            Ok(bytes) => {
                runtime.stats.tx_bytes.fetch_add(bytes, Ordering::Relaxed);
                device.tx_bytes.fetch_add(bytes, Ordering::Relaxed);
                device.last_active_ts.store(chrono::Utc::now().timestamp(), Ordering::Relaxed);
            }
            Err(e) => {
                tracing::warn!(endpoint_id = %runtime.id, client_addr = %caddr, error = %e, "udp send back failed");
                break;
            }
        }
    }
}
