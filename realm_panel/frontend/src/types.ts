export interface NetConf {
  no_tcp?: boolean;
  use_udp?: boolean;
  ipv6_only?: boolean;
  send_mptcp?: boolean;
  accept_mptcp?: boolean;
  send_proxy?: boolean;
  accept_proxy?: boolean;
  send_proxy_version?: number;
  accept_proxy_timeout?: number;
  tcp_keepalive?: number;
  tcp_keepalive_probe?: number;
  tcp_timeout?: number;
  udp_timeout?: number;
}

export interface LiveDeviceSnapshot {
  endpoint_id: string;
  ip: string;
  proto: string;
  active_conns: number;
  total_conns: number;
  rx_bytes: number;
  tx_bytes: number;
  rx_bps: number;
  tx_bps: number;
  first_seen_ts: number;
  last_active_ts: number;
  recent_ports: number[];
}

export interface TargetProbeStatus {
  endpoint_id: string;
  target: string;
  protocol: string;
  reachable: boolean;
  dns_ms: number | null;
  rtt_ms: number | null;
  resolved_ip: string | null;
  consecutive_failures: number;
  last_checked_ts: number;
  error_msg: string | null;
}

export interface EndpointView {
  id: string;
  name: string;
  enabled: boolean;
  probe_enabled: boolean;
  probe_interval_sec: number;
  probe_timeout_ms: number;
  listen: string;
  remote: string;
  extra_remotes?: string[];
  balance?: string | null;
  through?: string | null;
  interface?: string | null;
  listen_interface?: string | null;
  listen_transport?: string | null;
  remote_transport?: string | null;
  network?: NetConf;
  running: boolean;
  last_error: string | null;
  rx_bytes: number;
  tx_bytes: number;
  rx_bps: number;
  tx_bps: number;
  active_tcp_conns: number;
  active_udp_assocs: number;
  total_tcp_conns: number;
  zero_copy_conns: number;
  connect_errors: number;
  last_rtt_us: number;
  devices: LiveDeviceSnapshot[];
  probes: TargetProbeStatus[];
}

export interface PerfSample {
  ts: number;
  proc_cpu_pct: number;
  proc_mem_rss_kb: number;
  proc_mem_vsz_kb: number;
  open_fds: number;
  fd_limit: number;
  threads: number;
  tokio_workers: number;
  tokio_alive_tasks: number;
  sys_cpu_pct: number;
  sys_mem_used_kb: number;
  sys_mem_total_kb: number;
}

export interface EndpointLiveMetric {
  endpoint_id: string;
  name: string;
  listen: string;
  remote: string;
  rx_bytes: number;
  tx_bytes: number;
  rx_bps: number;
  tx_bps: number;
  active_tcp_conns: number;
  active_udp_assocs: number;
  total_tcp_conns: number;
  zero_copy_conns: number;
  connect_errors: number;
  last_rtt_us: number;
  device_count: number;
  last_error: string | null;
}

export interface RealtimeSnapshot {
  ts: number;
  total_rx_bps: number;
  total_tx_bps: number;
  total_rx_bytes: number;
  total_tx_bytes: number;
  total_active_conns: number;
  endpoints: EndpointLiveMetric[];
  devices: LiveDeviceSnapshot[];
  perf: PerfSample;
}

export interface TrafficPoint {
  ts: number;
  rx_bytes: number;
  tx_bytes: number;
  peak_rx_bps: number;
  peak_tx_bps: number;
  active_conns_max: number;
  errors: number;
}

export interface EndpointTrafficSummary {
  endpoint_id: string;
  rx_bytes: number;
  tx_bytes: number;
}

export interface LogEntry {
  id: number;
  ts: number;
  level: string;
  target: string;
  endpoint_id: string | null;
  conn_id: string | null;
  message: string;
  fields: Record<string, unknown>;
}

export interface ConfigSnapshot {
  id: number;
  created_at: number;
  source: string;
  format: string;
  content: string;
}

export interface EndpointPayload {
  id?: string;
  name?: string;
  enabled: boolean;
  probe_enabled: boolean;
  probe_interval_sec: number;
  probe_timeout_ms: number;
  listen: string;
  remote: string;
  extra_remotes?: string[];
  balance?: string | null;
  through?: string | null;
  interface?: string | null;
  listen_interface?: string | null;
  listen_transport?: string | null;
  remote_transport?: string | null;
  network?: NetConf;
}
