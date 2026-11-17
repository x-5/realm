import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Activity,
  ArrowDownLeft,
  ArrowUpRight,
  Cpu,
  Edit3,
  HardDrive,
  HeartPulse,
  LayoutDashboard,
  LogOut,
  Network,
  Play,
  Plus,
  RefreshCw,
  RotateCcw,
  ScrollText,
  Settings,
  ShieldCheck,
  Square,
  Trash2,
  Wifi,
  WifiOff,
  ChevronDown,
  ChevronRight,
} from 'lucide-react';
import type * as echarts from 'echarts';
import { api, createWsUrl, getToken, setToken } from './api';
import { EChart } from './components/EChart';
import { EndpointModal } from './components/EndpointModal';
import { LiveDevicesTable } from './components/LiveDevicesTable';
import type {
  ConfigSnapshot,
  EndpointPayload,
  EndpointTrafficSummary,
  EndpointView,
  LogEntry,
  PerfSample,
  RealtimeSnapshot,
  TargetProbeStatus,
  TrafficPoint,
} from './types';
import { formatBps, formatBytes, formatKb, formatTime, formatTimeMs } from './utils';

type TabKey = 'overview' | 'endpoints' | 'traffic' | 'performance' | 'probes' | 'logs' | 'settings';

export function App() {
  const [authed, setAuthed] = useState<boolean>(!!getToken());
  const [username, setUsername] = useState('admin');
  const [tab, setTab] = useState<TabKey>('overview');

  // Realtime WebSocket state
  const [latest, setLatest] = useState<RealtimeSnapshot | null>(null);
  const [recentRealtime, setRecentRealtime] = useState<RealtimeSnapshot[]>([]);
  const [probes, setProbes] = useState<TargetProbeStatus[]>([]);
  const [endpoints, setEndpoints] = useState<EndpointView[]>([]);
  const [wsConnected, setWsConnected] = useState(false);

  // Filters & modals
  const [deviceEpFilter, setDeviceEpFilter] = useState('');
  const [expandedEpIds, setExpandedEpIds] = useState<Record<string, boolean>>({});
  const [modalOpen, setModalOpen] = useState(false);
  const [editingEp, setEditingEp] = useState<EndpointView | null>(null);
  const [toast, setToast] = useState<{ ok: boolean; msg: string } | null>(null);

  const notify = useCallback((msg: string, ok = true) => {
    setToast({ ok, msg });
    setTimeout(() => setToast(null), 3500);
  }, []);

  useEffect(() => {
    const onUnauth = () => setAuthed(false);
    window.addEventListener('realm-unauthorized', onUnauth);
    return () => window.removeEventListener('realm-unauthorized', onUnauth);
  }, []);

  const refreshEndpoints = useCallback(async () => {
    try {
      const list = await api.listEndpoints();
      setEndpoints(list);
    } catch {
      // handled by api
    }
  }, []);

  useEffect(() => {
    if (!authed) return;
    api
      .me()
      .then((res) => setUsername(res.username))
      .catch(() => {});
    refreshEndpoints();
    api
      .statsOverview()
      .then((res) => {
        if (res.latest) setLatest(res.latest);
        if (res.recent) setRecentRealtime(res.recent);
        if (res.probes) setProbes(res.probes);
      })
      .catch(() => {});

    let closed = false;
    let ws: WebSocket | null = null;
    let retryTimer: ReturnType<typeof setTimeout> | null = null;

    const connect = () => {
      if (closed) return;
      ws = new WebSocket(createWsUrl('/ws/realtime'));
      ws.onopen = () => setWsConnected(true);
      ws.onerror = () => {
        ws?.close();
      };
      ws.onclose = () => {
        setWsConnected(false);
        if (!closed) {
          retryTimer = setTimeout(connect, 2000);
        }
      };
      ws.onmessage = (ev) => {
        try {
          const msg = JSON.parse(ev.data);
          if (msg.type === 'realtime' && msg.snapshot) {
            const snap: RealtimeSnapshot = msg.snapshot;
            setLatest(snap);
            setRecentRealtime((prev) => {
              if (prev.length > 0 && prev[prev.length - 1].ts === snap.ts) {
                return [...prev.slice(0, -1), snap];
              }
              return [...prev.slice(-119), snap];
            });
            if (msg.probes) setProbes(msg.probes);
          }
        } catch {
          // ignore
        }
      };
    };

    connect();
    const epTimer = setInterval(() => {
      refreshEndpoints();
    }, 5000);

    return () => {
      closed = true;
      if (retryTimer) clearTimeout(retryTimer);
      clearInterval(epTimer);
      ws?.close();
    };
  }, [authed, refreshEndpoints]);

  // Fallback polling when WebSocket is disconnected so metrics/KPIs still refresh
  useEffect(() => {
    if (!authed || wsConnected) return;
    const poll = () => {
      api
        .statsOverview()
        .then((res) => {
          if (res.latest) setLatest(res.latest);
          if (res.recent) setRecentRealtime(res.recent);
          if (res.probes) setProbes(res.probes);
        })
        .catch(() => {});
    };
    const timer = setInterval(poll, 2000);
    return () => clearInterval(timer);
  }, [authed, wsConnected]);

  if (!authed) {
    return (
      <LoginScreen
        onSuccess={(u) => {
          setUsername(u);
          setAuthed(true);
        }}
      />
    );
  }

  const handleSaveEndpoint = async (payload: EndpointPayload, existingId?: string) => {
    if (existingId) {
      await api.updateEndpoint(existingId, payload);
      notify('转发规则已更新并热重载');
    } else {
      await api.createEndpoint(payload);
      notify('转发规则已新增并生效');
    }
    await refreshEndpoints();
  };

  const handleToggle = async (ep: EndpointView) => {
    try {
      await api.toggleEndpoint(ep.id, !ep.enabled);
      notify(ep.enabled ? '已停用规则并断开连接' : '已启用规则');
      await refreshEndpoints();
    } catch (e) {
      notify(e instanceof Error ? e.message : '操作失败', false);
    }
  };

  const handleDelete = async (ep: EndpointView) => {
    if (!confirm(`确定删除规则 "${ep.name}" (${ep.listen} -> ${ep.remote})？存量连接将被强制断开。`)) {
      return;
    }
    try {
      await api.deleteEndpoint(ep.id);
      notify('已删除规则');
      await refreshEndpoints();
    } catch (e) {
      notify(e instanceof Error ? e.message : '删除失败', false);
    }
  };

  const handleManualReload = async () => {
    try {
      await api.reloadConfig();
      notify('配置文件热重载完成');
      await refreshEndpoints();
    } catch (e) {
      notify(e instanceof Error ? e.message : '热重载失败', false);
    }
  };

  // Merge live metrics onto endpoints
  const liveMap = new Map((latest?.endpoints ?? []).map((m) => [m.endpoint_id, m]));
  const devicesByEp = new Map<string, typeof latest extends null ? never : RealtimeSnapshot['devices']>();
  for (const d of latest?.devices ?? []) {
    const arr = devicesByEp.get(d.endpoint_id) ?? [];
    arr.push(d);
    devicesByEp.set(d.endpoint_id, arr);
  }
  const probesByEp = new Map<string, TargetProbeStatus[]>();
  for (const p of probes) {
    const arr = probesByEp.get(p.endpoint_id) ?? [];
    arr.push(p);
    probesByEp.set(p.endpoint_id, arr);
  }

  const navItems: { key: TabKey; label: string; icon: React.ReactNode }[] = [
    { key: 'overview', label: '实时总览', icon: <LayoutDashboard className="h-4 w-4" /> },
    { key: 'endpoints', label: '转发规则与设备', icon: <Network className="h-4 w-4" /> },
    { key: 'traffic', label: '流量统计', icon: <Activity className="h-4 w-4" /> },
    { key: 'performance', label: '性能开销', icon: <Cpu className="h-4 w-4" /> },
    { key: 'probes', label: '端口探活', icon: <HeartPulse className="h-4 w-4" /> },
    { key: 'logs', label: '可观测日志', icon: <ScrollText className="h-4 w-4" /> },
    { key: 'settings', label: '配置与安全', icon: <Settings className="h-4 w-4" /> },
  ];

  return (
    <div className="flex min-h-screen bg-slate-950 text-slate-100">
      {/* Sidebar */}
      <aside className="flex w-60 shrink-0 flex-col border-r border-slate-800/80 bg-slate-900/60">
        <div className="flex items-center gap-3 border-b border-slate-800 px-5 py-4">
          <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-gradient-to-br from-cyan-400 to-blue-600 text-slate-950 font-bold shadow-lg shadow-cyan-500/20">
            R
          </div>
          <div>
            <div className="text-sm font-bold tracking-wide text-slate-100">Realm Panel</div>
            <div className="flex items-center gap-1.5 text-[11px] text-slate-400">
              <span
                className={`h-1.5 w-1.5 rounded-full ${
                  wsConnected ? 'bg-emerald-400' : 'bg-amber-400'
                }`}
              />
              {wsConnected ? '实时流已连接' : '重连中...'}
            </div>
          </div>
        </div>

        <nav className="flex-1 space-y-1 p-3">
          {navItems.map((item) => (
            <button
              key={item.key}
              onClick={() => setTab(item.key)}
              className={`flex w-full items-center gap-3 rounded-xl px-3.5 py-2.5 text-xs font-medium transition ${
                tab === item.key
                  ? 'bg-cyan-500/15 text-cyan-300 ring-1 ring-cyan-500/30'
                  : 'text-slate-400 hover:bg-slate-800/60 hover:text-slate-200'
              }`}
            >
              {item.icon}
              {item.label}
            </button>
          ))}
        </nav>

        <div className="border-t border-slate-800 p-4">
          <div className="mb-2 flex items-center justify-between text-xs text-slate-400">
            <span>管理员: {username}</span>
            <button
              onClick={() => {
                setToken(null);
                setAuthed(false);
              }}
              className="inline-flex items-center gap-1 text-slate-400 hover:text-rose-400"
              title="退出登录"
            >
              <LogOut className="h-3.5 w-3.5" /> 退出
            </button>
          </div>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 overflow-y-auto p-6">
        {toast && (
          <div
            className={`fixed right-6 top-6 z-50 rounded-xl border px-4 py-2.5 text-xs font-medium shadow-xl backdrop-blur ${
              toast.ok
                ? 'border-emerald-500/40 bg-emerald-950/90 text-emerald-200'
                : 'border-rose-500/40 bg-rose-950/90 text-rose-200'
            }`}
          >
            {toast.msg}
          </div>
        )}

        {tab === 'overview' && (
          <OverviewView
            latest={latest}
            recent={recentRealtime}
            endpoints={endpoints}
            probes={probes}
            deviceEpFilter={deviceEpFilter}
            onSelectDeviceEp={setDeviceEpFilter}
            onJumpTab={setTab}
          />
        )}

        {tab === 'endpoints' && (
          <div className="space-y-5">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <h1 className="text-lg font-bold text-slate-100">转发规则管理</h1>
                <p className="text-xs text-slate-400">
                  支持单规则热重载（修改/删除强制断开旧连接，其他规则零中断）及实时入口设备追踪
                </p>
              </div>
              <div className="flex items-center gap-2.5">
                <button
                  onClick={handleManualReload}
                  className="inline-flex items-center gap-1.5 rounded-lg border border-slate-700 bg-slate-900 px-3.5 py-2 text-xs font-medium text-slate-200 hover:bg-slate-800"
                >
                  <RefreshCw className="h-3.5 w-3.5" /> 磁盘热重载
                </button>
                <button
                  onClick={() => {
                    setEditingEp(null);
                    setModalOpen(true);
                  }}
                  className="inline-flex items-center gap-1.5 rounded-lg bg-cyan-500 px-4 py-2 text-xs font-semibold text-slate-950 hover:bg-cyan-400"
                >
                  <Plus className="h-4 w-4" /> 新增转发规则
                </button>
              </div>
            </div>

            <div className="space-y-3">
              {endpoints.length === 0 ? (
                <div className="rounded-xl border border-dashed border-slate-800 p-12 text-center text-sm text-slate-500">
                  暂无转发规则，点击右上角「新增转发规则」开始配置。
                </div>
              ) : (
                endpoints.map((ep) => {
                  const live = liveMap.get(ep.id);
                  const epDevs = devicesByEp.get(ep.id) ?? ep.devices ?? [];
                  const epProbes = probesByEp.get(ep.id) ?? ep.probes ?? [];
                  const rxBps = live?.rx_bps ?? ep.rx_bps;
                  const txBps = live?.tx_bps ?? ep.tx_bps;
                  const rxBytes = live?.rx_bytes ?? ep.rx_bytes;
                  const txBytes = live?.tx_bytes ?? ep.tx_bytes;
                  const activeConns =
                    (live?.active_tcp_conns ?? ep.active_tcp_conns) +
                    (live?.active_udp_assocs ?? ep.active_udp_assocs);
                  const activeDevCount = epDevs.filter((d) => d.active_conns > 0).length;
                  const expanded = !!expandedEpIds[ep.id];
                  const allProbesUp =
                    epProbes.length > 0 && epProbes.every((p) => p.reachable);
                  const anyProbeDown = epProbes.some((p) => !p.reachable);

                  return (
                    <div
                      key={ep.id}
                      className="rounded-xl border border-slate-800 bg-slate-900/70 transition hover:border-slate-700"
                    >
                      <div
                        onClick={() =>
                          setExpandedEpIds((prev) => ({ ...prev, [ep.id]: !prev[ep.id] }))
                        }
                        className="flex cursor-pointer flex-wrap items-center justify-between gap-4 p-4 select-none"
                        title="点击展开/收起规则详情与接入客户端"
                      >
                        <div className="min-w-[240px] flex-1">
                          <div className="flex items-center gap-2.5">
                            <span
                              className={`h-2.5 w-2.5 rounded-full ${
                                !ep.enabled
                                  ? 'bg-slate-600'
                                  : live?.last_error || ep.last_error
                                    ? 'bg-rose-500'
                                    : 'bg-emerald-400 shadow-[0_0_8px_#34d399]'
                              }`}
                            />
                            <span className="text-sm font-semibold text-slate-100">{ep.name}</span>
                            <span className="rounded bg-slate-800 px-1.5 py-0.5 font-mono text-[11px] text-slate-400">
                              {!ep.network?.no_tcp ? 'TCP' : ''}
                              {!ep.network?.no_tcp && ep.network?.use_udp ? '+' : ''}
                              {ep.network?.use_udp ? 'UDP' : ''}
                            </span>
                            {ep.balance && (
                              <span className="rounded bg-indigo-500/15 px-1.5 py-0.5 font-mono text-[11px] text-indigo-300">
                                LB
                              </span>
                            )}
                            {(ep.listen_transport || ep.remote_transport) && (
                              <span className="rounded bg-amber-500/15 px-1.5 py-0.5 font-mono text-[11px] text-amber-300">
                                Transport
                              </span>
                            )}
                            {epProbes.length > 0 && (
                              <span
                                className={`inline-flex items-center gap-1 rounded px-1.5 py-0.5 font-mono text-[11px] ${
                                  anyProbeDown
                                    ? 'bg-rose-500/15 text-rose-300'
                                    : allProbesUp
                                      ? 'bg-emerald-500/15 text-emerald-300'
                                      : 'bg-slate-800 text-slate-400'
                                }`}
                              >
                                {anyProbeDown ? (
                                  <WifiOff className="h-3 w-3" />
                                ) : (
                                  <Wifi className="h-3 w-3" />
                                )}
                                {epProbes[0].rtt_ms != null ? `${epProbes[0].rtt_ms}ms` : '异常'}
                              </span>
                            )}
                          </div>
                          <div className="mt-1.5 font-mono text-xs text-slate-400">
                            <span className="text-cyan-300">{ep.listen}</span>
                            <span className="mx-2 text-slate-600">→</span>
                            <span className="text-slate-200">{ep.remote}</span>
                            {ep.extra_remotes && ep.extra_remotes.length > 0 && (
                              <span className="ml-2 text-slate-500">
                                (+{ep.extra_remotes.length} 节点)
                              </span>
                            )}
                          </div>
                          {(live?.last_error || ep.last_error) && (
                            <div className="mt-1 text-xs text-rose-400">
                              绑定/运行异常: {live?.last_error || ep.last_error}
                            </div>
                          )}
                        </div>

                        <div className="flex flex-wrap items-center gap-6 text-xs">
                          <div>
                            <div className="text-slate-500">实时入 / 出速率</div>
                            <div className="mt-0.5 font-mono font-semibold">
                              <span className="text-cyan-300">{formatBps(rxBps)}</span>
                              <span className="mx-1.5 text-slate-600">/</span>
                              <span className="text-emerald-300">{formatBps(txBps)}</span>
                            </div>
                          </div>
                          <div>
                            <div className="text-slate-500">累计 RX / TX</div>
                            <div className="mt-0.5 font-mono text-slate-300">
                              {formatBytes(rxBytes)} / {formatBytes(txBytes)}
                            </div>
                          </div>
                          <div>
                            <div className="text-slate-500">活跃连接 / 设备</div>
                            <div className="mt-0.5 font-mono text-slate-200">
                              {activeConns} 连接 ·{' '}
                              <span className="text-cyan-300">{activeDevCount} 台在线</span>
                            </div>
                          </div>
                        </div>

                        <div
                          className="flex items-center gap-1.5"
                          onClick={(e) => e.stopPropagation()}
                        >
                          <button
                            onClick={() =>
                              setExpandedEpIds((prev) => ({ ...prev, [ep.id]: !prev[ep.id] }))
                            }
                            className="inline-flex items-center gap-1 rounded-lg border border-slate-700 bg-slate-950 px-2.5 py-1.5 text-xs text-slate-300 hover:bg-slate-800"
                          >
                            {expanded ? (
                              <ChevronDown className="h-3.5 w-3.5" />
                            ) : (
                              <ChevronRight className="h-3.5 w-3.5" />
                            )}
                            详情 / 客户端 ({epDevs.length})
                          </button>
                          <button
                            onClick={() => handleToggle(ep)}
                            title={ep.enabled ? '停用规则' : '启用规则'}
                            className={`rounded-lg border p-1.5 transition ${
                              ep.enabled
                                ? 'border-amber-500/30 text-amber-300 hover:bg-amber-500/10'
                                : 'border-emerald-500/30 text-emerald-300 hover:bg-emerald-500/10'
                            }`}
                          >
                            {ep.enabled ? <Square className="h-4 w-4" /> : <Play className="h-4 w-4" />}
                          </button>
                          <button
                            onClick={() => {
                              setEditingEp(ep);
                              setModalOpen(true);
                            }}
                            title="编辑规则"
                            className="rounded-lg border border-slate-700 p-1.5 text-slate-300 hover:bg-slate-800"
                          >
                            <Edit3 className="h-4 w-4" />
                          </button>
                          <button
                            onClick={() => handleDelete(ep)}
                            title="删除规则"
                            className="rounded-lg border border-rose-500/30 p-1.5 text-rose-400 hover:bg-rose-500/10"
                          >
                            <Trash2 className="h-4 w-4" />
                          </button>
                        </div>
                      </div>

                      {expanded && (
                        <div className="space-y-4 border-t border-slate-800 bg-slate-950/50 p-4">
                          {/* Rule configuration & runtime details */}
                          <div className="grid grid-cols-1 gap-3 rounded-xl border border-slate-800/90 bg-slate-900/60 p-3.5 text-xs md:grid-cols-2 xl:grid-cols-4">
                            <div>
                              <div className="text-slate-500">规则 ID / 状态</div>
                              <div className="mt-1 font-mono text-slate-200">
                                {ep.id}{' '}
                                <span
                                  className={`ml-1 rounded px-1.5 py-0.5 text-[10px] ${
                                    !ep.enabled
                                      ? 'bg-slate-800 text-slate-400'
                                      : ep.running
                                        ? 'bg-emerald-500/15 text-emerald-300'
                                        : 'bg-rose-500/15 text-rose-300'
                                  }`}
                                >
                                  {!ep.enabled ? '已停用' : ep.running ? '运行中' : '未就绪'}
                                </span>
                              </div>
                            </div>
                            <div>
                              <div className="text-slate-500">监听与目标地址</div>
                              <div className="mt-1 font-mono text-slate-200 break-all">
                                <span className="text-cyan-300">{ep.listen}</span> → {ep.remote}
                              </div>
                              {ep.extra_remotes && ep.extra_remotes.length > 0 && (
                                <div className="mt-0.5 font-mono text-[11px] text-slate-400 break-all">
                                  备选节点: {ep.extra_remotes.join(', ')}
                                </div>
                              )}
                            </div>
                            <div>
                              <div className="text-slate-500">均衡 / 传输层 / 接口绑定</div>
                              <div className="mt-1 font-mono text-slate-300">
                                LB: {ep.balance || '默认'} · L:{ep.listen_transport || 'raw'} · R:{ep.remote_transport || 'raw'}
                              </div>
                              {(ep.through || ep.interface || ep.listen_interface) && (
                                <div className="mt-0.5 font-mono text-[11px] text-slate-400">
                                  through={ep.through || '-'} if={ep.interface || '-'} lif={ep.listen_interface || '-'}
                                </div>
                              )}
                            </div>
                            <div>
                              <div className="text-slate-500">网络参数与连接累计</div>
                              <div className="mt-1 font-mono text-slate-300">
                                TCP超时 {ep.network?.tcp_timeout ?? '-'}s · UDP超时 {ep.network?.udp_timeout ?? '-'}s
                                {ep.network?.send_proxy ? ' · SendPROXY' : ''}
                                {ep.network?.accept_proxy ? ' · AcceptPROXY' : ''}
                              </div>
                              <div className="mt-0.5 font-mono text-[11px] text-slate-400">
                                累计TCP {live?.total_tcp_conns ?? ep.total_tcp_conns} · 零拷贝 {live?.zero_copy_conns ?? ep.zero_copy_conns} · 失败 {live?.connect_errors ?? ep.connect_errors}
                              </div>
                            </div>
                          </div>

                          {/* Target reachability probe summary */}
                          <div className="rounded-xl border border-slate-800/90 bg-slate-900/60 p-3.5 text-xs">
                            <div className="mb-2 flex items-center justify-between">
                              <span className="font-semibold text-slate-200">
                                目的端口可达性检测（{ep.probe_enabled ? `周期 ${ep.probe_interval_sec}s / 超时 ${ep.probe_timeout_ms}ms` : '已关闭'}）
                              </span>
                              <button
                                onClick={async () => {
                                  try {
                                    await api.runProbeNow(ep.id);
                                    setProbes(await api.listProbes());
                                    notify('探活已完成');
                                  } catch (err) {
                                    notify(err instanceof Error ? err.message : '探活失败', false);
                                  }
                                }}
                                className="rounded-lg border border-slate-700 bg-slate-950 px-2.5 py-1 text-slate-200 hover:bg-slate-800"
                              >
                                立即探测
                              </button>
                            </div>
                            {epProbes.length === 0 ? (
                              <div className="text-slate-500">暂无该规则的探活数据</div>
                            ) : (
                              <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
                                {epProbes.map((p) => (
                                  <div
                                    key={`${p.endpoint_id}-${p.target}`}
                                    className="flex items-center justify-between rounded-lg border border-slate-800 bg-slate-950/60 px-3 py-2 font-mono"
                                  >
                                    <div>
                                      <div className="text-slate-200">
                                        {p.target} ({p.protocol.toUpperCase()})
                                      </div>
                                      <div className="text-[11px] text-slate-500">
                                        IP: {p.resolved_ip || '-'} · 检查于 {formatTime(p.last_checked_ts, true)}
                                        {p.error_msg ? ` · ${p.error_msg}` : ''}
                                      </div>
                                    </div>
                                    <span
                                      className={`rounded-full px-2 py-0.5 text-[11px] ${
                                        p.reachable
                                          ? 'bg-emerald-500/15 text-emerald-300'
                                          : 'bg-rose-500/15 text-rose-300'
                                      }`}
                                    >
                                      {p.reachable ? `可达 ${p.rtt_ms ?? '-'}ms` : `不可达 (${p.consecutive_failures})`}
                                    </span>
                                  </div>
                                ))}
                              </div>
                            )}
                          </div>

                          {/* Connected entry clients */}
                          <LiveDevicesTable devices={epDevs} />
                        </div>
                      )}
                    </div>
                  );
                })
              )}
            </div>
          </div>
        )}

        {tab === 'traffic' && <TrafficView endpoints={endpoints} />}
        {tab === 'performance' && <PerformanceView latestPerf={latest?.perf ?? null} />}
        {tab === 'probes' && (
          <ProbesView
            probes={probes}
            endpoints={endpoints}
            onRefreshProbes={async () => setProbes(await api.listProbes())}
            notify={notify}
          />
        )}
        {tab === 'logs' && <LogsView endpoints={endpoints} />}
        {tab === 'settings' && (
          <SettingsView
            notify={notify}
            onReloaded={refreshEndpoints}
          />
        )}
      </main>

      {modalOpen && (
        <EndpointModal
          initial={editingEp}
          onClose={() => setModalOpen(false)}
          onSave={handleSaveEndpoint}
        />
      )}
    </div>
  );
}

function OverviewView({
  latest,
  recent,
  endpoints,
  probes,
  deviceEpFilter,
  onSelectDeviceEp,
  onJumpTab,
}: {
  latest: RealtimeSnapshot | null;
  recent: RealtimeSnapshot[];
  endpoints: EndpointView[];
  probes: TargetProbeStatus[];
  deviceEpFilter: string;
  onSelectDeviceEp: (id: string) => void;
  onJumpTab: (t: TabKey) => void;
}) {
  const chartOption: echarts.EChartsOption = useMemo(() => {
    const times = recent.map((r) => formatTime(r.ts));
    const rx = recent.map((r) => r.total_rx_bps);
    const tx = recent.map((r) => r.total_tx_bps);
    return {
      animation: false,
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#0f172a',
        borderColor: '#334155',
        textStyle: { color: '#e2e8f0', fontSize: 12 },
        formatter: (params: unknown) => {
          const items = params as { seriesName: string; value: number; axisValue: string }[];
          if (!items.length) return '';
          return `${items[0].axisValue}<br/>` + items.map((i) => `${i.seriesName}: <b>${formatBps(i.value)}</b>`).join('<br/>');
        },
      },
      legend: { data: ['入站速率 (RX)', '出站速率 (TX)'], textStyle: { color: '#94a3b8' }, top: 0 },
      grid: { left: 16, right: 16, top: 38, bottom: 14, containLabel: true },
      xAxis: {
        type: 'category',
        boundaryGap: false,
        data: times,
        axisLine: { lineStyle: { color: '#334155' } },
        axisLabel: { color: '#64748b', fontSize: 11 },
      },
      yAxis: {
        type: 'value',
        min: 0,
        minInterval: 1,
        splitNumber: 4,
        splitLine: { lineStyle: { color: '#1e293b' } },
        axisLabel: { color: '#64748b', fontSize: 11, formatter: (v: number) => formatBps(v) },
      },
      series: [
        {
          name: '入站速率 (RX)',
          type: 'line',
          smooth: false,
          showSymbol: recent.length <= 20,
          symbolSize: 4,
          data: rx,
          itemStyle: { color: '#22d3ee' },
          lineStyle: { color: '#22d3ee', width: 2 },
          areaStyle: { color: 'rgba(34,211,238,0.10)' },
        },
        {
          name: '出站速率 (TX)',
          type: 'line',
          smooth: false,
          showSymbol: recent.length <= 20,
          symbolSize: 4,
          data: tx,
          itemStyle: { color: '#34d399' },
          lineStyle: { color: '#34d399', width: 2 },
          areaStyle: { color: 'rgba(52,211,153,0.10)' },
        },
      ],
    };
  }, [recent]);

  const downProbes = probes.filter((p) => !p.reachable);
  const perf = latest?.perf;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-bold text-slate-100">实时运行总览</h1>
          <p className="text-xs text-slate-400">全节点实时转发吞吐、接入设备与资源开销</p>
        </div>
        {downProbes.length > 0 && (
          <button
            onClick={() => onJumpTab('probes')}
            className="inline-flex items-center gap-2 rounded-lg border border-rose-500/40 bg-rose-500/10 px-3 py-1.5 text-xs font-medium text-rose-300"
          >
            <WifiOff className="h-4 w-4" />
            {downProbes.length} 个目标端口不可达
          </button>
        )}
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
          <div className="flex items-center justify-between text-xs text-slate-400">
            <span>实时总入站 / 出站速率</span>
            <ArrowDownLeft className="h-4 w-4 text-cyan-400" />
          </div>
          <div className="mt-2 font-mono text-lg font-bold text-cyan-300">
            {formatBps(latest?.total_rx_bps ?? 0)}
          </div>
          <div className="mt-0.5 flex items-center gap-1 font-mono text-xs text-emerald-300">
            <ArrowUpRight className="h-3.5 w-3.5" />
            {formatBps(latest?.total_tx_bps ?? 0)}
          </div>
        </div>

        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
          <div className="flex items-center justify-between text-xs text-slate-400">
            <span>累计转发流量 (RX / TX)</span>
            <Activity className="h-4 w-4 text-emerald-400" />
          </div>
          <div className="mt-2 font-mono text-lg font-bold text-slate-100">
            {formatBytes((latest?.total_rx_bytes ?? 0) + (latest?.total_tx_bytes ?? 0))}
          </div>
          <div className="mt-0.5 font-mono text-xs text-slate-400">
            RX {formatBytes(latest?.total_rx_bytes ?? 0)} · TX {formatBytes(latest?.total_tx_bytes ?? 0)}
          </div>
        </div>

        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
          <div className="flex items-center justify-between text-xs text-slate-400">
            <span>活跃连接 / 在线入口设备</span>
            <Network className="h-4 w-4 text-indigo-400" />
          </div>
          <div className="mt-2 font-mono text-lg font-bold text-slate-100">
            {latest?.total_active_conns ?? 0} 连接
          </div>
          <div className="mt-0.5 text-xs text-slate-400">
            运行规则 {latest?.endpoints.length ?? 0} / {endpoints.length} · 在线设备{' '}
            {(latest?.devices ?? []).filter((d) => d.active_conns > 0).length} 台
          </div>
        </div>

        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
          <div className="flex items-center justify-between text-xs text-slate-400">
            <span>进程性能开销</span>
            <HardDrive className="h-4 w-4 text-amber-400" />
          </div>
          <div className="mt-2 font-mono text-lg font-bold text-slate-100">
            CPU {(perf?.proc_cpu_pct ?? 0).toFixed(1)}% · {formatKb(perf?.proc_mem_rss_kb ?? 0)}
          </div>
          <div className="mt-0.5 font-mono text-xs text-slate-400">
            FD {perf?.open_fds ?? 0}/{perf?.fd_limit ?? 0} · Tokio 任务 {perf?.tokio_alive_tasks ?? 0}
          </div>
        </div>
      </div>

      {/* Realtime Rate Chart */}
      <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
        <h3 className="mb-2 text-sm font-semibold text-slate-200">秒级实时双向吞吐速率（最近 2 分钟）</h3>
        <EChart option={chartOption} height={260} />
      </div>

      {/* Live Entry Devices */}
      <LiveDevicesTable
        devices={latest?.devices ?? []}
        endpoints={endpoints}
        selectedEndpointId={deviceEpFilter}
        onSelectEndpoint={onSelectDeviceEp}
      />
    </div>
  );
}

function formatDateInput(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function TrafficView({ endpoints }: { endpoints: EndpointView[] }) {
  type TrafficMode = '1h' | '24h' | '7d' | '30d' | 'custom';
  const [mode, setMode] = useState<TrafficMode>('7d');
  const [startDate, setStartDate] = useState<string>(() => {
    const d = new Date();
    d.setDate(d.getDate() - 6);
    return formatDateInput(d);
  });
  const [endDate, setEndDate] = useState<string>(() => formatDateInput(new Date()));
  const [appliedDates, setAppliedDates] = useState<{ start: string; end: string }>({
    start: startDate,
    end: endDate,
  });
  const [dateError, setDateError] = useState('');
  const [endpointId, setEndpointId] = useState('');
  const [unit, setUnit] = useState<'bps' | 'bytes'>('bytes');
  const [series, setSeries] = useState<TrafficPoint[]>([]);
  const [byEndpoint, setByEndpoint] = useState<EndpointTrafficSummary[]>([]);

  const loadTraffic = useCallback(() => {
    if (mode === 'custom') {
      const fromSec = Math.floor(new Date(`${appliedDates.start}T00:00:00`).getTime() / 1000);
      const toSec = Math.floor(new Date(`${appliedDates.end}T23:59:59`).getTime() / 1000);
      if (!Number.isFinite(fromSec) || !Number.isFinite(toSec) || fromSec > toSec) {
        return;
      }
      api
        .statsTraffic({
          from: fromSec,
          to: toSec,
          endpointId: endpointId || undefined,
        })
        .then((res) => {
          setUnit(res.unit);
          setSeries(res.series);
          setByEndpoint(res.by_endpoint);
        })
        .catch(() => {});
      return;
    }

    api
      .statsTraffic({
        range: mode,
        endpointId: endpointId || undefined,
      })
      .then((res) => {
        setUnit(res.unit);
        setSeries(res.series);
        setByEndpoint(res.by_endpoint);
      })
      .catch(() => {});
  }, [mode, appliedDates, endpointId]);

  useEffect(() => {
    loadTraffic();
    const timer = setInterval(loadTraffic, mode === '1h' ? 10000 : 30000);
    return () => clearInterval(timer);
  }, [loadTraffic, mode]);

  const applyCustomDates = () => {
    if (!startDate || !endDate) {
      setDateError('请选择完整的起止日期');
      return;
    }
    if (startDate > endDate) {
      setDateError('开始日期不能晚于结束日期');
      return;
    }
    setDateError('');
    setAppliedDates({ start: startDate, end: endDate });
    setMode('custom');
  };

  const epName = new Map(endpoints.map((e) => [e.id, `${e.name} (${e.listen})`]));
  const totalRx = series.reduce((a, b) => a + b.rx_bytes, 0);
  const totalTx = series.reduce((a, b) => a + b.tx_bytes, 0);

  const chartOption: echarts.EChartsOption = useMemo(() => {
    const isLine = unit === 'bps';
    const labels = series.map((p) => formatTime(p.ts, mode !== '1h'));
    return {
      animation: false,
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#0f172a',
        borderColor: '#334155',
        textStyle: { color: '#e2e8f0', fontSize: 12 },
        formatter: (params: unknown) => {
          const items = params as { seriesName: string; value: number; axisValue: string }[];
          if (!items.length) return '';
          const fmt = unit === 'bps' ? formatBps : formatBytes;
          return `${items[0].axisValue}<br/>` + items.map((i) => `${i.seriesName}: <b>${fmt(i.value)}</b>`).join('<br/>');
        },
      },
      legend: { data: ['入站 (RX)', '出站 (TX)'], textStyle: { color: '#94a3b8' }, top: 0 },
      grid: { left: 16, right: 16, top: 38, bottom: 14, containLabel: true },
      xAxis: {
        type: 'category',
        boundaryGap: !isLine,
        data: labels,
        axisLine: { lineStyle: { color: '#334155' } },
        axisLabel: { color: '#64748b', fontSize: 11 },
      },
      yAxis: {
        type: 'value',
        min: 0,
        minInterval: 1,
        splitLine: { lineStyle: { color: '#1e293b' } },
        axisLabel: {
          color: '#64748b',
          fontSize: 11,
          formatter: (v: number) => (unit === 'bps' ? formatBps(v) : formatBytes(v)),
        },
      },
      series: [
        {
          name: '入站 (RX)',
          type: isLine ? 'line' : 'bar',
          smooth: false,
          showSymbol: false,
          barMaxWidth: 28,
          data: series.map((s) => s.rx_bytes),
          itemStyle: { color: '#22d3ee' },
          lineStyle: { color: '#22d3ee', width: 2 },
        },
        {
          name: '出站 (TX)',
          type: isLine ? 'line' : 'bar',
          smooth: false,
          showSymbol: false,
          barMaxWidth: 28,
          data: series.map((s) => s.tx_bytes),
          itemStyle: { color: '#34d399' },
          lineStyle: { color: '#34d399', width: 2 },
        },
      ],
    };
  }, [series, mode, unit]);

  const presetItems: { key: TrafficMode; label: string }[] = [
    { key: '1h', label: '最近 1 小时' },
    { key: '24h', label: '最近 24 小时' },
    { key: '7d', label: '最近 7 天' },
    { key: '30d', label: '最近 30 天' },
    { key: 'custom', label: '自定义日期' },
  ];

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-lg font-bold text-slate-100">双向流量与历史趋势</h1>
          <p className="text-xs text-slate-400">按时间区间、指定日期范围或转发规则查看双向入出流量趋势与排行</p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <select
            value={endpointId}
            onChange={(e) => setEndpointId(e.target.value)}
            className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-1.5 text-xs text-slate-200"
          >
            <option value="">全部转发规则</option>
            {endpoints.map((ep) => (
              <option key={ep.id} value={ep.id}>
                {ep.name} ({ep.listen})
              </option>
            ))}
          </select>
          <div className="flex flex-wrap rounded-lg border border-slate-700 bg-slate-900 p-0.5 text-xs">
            {presetItems.map((item) => (
              <button
                key={item.key}
                onClick={() => {
                  setDateError('');
                  setMode(item.key);
                }}
                className={`rounded-md px-3 py-1 font-medium transition ${
                  mode === item.key ? 'bg-cyan-500 text-slate-950' : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                {item.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      {mode === 'custom' && (
        <div className="flex flex-wrap items-center gap-3 rounded-xl border border-slate-800 bg-slate-900/70 px-4 py-3 text-xs">
          <span className="font-medium text-slate-300">选择日期区间:</span>
          <label className="flex items-center gap-2 text-slate-400">
            <span>开始</span>
            <input
              type="date"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              className="rounded-lg border border-slate-700 bg-slate-950 px-2.5 py-1.5 text-slate-100 focus:border-cyan-500 focus:outline-none"
            />
          </label>
          <span className="text-slate-500">至</span>
          <label className="flex items-center gap-2 text-slate-400">
            <span>结束</span>
            <input
              type="date"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              className="rounded-lg border border-slate-700 bg-slate-950 px-2.5 py-1.5 text-slate-100 focus:border-cyan-500 focus:outline-none"
            />
          </label>
          <button
            onClick={applyCustomDates}
            className="rounded-lg bg-cyan-500 px-3.5 py-1.5 font-semibold text-slate-950 hover:bg-cyan-400"
          >
            查询区间
          </button>
          {dateError && <span className="text-rose-400">{dateError}</span>}
          {!dateError && (
            <span className="text-slate-500">
              当前统计范围：{appliedDates.start} 00:00:00 ~ {appliedDates.end} 23:59:59
            </span>
          )}
        </div>
      )}

      {unit === 'bytes' && (
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
          <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
            <div className="text-xs text-slate-400">区间总入站 (RX)</div>
            <div className="mt-1 font-mono text-xl font-bold text-cyan-300">{formatBytes(totalRx)}</div>
          </div>
          <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
            <div className="text-xs text-slate-400">区间总出站 (TX)</div>
            <div className="mt-1 font-mono text-xl font-bold text-emerald-300">{formatBytes(totalTx)}</div>
          </div>
          <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
            <div className="text-xs text-slate-400">区间双向总计</div>
            <div className="mt-1 font-mono text-xl font-bold text-slate-100">{formatBytes(totalRx + totalTx)}</div>
          </div>
        </div>
      )}

      <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
        <EChart option={chartOption} height={320} />
      </div>

      <div className="rounded-xl border border-slate-800 bg-slate-900/70">
        <div className="border-b border-slate-800 px-5 py-3 text-sm font-semibold">各规则区间流量排行</div>
        <table className="w-full text-left text-xs">
          <thead className="border-b border-slate-800 bg-slate-950/60 text-slate-400">
            <tr>
              <th className="px-4 py-2.5">规则</th>
              <th className="px-4 py-2.5 text-right">入站 (RX)</th>
              <th className="px-4 py-2.5 text-right">出站 (TX)</th>
              <th className="px-4 py-2.5 text-right">总计</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/70">
            {byEndpoint.length === 0 ? (
              <tr>
                <td colSpan={4} className="px-4 py-6 text-center text-slate-500">
                  该时间段暂无持久化流量记录
                </td>
              </tr>
            ) : (
              byEndpoint.map((row) => (
                <tr key={row.endpoint_id}>
                  <td className="px-4 py-2.5 text-slate-200">{epName.get(row.endpoint_id) || row.endpoint_id}</td>
                  <td className="px-4 py-2.5 text-right font-mono text-cyan-300">{formatBytes(row.rx_bytes)}</td>
                  <td className="px-4 py-2.5 text-right font-mono text-emerald-300">{formatBytes(row.tx_bytes)}</td>
                  <td className="px-4 py-2.5 text-right font-mono font-semibold text-slate-100">
                    {formatBytes(row.rx_bytes + row.tx_bytes)}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function PerformanceView({ latestPerf }: { latestPerf: PerfSample | null }) {
  const [range, setRange] = useState<'1h' | '24h' | '7d'>('1h');
  const [series, setSeries] = useState<PerfSample[]>([]);
  const [latest, setLatest] = useState<PerfSample | null>(latestPerf);

  useEffect(() => {
    const fetchPerf = () => {
      api
        .statsPerformance(range)
        .then((res) => {
          setSeries(res.series);
          if (res.latest) setLatest(res.latest);
        })
        .catch(() => {});
    };
    fetchPerf();
    if (range !== '1h') {
      const timer = setInterval(fetchPerf, 30000);
      return () => clearInterval(timer);
    }
  }, [range]);

  // Append live 1s performance samples in real time when viewing recent 1h
  useEffect(() => {
    if (!latestPerf) return;
    setLatest(latestPerf);
    if (range !== '1h') return;
    setSeries((prev) => {
      if (prev.length > 0 && prev[prev.length - 1].ts === latestPerf.ts) {
        return [...prev.slice(0, -1), latestPerf];
      }
      return [...prev.slice(-599), latestPerf];
    });
  }, [latestPerf, range]);

  const activeLatest = latestPerf ?? latest;

  const cpuChart: echarts.EChartsOption = useMemo(() => {
    const times = series.map((s) => formatTime(s.ts, range !== '1h'));
    return {
      animation: false,
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis', backgroundColor: '#0f172a', borderColor: '#334155', textStyle: { color: '#e2e8f0' } },
      legend: { data: ['进程 CPU %', '系统总 CPU %'], textStyle: { color: '#94a3b8' }, top: 0 },
      grid: { left: 16, right: 16, top: 36, bottom: 14, containLabel: true },
      xAxis: { type: 'category', boundaryGap: false, data: times, axisLabel: { color: '#64748b', fontSize: 11 } },
      yAxis: { type: 'value', min: 0, splitLine: { lineStyle: { color: '#1e293b' } }, axisLabel: { color: '#64748b', formatter: '{value}%' } },
      series: [
        { name: '进程 CPU %', type: 'line', smooth: false, showSymbol: false, data: series.map((s) => Number(s.proc_cpu_pct.toFixed(2))), itemStyle: { color: '#22d3ee' }, lineStyle: { color: '#22d3ee', width: 2 } },
        { name: '系统总 CPU %', type: 'line', smooth: false, showSymbol: false, data: series.map((s) => Number(s.sys_cpu_pct.toFixed(2))), itemStyle: { color: '#f59e0b' }, lineStyle: { color: '#f59e0b', width: 2 } },
      ],
    };
  }, [series, range]);

  const memChart: echarts.EChartsOption = useMemo(() => {
    const times = series.map((s) => formatTime(s.ts, range !== '1h'));
    return {
      animation: false,
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis', backgroundColor: '#0f172a', borderColor: '#334155', textStyle: { color: '#e2e8f0' } },
      legend: { data: ['RSS 物理内存 (MB)', '活跃 Tokio 任务'], textStyle: { color: '#94a3b8' }, top: 0 },
      grid: { left: 16, right: 16, top: 36, bottom: 14, containLabel: true },
      xAxis: { type: 'category', boundaryGap: false, data: times, axisLabel: { color: '#64748b', fontSize: 11 } },
      yAxis: [
        { type: 'value', min: 0, splitLine: { lineStyle: { color: '#1e293b' } }, axisLabel: { color: '#64748b', formatter: '{value} MB' } },
        { type: 'value', min: 0, minInterval: 1, splitLine: { show: false }, axisLabel: { color: '#64748b' } },
      ],
      series: [
        { name: 'RSS 物理内存 (MB)', type: 'line', smooth: false, showSymbol: false, yAxisIndex: 0, data: series.map((s) => Number((s.proc_mem_rss_kb / 1024).toFixed(2))), itemStyle: { color: '#34d399' }, lineStyle: { color: '#34d399', width: 2 } },
        { name: '活跃 Tokio 任务', type: 'line', smooth: false, showSymbol: false, yAxisIndex: 1, data: series.map((s) => s.tokio_alive_tasks), itemStyle: { color: '#a78bfa' }, lineStyle: { color: '#a78bfa', width: 2 } },
      ],
    };
  }, [series, range]);

  const perfRanges: { key: '1h' | '24h' | '7d'; label: string }[] = [
    { key: '1h', label: '最近 1 小时' },
    { key: '24h', label: '最近 24 小时' },
    { key: '7d', label: '最近 7 天' },
  ];

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-lg font-bold text-slate-100">进程与运行时性能开销</h1>
          <p className="text-xs text-slate-400">实时监控进程 CPU、RSS/VSZ 内存、文件描述符 FD 占用及 Tokio 异步运行时负载</p>
        </div>
        <div className="flex rounded-lg border border-slate-700 bg-slate-900 p-0.5 text-xs">
          {perfRanges.map((r) => (
            <button
              key={r.key}
              onClick={() => setRange(r.key)}
              className={`rounded-md px-3 py-1 font-medium transition ${range === r.key ? 'bg-cyan-500 text-slate-950' : 'text-slate-400 hover:text-slate-200'}`}
            >
              {r.label}
            </button>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
          <div className="text-xs text-slate-400">进程 CPU / 系统 CPU</div>
          <div className="mt-1 font-mono text-lg font-bold text-cyan-300">
            {(activeLatest?.proc_cpu_pct ?? 0).toFixed(2)}%
            <span className="ml-2 text-xs font-normal text-slate-400">
              (Sys {(activeLatest?.sys_cpu_pct ?? 0).toFixed(1)}%)
            </span>
          </div>
        </div>
        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
          <div className="text-xs text-slate-400">RSS 物理 / VSZ 虚拟内存</div>
          <div className="mt-1 font-mono text-lg font-bold text-emerald-300">
            {formatKb(activeLatest?.proc_mem_rss_kb ?? 0)}
            <span className="ml-2 text-xs font-normal text-slate-400">
              / {formatKb(activeLatest?.proc_mem_vsz_kb ?? 0)}
            </span>
          </div>
        </div>
        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
          <div className="text-xs text-slate-400">打开 FD / 系统上限</div>
          <div className="mt-1 font-mono text-lg font-bold text-amber-300">
            {activeLatest?.open_fds ?? 0}
            <span className="ml-1 text-xs font-normal text-slate-400">/ {activeLatest?.fd_limit ?? 0}</span>
          </div>
        </div>
        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
          <div className="text-xs text-slate-400">OS 线程 / Tokio 活跃任务</div>
          <div className="mt-1 font-mono text-lg font-bold text-violet-300">
            {activeLatest?.threads ?? 0} 线程 · {activeLatest?.tokio_alive_tasks ?? 0} 任务
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
          <h3 className="mb-2 text-sm font-semibold">CPU 开销趋势</h3>
          <EChart option={cpuChart} height={260} />
        </div>
        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
          <h3 className="mb-2 text-sm font-semibold">内存与 Tokio 任务趋势</h3>
          <EChart option={memChart} height={260} />
        </div>
      </div>
    </div>
  );
}

function ProbesView({
  probes,
  endpoints,
  onRefreshProbes,
  notify,
}: {
  probes: TargetProbeStatus[];
  endpoints: EndpointView[];
  onRefreshProbes: () => Promise<void>;
  notify: (msg: string, ok?: boolean) => void;
}) {
  const epMap = new Map(endpoints.map((e) => [e.id, e]));
  const [runningId, setRunningId] = useState<string | null>(null);

  const handleRun = async (epId: string) => {
    setRunningId(epId);
    try {
      await api.runProbeNow(epId);
      await onRefreshProbes();
      notify('探活已完成');
    } catch (e) {
      notify(e instanceof Error ? e.message : '探活失败', false);
    } finally {
      setRunningId(null);
    }
  };

  return (
    <div className="space-y-5">
      <div>
        <h1 className="text-lg font-bold text-slate-100">目的端口可达性监测</h1>
        <p className="text-xs text-slate-400">周期探测每条转发规则及其备用节点的 DNS 解析、TCP 握手延迟与可达状态</p>
      </div>

      <div className="rounded-xl border border-slate-800 bg-slate-900/70">
        <table className="w-full text-left text-xs">
          <thead className="border-b border-slate-800 bg-slate-950/60 text-slate-400">
            <tr>
              <th className="px-4 py-3">规则</th>
              <th className="px-4 py-3">目的地址</th>
              <th className="px-4 py-3">状态</th>
              <th className="px-4 py-3">解析 IP</th>
              <th className="px-4 py-3 text-right">DNS / RTT 延迟</th>
              <th className="px-4 py-3 text-right">连续失败</th>
              <th className="px-4 py-3">最近检查 / 错误</th>
              <th className="px-4 py-3 text-right">操作</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/70">
            {probes.length === 0 ? (
              <tr>
                <td colSpan={8} className="px-4 py-8 text-center text-slate-500">
                  暂无探活记录，规则启用后将按设定周期自动开始探测。
                </td>
              </tr>
            ) : (
              probes.map((p) => {
                const ep = epMap.get(p.endpoint_id);
                return (
                  <tr key={`${p.endpoint_id}-${p.target}`} className="hover:bg-slate-800/40">
                    <td className="px-4 py-3 font-medium text-slate-200">{ep ? ep.name : p.endpoint_id}</td>
                    <td className="px-4 py-3 font-mono text-slate-300">
                      {p.target} <span className="text-slate-500 uppercase">({p.protocol})</span>
                    </td>
                    <td className="px-4 py-3">
                      <span
                        className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 font-medium ${
                          p.reachable
                            ? 'bg-emerald-500/15 text-emerald-300'
                            : 'bg-rose-500/15 text-rose-300'
                        }`}
                      >
                        {p.reachable ? <Wifi className="h-3 w-3" /> : <WifiOff className="h-3 w-3" />}
                        {p.reachable ? '可达' : '不可达'}
                      </span>
                    </td>
                    <td className="px-4 py-3 font-mono text-slate-400">{p.resolved_ip || '-'}</td>
                    <td className="px-4 py-3 text-right font-mono text-slate-200">
                      {p.dns_ms ?? '-'}ms / <span className="text-cyan-300">{p.rtt_ms ?? '-'}ms</span>
                    </td>
                    <td className="px-4 py-3 text-right font-mono text-slate-300">{p.consecutive_failures}</td>
                    <td className="px-4 py-3 text-slate-400">
                      <div>{formatTime(p.last_checked_ts, true)}</div>
                      {p.error_msg && <div className="text-rose-400">{p.error_msg}</div>}
                    </td>
                    <td className="px-4 py-3 text-right">
                      <button
                        disabled={runningId === p.endpoint_id}
                        onClick={() => handleRun(p.endpoint_id)}
                        className="rounded-lg border border-slate-700 px-2.5 py-1 text-slate-200 hover:bg-slate-800 disabled:opacity-50"
                      >
                        立即探测
                      </button>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function LogsView({ endpoints }: { endpoints: EndpointView[] }) {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [levelFilter, setLevelFilter] = useState('');
  const [epFilter, setEpFilter] = useState('');
  const [keyword, setKeyword] = useState('');
  const [sysLevel, setSysLevel] = useState('info');
  const [paused, setPaused] = useState(false);
  const pausedRef = useRef(paused);
  pausedRef.current = paused;

  useEffect(() => {
    api
      .queryLogs({ level: levelFilter || undefined, endpoint_id: epFilter || undefined, keyword: keyword || undefined, limit: 300 })
      .then((res) => {
        setLogs(res.logs);
        setSysLevel(res.level);
      })
      .catch(() => {});
  }, [levelFilter, epFilter, keyword]);

  useEffect(() => {
    let closed = false;
    let ws: WebSocket | null = null;
    let retryTimer: ReturnType<typeof setTimeout> | null = null;

    const connect = () => {
      if (closed) return;
      ws = new WebSocket(createWsUrl('/ws/logs'));
      ws.onerror = () => ws?.close();
      ws.onclose = () => {
        if (!closed) {
          retryTimer = setTimeout(connect, 2000);
        }
      };
      ws.onmessage = (ev) => {
        if (pausedRef.current) return;
        try {
          const entry: LogEntry = JSON.parse(ev.data);
          setLogs((prev) => {
            if (prev.some((x) => x.id === entry.id)) return prev;
            return [...prev.slice(-999), entry];
          });
        } catch {
          // ignore
        }
      };
    };

    connect();
    return () => {
      closed = true;
      if (retryTimer) clearTimeout(retryTimer);
      ws?.close();
    };
  }, []);

  const visibleLogs = useMemo(() => {
    const kw = keyword.toLowerCase();
    return logs.filter((l) => {
      if (levelFilter && l.level.toLowerCase() !== levelFilter.toLowerCase()) return false;
      if (epFilter && l.endpoint_id !== epFilter) return false;
      if (kw) {
        const s = `${l.message} ${l.target} ${JSON.stringify(l.fields)}`.toLowerCase();
        if (!s.includes(kw)) return false;
      }
      return true;
    });
  }, [logs, levelFilter, epFilter, keyword]);

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-lg font-bold text-slate-100">实时结构化可观测日志</h1>
          <p className="text-xs text-slate-400">记录连接生命周期、出入字节、耗时、探活切换与配置热重载事件</p>
        </div>
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <span className="text-slate-400">运行级别:</span>
          <select
            value={sysLevel}
            onChange={async (e) => {
              const lv = e.target.value;
              setSysLevel(lv);
              await api.setLogLevel(lv);
            }}
            className="rounded-lg border border-slate-700 bg-slate-900 px-2.5 py-1.5 text-slate-200"
          >
            {['error', 'warn', 'info', 'debug', 'trace'].map((lv) => (
              <option key={lv} value={lv}>
                {lv.toUpperCase()}
              </option>
            ))}
          </select>
          <button
            onClick={() => setPaused((p) => !p)}
            className={`rounded-lg border px-3 py-1.5 ${
              paused ? 'border-amber-500/40 text-amber-300' : 'border-slate-700 text-slate-300'
            }`}
          >
            {paused ? '继续滚动' : '暂停'}
          </button>
          <button
            onClick={() => setLogs([])}
            className="rounded-lg border border-slate-700 px-3 py-1.5 text-slate-300 hover:bg-slate-800"
          >
            清空
          </button>
        </div>
      </div>

      <div className="flex flex-wrap gap-3 text-xs">
        <select
          value={levelFilter}
          onChange={(e) => setLevelFilter(e.target.value)}
          className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-1.5 text-slate-200"
        >
          <option value="">全部级别</option>
          {['ERROR', 'WARN', 'INFO', 'DEBUG', 'TRACE'].map((l) => (
            <option key={l} value={l}>
              {l}
            </option>
          ))}
        </select>

        <select
          value={epFilter}
          onChange={(e) => setEpFilter(e.target.value)}
          className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-1.5 text-slate-200"
        >
          <option value="">全部规则</option>
          {endpoints.map((ep) => (
            <option key={ep.id} value={ep.id}>
              {ep.name} ({ep.id})
            </option>
          ))}
        </select>

        <input
          value={keyword}
          onChange={(e) => setKeyword(e.target.value)}
          placeholder="搜索 IP、连接 ID 或关键词..."
          className="min-w-[240px] flex-1 rounded-lg border border-slate-700 bg-slate-900 px-3 py-1.5 text-slate-200 focus:border-cyan-500 focus:outline-none"
        />
      </div>

      <div className="h-[540px] overflow-y-auto rounded-xl border border-slate-800 bg-slate-950 p-3 font-mono text-xs">
        {visibleLogs.length === 0 ? (
          <div className="py-12 text-center text-slate-600">暂无匹配日志</div>
        ) : (
          visibleLogs.map((l) => {
            const color =
              l.level === 'ERROR'
                ? 'text-rose-400'
                : l.level === 'WARN'
                  ? 'text-amber-400'
                  : l.level === 'INFO'
                    ? 'text-cyan-400'
                    : 'text-slate-400';
            const fieldsStr = Object.entries(l.fields || {})
              .filter(([k]) => !k.startsWith('log.'))
              .map(([k, v]) => `${k}=${typeof v === 'string' ? v : JSON.stringify(v)}`)
              .join(' ');
            return (
              <div key={l.id} className="border-b border-slate-900/80 py-1.5 leading-relaxed">
                <span className="mr-2 text-slate-500">{formatTimeMs(l.ts)}</span>
                <span className={`mr-2 font-semibold ${color}`}>[{l.level}]</span>
                {l.endpoint_id && (
                  <span className="mr-2 rounded bg-slate-800 px-1 py-0.5 text-[10px] text-cyan-300">
                    {l.endpoint_id}
                  </span>
                )}
                {l.conn_id && (
                  <span className="mr-2 text-slate-500">#{l.conn_id}</span>
                )}
                <span className="text-slate-200">{l.message}</span>
                {fieldsStr && <span className="ml-2 text-slate-500">{fieldsStr}</span>}
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}

function SettingsView({
  notify,
  onReloaded,
}: {
  notify: (msg: string, ok?: boolean) => void;
  onReloaded: () => Promise<void>;
}) {
  const [content, setContent] = useState('');
  const [snapshots, setSnapshots] = useState<ConfigSnapshot[]>([]);
  const [curPass, setCurPass] = useState('');
  const [newPass, setNewPass] = useState('');

  const loadAll = useCallback(async () => {
    try {
      const [exp, snaps] = await Promise.all([api.exportConfig(), api.listSnapshots()]);
      setContent(exp.content);
      setSnapshots(snaps);
    } catch {
      // ignore
    }
  }, []);

  useEffect(() => {
    loadAll();
  }, [loadAll]);

  const handleApplyConfig = async () => {
    try {
      await api.importConfig(content);
      notify('配置校验通过，已原子写入并热重载');
      await onReloaded();
      await loadAll();
    } catch (e) {
      notify(e instanceof Error ? e.message : '配置应用失败', false);
    }
  };

  const handleRestore = async (id: number) => {
    if (!confirm(`确认回滚到历史快照 #${id}？`)) return;
    try {
      await api.restoreSnapshot(id);
      notify(`已回滚至快照 #${id}`);
      await onReloaded();
      await loadAll();
    } catch (e) {
      notify(e instanceof Error ? e.message : '回滚失败', false);
    }
  };

  const handlePassword = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await api.changePassword(curPass, newPass);
      setCurPass('');
      setNewPass('');
      notify('管理员密码已更新');
    } catch (err) {
      notify(err instanceof Error ? err.message : '修改密码失败', false);
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-lg font-bold text-slate-100">配置导入导出、快照回滚与安全</h1>
        <p className="text-xs text-slate-400">支持原版 realm TOML / JSON 配置直接编辑、校验与一键回滚</p>
      </div>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4 lg:col-span-2">
          <div className="mb-3 flex items-center justify-between">
            <h3 className="text-sm font-semibold">当前配置文件内容</h3>
            <div className="flex gap-2">
              <button
                onClick={async () => {
                  const res = await api.exportConfig('toml');
                  setContent(res.content);
                }}
                className="rounded border border-slate-700 px-2.5 py-1 text-xs text-slate-300 hover:bg-slate-800"
              >
                导出 TOML
              </button>
              <button
                onClick={async () => {
                  const res = await api.exportConfig('json');
                  setContent(res.content);
                }}
                className="rounded border border-slate-700 px-2.5 py-1 text-xs text-slate-300 hover:bg-slate-800"
              >
                导出 JSON
              </button>
              <button
                onClick={handleApplyConfig}
                className="rounded bg-cyan-500 px-3 py-1 text-xs font-semibold text-slate-950 hover:bg-cyan-400"
              >
                校验并热重载应用
              </button>
            </div>
          </div>
          <textarea
            value={content}
            onChange={(e) => setContent(e.target.value)}
            rows={16}
            className="w-full rounded-lg border border-slate-800 bg-slate-950 p-3 font-mono text-xs text-slate-200 focus:border-cyan-500 focus:outline-none"
          />
        </div>

        <div className="space-y-5">
          <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
            <h3 className="mb-3 flex items-center gap-2 text-sm font-semibold">
              <ShieldCheck className="h-4 w-4 text-cyan-400" /> 修改管理员密码
            </h3>
            <form onSubmit={handlePassword} className="space-y-3 text-xs">
              <input
                type="password"
                required
                value={curPass}
                onChange={(e) => setCurPass(e.target.value)}
                placeholder="当前密码"
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-slate-100"
              />
              <input
                type="password"
                required
                minLength={6}
                value={newPass}
                onChange={(e) => setNewPass(e.target.value)}
                placeholder="新密码（至少 6 位）"
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-slate-100"
              />
              <button
                type="submit"
                className="w-full rounded-lg bg-slate-800 py-2 font-medium text-slate-200 hover:bg-slate-700"
              >
                更新密码
              </button>
            </form>
          </div>

          <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
            <h3 className="mb-3 text-sm font-semibold">配置历史快照（最近 20 条）</h3>
            <div className="max-h-64 space-y-2 overflow-y-auto text-xs">
              {snapshots.length === 0 ? (
                <div className="text-slate-500">暂无变更快照</div>
              ) : (
                snapshots.map((s) => (
                  <div
                    key={s.id}
                    className="flex items-center justify-between rounded-lg border border-slate-800 bg-slate-950/60 px-3 py-2"
                  >
                    <div>
                      <div className="font-mono text-slate-200">
                        #{s.id} · {s.source}
                      </div>
                      <div className="text-[11px] text-slate-500">{formatTime(s.created_at, true)}</div>
                    </div>
                    <button
                      onClick={() => handleRestore(s.id)}
                      className="inline-flex items-center gap-1 rounded border border-slate-700 px-2 py-1 text-slate-300 hover:bg-slate-800"
                    >
                      <RotateCcw className="h-3 w-3" /> 回滚
                    </button>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function LoginScreen({ onSuccess }: { onSuccess: (username: string) => void }) {
  const [username, setUsername] = useState('admin');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      const res = await api.login(username, password);
      setToken(res.token);
      onSuccess(res.username);
    } catch (err) {
      setError(err instanceof Error ? err.message : '登录失败');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex min-h-screen items-center justify-center bg-slate-950 p-4">
      <div className="w-full max-w-sm rounded-2xl border border-slate-800 bg-slate-900/80 p-7 shadow-2xl">
        <div className="mb-6 flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-gradient-to-br from-cyan-400 to-blue-600 text-lg font-bold text-slate-950">
            R
          </div>
          <div>
            <h1 className="text-base font-bold text-slate-100">Realm Panel</h1>
            <p className="text-xs text-slate-400">网络转发与可观测管理控制台</p>
          </div>
        </div>

        <form onSubmit={submit} className="space-y-4 text-sm">
          {error && (
            <div className="rounded-lg border border-rose-500/40 bg-rose-500/10 px-3 py-2 text-xs text-rose-300">
              {error}
            </div>
          )}
          <div>
            <label className="mb-1 block text-xs text-slate-400">用户名</label>
            <input
              required
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-slate-100 focus:border-cyan-500 focus:outline-none"
            />
          </div>
          <div>
            <label className="mb-1 block text-xs text-slate-400">密码</label>
            <input
              type="password"
              required
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-slate-100 focus:border-cyan-500 focus:outline-none"
            />
          </div>
          <button
            type="submit"
            disabled={loading}
            className="w-full rounded-lg bg-cyan-500 py-2.5 text-xs font-semibold text-slate-950 transition hover:bg-cyan-400 disabled:opacity-50"
          >
            {loading ? '登录中...' : '登录控制台'}
          </button>
        </form>
      </div>
    </div>
  );
}
