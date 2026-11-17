import { MonitorSmartphone, ArrowDownLeft, ArrowUpRight, Activity } from 'lucide-react';
import type { EndpointView, LiveDeviceSnapshot } from '../types';
import { formatBps, formatBytes, formatDuration } from '../utils';

interface Props {
  devices: LiveDeviceSnapshot[];
  endpoints?: EndpointView[];
  selectedEndpointId?: string;
  onSelectEndpoint?: (id: string) => void;
}

export function LiveDevicesTable({
  devices,
  endpoints = [],
  selectedEndpointId = '',
  onSelectEndpoint,
}: Props) {
  const epMap = new Map(endpoints.map((e) => [e.id, e]));
  const filtered = selectedEndpointId
    ? devices.filter((d) => d.endpoint_id === selectedEndpointId)
    : devices;

  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900/70 backdrop-blur">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800 px-5 py-3.5">
        <div className="flex items-center gap-2.5">
          <MonitorSmartphone className="h-5 w-5 text-cyan-400" />
          <div>
            <h3 className="text-sm font-semibold text-slate-100">实时入口接入设备与速率监控</h3>
            <p className="text-xs text-slate-400">
              实时追踪接入转发入口的客户端设备 IP、源端口、活跃连接与秒级双向速率（断开后保留 60s）
            </p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          {onSelectEndpoint && endpoints.length > 0 && (
            <select
              value={selectedEndpointId}
              onChange={(e) => onSelectEndpoint(e.target.value)}
              className="rounded-lg border border-slate-700 bg-slate-950 px-3 py-1.5 text-xs text-slate-200 focus:border-cyan-500 focus:outline-none"
            >
              <option value="">全部转发规则 ({devices.length})</option>
              {endpoints.map((ep) => (
                <option key={ep.id} value={ep.id}>
                  {ep.name} ({ep.listen})
                </option>
              ))}
            </select>
          )}
          <span className="inline-flex items-center gap-1.5 rounded-full bg-emerald-500/10 px-2.5 py-1 text-xs font-medium text-emerald-400">
            <Activity className="h-3.5 w-3.5 animate-pulse" />
            活跃设备 {filtered.filter((d) => d.active_conns > 0).length}
          </span>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-left text-xs">
          <thead className="border-b border-slate-800 bg-slate-950/60 text-slate-400">
            <tr>
              <th className="px-4 py-2.5 font-medium">入口设备 IP</th>
              <th className="px-4 py-2.5 font-medium">所属规则</th>
              <th className="px-4 py-2.5 font-medium">协议 / 状态</th>
              <th className="px-4 py-2.5 font-medium">活跃连接 / 源端口</th>
              <th className="px-4 py-2.5 font-medium text-right">实时入站 (RX/s)</th>
              <th className="px-4 py-2.5 font-medium text-right">实时出站 (TX/s)</th>
              <th className="px-4 py-2.5 font-medium text-right">累计流量 (RX / TX)</th>
              <th className="px-4 py-2.5 font-medium text-right">在线时长</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/70">
            {filtered.length === 0 ? (
              <tr>
                <td colSpan={8} className="px-4 py-8 text-center text-slate-500">
                  当前暂无设备接入。当客户端连入任意转发监听端口时，将在此实时显示设备信息与速率。
                </td>
              </tr>
            ) : (
              filtered.map((dev) => {
                const ep = epMap.get(dev.endpoint_id);
                const isOnline = dev.active_conns > 0;
                return (
                  <tr
                    key={`${dev.endpoint_id}-${dev.ip}-${dev.proto}`}
                    className="transition hover:bg-slate-800/40"
                  >
                    <td className="px-4 py-2.5 font-mono font-medium text-slate-100">
                      <div className="flex items-center gap-2">
                        <span
                          className={`h-2 w-2 rounded-full ${
                            isOnline ? 'bg-emerald-400 shadow-[0_0_8px_#34d399]' : 'bg-slate-600'
                          }`}
                        />
                        {dev.ip}
                      </div>
                    </td>
                    <td className="px-4 py-2.5 text-slate-300">
                      <div className="font-medium">{ep ? ep.name : dev.endpoint_id}</div>
                      {ep && <div className="font-mono text-[11px] text-slate-500">{ep.listen}</div>}
                    </td>
                    <td className="px-4 py-2.5">
                      <span
                        className={`mr-1.5 rounded px-1.5 py-0.5 font-mono text-[11px] uppercase ${
                          dev.proto === 'tcp'
                            ? 'bg-cyan-500/15 text-cyan-300'
                            : 'bg-violet-500/15 text-violet-300'
                        }`}
                      >
                        {dev.proto}
                      </span>
                      <span className={isOnline ? 'text-emerald-400' : 'text-slate-500'}>
                        {isOnline ? '在线' : '已断开'}
                      </span>
                    </td>
                    <td className="px-4 py-2.5 font-mono text-slate-300">
                      <div>
                        {dev.active_conns} 活跃{' '}
                        <span className="text-slate-500">/ 累计 {dev.total_conns}</span>
                      </div>
                      {dev.recent_ports.length > 0 && (
                        <div className="text-[11px] text-slate-500">
                          端口: {dev.recent_ports.slice(-4).map((p) => `:${p}`).join(', ')}
                        </div>
                      )}
                    </td>
                    <td className="px-4 py-2.5 text-right font-mono font-medium text-cyan-300">
                      <span className="inline-flex items-center gap-1">
                        <ArrowDownLeft className="h-3.5 w-3.5" />
                        {formatBps(dev.rx_bps)}
                      </span>
                    </td>
                    <td className="px-4 py-2.5 text-right font-mono font-medium text-emerald-300">
                      <span className="inline-flex items-center gap-1">
                        <ArrowUpRight className="h-3.5 w-3.5" />
                        {formatBps(dev.tx_bps)}
                      </span>
                    </td>
                    <td className="px-4 py-2.5 text-right font-mono text-slate-300">
                      {formatBytes(dev.rx_bytes)} / {formatBytes(dev.tx_bytes)}
                    </td>
                    <td className="px-4 py-2.5 text-right font-mono text-slate-400">
                      {formatDuration(dev.first_seen_ts)}
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
