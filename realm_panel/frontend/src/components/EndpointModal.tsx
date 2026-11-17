import { useState } from 'react';
import { X } from 'lucide-react';
import type { EndpointPayload, EndpointView } from '../types';

interface Props {
  initial?: EndpointView | null;
  onClose: () => void;
  onSave: (payload: EndpointPayload, existingId?: string) => Promise<void>;
}

export function EndpointModal({ initial, onClose, onSave }: Props) {
  const [name, setName] = useState(initial?.name ?? '');
  const [listen, setListen] = useState(initial?.listen ?? '0.0.0.0:10000');
  const [remote, setRemote] = useState(initial?.remote ?? '127.0.0.1:80');
  const [extraRemotes, setExtraRemotes] = useState((initial?.extra_remotes ?? []).join(', '));
  const [balance, setBalance] = useState(initial?.balance ?? '');
  const [enabled, setEnabled] = useState(initial?.enabled ?? true);
  const [noTcp, setNoTcp] = useState(initial?.network?.no_tcp ?? false);
  const [useUdp, setUseUdp] = useState(initial?.network?.use_udp ?? false);
  const [through, setThrough] = useState(initial?.through ?? '');
  const [iface, setIface] = useState(initial?.interface ?? '');
  const [listenIface, setListenIface] = useState(initial?.listen_interface ?? '');
  const [listenTransport, setListenTransport] = useState(initial?.listen_transport ?? '');
  const [remoteTransport, setRemoteTransport] = useState(initial?.remote_transport ?? '');
  const [sendProxy, setSendProxy] = useState(initial?.network?.send_proxy ?? false);
  const [acceptProxy, setAcceptProxy] = useState(initial?.network?.accept_proxy ?? false);
  const [tcpTimeout, setTcpTimeout] = useState(String(initial?.network?.tcp_timeout ?? 5));
  const [udpTimeout, setUdpTimeout] = useState(String(initial?.network?.udp_timeout ?? 30));
  const [probeEnabled, setProbeEnabled] = useState(initial?.probe_enabled ?? true);
  const [probeInterval, setProbeInterval] = useState(String(initial?.probe_interval_sec ?? 15));
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setSaving(true);
    try {
      const extras = extraRemotes
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean);

      const payload: EndpointPayload = {
        name: name.trim() || `${listen.trim()} -> ${remote.trim()}`,
        enabled,
        probe_enabled: probeEnabled,
        probe_interval_sec: Math.max(3, Number(probeInterval) || 15),
        probe_timeout_ms: 3000,
        listen: listen.trim(),
        remote: remote.trim(),
        extra_remotes: extras,
        balance: balance.trim() || null,
        through: through.trim() || null,
        interface: iface.trim() || null,
        listen_interface: listenIface.trim() || null,
        listen_transport: listenTransport.trim() || null,
        remote_transport: remoteTransport.trim() || null,
        network: {
          no_tcp: noTcp,
          use_udp: useUdp,
          send_proxy: sendProxy,
          accept_proxy: acceptProxy,
          tcp_timeout: Number(tcpTimeout) || 5,
          udp_timeout: Number(udpTimeout) || 30,
        },
      };

      await onSave(payload, initial?.id);
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4 backdrop-blur-sm">
      <div className="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-2xl border border-slate-700 bg-slate-900 shadow-2xl">
        <div className="flex items-center justify-between border-b border-slate-800 px-6 py-4">
          <h2 className="text-base font-semibold text-slate-100">
            {initial ? '编辑转发规则（保存后热重载生效）' : '新增转发规则'}
          </h2>
          <button
            onClick={onClose}
            className="rounded-lg p-1 text-slate-400 hover:bg-slate-800 hover:text-slate-200"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4 p-6 text-sm">
          {error && (
            <div className="rounded-lg border border-rose-500/40 bg-rose-500/10 px-4 py-2.5 text-xs text-rose-300">
              {error}
            </div>
          )}

          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <div className="sm:col-span-2">
              <label className="mb-1 block text-xs text-slate-400">规则名称 / 备注</label>
              <input
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="例如：HK-Relay-Web"
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-slate-100 focus:border-cyan-500 focus:outline-none"
              />
            </div>

            <div>
              <label className="mb-1 block text-xs text-slate-400">本地监听地址 (listen) *</label>
              <input
                required
                value={listen}
                onChange={(e) => setListen(e.target.value)}
                placeholder="0.0.0.0:5000"
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 font-mono text-slate-100 focus:border-cyan-500 focus:outline-none"
              />
            </div>

            <div>
              <label className="mb-1 block text-xs text-slate-400">远端目标地址 (remote) *</label>
              <input
                required
                value={remote}
                onChange={(e) => setRemote(e.target.value)}
                placeholder="1.1.1.1:443 或 example.com:443"
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 font-mono text-slate-100 focus:border-cyan-500 focus:outline-none"
              />
            </div>
          </div>

          <div className="flex flex-wrap gap-5 rounded-xl border border-slate-800 bg-slate-950/60 px-4 py-3 text-xs">
            <label className="flex items-center gap-2 text-slate-200">
              <input
                type="checkbox"
                checked={enabled}
                onChange={(e) => setEnabled(e.target.checked)}
                className="rounded border-slate-600 bg-slate-900 text-cyan-500"
              />
              启用规则
            </label>
            <label className="flex items-center gap-2 text-slate-200">
              <input
                type="checkbox"
                checked={!noTcp}
                onChange={(e) => setNoTcp(!e.target.checked)}
                className="rounded border-slate-600 bg-slate-900 text-cyan-500"
              />
              启用 TCP 转发
            </label>
            <label className="flex items-center gap-2 text-slate-200">
              <input
                type="checkbox"
                checked={useUdp}
                onChange={(e) => setUseUdp(e.target.checked)}
                className="rounded border-slate-600 bg-slate-900 text-cyan-500"
              />
              启用 UDP 转发
            </label>
            <label className="flex items-center gap-2 text-slate-200">
              <input
                type="checkbox"
                checked={probeEnabled}
                onChange={(e) => setProbeEnabled(e.target.checked)}
                className="rounded border-slate-600 bg-slate-900 text-cyan-500"
              />
              目的端口探活
            </label>
          </div>

          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <div>
              <label className="mb-1 block text-xs text-slate-400">
                备用远端节点 (extra_remotes，逗号分隔)
              </label>
              <input
                value={extraRemotes}
                onChange={(e) => setExtraRemotes(e.target.value)}
                placeholder="1.1.1.2:443, 1.1.1.3:443"
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 font-mono text-xs text-slate-100 focus:border-cyan-500 focus:outline-none"
              />
            </div>

            <div>
              <label className="mb-1 block text-xs text-slate-400">负载均衡 (balance)</label>
              <input
                value={balance}
                onChange={(e) => setBalance(e.target.value)}
                placeholder="roundrobin: 4, 2, 1 或 iphash: 1, 1"
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 font-mono text-xs text-slate-100 focus:border-cyan-500 focus:outline-none"
              />
            </div>

            <div>
              <label className="mb-1 block text-xs text-slate-400">监听传输 (listen_transport)</label>
              <input
                value={listenTransport}
                onChange={(e) => setListenTransport(e.target.value)}
                placeholder="如 ws;host=a.com;path=/ws"
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 font-mono text-xs text-slate-100 focus:border-cyan-500 focus:outline-none"
              />
            </div>

            <div>
              <label className="mb-1 block text-xs text-slate-400">远端传输 (remote_transport)</label>
              <input
                value={remoteTransport}
                onChange={(e) => setRemoteTransport(e.target.value)}
                placeholder="如 tls;servername=a.com;insecure"
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 font-mono text-xs text-slate-100 focus:border-cyan-500 focus:outline-none"
              />
            </div>

            <div>
              <label className="mb-1 block text-xs text-slate-400">出站绑定 IP (through)</label>
              <input
                value={through}
                onChange={(e) => setThrough(e.target.value)}
                placeholder="可选，如 10.0.0.2"
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 font-mono text-xs text-slate-100 focus:border-cyan-500 focus:outline-none"
              />
            </div>

            <div className="grid grid-cols-2 gap-2">
              <div>
                <label className="mb-1 block text-xs text-slate-400">出站网卡</label>
                <input
                  value={iface}
                  onChange={(e) => setIface(e.target.value)}
                  placeholder="eth0"
                  className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 font-mono text-xs text-slate-100 focus:border-cyan-500 focus:outline-none"
                />
              </div>
              <div>
                <label className="mb-1 block text-xs text-slate-400">入站网卡</label>
                <input
                  value={listenIface}
                  onChange={(e) => setListenIface(e.target.value)}
                  placeholder="eth0"
                  className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 font-mono text-xs text-slate-100 focus:border-cyan-500 focus:outline-none"
                />
              </div>
            </div>

            <div className="grid grid-cols-3 gap-2">
              <div>
                <label className="mb-1 block text-xs text-slate-400">TCP 超时(s)</label>
                <input
                  type="number"
                  value={tcpTimeout}
                  onChange={(e) => setTcpTimeout(e.target.value)}
                  className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 font-mono text-xs text-slate-100 focus:border-cyan-500 focus:outline-none"
                />
              </div>
              <div>
                <label className="mb-1 block text-xs text-slate-400">UDP 超时(s)</label>
                <input
                  type="number"
                  value={udpTimeout}
                  onChange={(e) => setUdpTimeout(e.target.value)}
                  className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 font-mono text-xs text-slate-100 focus:border-cyan-500 focus:outline-none"
                />
              </div>
              <div>
                <label className="mb-1 block text-xs text-slate-400">探活周期(s)</label>
                <input
                  type="number"
                  value={probeInterval}
                  onChange={(e) => setProbeInterval(e.target.value)}
                  className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 font-mono text-xs text-slate-100 focus:border-cyan-500 focus:outline-none"
                />
              </div>
            </div>

            <div className="flex items-end gap-4 pb-2 text-xs">
              <label className="flex items-center gap-2 text-slate-300">
                <input
                  type="checkbox"
                  checked={acceptProxy}
                  onChange={(e) => setAcceptProxy(e.target.checked)}
                />
                accept_proxy
              </label>
              <label className="flex items-center gap-2 text-slate-300">
                <input
                  type="checkbox"
                  checked={sendProxy}
                  onChange={(e) => setSendProxy(e.target.checked)}
                />
                send_proxy
              </label>
            </div>
          </div>

          <div className="flex justify-end gap-3 border-t border-slate-800 pt-4">
            <button
              type="button"
              onClick={onClose}
              className="rounded-lg border border-slate-700 px-4 py-2 text-xs font-medium text-slate-300 hover:bg-slate-800"
            >
              取消
            </button>
            <button
              type="submit"
              disabled={saving}
              className="rounded-lg bg-cyan-500 px-5 py-2 text-xs font-semibold text-slate-950 transition hover:bg-cyan-400 disabled:opacity-50"
            >
              {saving ? '保存并热重载中...' : '保存并热重载'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
