import type {
  ConfigSnapshot,
  EndpointPayload,
  EndpointTrafficSummary,
  EndpointView,
  LiveDeviceSnapshot,
  LogEntry,
  PerfSample,
  RealtimeSnapshot,
  TargetProbeStatus,
  TrafficPoint,
} from './types';

const TOKEN_KEY = 'realm_panel_token';

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}

export function setToken(token: string | null) {
  if (token) localStorage.setItem(TOKEN_KEY, token);
  else localStorage.removeItem(TOKEN_KEY);
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = getToken();
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(init.headers as Record<string, string> | undefined),
  };
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }

  const res = await fetch(`/api${path}`, { ...init, headers });
  if (res.status === 401) {
    setToken(null);
    window.dispatchEvent(new Event('realm-unauthorized'));
    throw new Error('登录已过期，请重新登录');
  }

  const text = await res.text();
  const data = text ? JSON.parse(text) : {};
  if (!res.ok) {
    throw new Error(data.error || `请求失败 (${res.status})`);
  }
  return data as T;
}

export const api = {
  login: (username: string, password: string) =>
    request<{ token: string; username: string }>('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    }),
  me: () => request<{ username: string }>('/auth/me'),
  changePassword: (current_password: string, new_password: string) =>
    request<{ ok: boolean }>('/auth/password', {
      method: 'POST',
      body: JSON.stringify({ current_password, new_password }),
    }),

  listEndpoints: () => request<EndpointView[]>('/endpoints'),
  createEndpoint: (payload: EndpointPayload) =>
    request<{ id: string }>('/endpoints', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  updateEndpoint: (id: string, payload: EndpointPayload) =>
    request<{ id: string }>(`/endpoints/${id}`, {
      method: 'PUT',
      body: JSON.stringify(payload),
    }),
  toggleEndpoint: (id: string, enabled: boolean) =>
    request(`/endpoints/${id}/toggle`, {
      method: 'POST',
      body: JSON.stringify({ enabled }),
    }),
  deleteEndpoint: (id: string) =>
    request(`/endpoints/${id}`, {
      method: 'DELETE',
    }),

  reloadConfig: () => request('/config/reload', { method: 'POST' }),
  exportConfig: (format?: 'toml' | 'json') =>
    request<{ format: string; content: string }>(
      `/config/export${format ? `?format=${format}` : ''}`,
    ),
  importConfig: (content: string) =>
    request('/config/import', {
      method: 'POST',
      body: JSON.stringify({ content }),
    }),
  listSnapshots: () => request<ConfigSnapshot[]>('/config/snapshots'),
  restoreSnapshot: (id: number) =>
    request(`/config/snapshots/${id}/restore`, { method: 'POST' }),

  statsOverview: () =>
    request<{
      latest: RealtimeSnapshot | null;
      recent: RealtimeSnapshot[];
      probes: TargetProbeStatus[];
    }>('/stats/overview'),

  statsTraffic: (range: string, endpointId?: string) => {
    const params = new URLSearchParams({ range });
    if (endpointId) params.set('endpoint_id', endpointId);
    return request<{
      range: string;
      unit: 'bps' | 'bytes';
      series: TrafficPoint[];
      by_endpoint: EndpointTrafficSummary[];
    }>(`/stats/traffic?${params.toString()}`);
  },

  statsPerformance: (range: string) =>
    request<{
      range: string;
      latest: PerfSample | null;
      series: PerfSample[];
    }>(`/stats/performance?range=${range}`),

  liveDevices: (endpointId?: string) =>
    request<LiveDeviceSnapshot[]>(
      `/devices${endpointId ? `?endpoint_id=${encodeURIComponent(endpointId)}` : ''}`,
    ),

  listProbes: () => request<TargetProbeStatus[]>('/probes'),
  runProbeNow: (id: string) =>
    request<TargetProbeStatus[]>(`/probes/${id}/run`, { method: 'POST' }),

  queryLogs: (params: { level?: string; endpoint_id?: string; keyword?: string; limit?: number }) => {
    const qs = new URLSearchParams();
    if (params.level) qs.set('level', params.level);
    if (params.endpoint_id) qs.set('endpoint_id', params.endpoint_id);
    if (params.keyword) qs.set('keyword', params.keyword);
    if (params.limit) qs.set('limit', String(params.limit));
    return request<{ level: string; logs: LogEntry[] }>(`/logs?${qs.toString()}`);
  },

  setLogLevel: (level: string) =>
    request<{ level: string }>('/logs/level', {
      method: 'PUT',
      body: JSON.stringify({ level }),
    }),
};

export function createWsUrl(path: string): string {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const token = encodeURIComponent(getToken() || '');
  const sep = path.includes('?') ? '&' : '?';
  return `${proto}//${window.location.host}/api${path}${sep}token=${token}`;
}
