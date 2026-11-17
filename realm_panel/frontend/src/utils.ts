export function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const idx = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const val = bytes / Math.pow(1024, idx);
  return `${val >= 100 ? val.toFixed(0) : val >= 10 ? val.toFixed(1) : val.toFixed(2)} ${units[idx]}`;
}

export function formatBps(bytesPerSec: number): string {
  return `${formatBytes(bytesPerSec)}/s`;
}

export function formatKb(kb: number): string {
  return formatBytes(kb * 1024);
}

export function formatTime(tsSec: number, withDate = false): string {
  if (!tsSec) return '-';
  const d = new Date(tsSec * 1000);
  if (withDate) {
    return d.toLocaleString('zh-CN', {
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    });
  }
  return d.toLocaleTimeString('zh-CN', { hour12: false });
}

export function formatTimeMs(tsMs: number): string {
  if (!tsMs) return '-';
  const d = new Date(tsMs);
  return d.toLocaleTimeString('zh-CN', { hour12: false });
}

export function formatDuration(sinceTsSec: number): string {
  const diff = Math.max(0, Math.floor(Date.now() / 1000) - sinceTsSec);
  if (diff < 60) return `${diff}s`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ${diff % 60}s`;
  const h = Math.floor(diff / 3600);
  const m = Math.floor((diff % 3600) / 60);
  return `${h}h ${m}m`;
}
