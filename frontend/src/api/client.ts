const BASE = '/api/v1';

async function json<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

function post<T>(url: string, body?: unknown): Promise<T> {
  return json<T>(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
}

function patch<T>(url: string, body: unknown): Promise<T> {
  return json<T>(url, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

// Engine
export const getEngineStatus = () => json<any>(`${BASE}/engine/status`);
export const postEngineOpen = () => post<any>(`${BASE}/engine/open`);
export const postEngineClose = () => post<any>(`${BASE}/engine/close`);

// KV
export const kvPut = (key: string, value: string) =>
  post<any>(`${BASE}/kv/put`, { key, value });
export const kvDelete = (key: string) =>
  post<any>(`${BASE}/kv/delete`, { key });
export const kvGet = (key: string) =>
  json<any>(`${BASE}/kv/get?key=${encodeURIComponent(key)}`);
export const kvTrace = (key: string) =>
  json<any>(`${BASE}/kv/trace?key=${encodeURIComponent(key)}`);
export const kvBatch = (ops: { op: string; key: string; value?: string }[]) =>
  post<any>(`${BASE}/kv/batch`, { ops });

// Mem
export const getMem = () => json<any>(`${BASE}/mem`);
export const getMemDetail = (id: string) => json<any>(`${BASE}/mem/${id}`);
export const postFlush = () => post<any>(`${BASE}/mem/flush`);

// Disk
export const getDisk = () => json<any>(`${BASE}/disk`);
export const getDiskDetail = (id: string) => json<any>(`${BASE}/disk/${id}`);
export const getDiskMeta = (id: string) => json<any>(`${BASE}/disk/${id}/meta`);
export const getDiskBloom = (id: string) => json<any>(`${BASE}/disk/${id}/bloom`);
export const getDiskIndex = (id: string) => json<any>(`${BASE}/disk/${id}/index`);

// Compaction
export const getCompactionStatus = () => json<any>(`${BASE}/compaction/status`);
export const postCompactionTrigger = () => post<any>(`${BASE}/compaction/trigger`);
export const getCompactionHistory = () => json<any>(`${BASE}/compaction/history`);

// Stats
export const getStats = () => json<any>(`${BASE}/stats`);
export const getStatsHistory = () => json<any>(`${BASE}/stats/history`);
export const getWriteAmp = () => json<any>(`${BASE}/stats/write-amp`);
export const getWal = () => json<any>(`${BASE}/wal`);

// Config
export const getConfig = () => json<any>(`${BASE}/config`);
export const patchConfig = (key: string, value: any) =>
  patch<any>(`${BASE}/config`, { key, value });
export const getConfigSchema = () => json<any>(`${BASE}/config/schema`);

// Terminal
export const terminalRun = (command: string) =>
  post<{ output: string; error: boolean }>(`${BASE}/terminal/run`, { command });
