import { useState, useCallback, useEffect } from 'react';
import { kvPut, kvDelete, kvGet, kvBatch } from '../api/client';
import { useQueryClient } from '@tanstack/react-query';

interface OpRecord {
  ts: number;
  op: string;
  key: string;
  value: string;
  seq?: number;
  latency_ms: number;
  source?: string;
}

export default function WriteRead() {
  const [tab, setTab] = useState<'put' | 'get' | 'delete' | 'batch'>('put');
  const [key, setKey] = useState('');
  const [value, setValue] = useState('');
  const [batchText, setBatchText] = useState('');
  const [result, setResult] = useState<string>('');
  const [history, setHistory] = useState<OpRecord[]>([]);
  const [quickKey, setQuickKey] = useState('');
  const [quickResult, setQuickResult] = useState<any>(null);
  const qc = useQueryClient();

  const addHistory = (rec: OpRecord) => {
    setHistory((prev) => [rec, ...prev].slice(0, 50));
  };

  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ['stats'] });
    qc.invalidateQueries({ queryKey: ['mem'] });
  };

  const handlePut = async () => {
    if (!key) return;
    const t0 = performance.now();
    const res = await kvPut(key, value);
    const ms = Math.round(performance.now() - t0);
    setResult(`OK seq=${res.seq}`);
    addHistory({ ts: Date.now(), op: 'PUT', key, value, seq: res.seq, latency_ms: ms });
    invalidate();
  };

  const handleGet = async () => {
    if (!key) return;
    const t0 = performance.now();
    const res = await kvGet(key);
    const ms = Math.round(performance.now() - t0);
    setResult(res.found ? `"${res.value}" (source: ${res.source})` : 'NOT FOUND');
    addHistory({ ts: Date.now(), op: 'GET', key, value: res.value || '', seq: res.seq, latency_ms: ms, source: res.source });
    invalidate();
  };

  const handleDelete = async () => {
    if (!key) return;
    const t0 = performance.now();
    const res = await kvDelete(key);
    const ms = Math.round(performance.now() - t0);
    setResult(`DELETED seq=${res.seq}`);
    addHistory({ ts: Date.now(), op: 'DEL', key, value: '', seq: res.seq, latency_ms: ms });
    invalidate();
  };

  const handleBatch = async () => {
    const lines = batchText.trim().split('\n').filter(Boolean);
    const ops = lines.map((line) => {
      const parts = line.split(/\s+/);
      if (parts[0]?.toLowerCase() === 'del') return { op: 'del', key: parts[1] || '' };
      return { op: 'put', key: parts[1] || parts[0] || '', value: parts[2] || parts[1] || '' };
    });
    const t0 = performance.now();
    const res = await kvBatch(ops);
    const ms = Math.round(performance.now() - t0);
    setResult(`Batch: ${res.count} ops, seq range ${res.seq_range}`);
    addHistory({ ts: Date.now(), op: 'BATCH', key: `${res.count} ops`, value: '', latency_ms: ms });
    invalidate();
  };

  // Quick lookup with debounce
  useEffect(() => {
    if (!quickKey) { setQuickResult(null); return; }
    const timer = setTimeout(async () => {
      try {
        const res = await kvGet(quickKey);
        setQuickResult(res);
      } catch { setQuickResult(null); }
    }, 500);
    return () => clearTimeout(timer);
  }, [quickKey]);

  const opColors: Record<string, string> = {
    PUT: 'text-accent-green',
    GET: 'text-accent-blue',
    DEL: 'text-accent-red',
    BATCH: 'text-accent-amber',
  };

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-mono font-bold text-white">Write / Read</h2>

      <div className="grid grid-cols-2 gap-6">
        {/* Left panel - Command input */}
        <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
          {/* Tabs */}
          <div className="flex gap-1 mb-4">
            {(['put', 'get', 'delete', 'batch'] as const).map((t) => (
              <button
                key={t}
                onClick={() => setTab(t)}
                className={`px-3 py-1 text-xs font-mono rounded uppercase ${
                  tab === t
                    ? 'bg-accent-blue/20 text-accent-blue border border-accent-blue/40'
                    : 'text-gray-500 hover:text-gray-300 border border-transparent'
                }`}
              >
                {t}
              </button>
            ))}
          </div>

          {tab !== 'batch' && (
            <input
              className="w-full bg-navy-900 border border-navy-600 rounded px-3 py-2 font-mono text-sm text-gray-200 mb-3 focus:border-accent-blue focus:outline-none"
              placeholder="Key"
              value={key}
              onChange={(e) => setKey(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  if (tab === 'put') handlePut();
                  else if (tab === 'get') handleGet();
                  else if (tab === 'delete') handleDelete();
                }
              }}
            />
          )}

          {tab === 'put' && (
            <textarea
              className="w-full bg-navy-900 border border-navy-600 rounded px-3 py-2 font-mono text-sm text-gray-200 mb-3 h-20 resize-none focus:border-accent-blue focus:outline-none"
              placeholder="Value"
              value={value}
              onChange={(e) => setValue(e.target.value)}
            />
          )}

          {tab === 'batch' && (
            <textarea
              className="w-full bg-navy-900 border border-navy-600 rounded px-3 py-2 font-mono text-sm text-gray-200 mb-3 h-32 resize-none focus:border-accent-blue focus:outline-none"
              placeholder={"put key1 value1\nput key2 value2\ndel key3"}
              value={batchText}
              onChange={(e) => setBatchText(e.target.value)}
            />
          )}

          <button
            onClick={() => {
              if (tab === 'put') handlePut();
              else if (tab === 'get') handleGet();
              else if (tab === 'delete') handleDelete();
              else handleBatch();
            }}
            className="w-full py-2 bg-accent-blue/20 text-accent-blue border border-accent-blue/40 rounded font-mono text-sm hover:bg-accent-blue/30 transition-colors"
          >
            {tab === 'put' ? 'PUT' : tab === 'get' ? 'GET' : tab === 'delete' ? 'DELETE' : 'BATCH'}
          </button>

          {result && (
            <div className="mt-3 p-2 bg-navy-900 rounded font-mono text-xs text-gray-300">
              {result}
            </div>
          )}
        </div>

        {/* Right panel - History */}
        <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
          <div className="text-xs font-mono text-gray-500 uppercase tracking-wider mb-3">
            Operation History
          </div>
          <div className="max-h-80 overflow-y-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="text-gray-500">
                  <th className="text-left pb-2">Time</th>
                  <th className="text-left pb-2">Op</th>
                  <th className="text-left pb-2">Key</th>
                  <th className="text-left pb-2">Value</th>
                  <th className="text-right pb-2">ms</th>
                </tr>
              </thead>
              <tbody>
                {history.map((rec, i) => (
                  <tr key={i} className="border-t border-navy-700 hover:bg-navy-700/50">
                    <td className="py-1 text-gray-600">
                      {new Date(rec.ts).toLocaleTimeString()}
                    </td>
                    <td className={`py-1 ${opColors[rec.op] || 'text-gray-400'}`}>
                      {rec.op}
                    </td>
                    <td className="py-1 text-gray-300 max-w-[100px] truncate">
                      {rec.key}
                    </td>
                    <td className="py-1 text-gray-500 max-w-[80px] truncate">
                      {rec.value}
                    </td>
                    <td className="py-1 text-right text-gray-600">{rec.latency_ms}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* Quick lookup */}
      <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
        <div className="flex items-center gap-4">
          <span className="text-xs font-mono text-gray-500">Quick Lookup:</span>
          <input
            className="flex-1 bg-navy-900 border border-navy-600 rounded px-3 py-1.5 font-mono text-sm text-gray-200 focus:border-accent-blue focus:outline-none"
            placeholder="Type a key..."
            value={quickKey}
            onChange={(e) => setQuickKey(e.target.value)}
          />
          {quickResult && (
            <span className="font-mono text-sm">
              {quickResult.found ? (
                <span className="text-accent-green">
                  "{quickResult.value}" <span className="text-gray-500">({quickResult.source})</span>
                </span>
              ) : (
                <span className="text-gray-500">NOT FOUND</span>
              )}
            </span>
          )}
        </div>
      </div>
    </div>
  );
}
