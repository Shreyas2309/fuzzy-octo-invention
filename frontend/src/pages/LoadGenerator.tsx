import { useState, useRef, useCallback, useEffect } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { kvBatch, kvGet, getStats, getDisk, getMem, postFlush, getCompactionStatus } from '../api/client';
import { useStats } from '../hooks/useStats';
import { useDisk } from '../hooks/useDisk';
import { useMem } from '../hooks/useMem';
import StatCard from '../components/StatCard';
import LevelDiagram from '../components/LevelDiagram';
import { AreaChart, Area, XAxis, YAxis, ResponsiveContainer, Tooltip, CartesianGrid, ReferenceLine, Legend } from 'recharts';

// ─── Helpers ──────────────────────────────────────────────────────────────────

function randomString(len: number, charset: string): string {
  let result = '';
  for (let i = 0; i < len; i++) {
    result += charset[Math.floor(Math.random() * charset.length)];
  }
  return result;
}

const CHARSETS: Record<string, string> = {
  alpha: 'abcdefghijklmnopqrstuvwxyz',
  alphanumeric: 'abcdefghijklmnopqrstuvwxyz0123456789',
  hex: '0123456789abcdef',
  numeric: '0123456789',
};

type KeyPattern = 'sequential' | 'random' | 'zipfian' | 'hotspot';
type ValuePattern = 'fixed' | 'random' | 'counter';

interface RunStats {
  keysWritten: number;
  batchesSent: number;
  startTime: number;
  elapsed: number;
  opsPerSec: number;
  samples: { t: number; seq: number; l0: number; memEntries: number; keysWritten: number; pct: number; flushes: number; compactions: number }[];
  flushesObserved: number;
  compactionsObserved: number;
  startSeq: number;
  endSeq: number;
  startL0: number;
  endL0: number;
  errors: number;
}

const EMPTY_RUN: RunStats = {
  keysWritten: 0,
  batchesSent: 0,
  startTime: 0,
  elapsed: 0,
  opsPerSec: 0,
  samples: [],
  flushesObserved: 0,
  compactionsObserved: 0,
  startSeq: 0,
  endSeq: 0,
  startL0: 0,
  endL0: 0,
  errors: 0,
};

// ─── Component ────────────────────────────────────────────────────────────────

export default function LoadGenerator() {
  const qc = useQueryClient();
  const { data: stats } = useStats();
  const { data: diskData } = useDisk();
  const { data: memData } = useMem();

  // ── Parameters ──────────────────────────────────────────────────────────
  const [totalKeys, setTotalKeys] = useState(200);
  const [batchSize, setBatchSize] = useState(25);
  const [keyPrefix, setKeyPrefix] = useState('load_');
  const [keyLength, setKeyLength] = useState(8);
  const [keyPattern, setKeyPattern] = useState<KeyPattern>('sequential');
  const [keyCharset, setKeyCharset] = useState<string>('alphanumeric');
  const [valueSize, setValueSize] = useState(64);
  const [valuePattern, setValuePattern] = useState<ValuePattern>('random');
  const [delayMs, setDelayMs] = useState(50);
  const [deleteRatio, setDeleteRatio] = useState(0);
  const [duplicateRatio, setDuplicateRatio] = useState(0);
  const [autoFlush, setAutoFlush] = useState(false);
  const [flushEvery, setFlushEvery] = useState(50);

  // ── Run state ───────────────────────────────────────────────────────────
  const [running, setRunning] = useState(false);
  const [progress, setProgress] = useState(0);
  const [runStats, setRunStats] = useState<RunStats>(EMPTY_RUN);
  const [phase, setPhase] = useState<string>('idle');
  const abortRef = useRef(false);

  // Track previous L0 count + compaction for flush/compaction counting
  const prevL0Ref = useRef(0);
  const prevCompRef = useRef(false);
  const flushCountRef = useRef(0);
  const compactionCountRef = useRef(0);

  // ── Key generators ──────────────────────────────────────────────────────
  const generateKey = useCallback(
    (index: number, hotKeys: string[]): string => {
      switch (keyPattern) {
        case 'sequential':
          return `${keyPrefix}${String(index).padStart(keyLength, '0')}`;
        case 'random':
          return `${keyPrefix}${randomString(keyLength, CHARSETS[keyCharset] || CHARSETS.alphanumeric)}`;
        case 'zipfian': {
          // ~80% of ops hit ~20% of keys
          const skewed = Math.floor(Math.pow(Math.random(), 2) * totalKeys);
          return `${keyPrefix}${String(skewed).padStart(keyLength, '0')}`;
        }
        case 'hotspot': {
          // 90% hit the hot keys, 10% are random
          if (Math.random() < 0.9 && hotKeys.length > 0) {
            return hotKeys[Math.floor(Math.random() * hotKeys.length)];
          }
          return `${keyPrefix}${randomString(keyLength, CHARSETS[keyCharset] || CHARSETS.alphanumeric)}`;
        }
        default:
          return `${keyPrefix}${index}`;
      }
    },
    [keyPrefix, keyLength, keyPattern, keyCharset, totalKeys],
  );

  const generateValue = useCallback(
    (index: number): string => {
      switch (valuePattern) {
        case 'fixed':
          return 'x'.repeat(valueSize);
        case 'random':
          return randomString(valueSize, CHARSETS.alphanumeric);
        case 'counter':
          return `val_${index}_${'p'.repeat(Math.max(0, valueSize - 10))}`;
        default:
          return randomString(valueSize, CHARSETS.alphanumeric);
      }
    },
    [valueSize, valuePattern],
  );

  // ── Run logic ───────────────────────────────────────────────────────────
  const handleRun = useCallback(async () => {
    abortRef.current = false;
    setRunning(true);
    setProgress(0);
    setPhase('starting');
    flushCountRef.current = 0;
    compactionCountRef.current = 0;

    // Snapshot initial state
    let initialStats: any;
    try {
      initialStats = await getStats();
    } catch {
      initialStats = { seq: 0, l0_sstable_count: 0 };
    }

    prevL0Ref.current = initialStats.l0_sstable_count ?? 0;
    const startSeq = initialStats.seq ?? 0;
    const startL0 = initialStats.l0_sstable_count ?? 0;

    const run: RunStats = {
      ...EMPTY_RUN,
      startTime: performance.now(),
      startSeq: startSeq,
      startL0: startL0,
    };

    // Pre-generate hot keys for hotspot mode
    const hotKeys: string[] = [];
    if (keyPattern === 'hotspot') {
      for (let i = 0; i < Math.max(5, Math.floor(totalKeys * 0.05)); i++) {
        hotKeys.push(`${keyPrefix}hot_${i}`);
      }
    }

    const totalBatches = Math.ceil(totalKeys / batchSize);
    let keysWritten = 0;

    setPhase('writing');

    for (let b = 0; b < totalBatches; b++) {
      if (abortRef.current) break;

      const remaining = totalKeys - keysWritten;
      const count = Math.min(batchSize, remaining);
      const ops: { op: string; key: string; value?: string }[] = [];

      for (let i = 0; i < count; i++) {
        const globalIdx = keysWritten + i;
        const key = generateKey(globalIdx, hotKeys);

        // Check if this should be a delete
        if (deleteRatio > 0 && Math.random() < deleteRatio / 100) {
          ops.push({ op: 'del', key });
          continue;
        }

        // Check if this should be a duplicate (overwrite existing key)
        if (duplicateRatio > 0 && Math.random() < duplicateRatio / 100 && globalIdx > 0) {
          const dupIdx = Math.floor(Math.random() * globalIdx);
          const dupKey = keyPattern === 'sequential'
            ? `${keyPrefix}${String(dupIdx).padStart(keyLength, '0')}`
            : key; // for random patterns, just use same key
          ops.push({ op: 'put', key: dupKey, value: generateValue(globalIdx) });
          continue;
        }

        ops.push({ op: 'put', key, value: generateValue(globalIdx) });
      }

      try {
        await kvBatch(ops);
        keysWritten += count;
        run.batchesSent += 1;
        run.keysWritten = keysWritten;
      } catch (err) {
        run.errors += 1;
      }

      // Auto-flush check
      if (autoFlush && keysWritten > 0 && keysWritten % flushEvery === 0) {
        try {
          await postFlush();
        } catch {}
      }

      // Sample stats for the chart
      try {
        const s = await getStats();
        const mem = await getMem();
        const now = performance.now();

        // Detect flush events (L0 count increased)
        const currentL0 = s.l0_sstable_count ?? 0;
        if (currentL0 > prevL0Ref.current) {
          flushCountRef.current += currentL0 - prevL0Ref.current;
        }
        prevL0Ref.current = currentL0;

        // Detect compaction
        if (s.compaction_active && !prevCompRef.current) {
          compactionCountRef.current += 1;
        }
        prevCompRef.current = s.compaction_active;

        run.samples.push({
          t: Math.round((now - run.startTime) / 100) / 10,
          seq: s.seq ?? 0,
          l0: currentL0,
          memEntries: mem?.active?.entry_count ?? 0,
          keysWritten,
          pct: Math.round((keysWritten / totalKeys) * 100),
          flushes: flushCountRef.current,
          compactions: compactionCountRef.current,
        });

        run.endSeq = s.seq ?? 0;
        run.endL0 = currentL0;
        run.flushesObserved = flushCountRef.current;
        run.compactionsObserved = compactionCountRef.current;
      } catch {}

      const elapsed = (performance.now() - run.startTime) / 1000;
      run.elapsed = Math.round(elapsed * 10) / 10;
      run.opsPerSec = Math.round(keysWritten / Math.max(elapsed, 0.001));

      setProgress(Math.round((keysWritten / totalKeys) * 100));
      setRunStats({ ...run });

      // Invalidate queries so other components on the page update
      qc.invalidateQueries({ queryKey: ['stats'] });
      qc.invalidateQueries({ queryKey: ['mem'] });
      qc.invalidateQueries({ queryKey: ['disk'] });

      // Inter-batch delay
      if (delayMs > 0 && b < totalBatches - 1) {
        await new Promise((r) => setTimeout(r, delayMs));
      }
    }

    // Final snapshot
    try {
      const finalStats = await getStats();
      const finalComp = await getCompactionStatus();
      run.endSeq = finalStats.seq ?? 0;
      run.endL0 = finalStats.l0_sstable_count ?? 0;
      run.flushesObserved = flushCountRef.current;
      run.compactionsObserved = compactionCountRef.current;
      if ((finalComp.active_jobs?.length ?? 0) > 0) {
        run.compactionsObserved = Math.max(run.compactionsObserved, 1);
      }
    } catch {}

    const elapsed = (performance.now() - run.startTime) / 1000;
    run.elapsed = Math.round(elapsed * 10) / 10;
    run.opsPerSec = Math.round(keysWritten / Math.max(elapsed, 0.001));

    setRunStats({ ...run });
    setPhase(abortRef.current ? 'aborted' : 'done');
    setRunning(false);

    qc.invalidateQueries({ queryKey: ['stats'] });
    qc.invalidateQueries({ queryKey: ['mem'] });
    qc.invalidateQueries({ queryKey: ['disk'] });
  }, [
    totalKeys, batchSize, keyPrefix, keyLength, keyPattern, keyCharset,
    valueSize, valuePattern, delayMs, deleteRatio, duplicateRatio,
    autoFlush, flushEvery, generateKey, generateValue, qc,
  ]);

  const handleAbort = () => {
    abortRef.current = true;
  };

  const handleReset = () => {
    setRunStats(EMPTY_RUN);
    setProgress(0);
    setPhase('idle');
  };

  // ── Derived values ──────────────────────────────────────────────────────
  const active = memData?.active;
  const immutableQ = memData?.immutable || [];
  const threshold = 10;

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-mono font-bold text-white">Load Generator</h2>
        <div className="flex items-center gap-2">
          {phase !== 'idle' && (
            <span className={`text-xs font-mono px-2 py-0.5 rounded ${
              phase === 'done' ? 'bg-accent-green/20 text-accent-green' :
              phase === 'aborted' ? 'bg-accent-red/20 text-accent-red' :
              phase === 'writing' ? 'bg-accent-amber/20 text-accent-amber animate-pulse' :
              'bg-navy-700 text-gray-400'
            }`}>
              {phase}
            </span>
          )}
        </div>
      </div>

      {/* ── Parameter Controls ─────────────────────────────────────────── */}
      <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
        <div className="text-xs font-mono text-gray-500 uppercase tracking-wider mb-3">
          Parameters
        </div>
        <div className="grid grid-cols-4 gap-4">
          {/* Total keys */}
          <div>
            <label className="block text-xs font-mono text-gray-400 mb-1">
              Total Keys: <span className="text-accent-blue">{totalKeys}</span>
            </label>
            <input
              type="range" min={10} max={2000} step={10}
              value={totalKeys}
              onChange={(e) => setTotalKeys(Number(e.target.value))}
              disabled={running}
              className="w-full accent-accent-blue"
            />
            <div className="flex justify-between text-[10px] text-gray-600 font-mono">
              <span>10</span><span>2000</span>
            </div>
          </div>

          {/* Batch size */}
          <div>
            <label className="block text-xs font-mono text-gray-400 mb-1">
              Batch Size: <span className="text-accent-blue">{batchSize}</span>
            </label>
            <input
              type="range" min={1} max={100} step={1}
              value={batchSize}
              onChange={(e) => setBatchSize(Number(e.target.value))}
              disabled={running}
              className="w-full accent-accent-blue"
            />
            <div className="flex justify-between text-[10px] text-gray-600 font-mono">
              <span>1</span><span>100</span>
            </div>
          </div>

          {/* Value size */}
          <div>
            <label className="block text-xs font-mono text-gray-400 mb-1">
              Value Size (bytes): <span className="text-accent-blue">{valueSize}</span>
            </label>
            <input
              type="range" min={4} max={4096} step={4}
              value={valueSize}
              onChange={(e) => setValueSize(Number(e.target.value))}
              disabled={running}
              className="w-full accent-accent-blue"
            />
            <div className="flex justify-between text-[10px] text-gray-600 font-mono">
              <span>4B</span><span>4KB</span>
            </div>
          </div>

          {/* Inter-batch delay */}
          <div>
            <label className="block text-xs font-mono text-gray-400 mb-1">
              Batch Delay: <span className="text-accent-blue">{delayMs}ms</span>
            </label>
            <input
              type="range" min={0} max={500} step={10}
              value={delayMs}
              onChange={(e) => setDelayMs(Number(e.target.value))}
              disabled={running}
              className="w-full accent-accent-blue"
            />
            <div className="flex justify-between text-[10px] text-gray-600 font-mono">
              <span>0ms</span><span>500ms</span>
            </div>
          </div>

          {/* Key pattern */}
          <div>
            <label className="block text-xs font-mono text-gray-400 mb-1">Key Pattern</label>
            <select
              value={keyPattern}
              onChange={(e) => setKeyPattern(e.target.value as KeyPattern)}
              disabled={running}
              className="w-full bg-navy-900 border border-navy-600 rounded px-2 py-1.5 font-mono text-xs text-gray-200"
            >
              <option value="sequential">Sequential (key_000, key_001...)</option>
              <option value="random">Random</option>
              <option value="zipfian">Zipfian (skewed distribution)</option>
              <option value="hotspot">Hotspot (90% hit 5% of keys)</option>
            </select>
          </div>

          {/* Value pattern */}
          <div>
            <label className="block text-xs font-mono text-gray-400 mb-1">Value Pattern</label>
            <select
              value={valuePattern}
              onChange={(e) => setValuePattern(e.target.value as ValuePattern)}
              disabled={running}
              className="w-full bg-navy-900 border border-navy-600 rounded px-2 py-1.5 font-mono text-xs text-gray-200"
            >
              <option value="random">Random bytes</option>
              <option value="fixed">Fixed (repeated 'x')</option>
              <option value="counter">Counter (val_N_pad)</option>
            </select>
          </div>

          {/* Key prefix */}
          <div>
            <label className="block text-xs font-mono text-gray-400 mb-1">Key Prefix</label>
            <input
              type="text"
              value={keyPrefix}
              onChange={(e) => setKeyPrefix(e.target.value)}
              disabled={running}
              className="w-full bg-navy-900 border border-navy-600 rounded px-2 py-1.5 font-mono text-xs text-gray-200 focus:border-accent-blue focus:outline-none"
            />
          </div>

          {/* Key length */}
          <div>
            <label className="block text-xs font-mono text-gray-400 mb-1">
              Key Length: <span className="text-accent-blue">{keyLength}</span>
            </label>
            <input
              type="range" min={3} max={32} step={1}
              value={keyLength}
              onChange={(e) => setKeyLength(Number(e.target.value))}
              disabled={running}
              className="w-full accent-accent-blue"
            />
          </div>

          {/* Delete ratio */}
          <div>
            <label className="block text-xs font-mono text-gray-400 mb-1">
              Delete Ratio: <span className="text-accent-red">{deleteRatio}%</span>
            </label>
            <input
              type="range" min={0} max={50} step={1}
              value={deleteRatio}
              onChange={(e) => setDeleteRatio(Number(e.target.value))}
              disabled={running}
              className="w-full accent-accent-red"
            />
            <div className="text-[10px] text-gray-600 font-mono">
              Generates tombstones — visible in compaction dedup
            </div>
          </div>

          {/* Duplicate (overwrite) ratio */}
          <div>
            <label className="block text-xs font-mono text-gray-400 mb-1">
              Overwrite Ratio: <span className="text-accent-amber">{duplicateRatio}%</span>
            </label>
            <input
              type="range" min={0} max={80} step={1}
              value={duplicateRatio}
              onChange={(e) => setDuplicateRatio(Number(e.target.value))}
              disabled={running}
              className="w-full accent-accent-amber"
            />
            <div className="text-[10px] text-gray-600 font-mono">
              Rewrites existing keys — shows compaction deduplication
            </div>
          </div>

          {/* Key charset */}
          <div>
            <label className="block text-xs font-mono text-gray-400 mb-1">Key Charset</label>
            <select
              value={keyCharset}
              onChange={(e) => setKeyCharset(e.target.value)}
              disabled={running}
              className="w-full bg-navy-900 border border-navy-600 rounded px-2 py-1.5 font-mono text-xs text-gray-200"
            >
              <option value="alphanumeric">Alphanumeric (a-z, 0-9)</option>
              <option value="alpha">Alpha only (a-z)</option>
              <option value="hex">Hex (0-9, a-f)</option>
              <option value="numeric">Numeric (0-9)</option>
            </select>
          </div>

          {/* Auto flush */}
          <div>
            <label className="flex items-center gap-2 text-xs font-mono text-gray-400 mb-1">
              <input
                type="checkbox"
                checked={autoFlush}
                onChange={(e) => setAutoFlush(e.target.checked)}
                disabled={running}
                className="accent-accent-green"
              />
              Auto Flush every <span className="text-accent-green">{flushEvery}</span> keys
            </label>
            {autoFlush && (
              <input
                type="range" min={10} max={200} step={10}
                value={flushEvery}
                onChange={(e) => setFlushEvery(Number(e.target.value))}
                disabled={running}
                className="w-full accent-accent-green"
              />
            )}
            <div className="text-[10px] text-gray-600 font-mono">
              Forces memtable flushes — creates more L0 SSTables
            </div>
          </div>
        </div>

        {/* Action buttons */}
        <div className="flex gap-3 mt-4 pt-3 border-t border-navy-600">
          {!running ? (
            <button
              onClick={handleRun}
              className="px-6 py-2 bg-accent-green/20 text-accent-green border border-accent-green/40 rounded font-mono text-sm hover:bg-accent-green/30 transition-colors"
            >
              Generate {totalKeys} Keys
            </button>
          ) : (
            <button
              onClick={handleAbort}
              className="px-6 py-2 bg-accent-red/20 text-accent-red border border-accent-red/40 rounded font-mono text-sm hover:bg-accent-red/30 transition-colors"
            >
              Abort
            </button>
          )}
          <button
            onClick={handleReset}
            disabled={running}
            className="px-4 py-2 text-gray-500 border border-navy-600 rounded font-mono text-xs hover:text-gray-300 disabled:opacity-30"
          >
            Reset Stats
          </button>

          {/* Progress bar */}
          {(running || phase === 'done' || phase === 'aborted') && (
            <div className="flex-1 flex items-center gap-3">
              <div className="flex-1 bg-navy-700 rounded-full h-3 relative overflow-hidden">
                <div
                  className={`h-3 rounded-full transition-all duration-300 ${
                    phase === 'done' ? 'bg-accent-green' :
                    phase === 'aborted' ? 'bg-accent-red' :
                    'bg-accent-blue'
                  }`}
                  style={{ width: `${progress}%` }}
                />
              </div>
              <span className="font-mono text-xs text-gray-400 w-12 text-right">{progress}%</span>
            </div>
          )}
        </div>
      </div>

      {/* ── Live Metrics ───────────────────────────────────────────────── */}
      <div className="grid grid-cols-6 gap-3">
        <StatCard label="Keys Written" value={runStats.keysWritten} color="text-accent-green" />
        <StatCard label="Ops/sec" value={runStats.opsPerSec} color="text-accent-blue" />
        <StatCard label="Elapsed" value={`${runStats.elapsed}s`} />
        <StatCard label="Flushes" value={runStats.flushesObserved} color="text-accent-amber" />
        <StatCard label="Compactions" value={runStats.compactionsObserved} color="text-accent-amber" />
        <StatCard label="Errors" value={runStats.errors} color={runStats.errors > 0 ? 'text-accent-red' : 'text-gray-500'} />
      </div>

      {/* ── Live panels (row 1: memtable + level diagram) ─────────────── */}
      <div className="grid grid-cols-2 gap-4">
        {/* MemTable state */}
        <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
          <div className="text-xs font-mono text-gray-500 uppercase tracking-wider mb-3">
            MemTable
          </div>
          {active && (
            <>
              <div className="text-sm font-mono text-gray-300 mb-2">
                {active.entry_count} entries / {active.size_bytes} B
              </div>
              <div className="w-full bg-navy-700 rounded-full h-3 mb-2">
                <div
                  className="bg-accent-green h-3 rounded-full transition-all"
                  style={{
                    width: `${Math.min(100, (active.entry_count / threshold) * 100)}%`,
                  }}
                />
              </div>
              <div className="text-xs font-mono text-gray-500">
                Immutable Queue: {immutableQ.length}/4
              </div>
              <div className="flex gap-1 mt-1">
                {[0, 1, 2, 3].map((i) => (
                  <div
                    key={i}
                    className={`h-3 flex-1 rounded ${
                      i < immutableQ.length
                        ? 'bg-accent-amber/40 border border-accent-amber/60'
                        : 'bg-navy-700 border border-navy-600'
                    }`}
                  />
                ))}
              </div>
            </>
          )}
        </div>

        {/* Level diagram */}
        <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
          <LevelDiagram diskData={diskData} compactionActive={stats?.compaction_active} />
        </div>
      </div>

      {/* ── Charts (row 2: progress % + flushes & compactions) ────────── */}
      <div className="grid grid-cols-2 gap-4">
        {/* Progress % over time */}
        <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
          <div className="text-xs font-mono text-gray-500 uppercase tracking-wider mb-2">
            Keys Written (%)
          </div>
          {runStats.samples.length > 1 ? (
            <ResponsiveContainer width="100%" height={180}>
              <AreaChart data={runStats.samples}>
                <defs>
                  <linearGradient id="greenGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#22c55e" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="#22c55e" stopOpacity={0.02} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#1a2035" />
                <XAxis
                  dataKey="t"
                  tick={{ fontSize: 10, fill: '#6b7280', fontFamily: 'monospace' }}
                  unit="s"
                />
                <YAxis
                  domain={[0, 100]}
                  tick={{ fontSize: 10, fill: '#6b7280', fontFamily: 'monospace' }}
                  unit="%"
                  width={40}
                />
                <Tooltip
                  contentStyle={{ background: '#0c0c14', border: '1px solid #243049', fontSize: 11, fontFamily: 'monospace' }}
                  formatter={(value: number, name: string) => {
                    if (name === 'progress') return [`${value}%`, 'Progress'];
                    return [value, name];
                  }}
                />
                <Area
                  type="monotone"
                  dataKey="pct"
                  name="progress"
                  stroke="#22c55e"
                  fill="url(#greenGrad)"
                  strokeWidth={2}
                  dot={false}
                />
              </AreaChart>
            </ResponsiveContainer>
          ) : (
            <div className="text-xs text-gray-600 font-mono h-[180px] flex items-center justify-center">
              {running ? 'Collecting...' : 'Run to see chart'}
            </div>
          )}
        </div>

        {/* Engine internals over time */}
        <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
          <div className="text-xs font-mono text-gray-500 uppercase tracking-wider mb-2">
            Engine Internals
          </div>
          {runStats.samples.length > 1 ? (
            <ResponsiveContainer width="100%" height={180}>
              <AreaChart data={runStats.samples}>
                <defs>
                  <linearGradient id="blueGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.25} />
                    <stop offset="95%" stopColor="#3b82f6" stopOpacity={0.02} />
                  </linearGradient>
                  <linearGradient id="amberGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#f59e0b" stopOpacity={0.2} />
                    <stop offset="95%" stopColor="#f59e0b" stopOpacity={0.02} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#1a2035" />
                <XAxis
                  dataKey="t"
                  tick={{ fontSize: 10, fill: '#6b7280', fontFamily: 'monospace' }}
                  unit="s"
                />
                <YAxis
                  allowDecimals={false}
                  tick={{ fontSize: 10, fill: '#6b7280', fontFamily: 'monospace' }}
                  width={30}
                />
                <Tooltip
                  contentStyle={{ background: '#0c0c14', border: '1px solid #243049', fontSize: 11, fontFamily: 'monospace' }}
                />
                <Legend
                  iconType="line"
                  wrapperStyle={{ fontSize: 10, fontFamily: 'monospace' }}
                />
                <ReferenceLine
                  y={10}
                  stroke="#ef4444"
                  strokeDasharray="6 3"
                  strokeOpacity={0.6}
                  label={{ value: 'L0 compaction threshold', position: 'right', fill: '#ef4444', fontSize: 9, fontFamily: 'monospace' }}
                />
                <Area
                  type="monotone"
                  dataKey="l0"
                  name="L0 SSTables"
                  stroke="#3b82f6"
                  fill="url(#blueGrad)"
                  strokeWidth={2}
                  dot={false}
                />
                <Area
                  type="monotone"
                  dataKey="memEntries"
                  name="MemTable entries"
                  stroke="#f59e0b"
                  fill="url(#amberGrad)"
                  strokeWidth={1.5}
                  dot={false}
                />
              </AreaChart>
            </ResponsiveContainer>
          ) : (
            <div className="text-xs text-gray-600 font-mono h-[180px] flex items-center justify-center">
              {running ? 'Collecting...' : 'Run to see chart'}
            </div>
          )}
        </div>
      </div>

      {/* ── Summary (shown after run) ──────────────────────────────────── */}
      {(phase === 'done' || phase === 'aborted') && (
        <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
          <div className="text-xs font-mono text-gray-500 uppercase tracking-wider mb-3">
            Run Summary
          </div>
          <div className="grid grid-cols-2 gap-x-8 gap-y-1 text-xs font-mono">
            <div className="flex justify-between">
              <span className="text-gray-500">Total keys written</span>
              <span className="text-gray-200">{runStats.keysWritten}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">Batches sent</span>
              <span className="text-gray-200">{runStats.batchesSent}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">Duration</span>
              <span className="text-gray-200">{runStats.elapsed}s</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">Throughput</span>
              <span className="text-accent-green">{runStats.opsPerSec} ops/sec</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">Seq range</span>
              <span className="text-gray-200">{runStats.startSeq} → {runStats.endSeq}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">L0 count</span>
              <span className="text-gray-200">{runStats.startL0} → {runStats.endL0}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">Flushes observed</span>
              <span className="text-accent-amber">{runStats.flushesObserved}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">Compactions observed</span>
              <span className="text-accent-amber">{runStats.compactionsObserved}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">Delete ratio</span>
              <span className="text-gray-200">{deleteRatio}%</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">Overwrite ratio</span>
              <span className="text-gray-200">{duplicateRatio}%</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">Key pattern</span>
              <span className="text-gray-200">{keyPattern}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">Value size</span>
              <span className="text-gray-200">{valueSize}B</span>
            </div>
            {runStats.errors > 0 && (
              <div className="flex justify-between col-span-2">
                <span className="text-accent-red">Errors</span>
                <span className="text-accent-red">{runStats.errors}</span>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
