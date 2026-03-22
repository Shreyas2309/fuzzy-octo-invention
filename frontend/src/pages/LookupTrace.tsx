import { useState } from 'react';
import { kvTrace } from '../api/client';

interface TraceStep {
  step: number;
  component: string;
  result: string;
  bloom_check?: string;
  bisect_offset?: number;
  block_cache_hit?: boolean;
  seq?: number;
}

interface TraceResult {
  key: string;
  found: boolean;
  value: string | null;
  source: string | null;
  seq: number | null;
  steps: TraceStep[];
}

export default function LookupTrace() {
  const [key, setKey] = useState('');
  const [trace, setTrace] = useState<TraceResult | null>(null);
  const [loading, setLoading] = useState(false);

  const handleTrace = async () => {
    if (!key) return;
    setLoading(true);
    setTrace(null);
    try {
      const result = await kvTrace(key);
      setTrace(result);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const resultColor = (result: string) => {
    if (result === 'hit' || result === 'found') return 'bg-accent-green/20 text-accent-green border-accent-green/40';
    if (result === 'miss') return 'bg-gray-500/10 text-gray-400 border-gray-500/30';
    if (result === 'bloom_skip') return 'bg-accent-red/10 text-accent-red border-accent-red/30';
    if (result === 'tombstone') return 'bg-accent-red/20 text-accent-red border-accent-red/40';
    if (result.includes('false positive')) return 'bg-accent-amber/10 text-accent-amber border-accent-amber/30';
    return 'bg-navy-700 text-gray-400 border-navy-600';
  };

  const bloomBadge = (check: string) => {
    if (check === 'negative') return <span className="text-accent-red text-[10px] uppercase">NEGATIVE</span>;
    return <span className="text-accent-amber text-[10px] uppercase">POSITIVE</span>;
  };

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-mono font-bold text-white">Lookup Trace</h2>

      {/* Input */}
      <div className="flex gap-3 items-center">
        <input
          className="flex-1 max-w-md bg-navy-800 border border-navy-600 rounded px-4 py-2 font-mono text-sm text-gray-200 focus:border-accent-blue focus:outline-none"
          placeholder="Enter a key to trace..."
          value={key}
          onChange={(e) => setKey(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleTrace()}
        />
        <button
          onClick={handleTrace}
          disabled={loading}
          className="px-6 py-2 bg-accent-blue/20 text-accent-blue border border-accent-blue/40 rounded font-mono text-sm hover:bg-accent-blue/30 transition-colors disabled:opacity-50"
        >
          {loading ? 'Tracing...' : 'Trace Lookup'}
        </button>
      </div>

      {/* Trace output */}
      {trace && (
        <div className="space-y-3">
          {trace.steps.map((step, i) => (
            <div key={i} className="flex items-start gap-3">
              {/* Step number + connector */}
              <div className="flex flex-col items-center w-8 shrink-0">
                <div className="w-6 h-6 rounded-full bg-navy-700 border border-navy-600 flex items-center justify-center text-[10px] font-mono text-gray-500">
                  {step.step}
                </div>
                {i < trace.steps.length - 1 && (
                  <div className="w-px h-6 bg-navy-600 mt-1" />
                )}
              </div>

              {/* Card */}
              <div className={`flex-1 rounded-lg border p-3 ${resultColor(step.result)}`}>
                <div className="flex items-center justify-between mb-1">
                  <span className="font-mono text-xs font-bold uppercase">
                    {step.component}
                  </span>
                  <span className="font-mono text-xs uppercase font-bold">
                    {step.result}
                  </span>
                </div>
                <div className="flex gap-4 text-[11px] font-mono">
                  {step.bloom_check && (
                    <div>Bloom: {bloomBadge(step.bloom_check)}</div>
                  )}
                  {step.bisect_offset !== undefined && (
                    <div className="text-gray-500">
                      bisect_offset: {step.bisect_offset}
                    </div>
                  )}
                  {step.block_cache_hit !== undefined && (
                    <div className="text-gray-500">
                      cache: {step.block_cache_hit ? 'HIT' : 'MISS'}
                    </div>
                  )}
                  {step.seq !== undefined && (
                    <div className="text-gray-500">seq: {step.seq}</div>
                  )}
                </div>
              </div>
            </div>
          ))}

          {/* Final answer */}
          <div className={`mt-4 p-4 rounded-lg border font-mono text-sm ${
            trace.found
              ? 'bg-accent-green/10 border-accent-green/30 text-accent-green'
              : 'bg-gray-500/10 border-gray-500/30 text-gray-400'
          }`}>
            <div className="text-xs uppercase tracking-wider mb-1">Result</div>
            {trace.found ? (
              <div>
                <span className="font-bold">"{trace.value}"</span>
                <span className="text-gray-500 ml-3">source: {trace.source}</span>
                <span className="text-gray-500 ml-3">seq: {trace.seq}</span>
              </div>
            ) : (
              <div className="text-gray-500">Key not found (NIL)</div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
