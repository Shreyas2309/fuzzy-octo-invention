import { useState, useRef, useEffect } from 'react';
import { useLogs } from '../hooks/useLogs';
import LogLine from '../components/LogLine';

const LEVELS = ['ALL', 'DEBUG', 'INFO', 'WARNING', 'ERROR'] as const;

export default function LiveLogs() {
  const { logs, connected, setPaused, clear, sendFilter } = useLogs(500);
  const [levelFilter, setLevelFilter] = useState<string>('ALL');
  const [search, setSearch] = useState('');
  const [paused, setIsPaused] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);

  // Auto-scroll when not paused
  useEffect(() => {
    if (!paused && bottomRef.current) {
      bottomRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [logs, paused]);

  const togglePause = () => {
    const next = !paused;
    setIsPaused(next);
    setPaused(next);
  };

  const filtered = logs.filter((entry) => {
    if (levelFilter !== 'ALL' && entry.level?.toLowerCase() !== levelFilter.toLowerCase()) return false;
    if (search && !entry.raw.toLowerCase().includes(search.toLowerCase())) return false;
    return true;
  });

  return (
    <div className="space-y-4 h-[calc(100vh-7rem)] flex flex-col">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-mono font-bold text-white">Live Logs</h2>
        <div className="flex items-center gap-2">
          <span className={`w-2 h-2 rounded-full ${connected ? 'bg-accent-green animate-pulse' : 'bg-accent-red'}`} />
          <span className="text-xs font-mono text-gray-500">
            {connected ? 'streaming' : 'disconnected'}
          </span>
        </div>
      </div>

      {/* Controls */}
      <div className="flex gap-2 items-center">
        {LEVELS.map((level) => (
          <button
            key={level}
            onClick={() => setLevelFilter(level)}
            className={`px-2 py-1 text-[10px] font-mono rounded uppercase ${
              levelFilter === level
                ? 'bg-accent-blue/20 text-accent-blue border border-accent-blue/40'
                : 'text-gray-500 hover:text-gray-300 border border-transparent'
            }`}
          >
            {level}
          </button>
        ))}
        <input
          className="ml-2 bg-navy-800 border border-navy-600 rounded px-3 py-1 font-mono text-xs text-gray-200 w-48 focus:border-accent-blue focus:outline-none"
          placeholder="Search..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <button
          onClick={togglePause}
          className={`px-3 py-1 text-xs font-mono rounded ${
            paused
              ? 'bg-accent-amber/20 text-accent-amber border border-accent-amber/40'
              : 'text-gray-500 hover:text-gray-300 border border-navy-600'
          }`}
        >
          {paused ? 'Resume' : 'Pause'}
        </button>
        <button
          onClick={clear}
          className="px-3 py-1 text-xs font-mono text-gray-500 hover:text-gray-300 border border-navy-600 rounded"
        >
          Clear
        </button>
      </div>

      {/* Log stream */}
      <div className="flex-1 bg-navy-900 border border-navy-600 rounded-lg overflow-y-auto min-h-0">
        {filtered.length === 0 ? (
          <div className="p-4 text-xs font-mono text-gray-600 text-center">
            {connected ? 'Waiting for log events...' : 'WebSocket disconnected'}
          </div>
        ) : (
          <>
            {filtered.map((entry, i) => (
              <LogLine key={i} entry={entry} />
            ))}
            <div ref={bottomRef} />
          </>
        )}
      </div>
    </div>
  );
}
