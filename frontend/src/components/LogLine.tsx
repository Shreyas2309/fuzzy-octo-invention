import type { LogEntry } from '../hooks/useLogs';

const levelColors: Record<string, string> = {
  debug: 'text-gray-500',
  info: 'text-accent-blue',
  warning: 'text-accent-amber',
  error: 'text-accent-red',
  critical: 'text-accent-red font-bold',
};

const levelBg: Record<string, string> = {
  debug: 'bg-gray-500/10',
  info: 'bg-accent-blue/10',
  warning: 'bg-accent-amber/10',
  error: 'bg-accent-red/10',
  critical: 'bg-accent-red/20',
};

// Special highlight patterns
function getLeftBorder(entry: LogEntry): string {
  const ev = entry.event?.toLowerCase() || '';
  if (ev.includes('sstable committed')) return 'border-l-2 border-accent-green';
  if (ev.includes('compaction started')) return 'border-l-2 border-accent-amber';
  if (ev.includes('compaction committed')) return 'border-l-2 border-accent-green';
  if (ev.includes('backpressure')) return 'border-l-2 border-accent-red bg-accent-red/5';
  if (ev.includes('recovery')) return 'border-l-2 border-accent-blue';
  return 'border-l-2 border-transparent';
}

export default function LogLine({ entry }: { entry: LogEntry }) {
  const level = entry.level?.toLowerCase() || 'info';
  const color = levelColors[level] || 'text-gray-400';
  const bg = levelBg[level] || '';
  const border = getLeftBorder(entry);

  return (
    <div className={`px-3 py-0.5 font-mono text-xs ${bg} ${border} hover:bg-navy-700/50`}>
      {entry.ts && (
        <span className="text-gray-600 mr-2">
          {new Date(entry.ts).toLocaleTimeString()}
        </span>
      )}
      <span className={`${color} uppercase mr-2 text-[10px] font-bold`}>
        {level.slice(0, 4).padEnd(4)}
      </span>
      {entry.logger && (
        <span className="text-gray-600 mr-2">
          {entry.logger.split('.').pop()}
        </span>
      )}
      <span className="text-gray-200">{entry.event || entry.raw}</span>
    </div>
  );
}
