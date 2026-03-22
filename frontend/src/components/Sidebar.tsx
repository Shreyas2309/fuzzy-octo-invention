import { NavLink } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { getEngineStatus } from '../api/client';

const links = [
  { to: '/', label: 'Dashboard' },
  { to: '/write-read', label: 'Write / Read' },
  { to: '/memtable', label: 'MemTable' },
  { to: '/sstables', label: 'SSTables' },
  { to: '/compaction', label: 'Compaction' },
  { to: '/lookup-trace', label: 'Lookup Trace' },
  { to: '/live-logs', label: 'Live Logs' },
  { to: '/load-generator', label: 'Load Generator' },
  { to: '/terminal', label: 'Terminal' },
  { to: '/config', label: 'Config' },
];

export default function Sidebar() {
  const { data: status } = useQuery({
    queryKey: ['engine-status'],
    queryFn: getEngineStatus,
    refetchInterval: 2000,
  });

  const isOpen = status?.open ?? false;
  const uptime = status?.uptime_s ?? 0;
  const mins = Math.floor(uptime / 60);
  const secs = Math.floor(uptime % 60);

  return (
    <aside className="w-60 bg-navy-800 border-r border-navy-600 flex flex-col shrink-0">
      <div className="p-4 border-b border-navy-600">
        <h1 className="font-mono text-lg font-bold text-white tracking-tight">
          lsm-kv
        </h1>
        <span className="text-xs text-gray-500 font-mono">Explorer</span>
      </div>

      <nav className="flex-1 py-2">
        {links.map((link) => (
          <NavLink
            key={link.to}
            to={link.to}
            end={link.to === '/'}
            className={({ isActive }) =>
              `block px-4 py-2 text-sm font-mono transition-colors ${
                isActive
                  ? 'bg-navy-700 text-accent-green border-l-2 border-accent-green'
                  : 'text-gray-400 hover:text-gray-200 hover:bg-navy-700 border-l-2 border-transparent'
              }`
            }
          >
            {link.label}
          </NavLink>
        ))}
      </nav>

      <div className="p-4 border-t border-navy-600 text-xs font-mono space-y-1">
        <div className="flex items-center gap-2">
          <span
            className={`w-2 h-2 rounded-full ${
              isOpen ? 'bg-accent-green animate-pulse' : 'bg-accent-red'
            }`}
          />
          <span className="text-gray-400">
            Engine: {isOpen ? 'OPEN' : 'CLOSED'}
          </span>
        </div>
        {isOpen && (
          <div className="text-gray-500 pl-4">
            {mins}m {secs}s
          </div>
        )}
      </div>
    </aside>
  );
}
