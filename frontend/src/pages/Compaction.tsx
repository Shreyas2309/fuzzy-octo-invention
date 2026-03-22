import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getCompactionStatus, postCompactionTrigger, getCompactionHistory } from '../api/client';
import { useDisk } from '../hooks/useDisk';
import { useStats } from '../hooks/useStats';
import LevelDiagram from '../components/LevelDiagram';

export default function Compaction() {
  const qc = useQueryClient();
  const { data: stats } = useStats();
  const { data: diskData } = useDisk();

  const { data: status } = useQuery({
    queryKey: ['compaction-status'],
    queryFn: getCompactionStatus,
    refetchInterval: stats?.compaction_active ? 500 : 2000,
  });

  const { data: history } = useQuery({
    queryKey: ['compaction-history'],
    queryFn: getCompactionHistory,
    refetchInterval: 5000,
  });

  const handleTrigger = async () => {
    await postCompactionTrigger();
    qc.invalidateQueries({ queryKey: ['compaction-status'] });
    qc.invalidateQueries({ queryKey: ['compaction-history'] });
  };

  const l0Count = stats?.l0_sstable_count ?? 0;
  const threshold = 10;
  const l0Color = l0Count >= 10 ? 'text-accent-red' : l0Count >= 7 ? 'text-accent-amber' : 'text-accent-green';
  const activeJobs = status?.active_jobs || [];
  const events = history?.events || [];

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-mono font-bold text-white">Compaction</h2>

      {/* Status chips */}
      <div className="flex gap-4 items-center">
        <div className={`px-3 py-1.5 rounded border text-xs font-mono ${l0Color} border-current/40 bg-current/10`}>
          L0: {l0Count}/{threshold}
        </div>
        <div className={`px-3 py-1.5 rounded border text-xs font-mono ${
          activeJobs.length > 0 ? 'text-accent-amber border-accent-amber/40 bg-accent-amber/10' : 'text-gray-500 border-navy-600'
        }`}>
          {activeJobs.length > 0 ? `${activeJobs.length} job(s) running` : 'Idle'}
        </div>
        <div className="px-3 py-1.5 rounded border text-xs font-mono text-gray-500 border-navy-600">
          Reserved: [{(status?.active_levels || []).join(', ')}]
        </div>
        <button
          onClick={handleTrigger}
          className="px-4 py-1.5 bg-accent-amber/20 text-accent-amber border border-accent-amber/40 rounded font-mono text-xs hover:bg-accent-amber/30 transition-colors ml-auto"
        >
          Force Compaction Check
        </button>
      </div>

      <div className="grid grid-cols-2 gap-6">
        {/* Level diagram */}
        <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
          <LevelDiagram diskData={diskData} compactionActive={stats?.compaction_active} />
        </div>

        {/* Active job card */}
        <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
          <div className="text-xs font-mono text-gray-500 uppercase tracking-wider mb-3">
            Active Job
          </div>
          {activeJobs.length === 0 ? (
            <div className="text-sm font-mono text-gray-600 py-8 text-center">
              No active compaction
            </div>
          ) : (
            activeJobs.map((job: any, i: number) => (
              <div key={i} className="border border-accent-amber/30 rounded p-3 text-xs font-mono space-y-1">
                <div className="text-accent-amber">L{job.src} &rarr; L{job.dst}</div>
                <div className="text-gray-400">Task: {job.task_id}</div>
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 rounded-full bg-accent-amber animate-pulse" />
                  <span className="text-gray-500">Running</span>
                </div>
              </div>
            ))
          )}
        </div>
      </div>

      {/* History */}
      <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
        <div className="text-xs font-mono text-gray-500 uppercase tracking-wider mb-3">
          Compaction History
        </div>
        <div className="max-h-64 overflow-y-auto">
          <table className="w-full text-xs font-mono">
            <thead>
              <tr className="text-gray-500 border-b border-navy-600">
                <th className="text-left py-1 px-2">Timestamp</th>
                <th className="text-left py-1 px-2">Event</th>
                <th className="text-left py-1 px-2">Task ID</th>
                <th className="text-left py-1 px-2">Levels</th>
                <th className="text-right py-1 px-2">Inputs</th>
                <th className="text-right py-1 px-2">Output</th>
              </tr>
            </thead>
            <tbody>
              {events.map((ev: any, i: number) => (
                <tr key={i} className="border-t border-navy-700 hover:bg-navy-700/50">
                  <td className="py-1 px-2 text-gray-600">{ev.ts?.slice(11, 19)}</td>
                  <td className={`py-1 px-2 ${
                    ev.event === 'committed' ? 'text-accent-green' : 'text-accent-amber'
                  }`}>
                    {ev.event}
                  </td>
                  <td className="py-1 px-2 text-gray-500 truncate max-w-[100px]">{ev.task_id?.slice(0, 8)}</td>
                  <td className="py-1 px-2 text-gray-400">L{ev.src}&rarr;L{ev.dst}</td>
                  <td className="py-1 px-2 text-right text-gray-500">{ev.inputs?.length}</td>
                  <td className="py-1 px-2 text-right text-gray-500">{ev.output_records ?? '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
