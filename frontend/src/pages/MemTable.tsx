import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getMem, getMemDetail, postFlush } from '../api/client';

export default function MemTable() {
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const qc = useQueryClient();

  const { data: mem } = useQuery({
    queryKey: ['mem'],
    queryFn: getMem,
    refetchInterval: 1000,
  });

  const { data: detail } = useQuery({
    queryKey: ['mem-detail', selectedId],
    queryFn: () => getMemDetail(selectedId!),
    enabled: !!selectedId,
    refetchInterval: 1000,
  });

  const active = mem?.active;
  const immutable = mem?.immutable || [];
  const threshold = 10; // dev default

  const handleFlush = async () => {
    await postFlush();
    qc.invalidateQueries({ queryKey: ['mem'] });
    qc.invalidateQueries({ queryKey: ['stats'] });
    qc.invalidateQueries({ queryKey: ['disk'] });
  };

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-mono font-bold text-white">MemTable Inspector</h2>

      {/* Active MemTable */}
      {active && (
        <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
          <div className="flex items-center justify-between mb-3">
            <div>
              <div className="text-xs font-mono text-gray-500 uppercase tracking-wider">
                Active MemTable
              </div>
              <div
                className="text-sm font-mono text-gray-400 mt-1 cursor-pointer hover:text-gray-200"
                title={active.table_id}
                onClick={() => setSelectedId(active.table_id)}
              >
                {active.table_id?.slice(0, 12)}...
              </div>
            </div>
            <button
              onClick={handleFlush}
              className="px-4 py-1.5 bg-accent-amber/20 text-accent-amber border border-accent-amber/40 rounded font-mono text-xs hover:bg-accent-amber/30 transition-colors"
            >
              Force Flush
            </button>
          </div>

          <div className="flex items-center gap-4 mb-2">
            <span className="font-mono text-sm text-gray-300">
              {active.entry_count} entries
            </span>
            <span className="font-mono text-sm text-gray-500">
              {active.size_bytes} bytes
            </span>
          </div>

          <div className="w-full bg-navy-700 rounded-full h-3 relative">
            <div
              className="bg-accent-green h-3 rounded-full transition-all"
              style={{
                width: `${Math.min(100, (active.entry_count / threshold) * 100)}%`,
              }}
            />
            <div
              className="absolute top-0 h-3 w-px bg-accent-amber"
              style={{ left: `${Math.min(100, 100)}%` }}
              title={`Threshold: ${threshold}`}
            />
          </div>
          <div className="text-xs font-mono text-gray-600 mt-1">
            {active.entry_count}/{threshold} entries (dev threshold)
          </div>
        </div>
      )}

      {/* Immutable Queue */}
      <div>
        <div className="text-xs font-mono text-gray-500 uppercase tracking-wider mb-3">
          Immutable Queue ({immutable.length}/4)
        </div>
        <div className="grid grid-cols-4 gap-3">
          {[0, 1, 2, 3].map((i) => {
            const snap = immutable[i];
            if (!snap) {
              return (
                <div
                  key={i}
                  className="bg-navy-800 border border-dashed border-navy-600 rounded-lg p-3 h-28 flex items-center justify-center"
                >
                  <span className="font-mono text-xs text-gray-700">empty</span>
                </div>
              );
            }
            return (
              <div
                key={i}
                className="bg-navy-800 border border-accent-amber/30 rounded-lg p-3 cursor-pointer hover:border-accent-amber/60 transition-colors"
                onClick={() => setSelectedId(snap.snapshot_id)}
              >
                <div className="font-mono text-xs text-accent-amber truncate" title={snap.snapshot_id}>
                  {snap.snapshot_id?.slice(0, 12)}
                </div>
                <div className="mt-2 space-y-1 text-xs font-mono text-gray-400">
                  <div>{snap.entry_count} entries</div>
                  <div>{snap.size_bytes} B</div>
                  <div>
                    seq {snap.seq_min}..{snap.seq_max}
                  </div>
                  {snap.tombstone_count > 0 && (
                    <div className="text-accent-red">{snap.tombstone_count} tombstones</div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Entry Browser */}
      {detail && !detail.error && (
        <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
          <div className="flex items-center justify-between mb-3">
            <div className="text-xs font-mono text-gray-500 uppercase tracking-wider">
              Entries — {detail.type} ({detail.entry_count} entries)
            </div>
            <button
              onClick={() => setSelectedId(null)}
              className="text-xs font-mono text-gray-500 hover:text-gray-300"
            >
              Close
            </button>
          </div>
          <div className="max-h-72 overflow-y-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="text-gray-500 border-b border-navy-600">
                  <th className="text-left py-1 px-2">Seq</th>
                  <th className="text-left py-1 px-2">Key</th>
                  <th className="text-left py-1 px-2">Value</th>
                  <th className="text-left py-1 px-2">Tombstone</th>
                </tr>
              </thead>
              <tbody>
                {(detail.entries || []).map((e: any, i: number) => {
                  const isTomb = e.value === '\x00__tomb__\x00' || e.value === '\\x00__tomb__\\x00';
                  return (
                    <tr key={i} className="border-t border-navy-700 hover:bg-navy-700/50">
                      <td className="py-1 px-2 text-gray-500">{e.seq}</td>
                      <td className="py-1 px-2 text-gray-300">{typeof e.key === 'string' ? e.key : String(e.key)}</td>
                      <td className={`py-1 px-2 ${isTomb ? 'text-accent-red line-through' : 'text-gray-400'}`}>
                        {isTomb ? 'DELETED' : typeof e.value === 'string' ? e.value : String(e.value)}
                      </td>
                      <td className="py-1 px-2">
                        {isTomb && <span className="text-accent-red">x</span>}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
