import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getConfig, patchConfig, getConfigSchema } from '../api/client';

const CONFIG_DESCRIPTIONS: Record<string, string> = {
  env: 'Dev: entry-count freeze. Prod: size-based freeze.',
  max_memtable_entries: 'Entry count before freeze (dev mode only)',
  max_memtable_size_mb: 'Size in MB before freeze (prod mode only)',
  immutable_queue_max_len: 'Max frozen tables before backpressure stall',
  l0_compaction_threshold: 'L0 file count that triggers compaction',
  flush_max_workers: 'Parallel SSTable write concurrency',
  block_size: 'SSTable data block size in bytes',
  backpressure_timeout: 'Seconds to wait for queue space',
  compaction_check_interval: 'Seconds between compaction checks',
  compaction_max_workers: 'Max parallel compaction workers',
  max_levels: 'Maximum LSM tree depth',
  cache_data_entry_limit: 'Block cache data entries',
  cache_index_entry_limit: 'Block cache index entries',
  cache_bloom_entry_limit: 'Block cache bloom entries',
};

export default function Config() {
  const [editKey, setEditKey] = useState<string | null>(null);
  const [editValue, setEditValue] = useState<string>('');
  const [message, setMessage] = useState<string>('');
  const qc = useQueryClient();

  const { data: config } = useQuery({
    queryKey: ['config'],
    queryFn: getConfig,
  });

  const handleSave = async (key: string) => {
    try {
      let val: any = editValue;
      // Try to parse as number
      if (!isNaN(Number(val)) && val !== '') val = Number(val);
      const res = await patchConfig(key, val);
      setMessage(`${key}: ${res.old_value} -> ${res.new_value}`);
      setEditKey(null);
      qc.invalidateQueries({ queryKey: ['config'] });
      setTimeout(() => setMessage(''), 3000);
    } catch (err: any) {
      setMessage(`Error: ${err.message}`);
    }
  };

  const entries = config ? Object.entries(config) : [];

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-mono font-bold text-white">Config</h2>

      {message && (
        <div className="bg-accent-green/10 border border-accent-green/30 text-accent-green text-xs font-mono px-4 py-2 rounded">
          {message}
        </div>
      )}

      <div className="bg-navy-800 border border-navy-600 rounded-lg divide-y divide-navy-600">
        {entries.map(([key, value]) => (
          <div key={key} className="flex items-center px-4 py-3 hover:bg-navy-700/50">
            {/* Left: key + description */}
            <div className="flex-1 min-w-0">
              <div className="font-mono text-sm text-gray-200">{key}</div>
              <div className="text-xs text-gray-500 mt-0.5">
                {CONFIG_DESCRIPTIONS[key] || ''}
              </div>
            </div>

            {/* Right: value + edit */}
            <div className="w-64 flex items-center justify-end gap-2">
              {editKey === key ? (
                <>
                  {key === 'env' ? (
                    <select
                      className="bg-navy-900 border border-accent-blue/40 rounded px-2 py-1 font-mono text-xs text-gray-200"
                      value={editValue}
                      onChange={(e) => setEditValue(e.target.value)}
                    >
                      <option value="dev">dev</option>
                      <option value="prod">prod</option>
                    </select>
                  ) : (
                    <input
                      className="w-24 bg-navy-900 border border-accent-blue/40 rounded px-2 py-1 font-mono text-xs text-gray-200 focus:outline-none"
                      value={editValue}
                      onChange={(e) => setEditValue(e.target.value)}
                      onKeyDown={(e) => e.key === 'Enter' && handleSave(key)}
                      autoFocus
                    />
                  )}
                  <button
                    onClick={() => handleSave(key)}
                    className="px-2 py-1 text-[10px] font-mono bg-accent-green/20 text-accent-green border border-accent-green/40 rounded"
                  >
                    Save
                  </button>
                  <button
                    onClick={() => setEditKey(null)}
                    className="px-2 py-1 text-[10px] font-mono text-gray-500 border border-navy-600 rounded"
                  >
                    Cancel
                  </button>
                </>
              ) : (
                <>
                  <span className="font-mono text-sm text-accent-blue">{String(value)}</span>
                  <button
                    onClick={() => { setEditKey(key); setEditValue(String(value)); }}
                    className="px-2 py-1 text-[10px] font-mono text-gray-500 hover:text-gray-300 border border-navy-600 rounded"
                  >
                    Edit
                  </button>
                </>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
