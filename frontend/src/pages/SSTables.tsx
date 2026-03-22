import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getDisk, getDiskDetail, getDiskMeta, getDiskBloom, getDiskIndex } from '../api/client';

type Tab = 'records' | 'meta' | 'bloom' | 'index';

export default function SSTables() {
  const [selectedFile, setSelectedFile] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<Tab>('records');

  const { data: diskData } = useQuery({
    queryKey: ['disk'],
    queryFn: getDisk,
    refetchInterval: 2000,
  });

  const { data: detail } = useQuery({
    queryKey: ['disk-detail', selectedFile],
    queryFn: () => getDiskDetail(selectedFile!),
    enabled: !!selectedFile && activeTab === 'records',
  });

  const { data: meta } = useQuery({
    queryKey: ['disk-meta', selectedFile],
    queryFn: () => getDiskMeta(selectedFile!),
    enabled: !!selectedFile && activeTab === 'meta',
  });

  const { data: bloom } = useQuery({
    queryKey: ['disk-bloom', selectedFile],
    queryFn: () => getDiskBloom(selectedFile!),
    enabled: !!selectedFile && activeTab === 'bloom',
  });

  const { data: indexData } = useQuery({
    queryKey: ['disk-index', selectedFile],
    queryFn: () => getDiskIndex(selectedFile!),
    enabled: !!selectedFile && activeTab === 'index',
  });

  const levels = diskData
    ? Object.entries(diskData).filter(([_, files]) => Array.isArray(files))
    : [];

  return (
    <div className="flex gap-6 h-[calc(100vh-7rem)]">
      {/* Left panel - Level tree */}
      <div className="w-52 shrink-0 bg-navy-800 border border-navy-600 rounded-lg p-3 overflow-y-auto">
        <div className="text-xs font-mono text-gray-500 uppercase tracking-wider mb-3">
          SSTable Tree
        </div>
        {levels.map(([level, files]: [string, any]) => (
          <div key={level} className="mb-3">
            <div className="text-xs font-mono text-gray-400 mb-1">
              {level} ({(files as any[]).length})
            </div>
            {(files as any[]).map((f: any) => (
              <button
                key={f.file_id}
                onClick={() => { setSelectedFile(f.file_id); setActiveTab('records'); }}
                className={`block w-full text-left px-2 py-1 text-xs font-mono rounded mb-0.5 ${
                  selectedFile === f.file_id
                    ? 'bg-accent-blue/20 text-accent-blue'
                    : 'text-gray-500 hover:text-gray-300 hover:bg-navy-700'
                }`}
              >
                <div className="truncate">{f.file_id?.slice(0, 12)}</div>
                <div className="text-[10px] text-gray-600">
                  {f.record_count}r / {f.size_bytes}B
                </div>
              </button>
            ))}
          </div>
        ))}
      </div>

      {/* Right panel - Detail */}
      <div className="flex-1 bg-navy-800 border border-navy-600 rounded-lg p-4 overflow-y-auto">
        {!selectedFile ? (
          <div className="h-full flex items-center justify-center text-gray-600 font-mono text-sm">
            Select an SSTable from the tree
          </div>
        ) : (
          <>
            <div className="flex gap-1 mb-4">
              {(['records', 'meta', 'bloom', 'index'] as Tab[]).map((t) => (
                <button
                  key={t}
                  onClick={() => setActiveTab(t)}
                  className={`px-3 py-1 text-xs font-mono rounded capitalize ${
                    activeTab === t
                      ? 'bg-accent-blue/20 text-accent-blue border border-accent-blue/40'
                      : 'text-gray-500 hover:text-gray-300 border border-transparent'
                  }`}
                >
                  {t}
                </button>
              ))}
            </div>

            {activeTab === 'records' && detail && (
              <div>
                <div className="text-xs font-mono text-gray-500 mb-2">
                  {detail.record_count} records | {detail.min_key}..{detail.max_key}
                </div>
                <div className="max-h-96 overflow-y-auto">
                  <table className="w-full text-xs font-mono">
                    <thead>
                      <tr className="text-gray-500 border-b border-navy-600">
                        <th className="text-left py-1 px-2">Seq</th>
                        <th className="text-left py-1 px-2">Key</th>
                        <th className="text-left py-1 px-2">Value</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(detail.entries || []).map((e: any, i: number) => (
                        <tr key={i} className="border-t border-navy-700 hover:bg-navy-700/50">
                          <td className="py-1 px-2 text-gray-500">{e.seq}</td>
                          <td className="py-1 px-2 text-gray-300">{e.key}</td>
                          <td className="py-1 px-2 text-gray-400">{e.value}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {activeTab === 'meta' && meta && !meta.error && (
              <div className="grid grid-cols-2 gap-2 text-xs font-mono">
                {Object.entries(meta).map(([k, v]) => (
                  <div key={k} className="flex">
                    <span className="text-gray-500 w-32 shrink-0">{k}</span>
                    <span className="text-gray-300 truncate">{String(v)}</span>
                  </div>
                ))}
              </div>
            )}

            {activeTab === 'bloom' && bloom && !bloom.error && (
              <div className="space-y-3">
                <div className="grid grid-cols-2 gap-2 text-xs font-mono">
                  <div className="flex"><span className="text-gray-500 w-32">hash_count</span><span className="text-gray-300">{bloom.hash_count}</span></div>
                  <div className="flex"><span className="text-gray-500 w-32">bit_count</span><span className="text-gray-300">{bloom.bit_count}</span></div>
                  <div className="flex"><span className="text-gray-500 w-32">approx_fpr</span><span className="text-gray-300">{bloom.approx_fpr}</span></div>
                  <div className="flex"><span className="text-gray-500 w-32">size_bytes</span><span className="text-gray-300">{bloom.size_bytes}</span></div>
                </div>
              </div>
            )}

            {activeTab === 'index' && indexData && !indexData.error && (
              <div>
                <div className="text-xs font-mono text-gray-500 mb-2">
                  {indexData.entry_count} index entries
                </div>
                <div className="max-h-96 overflow-y-auto">
                  <table className="w-full text-xs font-mono">
                    <thead>
                      <tr className="text-gray-500 border-b border-navy-600">
                        <th className="text-left py-1 px-2">First Key</th>
                        <th className="text-right py-1 px-2">Block Offset</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(indexData.entries || []).map((e: any, i: number) => (
                        <tr key={i} className="border-t border-navy-700 hover:bg-navy-700/50">
                          <td className="py-1 px-2 text-gray-300">{e.first_key}</td>
                          <td className="py-1 px-2 text-right text-gray-500">{e.block_offset}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
