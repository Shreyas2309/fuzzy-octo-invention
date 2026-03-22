interface LevelDiagramProps {
  diskData: any;
  compactionActive?: boolean;
}

export default function LevelDiagram({ diskData, compactionActive }: LevelDiagramProps) {
  if (!diskData) return null;

  const l0Files = diskData.L0 || [];
  const l1Files = diskData.L1 || [];
  const l2Files = diskData.L2 || [];
  const l3Files = diskData.L3 || [];

  return (
    <div className="space-y-3">
      <div className="text-xs font-mono text-gray-500 uppercase tracking-wider">
        Level Diagram
      </div>

      {/* L0 */}
      <div>
        <div className="text-xs font-mono text-gray-400 mb-1">
          L0 ({l0Files.length} files)
        </div>
        <div className="flex gap-1 flex-wrap">
          {l0Files.length === 0 && (
            <div className="h-6 w-full border border-dashed border-navy-600 rounded text-xs font-mono text-gray-600 flex items-center justify-center">
              empty
            </div>
          )}
          {l0Files.map((f: any, i: number) => (
            <div
              key={i}
              className="h-6 bg-accent-blue/20 border border-accent-blue/40 rounded px-2 text-xs font-mono text-accent-blue flex items-center"
              title={`${f.file_id} - ${f.record_count} records`}
            >
              {f.file_id?.slice(0, 8)}
            </div>
          ))}
        </div>
      </div>

      {/* Compaction arrow */}
      {compactionActive && (
        <div className="flex justify-center text-accent-amber animate-pulse font-mono text-sm">
          compacting...
        </div>
      )}

      {/* L1 */}
      <div>
        <div className="text-xs font-mono text-gray-400 mb-1">
          L1 ({l1Files.length} files)
        </div>
        {l1Files.length === 0 ? (
          <div className="h-8 w-full border border-dashed border-navy-600 rounded text-xs font-mono text-gray-600 flex items-center justify-center">
            empty
          </div>
        ) : (
          l1Files.map((f: any, i: number) => (
            <div
              key={i}
              className="h-8 bg-accent-green/20 border border-accent-green/40 rounded px-3 text-xs font-mono text-accent-green flex items-center"
              title={`${f.file_id} - ${f.record_count} records`}
            >
              {f.file_id?.slice(0, 8)} - {f.record_count} records
            </div>
          ))
        )}
      </div>

      {/* L2 */}
      {l2Files.length > 0 && (
        <div>
          <div className="text-xs font-mono text-gray-400 mb-1">L2</div>
          {l2Files.map((f: any, i: number) => (
            <div
              key={i}
              className="h-8 bg-purple-500/20 border border-purple-500/40 rounded px-3 text-xs font-mono text-purple-400 flex items-center"
            >
              {f.file_id?.slice(0, 8)} - {f.record_count} records
            </div>
          ))}
        </div>
      )}

      {/* L3 */}
      {l3Files.length > 0 && (
        <div>
          <div className="text-xs font-mono text-gray-400 mb-1">L3</div>
          {l3Files.map((f: any, i: number) => (
            <div
              key={i}
              className="h-8 bg-gray-500/20 border border-gray-500/40 rounded px-3 text-xs font-mono text-gray-400 flex items-center"
            >
              {f.file_id?.slice(0, 8)} - {f.record_count} records
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
