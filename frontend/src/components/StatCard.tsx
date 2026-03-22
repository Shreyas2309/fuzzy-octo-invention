interface StatCardProps {
  label: string;
  value: string | number;
  color?: string;
}

export default function StatCard({ label, value, color = 'text-white' }: StatCardProps) {
  return (
    <div className="bg-navy-800 border border-navy-600 rounded-lg p-4">
      <div className={`text-2xl font-mono font-bold ${color}`}>{value}</div>
      <div className="text-xs text-gray-500 mt-1 font-mono uppercase tracking-wider">
        {label}
      </div>
    </div>
  );
}
