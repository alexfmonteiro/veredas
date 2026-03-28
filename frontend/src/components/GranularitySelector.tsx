import type { ChartGranularity } from '@/lib/api';

const GRANULARITIES: { value: ChartGranularity; label: string }[] = [
  { value: 'day', label: 'D' },
  { value: 'week', label: 'W' },
  { value: 'month', label: 'M' },
  { value: 'year', label: 'Y' },
];

interface GranularitySelectorProps {
  value: ChartGranularity;
  onChange: (g: ChartGranularity) => void;
}

export function GranularitySelector({ value, onChange }: GranularitySelectorProps) {
  return (
    <div className="inline-flex items-center gap-0.5 rounded-md bg-slate-800/80 p-0.5">
      {GRANULARITIES.map((g) => (
        <button
          key={g.value}
          onClick={() => onChange(g.value)}
          className={`cursor-pointer rounded px-1.5 py-0.5 text-[10px] font-medium transition-colors focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:outline-none ${
            value === g.value
              ? 'bg-brand-600 text-white'
              : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700/50'
          }`}
        >
          {g.label}
        </button>
      ))}
    </div>
  );
}
