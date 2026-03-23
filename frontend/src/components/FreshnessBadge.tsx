interface FreshnessBadgeProps {
  lastUpdated: string | null;
}

function getStatus(lastUpdated: string | null): { label: string; className: string } {
  if (!lastUpdated) {
    return { label: 'No data', className: 'bg-red-500/20 text-red-400 border-red-500/30' };
  }

  const hours = (Date.now() - new Date(lastUpdated).getTime()) / 3_600_000;

  if (hours < 26) {
    return { label: 'Fresh', className: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30' };
  }
  if (hours < 120) {
    return { label: 'Stale', className: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30' };
  }
  return { label: 'Critical', className: 'bg-red-500/20 text-red-400 border-red-500/30' };
}

export function FreshnessBadge({ lastUpdated }: FreshnessBadgeProps) {
  const { label, className } = getStatus(lastUpdated);

  return (
    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider border ${className}`}>
      {label}
    </span>
  );
}
