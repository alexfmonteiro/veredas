import { SERIES } from '@/lib/api';
import { useHealth, useMetrics, useSyncStatus, useQualityLatest } from '@/hooks/useMetrics';
import { FreshnessBadge } from '@/components/FreshnessBadge';
import type { TimeRange } from '@/lib/api';

function formatTimestamp(iso: string | null): string {
  if (!iso) return 'Never';
  return new Date(iso).toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function formatLag(seconds: number | null): string {
  if (seconds === null) return 'Unknown';
  if (seconds < 60) return `${Math.round(seconds)}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h ago`;
  return `${Math.round(seconds / 86400)}d ago`;
}

function SyncStatusPanel() {
  const { data, isLoading, isError } = useSyncStatus();

  if (isLoading) {
    return (
      <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6 animate-pulse">
        <div className="h-4 w-32 rounded bg-slate-700 mb-4" />
        <div className="h-20 rounded bg-slate-700" />
      </div>
    );
  }

  if (isError || !data) {
    return (
      <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
        <h3 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
          R2 Sync Status
        </h3>
        <p className="text-sm text-slate-500">Unable to load sync status</p>
      </div>
    );
  }

  const healthColor =
    data.sync_health === 'fresh'
      ? 'text-emerald-400'
      : data.sync_health === 'stale'
        ? 'text-yellow-400'
        : 'text-red-400';

  return (
    <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-slate-300 uppercase tracking-wider">
          R2 Sync Status
        </h3>
        <span className={`text-sm font-semibold uppercase ${healthColor}`}>
          {data.sync_health}
        </span>
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <div>
          <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Last Sync</p>
          <p className="text-sm text-slate-300">{formatTimestamp(data.last_sync_at)}</p>
        </div>
        <div>
          <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Sync Lag</p>
          <p className="text-sm text-slate-300">{formatLag(data.seconds_since_sync)}</p>
        </div>
        <div>
          <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Files Synced</p>
          <p className="text-sm text-slate-300">{data.files_synced}</p>
        </div>
        <div>
          <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Duration</p>
          <p className="text-sm text-slate-300">{formatDuration(data.sync_duration_ms)}</p>
        </div>
      </div>

      {data.run_id && (
        <p className="text-[10px] text-slate-600 mt-3">
          Run ID: {data.run_id} | Source: {data.source}
        </p>
      )}
    </div>
  );
}

function QualityStatusPanel() {
  const { data, isLoading, isError } = useQualityLatest();

  if (isLoading) {
    return (
      <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6 animate-pulse">
        <div className="h-4 w-32 rounded bg-slate-700 mb-4" />
        <div className="h-12 rounded bg-slate-700" />
      </div>
    );
  }

  if (isError || !data) {
    return (
      <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
        <h3 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
          Pipeline Quality
        </h3>
        <p className="text-sm text-slate-500">Unable to load quality status</p>
      </div>
    );
  }

  const statusColor =
    data.status === 'healthy'
      ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30'
      : data.status === 'degraded'
        ? 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30'
        : 'bg-red-500/20 text-red-400 border-red-500/30';

  return (
    <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-slate-300 uppercase tracking-wider">
          Pipeline Quality
        </h3>
        <span
          className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider border ${statusColor}`}
        >
          {data.status}
        </span>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div>
          <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Sync Health</p>
          <p className="text-sm text-slate-300 capitalize">{data.sync_health}</p>
        </div>
        <div>
          <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Last Sync</p>
          <p className="text-sm text-slate-300">
            {data.last_sync ? formatTimestamp(data.last_sync.last_sync_at) : 'N/A'}
          </p>
        </div>
      </div>

      {data.last_sync && (
        <p className="text-[10px] text-slate-600 mt-3">
          Files synced: {data.last_sync.files_synced}
          {data.last_sync.run_id && ` | Run: ${data.last_sync.run_id}`}
        </p>
      )}
    </div>
  );
}

const FRESHNESS_RANGE: TimeRange = 'ALL';

function SeriesFreshnessRow({ seriesId, label }: { seriesId: string; label: string }) {
  const { data, isLoading } = useMetrics(seriesId, FRESHNESS_RANGE);

  return (
    <div className="flex items-center justify-between py-2.5 border-b border-slate-700/30 last:border-0">
      <div className="flex items-center gap-3">
        <span className="text-sm text-slate-300 font-medium">{label}</span>
        <span className="text-[10px] text-slate-600">{seriesId}</span>
      </div>
      <div className="flex items-center gap-3">
        {isLoading ? (
          <div className="h-5 w-16 rounded bg-slate-700 animate-pulse" />
        ) : (
          <>
            <span className="text-[10px] text-slate-500">
              {data?.data_points.length ?? 0} pts
            </span>
            <FreshnessBadge lastUpdated={data?.last_updated ?? null} />
          </>
        )}
      </div>
    </div>
  );
}

export function QualityPage() {
  const { data: health } = useHealth();

  return (
    <div className="min-h-[calc(100vh-3.5rem)] p-4 sm:p-6 lg:p-8 max-w-7xl mx-auto">
      <header className="mb-8">
        <h1 className="text-2xl font-bold text-slate-100">Data Quality</h1>
        <p className="text-sm text-slate-500 mt-1">
          Pipeline transparency, data freshness, and sync status
        </p>
      </header>

      <div className="space-y-6">
        {/* Health overview */}
        <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
          <h3 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
            System Health
          </h3>
          {health ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
              <div>
                <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Status</p>
                <p className={`text-sm font-semibold ${
                  health.status === 'ok' ? 'text-emerald-400' : 'text-yellow-400'
                }`}>
                  {health.status}
                </p>
              </div>
              <div>
                <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Timestamp</p>
                <p className="text-sm text-slate-300">
                  {formatTimestamp(health.timestamp)}
                </p>
              </div>
              <div>
                <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Sync Health</p>
                <p className="text-sm text-slate-300 capitalize">
                  {health.sync?.sync_health ?? 'Unknown'}
                </p>
              </div>
            </div>
          ) : (
            <div className="h-12 flex items-center">
              <div className="h-4 w-4 rounded-full border-2 border-brand-500 border-t-transparent animate-spin" />
            </div>
          )}
        </div>

        {/* Quality and Sync panels side by side */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <QualityStatusPanel />
          <SyncStatusPanel />
        </div>

        {/* Per-series freshness */}
        <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
          <h3 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
            Series Freshness
          </h3>
          <div>
            {SERIES.map((s) => (
              <SeriesFreshnessRow key={s.id} seriesId={s.id} label={s.label} />
            ))}
          </div>
        </div>

        {/* Data freshness from health endpoint */}
        {health?.data_freshness && Object.keys(health.data_freshness).length > 0 && (
          <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
            <h3 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
              Data Freshness (from Health Check)
            </h3>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
              {Object.entries(health.data_freshness).map(([key, value]) => (
                <div
                  key={key}
                  className="flex items-center justify-between rounded-lg border border-slate-700/30 bg-slate-900/30 px-3 py-2"
                >
                  <span className="text-xs text-slate-400">{key}</span>
                  <span className="text-xs text-slate-300 font-medium">{value}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
