import { useInsights } from '@/hooks/useMetrics';

function formatTimestamp(iso: string): string {
  return new Date(iso).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export function InsightDigest() {
  const { data, isLoading, isError } = useInsights();

  const insights = data?.insights ?? [];
  const englishInsight = insights.find((i) => i.language === 'en') ?? insights[0];

  return (
    <section className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
      <div className="flex items-center gap-2 mb-4">
        <div className="h-2 w-2 rounded-full bg-brand-500 animate-pulse" />
        <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider">
          AI Insight Digest
        </h2>
      </div>

      {isLoading && (
        <div className="space-y-3 animate-pulse">
          <div className="h-4 w-3/4 rounded bg-slate-700" />
          <div className="h-4 w-1/2 rounded bg-slate-700" />
        </div>
      )}

      {isError && (
        <p className="text-sm text-slate-500">
          Unable to load insights. The API may be unavailable.
        </p>
      )}

      {!isLoading && !isError && !englishInsight && (
        <div className="space-y-3 text-sm text-slate-400">
          <p>
            No insights available yet. The InsightAgent will generate daily
            summaries of key economic indicators once data is available.
          </p>
          <div className="flex items-center gap-2 text-xs text-slate-600">
            <span className="inline-block h-1.5 w-1.5 rounded-full bg-slate-600" />
            Powered by Claude Sonnet
          </div>
        </div>
      )}

      {!isLoading && !isError && englishInsight && (
        <div className="space-y-3">
          <p className="text-sm text-slate-300 leading-relaxed whitespace-pre-line">
            {englishInsight.content}
          </p>

          <div className="flex flex-wrap items-center gap-3 text-xs text-slate-500">
            {englishInsight.confidence_flag ? (
              <span className="inline-flex items-center gap-1 rounded-full bg-emerald-500/20 text-emerald-400 border border-emerald-500/30 px-2 py-0.5 font-semibold uppercase tracking-wider text-[10px]">
                High confidence
              </span>
            ) : (
              <span className="inline-flex items-center gap-1 rounded-full bg-yellow-500/20 text-yellow-400 border border-yellow-500/30 px-2 py-0.5 font-semibold uppercase tracking-wider text-[10px]">
                Low confidence
              </span>
            )}

            <span>{formatTimestamp(englishInsight.generated_at)}</span>

            {englishInsight.metric_refs.length > 0 && (
              <span className="text-slate-600">
                Refs: {englishInsight.metric_refs.join(', ')}
              </span>
            )}
          </div>

          <div className="flex items-center gap-2 text-xs text-slate-600">
            <span className="inline-block h-1.5 w-1.5 rounded-full bg-slate-600" />
            Powered by {englishInsight.model_version}
          </div>
        </div>
      )}
    </section>
  );
}
