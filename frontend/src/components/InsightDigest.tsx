export function InsightDigest() {
  return (
    <section className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
      <div className="flex items-center gap-2 mb-4">
        <div className="h-2 w-2 rounded-full bg-brand-500 animate-pulse" />
        <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider">
          AI Insight Digest
        </h2>
      </div>

      <div className="space-y-3 text-sm text-slate-400">
        <p>
          Weekly economic commentary will appear here once the InsightAgent is
          enabled in Session 3. The agent analyzes gold-layer data daily and
          produces plain-language summaries of key economic indicators.
        </p>
        <div className="flex items-center gap-2 text-xs text-slate-600">
          <span className="inline-block h-1.5 w-1.5 rounded-full bg-slate-600" />
          Powered by Claude Sonnet
        </div>
      </div>
    </section>
  );
}
