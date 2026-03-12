import SectionHeader from "./SectionHeader";

function badgeClass(state) {
  const key = (state || "").toLowerCase();
  if (key === "success") return "bg-emerald-500/20 text-emerald-300 border-emerald-500/40";
  if (key === "failed") return "bg-rose-500/20 text-rose-300 border-rose-500/40";
  if (key === "running") return "bg-blue-500/20 text-blue-300 border-blue-500/40";
  return "bg-slate-700/40 text-slate-300 border-slate-600/50";
}

function sourceBadgeClass(source) {
  if (source === "direct") return "bg-emerald-500/20 text-emerald-300 border-emerald-500/40";
  if (source === "proxy") return "bg-violet-500/20 text-violet-300 border-violet-500/40";
  return "bg-slate-700/40 text-slate-300 border-slate-600/50";
}

export default function AirflowSection({
  airflow,
  loading,
  error,
  onRefresh,
  lastUpdated,
  refreshMs = 30000,
  cooldownSeconds = 0,
}) {
  const refreshedLabel = lastUpdated ? new Date(lastUpdated).toLocaleTimeString() : "never";
  const refreshSeconds = Math.round((refreshMs || 0) / 1000);

  return (
    <section className="bg-slate-900/60 border border-slate-800 rounded-2xl p-6">
      <div className="flex items-start justify-between gap-4 mb-5">
        <SectionHeader
          title="Airflow Pipeline Status"
          subtitle={`Live DAG metadata and recent run states from Airflow API · Auto-refresh ${refreshSeconds}s · Last updated ${refreshedLabel}`}
        />
        <div className="flex items-center gap-2">
          <span className={`px-2 py-1 rounded border text-xs ${sourceBadgeClass(airflow?.source)}`}>
            Source: {airflow?.source || "unknown"}
          </span>
          <button
            onClick={onRefresh}
            className="px-3 py-2 rounded-lg text-sm bg-slate-800 border border-slate-700 hover:bg-slate-700 transition"
          >
            Refresh
          </button>
        </div>
      </div>

      {loading ? (
        <div className="text-slate-400 text-sm">Loading Airflow status...</div>
      ) : error ? (
        <div className="text-rose-300 text-sm bg-rose-500/10 border border-rose-500/30 rounded-lg p-3">
          {error}
          {cooldownSeconds > 0 ? (
            <div className="mt-1 text-xs text-rose-200/90">Retrying automatically in ~{cooldownSeconds}s.</div>
          ) : null}
        </div>
      ) : airflow && airflow.reachable ? (
        <div className="space-y-5">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <div className="bg-slate-800/60 rounded-xl p-3 border border-slate-700">
              <div className="text-xs text-slate-400 uppercase tracking-wider">DAG</div>
              <div className="text-sm font-semibold mt-1">{airflow.dag?.dag_id || "-"}</div>
            </div>
            <div className="bg-slate-800/60 rounded-xl p-3 border border-slate-700">
              <div className="text-xs text-slate-400 uppercase tracking-wider">Active</div>
              <div className="text-sm font-semibold mt-1">{airflow.dag?.is_active ? "Yes" : "No"}</div>
            </div>
            <div className="bg-slate-800/60 rounded-xl p-3 border border-slate-700">
              <div className="text-xs text-slate-400 uppercase tracking-wider">Paused</div>
              <div className="text-sm font-semibold mt-1">{airflow.dag?.is_paused ? "Yes" : "No"}</div>
            </div>
            <div className="bg-slate-800/60 rounded-xl p-3 border border-slate-700">
              <div className="text-xs text-slate-400 uppercase tracking-wider">Tasks</div>
              <div className="text-sm font-semibold mt-1">{airflow.task_count ?? 0}</div>
            </div>
          </div>

          <div>
            <p className="text-sm text-slate-300 mb-2">Recent Runs</p>
            <div className="space-y-2">
              {(airflow.recent_runs || []).length === 0 ? (
                <div className="text-slate-400 text-sm">No runs found.</div>
              ) : (
                airflow.recent_runs.map((run) => (
                  <div
                    key={run.dag_run_id}
                    className="flex flex-wrap items-center justify-between gap-2 bg-slate-800/50 border border-slate-700 rounded-lg px-3 py-2"
                  >
                    <div className="text-sm text-slate-200">{run.dag_run_id}</div>
                    <div className="flex items-center gap-2 text-xs">
                      <span className={`px-2 py-1 rounded border ${badgeClass(run.state)}`}>{run.state || "unknown"}</span>
                      <span className="text-slate-400">
                        {run.duration_seconds == null ? "-" : `${run.duration_seconds}s`}
                      </span>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      ) : (
        <div className="text-slate-400 text-sm">
          {airflow?.configured === false
            ? "Airflow not configured for this deployment. App works without it."
            : airflow?.error || "Airflow is not reachable from this environment."}
        </div>
      )}
    </section>
  );
}
