const BASE = "/api";
const AIRFLOW_API =
  import.meta.env.NEXT_PUBLIC_AIRFLOW_API_URL ||
  import.meta.env.VITE_AIRFLOW_API_URL ||
  "";

export async function fetchDashboard({ locations = [], dateFrom, dateTo } = {}) {
  const url = new URL(`${BASE}/dashboard`, window.location.origin);
  locations.forEach((l) => url.searchParams.append("locations", l));
  if (dateFrom) url.searchParams.set("date_from", dateFrom);
  if (dateTo) url.searchParams.set("date_to", dateTo);

  const res = await fetch(url);
  if (!res.ok) throw new Error("Failed to fetch dashboard data");
  return res.json();
}

export async function fetchFilters() {
  const res = await fetch(`${BASE}/filters`);
  if (!res.ok) throw new Error("Failed to fetch filters");
  return res.json();
}

export async function fetchAirflowOverview({ dagId = "job_market_pipeline", runsLimit = 5 } = {}) {
  // If a public Airflow URL is configured, query it directly.
  if (AIRFLOW_API) {
    return fetchAirflowOverviewDirect({ dagId, runsLimit });
  }

  // Otherwise, use backend proxy (works for local Docker).
  return fetchAirflowOverviewProxy({ dagId, runsLimit });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJsonWithRetry(url, { retries = 2, retryDelayMs = 800 } = {}) {
  let lastErr = null;
  for (let i = 0; i <= retries; i += 1) {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      lastErr = err;
      if (i < retries) await sleep(retryDelayMs * (i + 1));
    }
  }
  throw lastErr || new Error("Unknown request error");
}

async function fetchAirflowOverviewDirect({ dagId, runsLimit }) {
  const base = AIRFLOW_API.replace(/\/+$/, "");
  try {
    const [dag, runs, tasks] = await Promise.all([
      fetchJsonWithRetry(`${base}/dags/${encodeURIComponent(dagId)}`),
      fetchJsonWithRetry(
        `${base}/dags/${encodeURIComponent(
          dagId,
        )}/dagRuns?order_by=-start_date&limit=${encodeURIComponent(String(runsLimit))}`,
      ),
      fetchJsonWithRetry(`${base}/dags/${encodeURIComponent(dagId)}/tasks`),
    ]);

    const dagRuns = runs?.dag_runs || [];
    const runSummary = dagRuns.reduce((acc, run) => {
      const state = run?.state || "unknown";
      acc[state] = (acc[state] || 0) + 1;
      return acc;
    }, {});

    return {
      reachable: true,
      configured: true,
      dag: {
        dag_id: dag?.dag_id || dagId,
        is_paused: dag?.is_paused,
        is_active: dag?.is_active,
        description: dag?.description,
        owners: dag?.owners || [],
        tags: (dag?.tags || []).map((tag) => (typeof tag === "string" ? tag : tag?.name)).filter(Boolean),
      },
      run_summary: runSummary,
      recent_runs: dagRuns.map((run) => ({
        dag_run_id: run?.dag_run_id,
        state: run?.state,
        run_type: run?.run_type,
        logical_date: run?.logical_date,
        start_date: run?.start_date,
        end_date: run?.end_date,
        duration_seconds:
          run?.start_date && run?.end_date
            ? (new Date(run.end_date).getTime() - new Date(run.start_date).getTime()) / 1000
            : null,
      })),
      task_count: (tasks?.tasks || []).length,
      task_ids: (tasks?.tasks || []).map((task) => task?.task_id).filter(Boolean),
      source: "direct",
    };
  } catch (_err) {
    // Fallback to backend proxy if direct mode fails (CORS, network, etc.).
    return fetchAirflowOverviewProxy({ dagId, runsLimit });
  }
}

async function fetchAirflowOverviewProxy({ dagId, runsLimit }) {
  const url = new URL(`${BASE}/airflow/overview`, window.location.origin);
  url.searchParams.set("dag_id", dagId);
  url.searchParams.set("runs_limit", String(runsLimit));
  const payload = await fetchJsonWithRetry(url, { retries: 1, retryDelayMs: 500 });
  return { ...payload, source: "proxy" };
}
