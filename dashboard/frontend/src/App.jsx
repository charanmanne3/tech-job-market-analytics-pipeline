import { useState, useEffect, useCallback, useRef } from "react";
import { fetchAirflowOverview, fetchDashboard, fetchFilters } from "./api";
import AirflowSection from "./components/AirflowSection";
import MetricCards from "./components/MetricCards";
import LocationCharts from "./components/LocationCharts";
import SkillsSection from "./components/SkillsSection";
import SalarySection from "./components/SalarySection";
import WorkModeSection from "./components/WorkModeSection";
import Sidebar from "./components/Sidebar";

export default function App() {
  const [data, setData] = useState(null);
  const [filters, setFilters] = useState(null);
  const [selectedLocations, setSelectedLocations] = useState([]);
  const [dateRange, setDateRange] = useState({ from: "", to: "" });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [airflow, setAirflow] = useState(null);
  const [airflowLoading, setAirflowLoading] = useState(true);
  const [airflowError, setAirflowError] = useState(null);
  const [airflowLastUpdated, setAirflowLastUpdated] = useState(null);
  const [airflowCooldownSeconds, setAirflowCooldownSeconds] = useState(0);
  const AIRFLOW_REFRESH_MS = 30_000;
  const AIRFLOW_MAX_FAILURES = 3;
  const AIRFLOW_COOLDOWN_MS = 120_000;
  const airflowFailureRef = useRef(0);
  const airflowPausedUntilRef = useRef(0);

  const loadData = useCallback(async () => {
    try {
      setLoading(true);
      const result = await fetchDashboard({
        locations: selectedLocations,
        dateFrom: dateRange.from,
        dateTo: dateRange.to,
      });
      if (result.error) {
        setError(result.error);
        setData(null);
      } else {
        setData(result);
        setError(null);
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [selectedLocations, dateRange]);

  useEffect(() => {
    fetchFilters().then(setFilters).catch(console.error);
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const loadAirflow = useCallback(async ({ manual = false } = {}) => {
    const now = Date.now();
    if (!manual && airflowPausedUntilRef.current > now) {
      const sec = Math.ceil((airflowPausedUntilRef.current - now) / 1000);
      setAirflowCooldownSeconds(sec);
      return;
    }

    try {
      setAirflowLoading(true);
      const result = await fetchAirflowOverview();
      if (result.configured && result.error) {
        const wrapped = new Error(result.error);
        wrapped.source = result.source || "proxy";
        throw wrapped;
      } else {
        setAirflowError(null);
      }
      setAirflow(result);
      setAirflowLastUpdated(new Date());
      setAirflowCooldownSeconds(0);
      airflowFailureRef.current = 0;
      airflowPausedUntilRef.current = 0;
    } catch (err) {
      airflowFailureRef.current += 1;
      setAirflow((prev) => ({
        ...(prev || {}),
        source: err?.source || prev?.source || "unknown",
      }));

      const baseMessage = err?.message
        ? `Airflow API is unreachable. ${err.message}`
        : "Airflow API is unreachable. Ensure the Airflow server is running and the API URL is correct.";
      if (airflowFailureRef.current >= AIRFLOW_MAX_FAILURES) {
        airflowPausedUntilRef.current = Date.now() + AIRFLOW_COOLDOWN_MS;
        setAirflowCooldownSeconds(Math.ceil(AIRFLOW_COOLDOWN_MS / 1000));
        setAirflowError(`${baseMessage} Auto-retry paused briefly to avoid spam.`);
      } else {
        setAirflowError(baseMessage);
      }
    } finally {
      setAirflowLoading(false);
    }
  }, []);

  useEffect(() => {
    loadAirflow();
  }, [loadAirflow]);

  useEffect(() => {
    const timer = setInterval(() => {
      const now = Date.now();
      if (airflowPausedUntilRef.current > now) {
        setAirflowCooldownSeconds(Math.ceil((airflowPausedUntilRef.current - now) / 1000));
      } else {
        setAirflowCooldownSeconds(0);
        loadAirflow();
      }
    }, AIRFLOW_REFRESH_MS);
    return () => clearInterval(timer);
  }, [loadAirflow]);

  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center px-4">
        <div className="bg-red-900/40 border border-red-800 rounded-2xl p-10 max-w-lg text-center">
          <p className="text-4xl mb-4">📊</p>
          <h2 className="text-xl font-bold text-red-400 mb-2">
            No data found
          </h2>
          <p className="text-slate-300 mb-4">{error}</p>
          <code className="block text-left bg-slate-900 rounded-lg p-4 text-sm text-slate-400">
            python data_ingestion/fetch_jobs.py
            <br />
            python transformations/clean_jobs.py
          </code>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-slate-950">
      <Sidebar
        open={sidebarOpen}
        onClose={() => setSidebarOpen(false)}
        filters={filters}
        selectedLocations={selectedLocations}
        onLocationsChange={setSelectedLocations}
        dateRange={dateRange}
        onDateRangeChange={setDateRange}
      />

      <div className="max-w-[1600px] mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <header className="mb-10">
          <div className="flex items-center justify-between">
            <button
              onClick={() => setSidebarOpen(true)}
              className="p-2.5 rounded-xl bg-slate-800/80 border border-slate-700 hover:bg-slate-700 transition"
              title="Filters"
            >
              <svg
                className="w-5 h-5"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z"
                />
              </svg>
            </button>
            <div className="text-center flex-1">
              <h1 className="text-3xl sm:text-4xl font-extrabold text-white tracking-tight">
                Tech Job Market Analytics
              </h1>
              <p className="text-slate-400 mt-1.5 text-sm sm:text-base">
                Real-time insights from tech job postings collected via public
                APIs
              </p>
            </div>
            <div className="w-11" />
          </div>
        </header>

        {loading ? (
          <div className="flex items-center justify-center py-40">
            <div className="animate-spin rounded-full h-14 w-14 border-[3px] border-slate-700 border-t-blue-500" />
          </div>
        ) : data ? (
          <div className="space-y-12">
            <MetricCards metrics={data.metrics} />
            <AirflowSection
              airflow={airflow}
              loading={airflowLoading}
              error={airflowError}
              onRefresh={() => loadAirflow({ manual: true })}
              lastUpdated={airflowLastUpdated}
              refreshMs={AIRFLOW_REFRESH_MS}
              cooldownSeconds={airflowCooldownSeconds}
            />
            <LocationCharts
              locations={data.top_locations}
              companies={data.top_companies}
              timeline={data.timeline}
            />
            <SkillsSection skills={data.skills} />
            <SalarySection salary={data.salary} />
            <WorkModeSection workMode={data.work_mode} />
          </div>
        ) : null}

        <footer className="mt-16 pt-6 border-t border-slate-800/60 text-center text-slate-600 text-sm pb-8">
          Tech Job Market Analytics Pipeline &middot; Built with React &
          Recharts
        </footer>
      </div>
    </div>
  );
}
