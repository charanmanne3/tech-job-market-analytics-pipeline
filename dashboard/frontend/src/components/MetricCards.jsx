const CARDS = [
  { key: "total_jobs", icon: "\uD83D\uDCBC", label: "Total Jobs", accent: "border-t-blue-500" },
  { key: "companies", icon: "\uD83C\uDFE2", label: "Companies", accent: "border-t-emerald-500" },
  { key: "locations", icon: "\uD83D\uDCCD", label: "Locations", accent: "border-t-violet-500" },
  { key: "avg_skills", icon: "\uD83D\uDEE0\uFE0F", label: "Avg Skills / Job", accent: "border-t-amber-500" },
  { key: "with_salary", icon: "\uD83D\uDCB0", label: "With Salary", accent: "border-t-rose-500" },
];

function fmt(v) {
  return typeof v === "number" ? v.toLocaleString() : v;
}

export default function MetricCards({ metrics }) {
  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">
      {CARDS.map(({ key, icon, label, accent }) => (
        <div
          key={key}
          className={`bg-gradient-to-br from-slate-800 to-slate-700/80 border border-slate-600/60 rounded-2xl
                      p-6 text-center shadow-lg hover:-translate-y-1 hover:shadow-xl
                      transition-all duration-200 border-t-4 ${accent}`}
        >
          <div className="text-2xl mb-2">{icon}</div>
          <div className="text-3xl font-extrabold text-white leading-tight">
            {fmt(metrics[key])}
          </div>
          <div className="text-[11px] font-semibold text-slate-400 uppercase tracking-widest mt-1.5">
            {label}
          </div>
        </div>
      ))}
    </div>
  );
}
