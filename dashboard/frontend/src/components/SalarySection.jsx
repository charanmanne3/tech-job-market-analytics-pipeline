import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import SectionHeader from "./SectionHeader";

const BLUE = "#3b82f6";
const ROSE = "#f43f5e";

function fmtSalary(n) {
  if (!n && n !== 0) return "N/A";
  return `$${Math.round(n).toLocaleString()}`;
}

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-slate-800 border border-slate-600 rounded-lg px-3 py-2 text-sm shadow-xl">
      <p className="text-slate-300 mb-1">{label}</p>
      {payload.map((p, i) => (
        <p key={i} style={{ color: p.color }} className="font-medium">
          {p.name}: {p.value}
        </p>
      ))}
    </div>
  );
}

export default function SalarySection({ salary }) {
  const { metrics, histogram, ranges, table } = salary;

  if (!metrics?.count) {
    return (
      <section>
        <SectionHeader
          title="Salary Distribution"
          subtitle="Compensation data from postings that include salary information"
        />
        <div className="bg-slate-900/50 border border-slate-800 rounded-2xl p-10 text-center text-slate-400">
          No valid salary data in the current selection.
        </div>
      </section>
    );
  }

  const cards = [
    { icon: "\uD83D\uDCCB", value: metrics.count.toLocaleString(), label: "Jobs with Salary", accent: "border-t-blue-500" },
    { icon: "\uD83D\uDCC9", value: fmtSalary(metrics.median_min), label: "Median Min", accent: "border-t-emerald-500" },
    { icon: "\uD83D\uDCC8", value: fmtSalary(metrics.median_max), label: "Median Max", accent: "border-t-violet-500" },
    { icon: "\u2194\uFE0F", value: `${fmtSalary(metrics.full_min)}\u2013${fmtSalary(metrics.full_max)}`, label: "Full Range", accent: "border-t-amber-500" },
  ];

  return (
    <section>
      <SectionHeader
        title="Salary Distribution"
        subtitle="Compensation data from postings that include salary information"
      />

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-6">
        {cards.map((c) => (
          <div
            key={c.label}
            className={`bg-gradient-to-br from-slate-800 to-slate-700/80 border border-slate-600/60
                        rounded-2xl p-5 text-center border-t-4 ${c.accent}`}
          >
            <div className="text-xl mb-1">{c.icon}</div>
            <div className="text-lg font-extrabold text-white">{c.value}</div>
            <div className="text-[10px] text-slate-400 uppercase tracking-widest mt-1">
              {c.label}
            </div>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {histogram.length > 0 && (
          <div className="bg-slate-900/50 border border-slate-800 rounded-2xl p-5">
            <h3 className="text-sm font-semibold text-slate-300 mb-4">
              Salary Distribution
            </h3>
            <ResponsiveContainer width="100%" height={420}>
              <BarChart
                data={histogram}
                margin={{ left: 10, right: 20, top: 10, bottom: 60 }}
              >
                <XAxis
                  dataKey="range"
                  tick={{ fill: "#94a3b8", fontSize: 10 }}
                  axisLine={false}
                  tickLine={false}
                  angle={-35}
                  textAnchor="end"
                />
                <YAxis
                  tick={{ fill: "#94a3b8", fontSize: 12 }}
                  axisLine={false}
                  tickLine={false}
                />
                <Tooltip content={<ChartTooltip />} />
                <Legend
                  wrapperStyle={{ fontSize: 12, color: "#e2e8f0" }}
                  iconType="square"
                />
                <Bar
                  dataKey="min_salary"
                  name="Min Salary"
                  fill={BLUE}
                  fillOpacity={0.85}
                  radius={[4, 4, 0, 0]}
                />
                <Bar
                  dataKey="max_salary"
                  name="Max Salary"
                  fill={ROSE}
                  fillOpacity={0.65}
                  radius={[4, 4, 0, 0]}
                />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {ranges.length > 0 && (
          <div className="bg-slate-900/50 border border-slate-800 rounded-2xl p-5">
            <h3 className="text-sm font-semibold text-slate-300 mb-4">
              Salary Ranges
            </h3>
            <div className="space-y-3 overflow-y-auto" style={{ maxHeight: 420 }}>
              {ranges.map((r, i) => {
                const span = metrics.full_max - metrics.full_min;
                const leftPct = ((r.min - metrics.full_min) / span) * 100;
                const widthPct = ((r.max - r.min) / span) * 100;
                return (
                  <div key={i}>
                    <div className="text-xs text-slate-400 mb-1 truncate">
                      {r.label}
                    </div>
                    <div className="relative h-4 bg-slate-800 rounded-full overflow-hidden">
                      <div
                        className="absolute h-full bg-gradient-to-r from-blue-500 to-rose-500 rounded-full"
                        style={{
                          left: `${leftPct}%`,
                          width: `${Math.max(widthPct, 1.5)}%`,
                        }}
                      />
                    </div>
                    <div className="flex justify-between text-[11px] text-slate-500 mt-0.5">
                      <span>{fmtSalary(r.min)}</span>
                      <span>{fmtSalary(r.max)}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>

      {table.length > 0 && (
        <div className="bg-slate-900/50 border border-slate-800 rounded-2xl overflow-hidden mt-6">
          <h3 className="text-sm font-semibold text-slate-300 p-5 pb-3">
            All Postings with Salary
          </h3>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-slate-800/80">
                <tr>
                  <th className="text-left px-4 py-2.5 text-slate-400 font-medium">
                    Title
                  </th>
                  <th className="text-left px-4 py-2.5 text-slate-400 font-medium">
                    Company
                  </th>
                  <th className="text-left px-4 py-2.5 text-slate-400 font-medium">
                    Location
                  </th>
                  <th className="text-right px-4 py-2.5 text-slate-400 font-medium">
                    Min Salary
                  </th>
                  <th className="text-right px-4 py-2.5 text-slate-400 font-medium">
                    Max Salary
                  </th>
                </tr>
              </thead>
              <tbody>
                {table.map((row, i) => (
                  <tr
                    key={i}
                    className="border-t border-slate-800/60 hover:bg-slate-800/40 transition"
                  >
                    <td className="px-4 py-2 text-slate-200 max-w-[220px] truncate">
                      {row.title}
                    </td>
                    <td className="px-4 py-2 text-slate-300">{row.company}</td>
                    <td className="px-4 py-2 text-slate-400">
                      {row.location}
                    </td>
                    <td className="px-4 py-2 text-right text-white font-medium">
                      {fmtSalary(row.salary_min)}
                    </td>
                    <td className="px-4 py-2 text-right text-white font-medium">
                      {fmtSalary(row.salary_max)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </section>
  );
}
