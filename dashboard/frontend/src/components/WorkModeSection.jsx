import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  ResponsiveContainer,
} from "recharts";
import SectionHeader from "./SectionHeader";

const MODE_COLORS = { Remote: "#10b981", Onsite: "#f43f5e" };
const MODE_ICONS = { Remote: "\uD83C\uDF10", Onsite: "\uD83C\uDFE2" };

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-slate-800 border border-slate-600 rounded-lg px-3 py-2 text-sm shadow-xl">
      <p className="text-slate-300">{label || payload[0].name}</p>
      <p className="font-bold text-white">
        {payload[0].value.toLocaleString()}
      </p>
    </div>
  );
}

const RADIAN = Math.PI / 180;
function renderLabel({ cx, cy, midAngle, outerRadius, mode, count, percent }) {
  const x = cx + (outerRadius + 30) * Math.cos(-midAngle * RADIAN);
  const y = cy + (outerRadius + 30) * Math.sin(-midAngle * RADIAN);
  return (
    <text
      x={x}
      y={y}
      fill="#e2e8f0"
      textAnchor={x > cx ? "start" : "end"}
      dominantBaseline="central"
      fontSize={13}
      fontWeight={600}
    >
      {`${mode} \u00B7 ${count.toLocaleString()} (${(percent * 100).toFixed(1)}%)`}
    </text>
  );
}

export default function WorkModeSection({ workMode }) {
  const { split, remote_locations } = workMode;
  const total = split.reduce((s, d) => s + d.count, 0);

  return (
    <section>
      <SectionHeader
        title="Remote vs Onsite"
        subtitle="Work-mode breakdown across collected postings"
      />

      <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">
        <div className="lg:col-span-3 bg-slate-900/50 border border-slate-800 rounded-2xl p-5">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Work Mode Split
          </h3>
          <ResponsiveContainer width="100%" height={400}>
            <PieChart>
              <Pie
                data={split}
                dataKey="count"
                nameKey="mode"
                cx="50%"
                cy="50%"
                innerRadius="50%"
                outerRadius="75%"
                paddingAngle={4}
                label={renderLabel}
                labelLine={{ stroke: "#64748b" }}
              >
                {split.map((s) => (
                  <Cell
                    key={s.mode}
                    fill={MODE_COLORS[s.mode] || "#6366f1"}
                    stroke="#1e293b"
                    strokeWidth={2}
                  />
                ))}
              </Pie>
              <Tooltip content={<ChartTooltip />} />
            </PieChart>
          </ResponsiveContainer>
        </div>

        <div className="lg:col-span-2 flex flex-col justify-center gap-4">
          {split.map((s) => {
            const pct = ((s.count / total) * 100).toFixed(1);
            const accent =
              s.mode === "Remote"
                ? "border-t-emerald-500"
                : "border-t-rose-500";
            return (
              <div
                key={s.mode}
                className={`bg-gradient-to-br from-slate-800 to-slate-700/80 border border-slate-600/60
                            rounded-2xl p-7 text-center border-t-4 ${accent}`}
              >
                <div className="text-3xl mb-2">
                  {MODE_ICONS[s.mode] || "\uD83D\uDCCA"}
                </div>
                <div className="text-3xl font-extrabold text-white">
                  {s.count.toLocaleString()}
                </div>
                <div className="text-sm text-slate-400 mt-1">
                  {s.mode} &middot; {pct}%
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {remote_locations.length > 0 && (
        <div className="bg-slate-900/50 border border-slate-800 rounded-2xl p-5 mt-6">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Where Remote Jobs Are Posted
          </h3>
          <ResponsiveContainer
            width="100%"
            height={Math.max(400, remote_locations.length * 36)}
          >
            <BarChart
              data={remote_locations}
              layout="vertical"
              margin={{ left: 10, right: 50, top: 0, bottom: 0 }}
            >
              <XAxis
                type="number"
                tick={{ fill: "#94a3b8", fontSize: 12 }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                type="category"
                dataKey="name"
                width={160}
                tick={{ fill: "#e2e8f0", fontSize: 12 }}
                axisLine={false}
                tickLine={false}
              />
              <Tooltip
                content={<ChartTooltip />}
                cursor={{ fill: "rgba(148,163,184,0.06)" }}
              />
              <Bar
                dataKey="count"
                radius={[0, 6, 6, 0]}
                fill="#10b981"
                label={{ position: "right", fill: "#e2e8f0", fontSize: 12 }}
              >
                {remote_locations.map((_, i) => (
                  <Cell
                    key={i}
                    fill="#10b981"
                    fillOpacity={1 - i * 0.05}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </section>
  );
}
