import { useState } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";
import SectionHeader from "./SectionHeader";

const SUNSET = [
  "#f97316", "#f59e0b", "#eab308", "#84cc16", "#22c55e",
  "#14b8a6", "#06b6d4", "#3b82f6", "#6366f1", "#8b5cf6",
  "#a855f7", "#d946ef",
];

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-slate-800 border border-slate-600 rounded-lg px-3 py-2 text-sm shadow-xl">
      <p className="text-slate-300">{label}</p>
      <p className="font-bold text-white">
        {payload[0].value.toLocaleString()}
      </p>
    </div>
  );
}

export default function SkillsSection({ skills }) {
  const maxN = Math.min(40, skills.rankings.length);
  const [topN, setTopN] = useState(Math.min(20, maxN));

  if (!skills.rankings.length) {
    return (
      <section>
        <SectionHeader
          title="Most In-Demand Tech Skills"
          subtitle="Extracted via keyword matching across 55+ technologies"
        />
        <div className="bg-slate-900/50 border border-slate-800 rounded-2xl p-10 text-center text-slate-400">
          No skills detected in the current selection.
        </div>
      </section>
    );
  }

  const displayed = skills.rankings.slice(0, topN);
  const chartH = Math.max(480, topN * 32);

  return (
    <section>
      <SectionHeader
        title="Most In-Demand Tech Skills"
        subtitle="Extracted via keyword matching across 55+ technologies"
      />

      <div className="flex items-center gap-4 mb-5">
        <label className="text-sm text-slate-400">Skills to display:</label>
        <input
          type="range"
          min={5}
          max={maxN}
          value={topN}
          onChange={(e) => setTopN(Number(e.target.value))}
          className="w-48"
        />
        <span className="text-sm font-semibold text-white w-6 text-right">
          {topN}
        </span>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">
        <div className="lg:col-span-3 bg-slate-900/50 border border-slate-800 rounded-2xl p-5">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Top {topN} Skills
          </h3>
          <ResponsiveContainer width="100%" height={chartH}>
            <BarChart
              data={displayed}
              layout="vertical"
              margin={{ left: 10, right: 55, top: 0, bottom: 0 }}
            >
              <XAxis
                type="number"
                tick={{ fill: "#94a3b8", fontSize: 12 }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                type="category"
                dataKey="skill"
                width={110}
                tick={{ fill: "#e2e8f0", fontSize: 12 }}
                axisLine={false}
                tickLine={false}
              />
              <Tooltip
                content={<ChartTooltip />}
                cursor={{ fill: "rgba(148,163,184,0.06)" }}
              />
              <Bar
                dataKey="demand"
                radius={[0, 6, 6, 0]}
                label={{ position: "right", fill: "#e2e8f0", fontSize: 12 }}
              >
                {displayed.map((_, i) => (
                  <Cell key={i} fill={SUNSET[i % SUNSET.length]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="lg:col-span-2 space-y-4">
          <h3 className="text-sm font-semibold text-slate-300">
            Skill Rankings
          </h3>
          <div
            className="bg-slate-900/50 border border-slate-800 rounded-2xl overflow-hidden"
            style={{ maxHeight: chartH }}
          >
            <div
              className="overflow-y-auto"
              style={{ maxHeight: chartH - 8 }}
            >
              <table className="w-full text-sm">
                <thead className="bg-slate-800/80 sticky top-0">
                  <tr>
                    <th className="text-left px-4 py-2.5 text-slate-400 font-medium w-10">
                      #
                    </th>
                    <th className="text-left px-4 py-2.5 text-slate-400 font-medium">
                      Skill
                    </th>
                    <th className="text-right px-4 py-2.5 text-slate-400 font-medium">
                      Demand
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {skills.rankings.map((s, i) => (
                    <tr
                      key={s.skill}
                      className="border-t border-slate-800/60 hover:bg-slate-800/40 transition"
                    >
                      <td className="px-4 py-2 text-slate-500">{i + 1}</td>
                      <td className="px-4 py-2 text-slate-200">{s.skill}</td>
                      <td className="px-4 py-2 text-right text-white font-medium">
                        {s.demand.toLocaleString()}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div className="bg-gradient-to-br from-slate-800 to-slate-700/80 border border-slate-600/60 rounded-2xl p-4 text-center border-t-4 border-t-blue-500">
              <div className="text-lg mb-1">{"\uD83D\uDD22"}</div>
              <div className="text-xl font-extrabold text-white">
                {skills.total_mentions.toLocaleString()}
              </div>
              <div className="text-[10px] text-slate-400 uppercase tracking-widest mt-1">
                Total Mentions
              </div>
            </div>
            <div className="bg-gradient-to-br from-slate-800 to-slate-700/80 border border-slate-600/60 rounded-2xl p-4 text-center border-t-4 border-t-violet-500">
              <div className="text-lg mb-1">{"\uD83E\uDDE9"}</div>
              <div className="text-xl font-extrabold text-white">
                {skills.unique_skills}
              </div>
              <div className="text-[10px] text-slate-400 uppercase tracking-widest mt-1">
                Unique Skills
              </div>
            </div>
          </div>
        </div>
      </div>

      {skills.cooccurrence.length > 0 && (
        <div className="bg-slate-900/50 border border-slate-800 rounded-2xl p-5 mt-6">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Skill Co-occurrence
          </h3>
          <ResponsiveContainer
            width="100%"
            height={Math.max(380, skills.cooccurrence.length * 35)}
          >
            <BarChart
              data={skills.cooccurrence}
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
                dataKey="pair"
                width={180}
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
                label={{ position: "right", fill: "#e2e8f0", fontSize: 12 }}
              >
                {skills.cooccurrence.map((_, i) => (
                  <Cell
                    key={i}
                    fill="#a855f7"
                    fillOpacity={1 - i * 0.04}
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
