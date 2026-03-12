import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  AreaChart,
  Area,
  ResponsiveContainer,
  Cell,
} from "recharts";
import SectionHeader from "./SectionHeader";

const BLUE = "#3b82f6";
const TEAL = "#14b8a6";

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

function HBar({ data, dataKey, nameKey, color, title }) {
  return (
    <div className="bg-slate-900/50 border border-slate-800 rounded-2xl p-5">
      <h3 className="text-sm font-semibold text-slate-300 mb-4">{title}</h3>
      <ResponsiveContainer width="100%" height={Math.max(400, data.length * 36)}>
        <BarChart
          data={data}
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
            dataKey={nameKey}
            width={150}
            tick={{ fill: "#e2e8f0", fontSize: 12 }}
            axisLine={false}
            tickLine={false}
          />
          <Tooltip
            content={<ChartTooltip />}
            cursor={{ fill: "rgba(148,163,184,0.06)" }}
          />
          <Bar
            dataKey={dataKey}
            radius={[0, 6, 6, 0]}
            label={{ position: "right", fill: "#e2e8f0", fontSize: 12 }}
          >
            {data.map((_, i) => (
              <Cell key={i} fill={color} fillOpacity={1 - i * 0.05} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

export default function LocationCharts({ locations, companies, timeline }) {
  return (
    <section>
      <SectionHeader
        title="Jobs by Location"
        subtitle="Geographic distribution of collected postings"
      />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <HBar
          data={locations}
          dataKey="count"
          nameKey="name"
          color={BLUE}
          title="Top Locations"
        />
        <HBar
          data={companies}
          dataKey="count"
          nameKey="name"
          color={TEAL}
          title="Top Hiring Companies"
        />
      </div>

      {timeline.length > 0 && (
        <div className="bg-slate-900/50 border border-slate-800 rounded-2xl p-5 mt-6">
          <h3 className="text-sm font-semibold text-slate-300 mb-4">
            Posting Volume Over Time
          </h3>
          <ResponsiveContainer width="100%" height={280}>
            <AreaChart
              data={timeline}
              margin={{ left: 0, right: 20, top: 10, bottom: 0 }}
            >
              <defs>
                <linearGradient id="areaBlue" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={BLUE} stopOpacity={0.35} />
                  <stop offset="100%" stopColor={BLUE} stopOpacity={0} />
                </linearGradient>
              </defs>
              <XAxis
                dataKey="date"
                tick={{ fill: "#94a3b8", fontSize: 11 }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                tick={{ fill: "#94a3b8", fontSize: 12 }}
                axisLine={false}
                tickLine={false}
              />
              <Tooltip content={<ChartTooltip />} />
              <Area
                type="monotone"
                dataKey="count"
                stroke={BLUE}
                fill="url(#areaBlue)"
                strokeWidth={2}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}
    </section>
  );
}
