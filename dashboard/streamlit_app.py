"""
Streamlit Dashboard — Tech Job Market Analytics

Single-page scrollable layout:
  1 — Metric cards   (Total Jobs · Companies · Locations)
  2 — Jobs by Location
  3 — Top Skills Demand
  4 — Salary Distribution
  5 — Remote vs Onsite

Reads from data/processed/clean_jobs.parquet (falls back to CSV, then DB).

Usage:
    streamlit run dashboard/streamlit_app.py
"""

import sys
from itertools import combinations
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from config.config import CLEAN_CSV_PATH, DATABASE_URL, PROCESSED_PARQUET_PATH

# ─────────────────────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Tech Job Market Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────────────────────
# CSS — dark metric cards, full-width, spacing
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(
    """<style>
.block-container{padding:2rem 2.5rem;max-width:100%}
.metric-card{
    background:linear-gradient(135deg,#1e293b 0%,#334155 100%);
    border:1px solid #475569;border-radius:14px;
    padding:28px 20px;text-align:center;
    box-shadow:0 4px 16px rgba(0,0,0,.25);
    transition:transform .15s,box-shadow .15s}
.metric-card:hover{transform:translateY(-3px);box-shadow:0 8px 24px rgba(0,0,0,.35)}
.metric-card .mv{font-size:2.6rem;font-weight:800;color:#fff;line-height:1.1;margin-bottom:6px}
.metric-card .ml{font-size:.9rem;font-weight:500;color:#94a3b8;text-transform:uppercase;letter-spacing:.6px}
.metric-card .mi{font-size:1.6rem;margin-bottom:8px}
.metric-card.blue{border-top:4px solid #3b82f6}
.metric-card.green{border-top:4px solid #10b981}
.metric-card.purple{border-top:4px solid #8b5cf6}
.metric-card.amber{border-top:4px solid #f59e0b}
.metric-card.rose{border-top:4px solid #f43f5e}
.sh{font-size:1.5rem;font-weight:700;margin-top:2.5rem;margin-bottom:.2rem}
.ss{font-size:.95rem;color:#94a3b8;margin-bottom:1.2rem}
</style>""",
    unsafe_allow_html=True,
)


def mc(icon, val, label, accent="blue"):
    return f'<div class="metric-card {accent}"><div class="mi">{icon}</div><div class="mv">{val}</div><div class="ml">{label}</div></div>'


def sh(t, sub=""):
    st.markdown(f'<div class="sh">{t}</div>', unsafe_allow_html=True)
    if sub:
        st.markdown(f'<div class="ss">{sub}</div>', unsafe_allow_html=True)


DARK = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(size=13, color="#e2e8f0"),
    margin=dict(l=10, r=30, t=50, b=30),
)


# ─────────────────────────────────────────────────────────────────────────────
# Data loading — Parquet → CSV → DB fallback chain
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=600)
def load_data():
    if PROCESSED_PARQUET_PATH.exists():
        return pd.read_parquet(PROCESSED_PARQUET_PATH)
    if CLEAN_CSV_PATH.exists():
        return pd.read_csv(CLEAN_CSV_PATH)
    try:
        from sqlalchemy import create_engine
        from sqlalchemy import text as sa_text

        eng = create_engine(DATABASE_URL)
        with eng.connect() as c:
            r = pd.read_sql(sa_text("SELECT * FROM jobs"), c)
            if not r.empty:
                return r
    except Exception:
        pass
    return pd.DataFrame()


@st.cache_data(ttl=600)
def skill_ranks(frame):
    if "skills" not in frame.columns:
        return pd.DataFrame(columns=["Skill", "Demand"])
    s = frame["skills"].dropna().loc[lambda x: x != ""].str.split(", ").explode().str.strip().str.lower()
    c = s.value_counts().reset_index()
    c.columns = ["Skill", "Demand"]
    return c


df = load_data()
if df.empty:
    st.error(
        "**No data found.** Run the pipeline first:\n\n```bash\npython data_ingestion/fetch_jobs.py\npython transformations/clean_jobs.py\n```"
    )
    st.stop()

df["posted_date"] = pd.to_datetime(df["posted_date"], errors="coerce")
for c in ("salary_min", "salary_max", "skill_count"):
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")
if "is_remote" in df.columns:
    df["is_remote"] = df["is_remote"].astype(bool)

sk = skill_ranks(df)

# ── Sidebar filters ──────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## Filters")
    locs = st.multiselect("Location", sorted(df["location"].dropna().unique()), placeholder="All locations")
    vd = df["posted_date"].dropna()
    dr = None
    if not vd.empty:
        dr = st.date_input(
            "Posted between",
            value=(vd.min().date(), vd.max().date()),
            min_value=vd.min().date(),
            max_value=vd.max().date(),
        )
    st.markdown("---")
    st.caption("Tech Job Market Analytics v1.0")

fd = df.copy()
if locs:
    fd = fd[fd["location"].isin(locs)]
if dr and len(dr) == 2:
    fd = fd[fd["posted_date"].between(pd.Timestamp(dr[0]), pd.Timestamp(dr[1]))]
fsk = skill_ranks(fd)

n_jobs = len(fd)
n_comp = fd["company"].nunique()
n_locs = fd["location"].nunique()
n_sal = int(fd["salary_min"].notna().sum()) if "salary_min" in fd.columns else 0
avg_sk = fd["skill_count"].mean() if "skill_count" in fd.columns else 0

# ═════════════════════════════════════════════════════════════════════════════
#  SECTION 1 — HEADER + METRIC CARDS
# ═════════════════════════════════════════════════════════════════════════════
st.markdown(
    "<h1 style='text-align:center;margin-bottom:.2rem'>📊 Tech Job Market Analytics</h1>", unsafe_allow_html=True
)
st.markdown(
    "<p style='text-align:center;color:#94a3b8;margin-bottom:2rem'>Real-time insights from tech job postings collected via public APIs</p>",
    unsafe_allow_html=True,
)

c1, c2, c3, c4, c5 = st.columns(5)
c1.markdown(mc("💼", f"{n_jobs:,}", "Total Jobs", "blue"), unsafe_allow_html=True)
c2.markdown(mc("🏢", f"{n_comp:,}", "Companies", "green"), unsafe_allow_html=True)
c3.markdown(mc("📍", f"{n_locs:,}", "Locations", "purple"), unsafe_allow_html=True)
c4.markdown(mc("🛠", f"{avg_sk:.1f}", "Avg Skills / Job", "amber"), unsafe_allow_html=True)
c5.markdown(mc("💰", f"{n_sal:,}", "With Salary", "rose"), unsafe_allow_html=True)

# ═════════════════════════════════════════════════════════════════════════════
#  SECTION 2 — JOBS BY LOCATION
# ═════════════════════════════════════════════════════════════════════════════
sh("Jobs by Location", "Geographic distribution of collected postings")

cl, cr = st.columns(2, gap="large")
with cl:
    ld = fd["location"].value_counts().head(12).reset_index()
    ld.columns = ["Location", "Count"]
    fig = px.bar(
        ld, y="Location", x="Count", orientation="h", text="Count", color="Count", color_continuous_scale="Viridis"
    )
    fig.update_layout(
        **DARK, title="Top Locations", height=500, yaxis=dict(autorange="reversed"), xaxis_title="", yaxis_title=""
    )
    fig.update_traces(textposition="outside", textfont=dict(color="#e2e8f0", size=12))
    fig.update_coloraxes(showscale=False)
    st.plotly_chart(fig, width="stretch")

with cr:
    cd = fd["company"].value_counts().head(12).reset_index()
    cd.columns = ["Company", "Openings"]
    fig = px.bar(
        cd,
        y="Company",
        x="Openings",
        orientation="h",
        text="Openings",
        color="Openings",
        color_continuous_scale="Tealgrn",
    )
    fig.update_layout(
        **DARK,
        title="Top Hiring Companies",
        height=500,
        yaxis=dict(autorange="reversed"),
        xaxis_title="",
        yaxis_title="",
    )
    fig.update_traces(textposition="outside", textfont=dict(color="#e2e8f0", size=12))
    fig.update_coloraxes(showscale=False)
    st.plotly_chart(fig, width="stretch")

td = fd.dropna(subset=["posted_date"])
if not td.empty:
    day = td.groupby(td["posted_date"].dt.date).size().reset_index(name="Jobs")
    day.columns = ["Date", "Jobs"]
    fig = px.area(day, x="Date", y="Jobs", color_discrete_sequence=["#3b82f6"])
    fig.update_layout(**DARK, title="Posting Volume Over Time", height=320, xaxis_title="", yaxis_title="Jobs Posted")
    fig.update_traces(line_shape="spline", fill="tozeroy", opacity=0.75)
    st.plotly_chart(fig, width="stretch")

# ═════════════════════════════════════════════════════════════════════════════
#  SECTION 3 — TOP SKILLS
# ═════════════════════════════════════════════════════════════════════════════
sh("Most In-Demand Tech Skills", "Extracted via keyword matching across 55+ technologies")

if fsk.empty:
    st.info("No skills detected in the current selection.")
else:
    tn = st.slider("Skills to display", 5, min(40, len(fsk)), min(20, len(fsk)))
    ts = fsk.head(tn).copy()
    ts["Skill"] = ts["Skill"].str.title()
    lc, rc = st.columns([3, 2], gap="large")
    with lc:
        fig = px.bar(
            ts, y="Skill", x="Demand", orientation="h", text="Demand", color="Demand", color_continuous_scale="Sunset"
        )
        fig.update_layout(
            **DARK,
            title=f"Top {tn} Skills",
            height=max(480, tn * 32),
            yaxis=dict(autorange="reversed"),
            xaxis_title="Job Postings",
            yaxis_title="",
        )
        fig.update_traces(textposition="outside", textfont=dict(color="#e2e8f0", size=12))
        fig.update_coloraxes(showscale=False)
        st.plotly_chart(fig, width="stretch")
    with rc:
        st.markdown("#### Skill Rankings")
        disp = fsk.copy()
        disp["Skill"] = disp["Skill"].str.title()
        disp.index = range(1, len(disp) + 1)
        disp.index.name = "#"
        st.dataframe(disp, height=max(480, tn * 32), width="stretch")
        m1, m2 = st.columns(2)
        m1.markdown(mc("🔢", f"{int(fsk['Demand'].sum()):,}", "Total Mentions", "blue"), unsafe_allow_html=True)
        m2.markdown(mc("🧩", f"{len(fsk)}", "Unique Skills", "purple"), unsafe_allow_html=True)

    if "skills" in fd.columns:
        pc: dict[tuple, int] = {}
        for ss in fd["skills"].dropna():
            items = [s.strip().lower() for s in ss.split(",") if s.strip()]
            if len(items) >= 2:
                for a, b in combinations(sorted(set(items)), 2):
                    pc[(a, b)] = pc.get((a, b), 0) + 1
        if pc:
            st.markdown("#### Skill Co-occurrence")
            tp = sorted(pc.items(), key=lambda x: x[1], reverse=True)[:15]
            pdf = pd.DataFrame([(f"{a.title()} + {b.title()}", c) for (a, b), c in tp], columns=["Pair", "Count"])
            fig = px.bar(
                pdf, y="Pair", x="Count", orientation="h", text="Count", color="Count", color_continuous_scale="Purp"
            )
            fig.update_layout(
                **DARK, height=max(380, len(tp) * 35), yaxis=dict(autorange="reversed"), xaxis_title="", yaxis_title=""
            )
            fig.update_traces(textposition="outside", textfont=dict(color="#e2e8f0", size=12))
            fig.update_coloraxes(showscale=False)
            st.plotly_chart(fig, width="stretch")

# ═════════════════════════════════════════════════════════════════════════════
#  SECTION 4 — SALARY DISTRIBUTION
# ═════════════════════════════════════════════════════════════════════════════
sh("Salary Distribution", "Compensation data from postings that include salary information")

sdf = fd.copy()
for c in ("salary_min", "salary_max"):
    if c in sdf.columns:
        sdf[c] = pd.to_numeric(sdf[c], errors="coerce")
sdf = sdf.dropna(subset=["salary_min"])
sdf = sdf[sdf["salary_min"] > 0]

if sdf.empty:
    st.info("No valid salary data in the current selection.")
else:
    med_lo = sdf["salary_min"].median()
    med_hi = sdf["salary_max"].dropna().median() if sdf["salary_max"].notna().any() else 0
    s1, s2, s3, s4 = st.columns(4)
    s1.markdown(mc("📋", f"{len(sdf):,}", "Jobs with Salary", "blue"), unsafe_allow_html=True)
    s2.markdown(mc("📉", f"${med_lo:,.0f}", "Median Min", "green"), unsafe_allow_html=True)
    s3.markdown(mc("📈", f"${med_hi:,.0f}" if med_hi else "N/A", "Median Max", "purple"), unsafe_allow_html=True)
    s4.markdown(
        mc(
            "↔️",
            f"${sdf['salary_min'].min():,.0f}–${sdf[['salary_min', 'salary_max']].max().max():,.0f}",
            "Full Range",
            "amber",
        ),
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    hl, hr = st.columns(2, gap="large")
    with hl:
        fig = go.Figure()
        fig.add_trace(
            go.Histogram(x=sdf["salary_min"], name="Min Salary", marker_color="#3b82f6", opacity=0.8, nbinsx=12)
        )
        if sdf["salary_max"].notna().any():
            fig.add_trace(
                go.Histogram(
                    x=sdf["salary_max"].dropna(), name="Max Salary", marker_color="#f43f5e", opacity=0.6, nbinsx=12
                )
            )
        fig.update_layout(
            **DARK,
            title="Salary Distribution",
            barmode="overlay",
            xaxis_title="Annual Salary (USD)",
            yaxis_title="Jobs",
            height=480,
            legend=dict(
                yanchor="top",
                y=0.97,
                xanchor="right",
                x=0.97,
                bgcolor="rgba(30,41,59,.8)",
                bordercolor="#475569",
                borderwidth=1,
            ),
        )
        st.plotly_chart(fig, width="stretch")

    with hr:
        rr = sdf.dropna(subset=["salary_max"]).sort_values("salary_min", ascending=True).tail(15).copy()
        if not rr.empty:
            rr["label"] = rr["title"].str[:28] + " — " + rr["company"].str[:12]
            fig = go.Figure()
            for _, r in rr.iterrows():
                fig.add_trace(
                    go.Scatter(
                        x=[r["salary_min"], r["salary_max"]],
                        y=[r["label"], r["label"]],
                        mode="lines+markers",
                        marker=dict(size=10, color=["#3b82f6", "#f43f5e"]),
                        line=dict(color="#64748b", width=3),
                        showlegend=False,
                        hovertemplate=f"<b>{r['title']}</b><br>{r['company']}<br>${r['salary_min']:,.0f}–${r['salary_max']:,.0f}<extra></extra>",
                    )
                )
            fig.update_layout(
                **DARK, title="Salary Ranges", height=480, xaxis_title="Annual Salary (USD)", yaxis_title=""
            )
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("No postings with both min and max salary.")

    st.markdown("#### All Postings with Salary")
    sc = [c for c in ["title", "company", "location", "salary", "salary_min", "salary_max"] if c in sdf.columns]
    st.dataframe(sdf[sc].sort_values("salary_min", ascending=False), hide_index=True, width="stretch")

# ═════════════════════════════════════════════════════════════════════════════
#  SECTION 5 — REMOTE VS ONSITE
# ═════════════════════════════════════════════════════════════════════════════
sh("Remote vs Onsite", "Work-mode breakdown across collected postings")

if "work_mode" not in fd.columns:
    fd["work_mode"] = fd["location"].apply(
        lambda l: "Remote" if "remote" in str(l).lower() or "worldwide" in str(l).lower() else "Onsite"
    )

mode_counts = fd["work_mode"].value_counts().reset_index()
mode_counts.columns = ["Mode", "Count"]
gt = int(mode_counts["Count"].sum())

dl, dri = st.columns([3, 2], gap="large")
with dl:
    fig = px.pie(
        mode_counts,
        names="Mode",
        values="Count",
        color="Mode",
        color_discrete_map={"Remote": "#10b981", "Onsite": "#f43f5e"},
        hole=0.55,
    )
    fig.update_traces(
        textinfo="percent+label+value",
        textfont=dict(size=15, color="#fff"),
        pull=[0.04, 0.04],
        marker=dict(line=dict(color="#1e293b", width=2)),
    )
    fig.update_layout(**DARK, title="Work Mode Split", height=480, showlegend=False)
    st.plotly_chart(fig, width="stretch")

with dri:
    st.markdown("<br><br>", unsafe_allow_html=True)
    for _, r in mode_counts.iterrows():
        pct = r["Count"] / gt * 100
        ac = "green" if r["Mode"] == "Remote" else "rose"
        ic = "🌐" if r["Mode"] == "Remote" else "🏢"
        st.markdown(mc(ic, f"{r['Count']:,}", f"{r['Mode']} · {pct:.1f}%", ac), unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

ro = fd[fd["work_mode"] == "Remote"]
if not ro.empty:
    rl = ro["location"].value_counts().head(12).reset_index()
    rl.columns = ["Location", "Jobs"]
    fig = px.bar(rl, y="Location", x="Jobs", orientation="h", text="Jobs", color="Jobs", color_continuous_scale="Emrld")
    fig.update_layout(
        **DARK,
        title="Where Remote Jobs Are Posted",
        height=480,
        yaxis=dict(autorange="reversed"),
        xaxis_title="",
        yaxis_title="",
    )
    fig.update_traces(textposition="outside", textfont=dict(color="#e2e8f0", size=12))
    fig.update_coloraxes(showscale=False)
    st.plotly_chart(fig, width="stretch")

st.markdown(
    "<br><hr style='border-color:#334155'><p style='text-align:center;color:#64748b;font-size:.85rem'>Tech Job Market Analytics Pipeline · Built with Streamlit & Plotly</p>",
    unsafe_allow_html=True,
)
