"""
FastAPI backend for the Tech Job Market Analytics dashboard.

Serves pre-computed chart data from Parquet / CSV / PostgreSQL and,
in production, the React static build.
"""

import base64
import json
import os
import sys
from datetime import datetime
from itertools import combinations
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.config import CLEAN_CSV_PATH, DATABASE_URL, PROCESSED_PARQUET_PATH

app = FastAPI(title="Tech Job Market Analytics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Data layer ────────────────────────────────────────────────────────────────

_cached_df: pd.DataFrame | None = None


def _load_data() -> pd.DataFrame:
    """Parquet -> CSV -> PostgreSQL fallback chain."""
    if PROCESSED_PARQUET_PATH.exists():
        try:
            return pd.read_parquet(PROCESSED_PARQUET_PATH)
        except ImportError:
            pass
    if CLEAN_CSV_PATH.exists():
        return pd.read_csv(CLEAN_CSV_PATH)
    try:
        from sqlalchemy import create_engine, text

        eng = create_engine(DATABASE_URL)
        with eng.connect() as conn:
            df = pd.read_sql(text("SELECT * FROM jobs"), conn)
            if not df.empty:
                return df
    except Exception:
        pass
    return pd.DataFrame()


def _get_data() -> pd.DataFrame:
    global _cached_df
    if _cached_df is None:
        _cached_df = _load_data()
    return _cached_df.copy()


def _normalize_airflow_url(raw: str) -> str:
    cleaned = raw.strip().rstrip("/")
    if cleaned.endswith("/api/v1"):
        return cleaned
    return f"{cleaned}/api/v1"


def _airflow_base_urls() -> list[str]:
    explicit = os.getenv("AIRFLOW_API_BASE_URL", "").strip()
    urls: list[str] = []
    if explicit:
        urls.append(_normalize_airflow_url(explicit))
    # Local developer default:
    # if no env var is set, use local Airflow endpoint during non-production runs.
    if not os.getenv("VERCEL") and os.getenv("ENV", "").lower() != "production":
        local_default = "http://localhost:8080/api/v1"
        if local_default not in urls:
            urls.append(local_default)
    # In hosted/prod contexts, keep Airflow integration opt-in.
    return urls


def _airflow_request(path: str, params: dict | None = None) -> dict:
    username = os.getenv("AIRFLOW_USERNAME", "").strip()
    password = os.getenv("AIRFLOW_PASSWORD", "").strip()
    if not username and not os.getenv("VERCEL") and os.getenv("ENV", "").lower() != "production":
        username = "admin"
        password = password or "admin"
    timeout = int(os.getenv("AIRFLOW_TIMEOUT_SECONDS", "10"))
    errors: list[str] = []

    for base in _airflow_base_urls():
        url = f"{base}/{path.lstrip('/')}"
        if params:
            url = f"{url}?{urlencode(params, doseq=True)}"

        req = Request(url)
        req.add_header("User-Agent", "Mozilla/5.0 (compatible; TechJobMarketBot/1.0)")
        req.add_header("Accept", "application/json, text/plain, */*")
        if username:
            token = base64.b64encode(f"{username}:{password}".encode()).decode("utf-8")
            req.add_header("Authorization", f"Basic {token}")

        try:
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{base}: {exc}")

    return {"_error": " | ".join(errors)}


def _duration_seconds(start_date: str | None, end_date: str | None) -> float | None:
    if not start_date or not end_date:
        return None
    try:
        start = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
        end = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        return round((end - start).total_seconds(), 1)
    except ValueError:
        return None


@app.post("/api/reload")
def reload_data():
    global _cached_df
    _cached_df = None
    return {"status": "ok"}


@app.get("/api/airflow/health")
def airflow_health():
    if not _airflow_base_urls():
        return {
            "reachable": False,
            "configured": False,
            "message": "Airflow integration not configured. Set AIRFLOW_API_BASE_URL to enable.",
        }
    payload = _airflow_request("/health")
    if "_error" in payload:
        return {"reachable": False, "configured": True, "error": payload["_error"]}
    return {
        "reachable": True,
        "configured": True,
        "metadatabase": payload.get("metadatabase", {}),
        "scheduler": payload.get("scheduler", {}),
        "triggerer": payload.get("triggerer", {}),
        "dag_processor": payload.get("dag_processor", {}),
    }


@app.get("/api/airflow/overview")
def airflow_overview(dag_id: str = "job_market_pipeline", runs_limit: int = 5):
    if not _airflow_base_urls():
        return {
            "reachable": False,
            "configured": False,
            "dag_id": dag_id,
            "message": "Airflow integration not configured. Set AIRFLOW_API_BASE_URL to enable.",
        }

    dag = _airflow_request(f"/dags/{dag_id}")
    runs = _airflow_request(
        f"/dags/{dag_id}/dagRuns",
        {"order_by": "-start_date", "limit": max(1, min(runs_limit, 20))},
    )
    tasks = _airflow_request(f"/dags/{dag_id}/tasks")

    errors = [v.get("_error") for v in (dag, runs, tasks) if "_error" in v]
    if errors:
        return {"reachable": False, "configured": True, "error": "; ".join(errors), "dag_id": dag_id}

    dag_runs = runs.get("dag_runs", [])
    formatted_runs = [
        {
            "dag_run_id": run.get("dag_run_id"),
            "state": run.get("state"),
            "run_type": run.get("run_type"),
            "logical_date": run.get("logical_date"),
            "start_date": run.get("start_date"),
            "end_date": run.get("end_date"),
            "duration_seconds": _duration_seconds(run.get("start_date"), run.get("end_date")),
        }
        for run in dag_runs
    ]

    task_list = tasks.get("tasks", [])
    state_counts: dict[str, int] = {}
    for run in dag_runs:
        state = run.get("state", "unknown")
        state_counts[state] = state_counts.get(state, 0) + 1

    return {
        "reachable": True,
        "configured": True,
        "dag": {
            "dag_id": dag.get("dag_id", dag_id),
            "is_paused": dag.get("is_paused"),
            "is_active": dag.get("is_active"),
            "description": dag.get("description"),
            "owners": dag.get("owners", []),
            "tags": [tag.get("name") for tag in dag.get("tags", []) if isinstance(tag, dict)],
        },
        "run_summary": state_counts,
        "recent_runs": formatted_runs,
        "task_count": len(task_list),
        "task_ids": [task.get("task_id") for task in task_list if task.get("task_id")],
    }


# ── Filters endpoint ─────────────────────────────────────────────────────────


@app.get("/api/filters")
def get_filters():
    df = _get_data()
    if df.empty:
        return {"locations": [], "date_range": None}

    locations = sorted(df["location"].dropna().unique().tolist())
    dates = pd.to_datetime(df["posted_date"], errors="coerce").dropna()
    date_range = None
    if not dates.empty:
        date_range = {
            "min": str(dates.min().date()),
            "max": str(dates.max().date()),
        }
    return {"locations": locations, "date_range": date_range}


# ── Main dashboard endpoint ──────────────────────────────────────────────────


@app.get("/api/dashboard")
def get_dashboard(
    locations: list[str] = Query(default=[]),
    date_from: str | None = None,
    date_to: str | None = None,
):
    df = _get_data()
    if df.empty:
        return {"error": "No data available. Run the pipeline first."}

    df["posted_date"] = pd.to_datetime(df["posted_date"], errors="coerce")
    for col in ("salary_min", "salary_max", "skill_count"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "is_remote" in df.columns:
        df["is_remote"] = df["is_remote"].astype(bool)

    if locations:
        df = df[df["location"].isin(locations)]
    if date_from:
        df = df[df["posted_date"] >= pd.Timestamp(date_from)]
    if date_to:
        df = df[df["posted_date"] <= pd.Timestamp(date_to)]

    return {
        "metrics": _metrics(df),
        "top_locations": _top_values(df, "location"),
        "top_companies": _top_values(df, "company"),
        "timeline": _timeline(df),
        "skills": _skills(df),
        "salary": _salary(df),
        "work_mode": _work_mode(df),
    }


# ── Helpers ───────────────────────────────────────────────────────────────────


def _metrics(df: pd.DataFrame) -> dict:
    avg_sk = 0.0
    if "skill_count" in df.columns and df["skill_count"].notna().any():
        avg_sk = round(float(df["skill_count"].mean()), 1)
    return {
        "total_jobs": len(df),
        "companies": int(df["company"].nunique()),
        "locations": int(df["location"].nunique()),
        "avg_skills": avg_sk,
        "with_salary": int(df["salary_min"].notna().sum()) if "salary_min" in df.columns else 0,
    }


def _top_values(df: pd.DataFrame, col: str, n: int = 12) -> list[dict]:
    counts = df[col].value_counts().head(n)
    return [{"name": str(k), "count": int(v)} for k, v in counts.items()]


def _timeline(df: pd.DataFrame) -> list[dict]:
    td = df.dropna(subset=["posted_date"])
    if td.empty:
        return []
    day = td.groupby(td["posted_date"].dt.date).size().sort_index()
    return [{"date": str(d), "count": int(c)} for d, c in day.items()]


def _skills(df: pd.DataFrame) -> dict:
    out: dict = {"rankings": [], "total_mentions": 0, "unique_skills": 0, "cooccurrence": []}
    if "skills" not in df.columns:
        return out

    exploded = df["skills"].dropna().loc[lambda x: x != ""].str.split(", ").explode().str.strip().str.lower()
    counts = exploded.value_counts()
    out["rankings"] = [{"skill": s.title(), "demand": int(d)} for s, d in counts.items()]
    out["total_mentions"] = int(counts.sum())
    out["unique_skills"] = len(counts)

    pairs: dict[tuple, int] = {}
    for skills_str in df["skills"].dropna():
        items = sorted({s.strip().lower() for s in skills_str.split(",") if s.strip()})
        for a, b in combinations(items, 2):
            pairs[(a, b)] = pairs.get((a, b), 0) + 1
    top_pairs = sorted(pairs.items(), key=lambda x: x[1], reverse=True)[:15]
    out["cooccurrence"] = [{"pair": f"{a.title()} + {b.title()}", "count": c} for (a, b), c in top_pairs]
    return out


def _salary(df: pd.DataFrame) -> dict:
    out: dict = {"metrics": {}, "histogram": [], "ranges": [], "table": []}
    if "salary_min" not in df.columns:
        return out

    sdf = df.dropna(subset=["salary_min"])
    sdf = sdf[sdf["salary_min"] > 0]
    if sdf.empty:
        return out

    has_max = sdf["salary_max"].notna().any()
    med_min = float(sdf["salary_min"].median())
    med_max = float(sdf["salary_max"].dropna().median()) if has_max else 0
    full_min = float(sdf["salary_min"].min())
    full_max = float(sdf[["salary_min", "salary_max"]].max().max())

    out["metrics"] = {
        "count": len(sdf),
        "median_min": med_min,
        "median_max": med_max,
        "full_min": full_min,
        "full_max": full_max,
    }

    all_vals = pd.concat([sdf["salary_min"], sdf["salary_max"].dropna()])
    _, edges = np.histogram(all_vals.values, bins=12)
    min_bins, _ = np.histogram(sdf["salary_min"].values, bins=edges)
    max_bins = np.zeros_like(min_bins)
    if has_max:
        max_bins, _ = np.histogram(sdf["salary_max"].dropna().values, bins=edges)
    out["histogram"] = [
        {
            "range": f"${int(edges[i]):,}\u2013${int(edges[i + 1]):,}",
            "min_salary": int(min_bins[i]),
            "max_salary": int(max_bins[i]),
        }
        for i in range(len(min_bins))
    ]

    if has_max:
        rr = sdf.dropna(subset=["salary_max"]).sort_values("salary_min").tail(15)
        out["ranges"] = [
            {
                "label": f"{r['title'][:28]} \u2014 {r['company'][:12]}",
                "min": float(r["salary_min"]),
                "max": float(r["salary_max"]),
            }
            for _, r in rr.iterrows()
        ]

    cols = [c for c in ["title", "company", "location", "salary_min", "salary_max"] if c in sdf.columns]
    out["table"] = sdf[cols].sort_values("salary_min", ascending=False).head(50).fillna("").to_dict(orient="records")
    return out


def _work_mode(df: pd.DataFrame) -> dict:
    wdf = df.copy()
    if "work_mode" not in wdf.columns:
        wdf["work_mode"] = wdf["location"].apply(
            lambda loc: "Remote" if "remote" in str(loc).lower() or "worldwide" in str(loc).lower() else "Onsite"
        )

    mode_counts = wdf["work_mode"].value_counts()
    split = [{"mode": m, "count": int(c)} for m, c in mode_counts.items()]

    remote_locs = []
    ro = wdf[wdf["work_mode"] == "Remote"]
    if not ro.empty:
        rl = ro["location"].value_counts().head(12)
        remote_locs = [{"name": str(loc), "count": int(cnt)} for loc, cnt in rl.items()]

    return {"split": split, "remote_locations": remote_locs}


# ── Serve React build in production ──────────────────────────────────────────

_frontend_dist = Path(__file__).parent.parent / "frontend" / "dist"
if _frontend_dist.exists():
    app.mount("/", StaticFiles(directory=str(_frontend_dist), html=True), name="spa")
