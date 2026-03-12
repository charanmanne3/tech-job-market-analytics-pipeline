"""
Vercel serverless function — self-contained, lightweight FastAPI app.

Uses only the Python standard library for data processing (no pandas/numpy)
to stay well under Vercel's 250 MB Lambda limit.
"""

from __future__ import annotations

import base64
import csv
import json
import math
import os
from collections import Counter
from datetime import datetime
from itertools import combinations
from pathlib import Path
from statistics import mean, median
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Tech Job Market Analytics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Data layer ───────────────────────────────────────────────────────────────

_BASE = Path(__file__).resolve().parent.parent
_CSV_PATHS = [
    _BASE / "data" / "processed" / "clean_jobs.csv",
    _BASE / "data" / "clean_jobs.csv",
]

_cached_rows: list[dict] | None = None


def _load_data() -> list[dict]:
    for p in _CSV_PATHS:
        if p.exists():
            with open(p, newline="", encoding="utf-8") as f:
                return list(csv.DictReader(f))
    return []


def _get_data() -> list[dict]:
    global _cached_rows
    if _cached_rows is None:
        _cached_rows = _load_data()
    return list(_cached_rows)


def _safe_float(val: str | None) -> float | None:
    if not val or val.strip() == "":
        return None
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (ValueError, TypeError):
        return None


def _normalize_airflow_url(raw: str) -> str:
    cleaned = raw.strip().rstrip("/")
    if cleaned.endswith("/api/v1"):
        return cleaned
    return f"{cleaned}/api/v1"


def _airflow_base_urls() -> list[str]:
    explicit = os.getenv("AIRFLOW_API_BASE_URL", "").strip()
    if explicit:
        return [_normalize_airflow_url(explicit)]
    # Zero-config deploy behavior:
    # If no Airflow URL is explicitly configured, mark integration as disabled.
    return []


def _airflow_request(path: str, params: dict | None = None) -> dict:
    username = os.getenv("AIRFLOW_USERNAME", "").strip()
    password = os.getenv("AIRFLOW_PASSWORD", "").strip()
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


# ── Endpoints ────────────────────────────────────────────────────────────────


@app.post("/api/reload")
def reload_data():
    global _cached_rows
    _cached_rows = None
    return {"status": "ok"}


@app.get("/api/airflow/health")
def airflow_health():
    if not os.getenv("AIRFLOW_API_BASE_URL", "").strip():
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
    if not os.getenv("AIRFLOW_API_BASE_URL", "").strip():
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


@app.get("/api/filters")
def get_filters():
    rows = _get_data()
    if not rows:
        return {"locations": [], "date_range": None}

    locations = sorted({r.get("location", "") for r in rows if r.get("location")})
    dates = sorted(d for r in rows if (d := r.get("posted_date", "").strip()))
    date_range = {"min": dates[0][:10], "max": dates[-1][:10]} if dates else None
    return {"locations": locations, "date_range": date_range}


@app.get("/api/dashboard")
def get_dashboard(
    locations: list[str] = Query(default=[]),
    date_from: str | None = None,
    date_to: str | None = None,
):
    rows = _get_data()
    if not rows:
        return {"error": "No data available. Run the pipeline first."}

    if locations:
        rows = [r for r in rows if r.get("location") in locations]
    if date_from:
        rows = [r for r in rows if (r.get("posted_date") or "") >= date_from]
    if date_to:
        rows = [r for r in rows if (r.get("posted_date") or "") <= date_to]

    return {
        "metrics": _metrics(rows),
        "top_locations": _top_values(rows, "location"),
        "top_companies": _top_values(rows, "company"),
        "timeline": _timeline(rows),
        "skills": _skills(rows),
        "salary": _salary(rows),
        "work_mode": _work_mode(rows),
    }


# ── Helpers ──────────────────────────────────────────────────────────────────


def _metrics(rows: list[dict]) -> dict:
    companies = {r.get("company") for r in rows if r.get("company")}
    locs = {r.get("location") for r in rows if r.get("location")}
    skill_counts = [int(r["skill_count"]) for r in rows if _safe_float(r.get("skill_count"))]
    salary_count = sum(1 for r in rows if _safe_float(r.get("salary_min")))
    return {
        "total_jobs": len(rows),
        "companies": len(companies),
        "locations": len(locs),
        "avg_skills": round(mean(skill_counts), 1) if skill_counts else 0.0,
        "with_salary": salary_count,
    }


def _top_values(rows: list[dict], col: str, n: int = 12) -> list[dict]:
    ctr = Counter(r.get(col) for r in rows if r.get(col))
    return [{"name": k, "count": v} for k, v in ctr.most_common(n)]


def _timeline(rows: list[dict]) -> list[dict]:
    day_ctr: Counter[str] = Counter()
    for r in rows:
        d = (r.get("posted_date") or "").strip()[:10]
        if d:
            day_ctr[d] += 1
    return [{"date": d, "count": c} for d, c in sorted(day_ctr.items())]


def _skills(rows: list[dict]) -> dict:
    out: dict = {"rankings": [], "total_mentions": 0, "unique_skills": 0, "cooccurrence": []}
    skill_counter: Counter[str] = Counter()
    pair_counter: Counter[tuple[str, str]] = Counter()

    for r in rows:
        raw = (r.get("skills") or "").strip()
        if not raw:
            continue
        items = sorted({s.strip().lower() for s in raw.split(",") if s.strip()})
        skill_counter.update(items)
        for a, b in combinations(items, 2):
            pair_counter[(a, b)] += 1

    if not skill_counter:
        return out

    out["rankings"] = [{"skill": s.title(), "demand": d} for s, d in skill_counter.most_common()]
    out["total_mentions"] = sum(skill_counter.values())
    out["unique_skills"] = len(skill_counter)
    out["cooccurrence"] = [
        {"pair": f"{a.title()} + {b.title()}", "count": c} for (a, b), c in pair_counter.most_common(15)
    ]
    return out


def _histogram(values: list[float], bins: int = 12) -> tuple[list[int], list[float]]:
    """Simple histogram using only stdlib."""
    if not values:
        return [], []
    lo, hi = min(values), max(values)
    if lo == hi:
        return [len(values)], [lo, hi + 1]
    width = (hi - lo) / bins
    edges = [lo + i * width for i in range(bins + 1)]
    edges[-1] = hi
    counts = [0] * bins
    for v in values:
        idx = min(int((v - lo) / width), bins - 1)
        counts[idx] += 1
    return counts, edges


def _salary(rows: list[dict]) -> dict:
    out: dict = {"metrics": {}, "histogram": [], "ranges": [], "table": []}

    srows = []
    for r in rows:
        smin = _safe_float(r.get("salary_min"))
        if smin and smin > 0:
            srows.append((r, smin, _safe_float(r.get("salary_max"))))

    if not srows:
        return out

    mins = [s[1] for s in srows]
    maxes = [s[2] for s in srows if s[2] is not None]

    out["metrics"] = {
        "count": len(srows),
        "median_min": median(mins),
        "median_max": median(maxes) if maxes else 0,
        "full_min": min(mins),
        "full_max": max(maxes) if maxes else max(mins),
    }

    all_vals = mins + maxes
    counts, edges = _histogram(all_vals, 12)
    min_counts, _ = _histogram(mins, 12)
    max_counts, _ = _histogram(maxes, 12) if maxes else ([0] * 12, [])

    n = min(len(counts), len(edges) - 1, len(min_counts))
    if len(max_counts) < n:
        max_counts.extend([0] * (n - len(max_counts)))
    out["histogram"] = [
        {
            "range": f"${int(edges[i]):,}\u2013${int(edges[i + 1]):,}",
            "min_salary": min_counts[i],
            "max_salary": max_counts[i],
        }
        for i in range(n)
    ]

    if maxes:
        with_max = [(r, smin, smax) for r, smin, smax in srows if smax is not None]
        with_max.sort(key=lambda x: x[1])
        for r, smin, smax in with_max[-15:]:
            out["ranges"].append(
                {
                    "label": f"{(r.get('title') or '')[:28]} \u2014 {(r.get('company') or '')[:12]}",
                    "min": smin,
                    "max": smax,
                }
            )

    sorted_rows = sorted(srows, key=lambda x: x[1], reverse=True)[:50]
    for r, smin, smax in sorted_rows:
        out["table"].append(
            {
                "title": r.get("title", ""),
                "company": r.get("company", ""),
                "location": r.get("location", ""),
                "salary_min": smin,
                "salary_max": smax or "",
            }
        )
    return out


def _work_mode(rows: list[dict]) -> dict:
    def classify(r: dict) -> str:
        wm = r.get("work_mode", "")
        if wm:
            return wm
        loc = str(r.get("location", "")).lower()
        return "Remote" if ("remote" in loc or "worldwide" in loc) else "Onsite"

    mode_ctr = Counter(classify(r) for r in rows)
    split = [{"mode": m, "count": c} for m, c in mode_ctr.most_common()]

    remote_rows = [r for r in rows if classify(r) == "Remote"]
    loc_ctr = Counter(r.get("location") for r in remote_rows if r.get("location"))
    remote_locs = [{"name": k, "count": v} for k, v in loc_ctr.most_common(12)]

    return {"split": split, "remote_locations": remote_locs}
