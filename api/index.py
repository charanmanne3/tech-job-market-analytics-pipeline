"""
Vercel serverless function — self-contained, lightweight FastAPI app.

Uses only the Python standard library for data processing (no pandas/numpy)
to stay well under Vercel's 250 MB Lambda limit.
"""

from __future__ import annotations

import csv
import math
from collections import Counter
from itertools import combinations
from pathlib import Path
from statistics import mean, median

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


# ── Endpoints ────────────────────────────────────────────────────────────────


@app.post("/api/reload")
def reload_data():
    global _cached_rows
    _cached_rows = None
    return {"status": "ok"}


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
