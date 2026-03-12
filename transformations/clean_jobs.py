"""
Data Cleaning — Tech Job Market Pipeline

Reads raw Parquet from the ingestion layer and applies:

  1. Deduplication
  2. HTML stripping
  3. Location normalisation
  4. Date parsing
  5. Salary parsing  (free-text → numeric min/max)
  6. Job-type normalisation
  7. Skill extraction  (delegates to skill_extraction.py)

Outputs:
  data/processed/clean_jobs.parquet   — primary output
  data/processed/clean_jobs.csv       — backward-compat CSV

Usage:
    python transformations/clean_jobs.py
"""

import html
import re
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.config import (
    CLEAN_CSV_PATH,
    PROCESSED_PARQUET_PATH,
    RAW_CSV_PATH,
    RAW_PARQUET_PATH,
)
from transformations.skill_extraction import extract_skills
from utils.logger import get_logger

logger = get_logger("transform.clean_jobs")


# ═════════════════════════════════════════════════════════════════════════════
#  LOAD
# ═════════════════════════════════════════════════════════════════════════════
def load_raw_data() -> pd.DataFrame:
    """Load raw data — prefer Parquet, fall back to CSV."""
    if RAW_PARQUET_PATH.exists():
        logger.info("Loading raw Parquet from %s", RAW_PARQUET_PATH)
        df = pd.read_parquet(RAW_PARQUET_PATH).fillna("")
    elif RAW_CSV_PATH.exists():
        logger.info("Parquet not found; loading CSV from %s", RAW_CSV_PATH)
        df = pd.read_csv(RAW_CSV_PATH, dtype=str).fillna("")
    else:
        logger.error("No raw data found in %s", RAW_PARQUET_PATH.parent)
        return pd.DataFrame()
    logger.info("Loaded %d rows × %d cols", *df.shape)
    return df


# ═════════════════════════════════════════════════════════════════════════════
#  TRANSFORMS
# ═════════════════════════════════════════════════════════════════════════════
def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=["job_id"], keep="first").reset_index(drop=True)
    logger.info("Deduplication: %d → %d  (dropped %d)", before, len(df), before - len(df))
    return df


# ── HTML ─────────────────────────────────────────────────────────────────────
_HTML_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")


def strip_html(text: str) -> str:
    return _WS.sub(" ", html.unescape(_HTML_TAG.sub(" ", text))).strip()


def clean_descriptions(df: pd.DataFrame) -> pd.DataFrame:
    df["description"] = df["description"].apply(strip_html)
    logger.info("Stripped HTML  (avg len: %.0f chars)", df["description"].str.len().mean())
    return df


# ── Work mode ────────────────────────────────────────────────────────────────
def classify_work_mode(location: str) -> str:
    """Classify a *raw* location string before normalisation touches it."""
    loc = str(location).strip().lower()
    if not loc or loc == "nan":
        return "Remote"
    if "remote" in loc or "worldwide" in loc or "anywhere" in loc:
        return "Remote"
    return "Onsite"


def add_work_mode(df: pd.DataFrame) -> pd.DataFrame:
    """Must run BEFORE normalise_locations (which rewrites the location col)."""
    df["work_mode"] = df["location"].apply(classify_work_mode)
    counts = df["work_mode"].value_counts()
    logger.info("Work-mode classification:\n%s", counts.to_string())
    return df


# ── Location ─────────────────────────────────────────────────────────────────
_LOC_ALIASES: dict[str, str] = {
    "sf": "San Francisco",
    "nyc": "New York",
    "ny": "New York",
    "la": "Los Angeles",
    "dc": "Washington D.C.",
    "worldwide": "Remote",
    "anywhere": "Remote",
    "global": "Remote",
    "u. s.": "United States",
}
_US_STATES = {
    "al",
    "ak",
    "az",
    "ar",
    "ca",
    "co",
    "ct",
    "de",
    "fl",
    "ga",
    "hi",
    "id",
    "il",
    "in",
    "ia",
    "ks",
    "ky",
    "la",
    "me",
    "md",
    "ma",
    "mi",
    "mn",
    "ms",
    "mo",
    "mt",
    "ne",
    "nv",
    "nh",
    "nj",
    "nm",
    "ny",
    "nc",
    "nd",
    "oh",
    "ok",
    "or",
    "pa",
    "ri",
    "sc",
    "sd",
    "tn",
    "tx",
    "ut",
    "vt",
    "va",
    "wa",
    "wv",
    "wi",
    "wy",
}


def _norm_loc(raw: str) -> str:
    loc = raw.strip().strip(",").strip()
    if not loc:
        return "Remote"
    key = loc.lower()
    if key in _LOC_ALIASES:
        return _LOC_ALIASES[key]
    for pat, repl in [
        (r"^remote\s*[-–—]\s*(.+)$", r"Remote (\1)"),
        (r"^(.+?)\s*\(\s*remote\s*\)$", r"Remote (\1)"),
        (r"^(.+?)\s*[-–—]\s*remote.*$", r"Remote (\1)"),
        (r"^(.+?)\s*/\s*remote$", r"Remote (\1)"),
    ]:
        m = re.match(pat, loc, re.IGNORECASE)
        if m:
            return f"Remote ({m.group(1).strip().title()})"
    m = re.match(r"^(.+?),\s*([A-Za-z]{2})$", loc)
    if m and m.group(2).lower() in _US_STATES:
        return m.group(1).strip().title()
    loc = re.sub(r",?\s*(USA|US|UK|CA|DE|FR|AU|IN)\s*$", "", loc, flags=re.IGNORECASE).strip()
    return loc.title() if loc else "Remote"


def normalise_locations(df: pd.DataFrame) -> pd.DataFrame:
    df["location"] = df["location"].apply(lambda r: _norm_loc(re.split(r"[|;]", r)[0]) if r.strip() else "Remote")
    df["is_remote"] = df["location"].str.lower().str.contains("remote") | df["job_type"].str.lower().str.contains(
        "remote"
    )
    logger.info("Locations: %d unique, %.0f%% remote", df["location"].nunique(), df["is_remote"].mean() * 100)
    return df


# ── Dates ────────────────────────────────────────────────────────────────────
def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    df["posted_date"] = pd.to_datetime(df["posted_date"], errors="coerce", utc=True)
    bad = df["posted_date"].isna().sum()
    if bad:
        logger.warning("%d rows with unparseable dates", bad)
    df["posted_date"] = df["posted_date"].dt.strftime("%Y-%m-%d")
    logger.info("Dates: %s → %s", df["posted_date"].min(), df["posted_date"].max())
    return df


# ── Salary ───────────────────────────────────────────────────────────────────
_SAL_RE = re.compile(r"[\d,]+\.?\d*")


def _parse_salary(raw: str) -> tuple[float | None, float | None]:
    if not raw:
        return None, None
    nums = [float(n) for n in _SAL_RE.findall(raw.replace(",", ""))]
    if not nums:
        return None, None
    lo, hi = min(nums), max(nums)
    if hi < 200:
        lo, hi = lo * 2080, hi * 2080
    elif hi < 10_000:
        lo, hi = lo * 12, hi * 12
    return (lo, hi) if len(nums) >= 2 else (lo, None)


def parse_salaries(df: pd.DataFrame) -> pd.DataFrame:
    parsed = df["salary"].apply(lambda s: pd.Series(_parse_salary(s), index=["salary_min", "salary_max"]))
    df = pd.concat([df, parsed], axis=1)
    n = df["salary_min"].notna().sum()
    logger.info("Salaries: %d of %d have numeric values (%.0f%%)", n, len(df), n / max(len(df), 1) * 100)
    return df


# ── Job type ─────────────────────────────────────────────────────────────────
_JOB_TYPE_MAP = {
    "full_time": "Full-time",
    "full time": "Full-time",
    "fulltime": "Full-time",
    "part_time": "Part-time",
    "part time": "Part-time",
    "parttime": "Part-time",
    "contract": "Contract",
    "freelance": "Freelance",
    "internship": "Internship",
    "remote": "Full-time",
    "": "Unknown",
}


def normalise_job_types(df: pd.DataFrame) -> pd.DataFrame:
    df["job_type"] = df["job_type"].str.lower().str.strip().map(_JOB_TYPE_MAP).fillna("Other")
    logger.info("Job types:\n%s", df["job_type"].value_counts().to_string())
    return df


# ═════════════════════════════════════════════════════════════════════════════
#  PIPELINE
# ═════════════════════════════════════════════════════════════════════════════
def run_cleaning() -> pd.DataFrame:
    """Full clean + skill-extract pipeline.  Writes Parquet + CSV."""
    logger.info("=" * 60)
    logger.info("STARTING DATA CLEANING")
    logger.info("=" * 60)

    df = load_raw_data()
    if df.empty:
        return df

    df = remove_duplicates(df)
    df = clean_descriptions(df)
    df = add_work_mode(df)
    df = normalise_locations(df)
    df = parse_dates(df)
    df = parse_salaries(df)
    df = normalise_job_types(df)
    df = extract_skills(df)

    col_order = [
        "job_id",
        "title",
        "company",
        "location",
        "is_remote",
        "work_mode",
        "salary",
        "salary_min",
        "salary_max",
        "job_type",
        "category",
        "skills",
        "skill_count",
        "description",
        "posted_date",
        "url",
        "tags",
        "source",
    ]
    present = [c for c in col_order if c in df.columns]
    extra = [c for c in df.columns if c not in col_order]
    df = df[present + extra]

    # ── Write Parquet (primary) ──────────────────────────────────────────────
    PROCESSED_PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(PROCESSED_PARQUET_PATH, index=False, engine="pyarrow")
    logger.info("Saved Parquet → %s", PROCESSED_PARQUET_PATH)

    # ── Write CSV (backward-compat) ──────────────────────────────────────────
    df.to_csv(CLEAN_CSV_PATH, index=False)
    logger.info("Saved CSV → %s", CLEAN_CSV_PATH)

    logger.info("-" * 50)
    logger.info("CLEANING SUMMARY")
    logger.info("  Records  : %d", len(df))
    logger.info("  Salary   : %d rows", df["salary_min"].notna().sum())
    logger.info("  Skills   : %d rows", (df["skill_count"] > 0).sum())
    logger.info("  Locations: %d unique", df["location"].nunique())
    logger.info("  Companies: %d unique", df["company"].nunique())
    logger.info("-" * 50)
    return df


if __name__ == "__main__":
    run_cleaning()
