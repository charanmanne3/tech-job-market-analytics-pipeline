"""
Data Ingestion — Tech Job Market Pipeline

Fetches tech job postings from public APIs (Remotive, RemoteOK),
normalises every record into a unified schema, and persists:

  data/raw/raw_jobs.json     — original API payload
  data/raw/raw_jobs.parquet  — normalised raw DataFrame

Usage:
    python data_ingestion/fetch_jobs.py
"""

import json
import sys
import time
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.config import (
    RAW_CSV_PATH,
    RAW_JSON_PATH,
    RAW_PARQUET_PATH,
    REMOTEOK_API_URL,
    REMOTIVE_API_URL,
)
from utils.logger import get_logger

logger = get_logger("ingestion.fetch_jobs")

REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helper
# ─────────────────────────────────────────────────────────────────────────────
def request_with_retries(
    url: str,
    headers: dict | None = None,
    params: dict | None = None,
) -> dict | list | None:
    """GET with exponential back-off.  Returns parsed JSON or None."""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(
                url,
                headers=headers or {},
                params=params or {},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.ConnectionError as exc:
            logger.warning("Connection error (attempt %d): %s", attempt, exc)
        except requests.Timeout:
            logger.warning("Timeout (attempt %d) for %s", attempt, url)
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else "?"
            logger.warning("HTTP %s (attempt %d) for %s", code, attempt, url)
        except requests.RequestException as exc:
            logger.warning("Request error (attempt %d): %s", attempt, exc)

        if attempt < RETRY_ATTEMPTS:
            wait = RETRY_BACKOFF ** attempt
            logger.info("Retrying in %ds …", wait)
            time.sleep(wait)

    logger.error("All %d attempts exhausted for %s", RETRY_ATTEMPTS, url)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Source fetchers
# ─────────────────────────────────────────────────────────────────────────────
def fetch_remotive_jobs() -> tuple[list[dict], pd.DataFrame]:
    """Returns (raw_json_list, normalised_df) from Remotive."""
    logger.info("Fetching jobs from Remotive …")
    payload = request_with_retries(REMOTIVE_API_URL)
    if payload is None or "jobs" not in payload:
        logger.warning("Remotive returned no usable data")
        return [], pd.DataFrame()

    raw_jobs = payload["jobs"]
    logger.info("Remotive responded with %d postings", len(raw_jobs))

    records = [
        {
            "job_id": f"remotive_{j.get('id', '')}",
            "title": j.get("title", ""),
            "company": j.get("company_name", ""),
            "location": j.get("candidate_required_location", "Anywhere"),
            "salary": j.get("salary", ""),
            "job_type": j.get("job_type", ""),
            "category": j.get("category", ""),
            "description": j.get("description", ""),
            "posted_date": j.get("publication_date", ""),
            "url": j.get("url", ""),
            "tags": ", ".join(j.get("tags", [])),
            "source": "remotive",
        }
        for j in raw_jobs
    ]
    return raw_jobs, pd.DataFrame(records)


def _parse_remoteok_salary(job: dict) -> str:
    sal_min, sal_max = job.get("salary_min"), job.get("salary_max")
    if sal_min and sal_max:
        return f"${sal_min} - ${sal_max}"
    if sal_min:
        return f"${sal_min}+"
    return ""


def fetch_remoteok_jobs() -> tuple[list[dict], pd.DataFrame]:
    """Returns (raw_json_list, normalised_df) from RemoteOK."""
    logger.info("Fetching jobs from RemoteOK …")
    headers = {"User-Agent": "TechJobMarketPipeline/1.0"}
    data = request_with_retries(REMOTEOK_API_URL, headers=headers)
    if data is None:
        logger.warning("RemoteOK returned no usable data")
        return [], pd.DataFrame()

    raw_jobs = [i for i in data if isinstance(i, dict) and "id" in i]
    logger.info("RemoteOK responded with %d postings", len(raw_jobs))

    records = [
        {
            "job_id": f"remoteok_{j.get('id', '')}",
            "title": j.get("position", ""),
            "company": j.get("company", ""),
            "location": j.get("location", "Remote"),
            "salary": _parse_remoteok_salary(j),
            "job_type": "remote",
            "category": "",
            "description": j.get("description", ""),
            "posted_date": j.get("date", ""),
            "url": j.get("url", ""),
            "tags": ", ".join(j.get("tags") or []),
            "source": "remoteok",
        }
        for j in raw_jobs
    ]
    return raw_jobs, pd.DataFrame(records)


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────
def run_ingestion() -> pd.DataFrame:
    """Fetch all sources → deduplicate → persist raw JSON + Parquet + CSV."""
    logger.info("=" * 60)
    logger.info("STARTING DATA INGESTION")
    logger.info("=" * 60)

    all_raw_json: list[dict] = []
    dataframes: list[pd.DataFrame] = []

    for name, fetcher in [("Remotive", fetch_remotive_jobs), ("RemoteOK", fetch_remoteok_jobs)]:
        try:
            raw_json, df = fetcher()
            all_raw_json.extend(raw_json)
            if df.empty:
                logger.warning("%s returned 0 records", name)
            else:
                logger.info("%s returned %d records", name, len(df))
                dataframes.append(df)
        except Exception:
            logger.exception("Unexpected error fetching from %s", name)

    if not dataframes:
        logger.error("All sources failed — no data to save")
        return pd.DataFrame()

    combined = pd.concat(dataframes, ignore_index=True)
    before = len(combined)
    combined = combined.drop_duplicates(subset=["job_id"], keep="first").reset_index(drop=True)
    logger.info("Deduplication: %d → %d", before, len(combined))

    # ── Persist raw JSON ─────────────────────────────────────────────────────
    RAW_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RAW_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(all_raw_json, f, ensure_ascii=False, indent=2, default=str)
    logger.info("Saved raw JSON → %s  (%d objects)", RAW_JSON_PATH, len(all_raw_json))

    # ── Persist Parquet (primary) ────────────────────────────────────────────
    combined.to_parquet(RAW_PARQUET_PATH, index=False, engine="pyarrow")
    logger.info("Saved raw Parquet → %s", RAW_PARQUET_PATH)

    # ── Persist CSV (backward-compat) ────────────────────────────────────────
    combined.to_csv(RAW_CSV_PATH, index=False)
    logger.info("Saved raw CSV → %s", RAW_CSV_PATH)

    # ── Summary ──────────────────────────────────────────────────────────────
    logger.info("-" * 50)
    logger.info("INGESTION SUMMARY")
    logger.info("  Total records : %d", len(combined))
    logger.info("  Sources       : %s", ", ".join(combined["source"].unique()))
    logger.info("  Companies     : %d", combined["company"].nunique())
    logger.info("  Locations     : %d", combined["location"].nunique())
    logger.info("  Date range    : %s → %s", combined["posted_date"].min(), combined["posted_date"].max())
    logger.info("-" * 50)
    return combined


if __name__ == "__main__":
    run_ingestion()
