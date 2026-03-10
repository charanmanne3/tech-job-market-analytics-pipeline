"""
Skill Extraction — Tech Job Market Pipeline

Pre-compiled regex patterns for 55+ tech skills.  Used by clean_jobs.py
and can also be run standalone on any DataFrame / Parquet file.

Usage:
    python transformations/skill_extraction.py                        # default
    python transformations/skill_extraction.py data/raw/raw_jobs.parquet
"""

import re
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.config import PROCESSED_PARQUET_PATH, TECH_SKILLS
from utils.logger import get_logger

logger = get_logger("transform.skill_extraction")


# ─────────────────────────────────────────────────────────────────────────────
# Pattern compilation (done once at import time)
# ─────────────────────────────────────────────────────────────────────────────
def _build_patterns(skills: list[str]) -> list[tuple[str, re.Pattern]]:
    return [(s, re.compile(rf"\b{re.escape(s)}\b", re.IGNORECASE)) for s in skills]


SKILL_PATTERNS = _build_patterns(TECH_SKILLS)


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────
def extract_skills_from_text(text: str) -> list[str]:
    """Return sorted, de-duplicated skills found in *text*."""
    if not text:
        return []
    return sorted({name for name, pat in SKILL_PATTERNS if pat.search(text)})


def extract_skills(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add ``skills`` (comma-separated) and ``skill_count`` columns.

    Combines the ``description`` and ``tags`` columns for wider coverage.
    """
    logger.info("Extracting skills from %d descriptions …", len(df))

    combined = df["description"].fillna("") + " " + df.get("tags", pd.Series(dtype=str)).fillna("")
    df["skills"] = combined.apply(lambda t: ", ".join(extract_skills_from_text(t)))
    df["skill_count"] = df["skills"].apply(lambda s: len(s.split(", ")) if s else 0)

    nonempty = df["skills"].loc[df["skills"] != ""]
    if not nonempty.empty:
        top = nonempty.str.split(", ").explode().str.strip().value_counts().head(15)
        logger.info("Top 15 skills:\n%s", top.to_string())

    logger.info(
        "Skill stats — mean %.1f/job, max %d, zero-skill jobs %d",
        df["skill_count"].mean(),
        df["skill_count"].max(),
        (df["skill_count"] == 0).sum(),
    )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────
def run_skill_extraction(path: Path | None = None) -> pd.DataFrame:
    target = path or PROCESSED_PARQUET_PATH
    logger.info("Loading %s", target)

    if target.suffix == ".parquet":
        df = pd.read_parquet(target)
    else:
        df = pd.read_csv(target, dtype=str).fillna("")

    df = extract_skills(df)

    if target.suffix == ".parquet":
        df.to_parquet(target, index=False, engine="pyarrow")
    else:
        df.to_csv(target, index=False)
    logger.info("Updated %s with skills columns (%d rows)", target, len(df))
    return df


if __name__ == "__main__":
    custom = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    run_skill_extraction(custom)
