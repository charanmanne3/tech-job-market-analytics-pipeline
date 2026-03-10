"""
Standalone skill extraction utility.

Can be run independently on any CSV that contains a 'description' column.
The core extraction logic lives in clean_jobs.extract_skills_from_text()
so there is a single source of truth for the skill-matching patterns.

Usage:
    python transformations/extract_skills.py              # uses clean_jobs.csv
    python transformations/extract_skills.py data/raw.csv # custom input
"""

import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.config import CLEAN_DATA_PATH, LOG_FORMAT, LOG_LEVEL
from transformations.clean_jobs import extract_skills, strip_html

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("extract_skills")


def run_skill_extraction(csv_path: Path | None = None) -> pd.DataFrame:
    path = csv_path or CLEAN_DATA_PATH
    logger.info("Loading data from %s", path)
    df = pd.read_csv(path, dtype=str).fillna("")

    if "description" not in df.columns:
        logger.error("CSV has no 'description' column — cannot extract skills")
        return df

    # Ensure descriptions are clean before extraction
    if df["description"].str.contains("<").any():
        logger.info("HTML detected in descriptions — stripping tags first")
        df["description"] = df["description"].apply(strip_html)

    df = extract_skills(df)

    df.to_csv(path, index=False)
    logger.info("Updated %s with skills column (%d rows)", path, len(df))
    return df


if __name__ == "__main__":
    custom = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    run_skill_extraction(custom)
