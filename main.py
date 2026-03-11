"""
Main entry point — Tech Job Market Analytics Pipeline

Runs the full ETL pipeline:
  1. Extract  — fetch job postings from Remotive & RemoteOK
  2. Transform — clean, normalise, extract skills
  3. Load     — upsert into PostgreSQL
"""

import sys

from data_ingestion.fetch_jobs import run_ingestion
from transformations.clean_jobs import run_cleaning
from database.load_data import run_load
from utils.logger import get_logger

logger = get_logger("pipeline.main")


def main() -> None:
    logger.info("=" * 60)
    logger.info("TECH JOB MARKET ANALYTICS PIPELINE")
    logger.info("=" * 60)

    logger.info("[1/3] Extracting job postings …")
    raw_df = run_ingestion()
    if raw_df.empty:
        logger.error("Extraction returned no data — aborting pipeline")
        sys.exit(1)
    logger.info("[1/3] Extraction complete: %d records", len(raw_df))

    logger.info("[2/3] Transforming data …")
    clean_df = run_cleaning()
    if clean_df.empty:
        logger.error("Transformation returned no data — aborting pipeline")
        sys.exit(1)
    logger.info("[2/3] Transformation complete: %d records", len(clean_df))

    logger.info("[3/3] Loading into PostgreSQL …")
    run_load()
    logger.info("[3/3] Load complete")

    logger.info("=" * 60)
    logger.info("PIPELINE FINISHED SUCCESSFULLY")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
