"""
Database Loader — Tech Job Market Pipeline

Reads the processed Parquet and upserts into PostgreSQL using
SQLAlchemy Core with ON CONFLICT (upsert) logic for idempotency.

Usage:
    python database/load_data.py
"""

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    create_engine,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.config import CLEAN_CSV_PATH, DATABASE_URL, PROCESSED_PARQUET_PATH
from utils.logger import get_logger

logger = get_logger("database.load_data")

metadata = MetaData()

jobs_table = Table(
    "jobs",
    metadata,
    Column("job_id", String(255), primary_key=True),
    Column("title", String(500)),
    Column("company", String(500)),
    Column("location", String(500)),
    Column("is_remote", Boolean),
    Column("salary_raw", Text),
    Column("salary_min", Numeric(12, 2)),
    Column("salary_max", Numeric(12, 2)),
    Column("job_type", String(100)),
    Column("category", String(200)),
    Column("description", Text),
    Column("posted_date", Date),
    Column("url", Text),
    Column("source", String(50)),
)

skills_table = Table(
    "skills",
    metadata,
    Column("skill_id", Integer, primary_key=True, autoincrement=True),
    Column("skill_name", String(100), unique=True),
)

job_skills_table = Table(
    "job_skills",
    metadata,
    Column("job_id", String(255)),
    Column("skill_id", Integer),
)


def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def init_schema(engine) -> None:
    schema_sql = Path(__file__).resolve().parent / "schema.sql"
    with engine.begin() as conn:
        conn.execute(text(schema_sql.read_text()))
    logger.info("Schema initialised")


def upsert_jobs(engine, df: pd.DataFrame) -> int:
    records = df.to_dict(orient="records")
    if not records:
        return 0
    for rec in records:
        rec.setdefault("salary_raw", rec.pop("salary", ""))
        for col in ("salary_min", "salary_max"):
            val = rec.get(col)
            if val is None or val == "" or (isinstance(val, float) and pd.isna(val)):
                rec[col] = None
        val = rec.get("is_remote")
        rec["is_remote"] = bool(val) if val not in (None, "") else False

    stmt = pg_insert(jobs_table).values(records)
    upsert = stmt.on_conflict_do_update(
        index_elements=["job_id"],
        set_={c.name: stmt.excluded[c.name] for c in jobs_table.columns if c.name != "job_id"},
    )
    with engine.begin() as conn:
        conn.execute(upsert)
    logger.info("Upserted %d job records", len(records))
    return len(records)


def upsert_skills(engine, skill_names: list[str]) -> dict[str, int]:
    with engine.begin() as conn:
        for name in skill_names:
            stmt = pg_insert(skills_table).values(skill_name=name)
            conn.execute(stmt.on_conflict_do_nothing(index_elements=["skill_name"]))
        rows = conn.execute(skills_table.select()).fetchall()
    return {r.skill_name: r.skill_id for r in rows}


def link_job_skills(engine, df: pd.DataFrame, skill_map: dict[str, int]) -> None:
    links = []
    for _, row in df.iterrows():
        for s in str(row.get("skills", "")).split(","):
            s = s.strip().lower()
            if s in skill_map:
                links.append({"job_id": row["job_id"], "skill_id": skill_map[s]})
    if not links:
        return
    stmt = pg_insert(job_skills_table).values(links)
    with engine.begin() as conn:
        conn.execute(stmt.on_conflict_do_nothing(index_elements=["job_id", "skill_id"]))
    logger.info("Linked %d job-skill pairs", len(links))


def run_load() -> None:
    """Load processed data into PostgreSQL."""
    logger.info("=" * 60)
    logger.info("STARTING DATABASE LOAD")
    logger.info("=" * 60)

    if PROCESSED_PARQUET_PATH.exists():
        logger.info("Reading %s", PROCESSED_PARQUET_PATH)
        df = pd.read_parquet(PROCESSED_PARQUET_PATH)
    elif CLEAN_CSV_PATH.exists():
        logger.info("Parquet not found; reading %s", CLEAN_CSV_PATH)
        df = pd.read_csv(CLEAN_CSV_PATH, dtype=str).fillna("")
    else:
        logger.error("No processed data found — run transformations first")
        return

    engine = get_engine()
    init_schema(engine)

    job_cols = [c.name for c in jobs_table.columns]
    job_df = df[[c for c in job_cols if c in df.columns]].copy()
    upsert_jobs(engine, job_df)

    all_skills: set[str] = set()
    for s_str in df.get("skills", pd.Series(dtype=str)).fillna(""):
        for s in s_str.split(","):
            s = s.strip().lower()
            if s:
                all_skills.add(s)

    if all_skills:
        skill_map = upsert_skills(engine, sorted(all_skills))
        link_job_skills(engine, df, skill_map)

    logger.info("Database load complete — %d jobs", len(df))


if __name__ == "__main__":
    run_load()
