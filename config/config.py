"""
Centralized configuration for the Tech Job Market Analytics Pipeline.

All credentials are loaded from environment variables with safe defaults
for local development.  No secrets are hard-coded.

Every module imports from this single file:
    from config.config import CLEAN_CSV_PATH, DATABASE_URL, ...
"""

import os
from pathlib import Path

# ── Project paths ────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
ANALYTICS_DIR = DATA_DIR / "analytics"
LOG_DIR = BASE_DIR / "logs"

for _d in (RAW_DIR, PROCESSED_DIR, ANALYTICS_DIR, LOG_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ── File paths ───────────────────────────────────────────────────────────────
RAW_JSON_PATH = RAW_DIR / "raw_jobs.json"
RAW_PARQUET_PATH = RAW_DIR / "raw_jobs.parquet"
RAW_CSV_PATH = RAW_DIR / "raw_jobs.csv"
PROCESSED_PARQUET_PATH = PROCESSED_DIR / "clean_jobs.parquet"
CLEAN_CSV_PATH = PROCESSED_DIR / "clean_jobs.csv"
ANALYTICS_PARQUET_PATH = ANALYTICS_DIR / "skill_demand.parquet"

# Convenience aliases used across the project
RAW_DATA_PATH = RAW_JSON_PATH
CLEAN_DATA_PATH = CLEAN_CSV_PATH

# ── Database ─────────────────────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "job_market")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ── API endpoints ────────────────────────────────────────────────────────────
API_URL = "https://remotive.io/api/remote-jobs"
REMOTIVE_API_URL = API_URL
REMOTEOK_API_URL = "https://remoteok.com/api"

# ── Skill extraction ────────────────────────────────────────────────────────
TECH_SKILLS = [
    "python", "sql", "spark", "aws", "airflow", "kafka", "docker",
    "kubernetes", "snowflake", "java", "scala", "go", "rust", "c++",
    "javascript", "typescript", "react", "node.js", "angular", "vue",
    "terraform", "gcp", "azure", "bigquery", "redshift", "dbt",
    "pandas", "numpy", "scikit-learn", "tensorflow", "pytorch",
    "hadoop", "hive", "presto", "tableau", "power bi", "looker",
    "git", "ci/cd", "jenkins", "github actions", "linux", "bash",
    "mongodb", "redis", "elasticsearch", "cassandra", "dynamodb",
    "postgresql", "mysql", "graphql", "rest", "grpc", "fastapi",
    "flask", "django", "spring", "microservices", "data modeling",
    "etl", "elt", "data warehouse", "data lake", "machine learning",
    "deep learning", "nlp", "computer vision", "mlops",
]
