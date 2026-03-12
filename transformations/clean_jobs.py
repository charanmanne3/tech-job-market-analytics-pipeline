"""
Data Cleaning — Tech Job Market Pipeline (PySpark version).
"""

import html
import re
import shutil
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    countDistinct,
    greatest,
    initcap,
    length,
    lit,
    lower,
    regexp_extract,
    regexp_replace,
    split,
    to_date,
    trim,
    udf,
    when,
)
from pyspark.sql.types import IntegerType, StringType

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.config import CLEAN_CSV_PATH, PROCESSED_PARQUET_PATH, RAW_CSV_PATH, RAW_PARQUET_PATH, TECH_SKILLS
from utils.logger import get_logger

logger = get_logger("transform.clean_jobs")

spark: SparkSession | None = None


def get_spark() -> SparkSession:
    """
    Lazily create SparkSession on the driver only.
    Avoid creating SparkContext at module import time because this module is also
    imported inside Spark Python workers for UDF execution.
    """
    global spark
    if spark is None:
        spark = SparkSession.builder.appName("TechJobPipeline").getOrCreate()
    return spark


def _ensure_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    for name in columns:
        if name not in df.columns:
            df = df.withColumn(name, lit(""))
    return df


def load_raw_data() -> DataFrame:
    """Load raw data — prefer Parquet, fall back to CSV."""
    current_spark = get_spark()
    if RAW_PARQUET_PATH.exists():
        logger.info("Loading raw Parquet from %s", RAW_PARQUET_PATH)
        df = current_spark.read.parquet(str(RAW_PARQUET_PATH))
    elif RAW_CSV_PATH.exists():
        logger.info("Parquet not found; loading CSV from %s", RAW_CSV_PATH)
        df = current_spark.read.csv(str(RAW_CSV_PATH), header=True)
    else:
        logger.error("No raw data found in %s", RAW_PARQUET_PATH.parent)
        return current_spark.createDataFrame([], schema="job_id string")

    df = _ensure_columns(
        df,
        ["job_id", "description", "location", "job_type", "posted_date", "salary", "tags", "company", "title"],
    )
    logger.info("Loaded %d rows × %d cols", df.count(), len(df.columns))
    return df


def remove_duplicates(df: DataFrame) -> DataFrame:
    before = df.count()
    df = df.dropDuplicates(["job_id"])
    after = df.count()
    logger.info("Deduplication: %d → %d  (dropped %d)", before, after, before - after)
    return df


_HTML_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")


def _strip_html(text: str) -> str:
    if not text:
        return ""
    return _WS.sub(" ", html.unescape(_HTML_TAG.sub(" ", text))).strip()


strip_html_udf = udf(_strip_html, StringType())


def clean_descriptions(df: DataFrame) -> DataFrame:
    df = df.withColumn("description", strip_html_udf(coalesce(col("description"), lit(""))))
    avg_len = df.selectExpr("avg(length(description)) as avg_len").collect()[0]["avg_len"] or 0
    logger.info("Stripped HTML  (avg len: %.0f chars)", avg_len)
    return df


def add_work_mode(df: DataFrame) -> DataFrame:
    loc_lower = lower(coalesce(col("location"), lit("")))
    df = df.withColumn(
        "work_mode",
        when(
            loc_lower.contains("remote") | loc_lower.contains("worldwide") | loc_lower.contains("anywhere"),
            lit("Remote"),
        ).otherwise(lit("Onsite")),
    )
    counts = {r["work_mode"]: r["count"] for r in df.groupBy("work_mode").count().collect()}
    logger.info("Work-mode classification: %s", counts)
    return df


def normalise_locations(df: DataFrame) -> DataFrame:
    base_loc = trim(split(coalesce(col("location"), lit("")), r"[|;]").getItem(0))
    loc_lower = lower(base_loc)

    df = df.withColumn("location", base_loc)
    df = df.withColumn(
        "location",
        when((col("location") == "") | col("location").isNull(), lit("Remote"))
        .when(loc_lower == "sf", lit("San Francisco"))
        .when(loc_lower == "nyc", lit("New York"))
        .when(loc_lower == "ny", lit("New York"))
        .when(loc_lower == "la", lit("Los Angeles"))
        .when(loc_lower == "dc", lit("Washington D.C."))
        .when(loc_lower.isin("worldwide", "anywhere", "global"), lit("Remote"))
        .otherwise(col("location")),
    )

    remote_prefix = regexp_extract(col("location"), r"(?i)^remote\s*[-–—]\s*(.+)$", 1)
    remote_paren = regexp_extract(col("location"), r"(?i)^(.+?)\s*\(\s*remote\s*\)$", 1)
    remote_suffix = regexp_extract(col("location"), r"(?i)^(.+?)\s*[-–—]\s*remote.*$", 1)
    remote_slash = regexp_extract(col("location"), r"(?i)^(.+?)\s*/\s*remote$", 1)

    df = df.withColumn(
        "location",
        when(length(remote_prefix) > 0, initcap(remote_prefix))
        .when(length(remote_paren) > 0, initcap(remote_paren))
        .when(length(remote_suffix) > 0, initcap(remote_suffix))
        .when(length(remote_slash) > 0, initcap(remote_slash))
        .otherwise(col("location")),
    )

    df = df.withColumn("location", regexp_replace(col("location"), r",\s*[A-Za-z]{2}$", ""))
    df = df.withColumn("location", regexp_replace(col("location"), r"(?i),?\s*(USA|US|UK|CA|DE|FR|AU|IN)\s*$", ""))
    df = df.withColumn("location", trim(col("location")))
    df = df.withColumn("location", when(col("location") == "", lit("Remote")).otherwise(initcap(col("location"))))

    job_type_lower = lower(coalesce(col("job_type"), lit("")))
    df = df.withColumn(
        "is_remote",
        lower(col("location")).contains("remote") | job_type_lower.contains("remote"),
    )

    total = df.count()
    remote_count = df.filter(col("is_remote")).count()
    unique_locations = df.select(countDistinct("location").alias("n")).collect()[0]["n"]
    remote_pct = round((remote_count / max(total, 1)) * 100)
    logger.info("Locations: %d unique, %.0f%% remote", unique_locations, remote_pct)
    return df


def parse_dates(df: DataFrame) -> DataFrame:
    parsed = to_date(col("posted_date"))
    df = df.withColumn("posted_date", parsed)
    bad = df.filter(col("posted_date").isNull()).count()
    if bad:
        logger.warning("%d rows with unparseable dates", bad)
    min_max = df.selectExpr("min(posted_date) as min_d", "max(posted_date) as max_d").collect()[0]
    logger.info("Dates: %s → %s", min_max["min_d"], min_max["max_d"])
    return df.withColumn("posted_date", col("posted_date").cast("string"))


def parse_salaries(df: DataFrame) -> DataFrame:
    cleaned_salary = regexp_replace(coalesce(col("salary"), lit("")), ",", "")
    n1_raw = regexp_extract(cleaned_salary, r"(\d+\.?\d*)", 1)
    n2_raw = regexp_extract(cleaned_salary, r"\d+\.?\d*\D+(\d+\.?\d*)", 1)
    n1 = when(length(n1_raw) > 0, n1_raw.cast("double")).otherwise(lit(None))
    n2 = when(length(n2_raw) > 0, n2_raw.cast("double")).otherwise(lit(None))

    df = df.withColumn("n1", n1).withColumn("n2", n2)
    hi_raw = greatest(coalesce(col("n1"), lit(0.0)), coalesce(col("n2"), lit(0.0)))

    factor = when(hi_raw < 200, lit(2080.0)).when(hi_raw < 10000, lit(12.0)).otherwise(lit(1.0))
    low_num = when(col("n1").isNull() & col("n2").isNull(), lit(None)).otherwise(
        when(col("n2").isNull(), col("n1")).otherwise(when(col("n1") <= col("n2"), col("n1")).otherwise(col("n2")))
    )
    high_num = when(col("n2").isNull(), lit(None)).otherwise(
        when(col("n1") >= col("n2"), col("n1")).otherwise(col("n2"))
    )

    df = df.withColumn("salary_min", low_num * factor)
    df = df.withColumn("salary_max", high_num * factor)
    df = df.drop("n1", "n2")

    n = df.filter(col("salary_min").isNotNull()).count()
    total = df.count()
    logger.info("Salaries: %d of %d have numeric values (%.0f%%)", n, total, (n / max(total, 1)) * 100)
    return df


def normalise_job_types(df: DataFrame) -> DataFrame:
    jt = lower(trim(coalesce(col("job_type"), lit(""))))
    df = df.withColumn(
        "job_type",
        when(jt.isin("full_time", "full time", "fulltime", "remote"), lit("Full-time"))
        .when(jt.isin("part_time", "part time", "parttime"), lit("Part-time"))
        .when(jt == "contract", lit("Contract"))
        .when(jt == "freelance", lit("Freelance"))
        .when(jt == "internship", lit("Internship"))
        .when(jt == "", lit("Unknown"))
        .otherwise(lit("Other")),
    )
    counts = {r["job_type"]: r["count"] for r in df.groupBy("job_type").count().collect()}
    logger.info("Job types: %s", counts)
    return df


_SKILL_PATTERNS = [(s, re.compile(rf"\b{re.escape(s)}\b", re.IGNORECASE)) for s in TECH_SKILLS]


def _extract_skills_from_text(text: str) -> str:
    if not text:
        return ""
    found = sorted({name for name, pat in _SKILL_PATTERNS if pat.search(text)})
    return ", ".join(found)


extract_skills_udf = udf(_extract_skills_from_text, StringType())
skill_count_udf = udf(lambda s: len(s.split(", ")) if s else 0, IntegerType())


def extract_skills(df: DataFrame) -> DataFrame:
    combined = concat(coalesce(col("description"), lit("")), lit(" "), coalesce(col("tags"), lit("")))
    df = df.withColumn("skills", extract_skills_udf(combined))
    df = df.withColumn("skill_count", skill_count_udf(col("skills")))
    return df


def _prepare_output_path(path: Path) -> None:
    if path.exists() and path.is_file():
        path.unlink()
    if path.exists() and path.is_dir():
        shutil.rmtree(path)
    path.parent.mkdir(parents=True, exist_ok=True)


def run_cleaning() -> DataFrame:
    """Full clean + skill-extract pipeline. Writes Parquet + CSV with Spark."""
    logger.info("=" * 60)
    logger.info("STARTING DATA CLEANING")
    logger.info("=" * 60)

    df = load_raw_data()
    if df.count() == 0:
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
    df = df.select(*(present + extra))

    _prepare_output_path(PROCESSED_PARQUET_PATH)
    _prepare_output_path(CLEAN_CSV_PATH)
    df.write.mode("overwrite").parquet(str(PROCESSED_PARQUET_PATH))
    logger.info("Saved Parquet → %s", PROCESSED_PARQUET_PATH)

    df.write.mode("overwrite").csv(str(CLEAN_CSV_PATH), header=True)
    logger.info("Saved CSV → %s", CLEAN_CSV_PATH)

    total = df.count()
    salary_rows = df.filter(col("salary_min").isNotNull()).count()
    skilled_rows = df.filter(col("skill_count") > 0).count()
    unique_locations = df.select(countDistinct("location").alias("n")).collect()[0]["n"]
    unique_companies = df.select(countDistinct("company").alias("n")).collect()[0]["n"]

    logger.info("-" * 50)
    logger.info("CLEANING SUMMARY")
    logger.info("  Records  : %d", total)
    logger.info("  Salary   : %d rows", salary_rows)
    logger.info("  Skills   : %d rows", skilled_rows)
    logger.info("  Locations: %d unique", unique_locations)
    logger.info("  Companies: %d unique", unique_companies)
    logger.info("-" * 50)
    return df


if __name__ == "__main__":
    run_cleaning()
