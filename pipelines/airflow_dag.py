"""
Airflow DAG — job_market_pipeline

Three-step daily ETL:
  extract_jobs   → fetch from APIs, write raw Parquet + JSON
  transform_jobs → clean, normalise, extract skills, write processed Parquet
  load_jobs      → upsert into PostgreSQL

Schedule: daily at 06:00 UTC
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="job_market_pipeline",
    default_args=default_args,
    description="Daily ETL for tech job market analytics",
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["data-engineering", "job-market"],
)


def _extract(**ctx):
    from data_ingestion.fetch_jobs import run_ingestion

    df = run_ingestion()
    ctx["ti"].xcom_push(key="raw_count", value=len(df))


def _transform(**ctx):
    from transformations.clean_jobs import run_cleaning

    df = run_cleaning()
    ctx["ti"].xcom_push(key="clean_count", value=len(df))


def _load(**ctx):
    from database.load_data import run_load

    run_load()


extract_task = PythonOperator(task_id="extract_jobs", python_callable=_extract, dag=dag)
transform_task = PythonOperator(task_id="transform_jobs", python_callable=_transform, dag=dag)
load_task = PythonOperator(task_id="load_jobs", python_callable=_load, dag=dag)

extract_task >> transform_task >> load_task
