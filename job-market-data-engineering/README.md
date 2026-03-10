# Tech Job Market Analytics Pipeline

A production-style data engineering project that collects tech job postings from public APIs, processes them through a multi-layer data lake, stores results in PostgreSQL, orchestrates workflows with Apache Airflow, and visualises insights on a Streamlit dashboard.

---

## Architecture

```
                    ┌──────────────┐   ┌──────────────┐
                    │ Remotive API │   │ RemoteOK API │
                    └──────┬───────┘   └──────┬───────┘
                           │                  │
                           ▼                  ▼
                    ┌─────────────────────────────────┐
                    │     DATA INGESTION              │
                    │  fetch_jobs.py                   │
                    └──────────────┬──────────────────┘
                                   │
                    ┌──────────────▼──────────────────┐
                    │     DATA LAKE — RAW LAYER       │
                    │  data/raw/raw_jobs.json          │
                    │  data/raw/raw_jobs.parquet       │
                    └──────────────┬──────────────────┘
                                   │
                    ┌──────────────▼──────────────────┐
                    │     TRANSFORMATION               │
                    │  clean_jobs.py                    │
                    │  skill_extraction.py              │
                    └──────────────┬──────────────────┘
                                   │
                    ┌──────────────▼──────────────────┐
                    │     DATA LAKE — PROCESSED LAYER  │
                    │  data/processed/clean_jobs.parquet│
                    └──────────────┬──────────────────┘
                                   │
                  ┌────────────────┼────────────────┐
                  ▼                                 ▼
        ┌─────────────────┐              ┌─────────────────┐
        │   PostgreSQL     │              │   Streamlit      │
        │  (jobs, skills,  │              │   Dashboard      │
        │   job_skills)    │              │   :8501           │
        └─────────────────┘              └─────────────────┘
                  ▲
                  │
        ┌─────────────────┐
        │  Apache Airflow  │
        │  (daily DAG)     │
        │  :8080           │
        └─────────────────┘
```

---

## Project Structure

```
job-market-data-engineering/
├── config/
│   └── config.py                  # Paths, DB creds (env vars), API URLs
├── utils/
│   └── logger.py                  # Centralized logging with file + console
├── data/
│   ├── raw/                       # Raw JSON + Parquet from APIs
│   ├── processed/                 # Cleaned Parquet + CSV
│   └── analytics/                 # Aggregated datasets
├── data_ingestion/
│   └── fetch_jobs.py              # API fetchers with retry logic
├── transformations/
│   ├── clean_jobs.py              # Full cleaning pipeline
│   └── skill_extraction.py        # Regex keyword extraction (55+ skills)
├── database/
│   ├── schema.sql                 # PostgreSQL DDL (3 tables)
│   └── load_data.py               # SQLAlchemy upsert loader
├── pipelines/
│   └── airflow_dag.py             # Daily Airflow DAG (extract→transform→load)
├── dashboard/
│   └── streamlit_app.py           # Interactive analytics dashboard
├── docker/
│   ├── docker-compose.yml         # Postgres + Airflow + Streamlit
│   ├── Dockerfile.airflow
│   └── Dockerfile.app
├── logs/                          # Runtime logs (auto-created)
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## Tech Stack

| Layer          | Technology             |
|----------------|------------------------|
| Language       | Python 3.11            |
| Processing     | Pandas + PyArrow       |
| Storage Format | Parquet (data lake)    |
| Database       | PostgreSQL 16          |
| Orchestration  | Apache Airflow 2.9     |
| Dashboard      | Streamlit + Plotly     |
| Containers     | Docker & Compose       |

---

## Data Lake Design

```
data/raw/        → Raw API payloads (JSON + Parquet)
data/processed/  → Cleaned, normalised, skill-tagged (Parquet + CSV)
data/analytics/  → Aggregated datasets for dashboards
```

All processed data is stored in **Parquet** format for columnar efficiency, type preservation, and compression.  CSV copies are kept for backward-compatibility.

---

## Setup & Usage

### Prerequisites

- Docker & Docker Compose, **or**
- Python 3.11+ for local development

### Option 1 — Docker (recommended)

```bash
cd docker
docker-compose up --build -d
```

| Service    | URL                          | Credentials   |
|------------|------------------------------|---------------|
| Airflow    | http://localhost:8080         | admin / admin |
| Streamlit  | http://localhost:8501         | —             |
| PostgreSQL | localhost:5432               | postgres / postgres |

### Option 2 — Local

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1. Ingest
python data_ingestion/fetch_jobs.py

# 2. Transform
python transformations/clean_jobs.py

# 3. Load (requires running Postgres)
python database/load_data.py

# 4. Dashboard
streamlit run dashboard/streamlit_app.py
```

---

## Pipeline Steps

| Step      | Script                            | Output                          |
|-----------|-----------------------------------|---------------------------------|
| Extract   | `data_ingestion/fetch_jobs.py`    | `data/raw/raw_jobs.parquet`     |
| Transform | `transformations/clean_jobs.py`   | `data/processed/clean_jobs.parquet` |
| Load      | `database/load_data.py`           | PostgreSQL `jobs`, `skills`, `job_skills` |
| Visualise | `dashboard/streamlit_app.py`      | Interactive Streamlit dashboard |

---

## Dashboard

Five sections in a single scrollable page:

1. **Metric Cards** — Total Jobs, Companies, Locations, Avg Skills, Salary Count
2. **Jobs by Location** — Top locations + top companies bar charts + timeline
3. **Top Skills** — Ranked bar chart, co-occurrence pairs, stats table
4. **Salary Distribution** — Histogram overlay, dumbbell ranges, scatter plot
5. **Remote vs Onsite** — Donut chart, breakdown cards, remote locations

---

## Future Improvements

- **Kafka streaming** — real-time ingestion pipeline
- **dbt** — SQL-based transformation layer with tests
- **Great Expectations** — data quality checks
- **CI/CD** — GitHub Actions for lint + test + deploy
- **Incremental loads** — only fetch new postings since last run
- **Additional sources** — LinkedIn, Indeed, Glassdoor
- **Alerting** — Slack/email on pipeline failures

---

## License

MIT
