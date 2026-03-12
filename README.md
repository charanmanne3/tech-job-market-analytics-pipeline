# Tech Job Market Analytics Pipeline

An end-to-end **Data Engineering pipeline** that collects, processes, and analyzes tech job market data using **Apache Airflow, Docker, Python, and PostgreSQL**.

This project demonstrates how to build a production-style **ETL pipeline** that ingests job postings from public APIs, transforms the data, and loads it into a database for analytics.

---

## Project Architecture

API Sources
⬇
Airflow ETL Pipeline
⬇
Data Transformation
⬇
PostgreSQL Database
⬇
Analytics / Dashboard

---

## Tech Stack

* **Python**
* **Apache Airflow**
* **Docker**
* **PostgreSQL**
* **Pandas**
* **Requests**
* **Streamlit (for visualization)**

---

## Project Structure

```
tech-job-market-analytics-pipeline
│
├── config/
│   └── settings.py
│
├── data/
│   ├── raw/
│   └── processed/
│
├── data_ingestion/
│   └── fetch_jobs.py
│
├── transformations/
│   └── clean_jobs.py
│
├── database/
│   └── load_data.py
│
├── pipelines/
│   └── airflow_dag.py
│
├── dashboard/
│   └── streamlit_app.py
│
├── docker/
│   └── Dockerfile
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Pipeline Workflow

The Airflow DAG runs a **daily ETL pipeline** consisting of three steps:

### 1. Extract

Fetch job postings from public APIs such as:

* Remotive API
* RemoteOK API

Raw data is saved to:

```
data/raw/raw_jobs.json
data/raw/raw_jobs.parquet
```

---

### 2. Transform

The transformation step:

* Cleans raw data
* Standardizes schema
* Extracts important fields such as:

  * Job title
  * Company
  * Location
  * Skills
  * Salary

Processed data is saved to:

```
data/processed/clean_jobs.parquet
```

---

### 3. Load

The final step loads processed data into a **PostgreSQL database**.

This allows the data to be used for:

* analytics
* dashboards
* reporting

---

## Running the Project

### 1. Clone the repository

```
git clone https://github.com/charanmanne3/tech-job-market-analytics-pipeline.git
cd tech-job-market-analytics-pipeline
```

---

## Airflow API Configuration

The dashboard Airflow panel supports a configurable API base URL so you do not
need to rely on temporary tunnel links.

Set this environment variable for the frontend deployment:

```env
NEXT_PUBLIC_AIRFLOW_API_URL=https://your-airflow-server/api/v1
```

Notes:

* If `NEXT_PUBLIC_AIRFLOW_API_URL` is set, the dashboard attempts direct Airflow
  API calls (DAG metadata and DAG runs).
* If direct access fails (CORS/network), the app falls back to backend proxy
  endpoints.
* If Airflow remains unreachable, the UI shows a friendly message and enters a
  short cooldown to avoid repeated failing requests while continuing the 30s
  refresh cycle.

---

### Run dashboard locally

1. Create a local environment file in the project root:
   ```env
   NEXT_PUBLIC_AIRFLOW_API_URL=http://localhost:8080/api/v1
   ```
2. Start Airflow locally:
   ```bash
   airflow standalone
   ```
3. Start the dashboard frontend:
   ```bash
   cd dashboard/frontend
   npm run dev
   ```

---

### Vercel deploy checklist (Airflow-enabled)

1. Import repo in Vercel.
2. In **Project Settings → Environment Variables**, set:
   * `NEXT_PUBLIC_AIRFLOW_API_URL=https://your-airflow-server/api/v1`
   * `AIRFLOW_API_BASE_URL=https://your-airflow-server/api/v1` (backend proxy fallback)
   * `AIRFLOW_USERNAME=<airflow-user>`
   * `AIRFLOW_PASSWORD=<airflow-password>`
3. Redeploy.
4. Open `/api/airflow/health` on your deployed URL to verify connectivity.

If you skip these vars, the dashboard still works and Airflow panel stays optional.

---

### 2. Start the pipeline

```
docker compose up --build
```

---

### 3. Open Airflow

Airflow UI will be available at:

```
http://localhost:8080
```

Default credentials:

```
username: airflow
password: airflow
```

---

### 4. Run the DAG

Enable the DAG:

```
job_market_pipeline
```

Then trigger the pipeline.

---

## Dashboard (Optional)

Run the Streamlit dashboard to explore job data:

```
streamlit run dashboard/streamlit_app.py
```

This dashboard shows:

* Job counts
* Top hiring companies
* Most requested skills
* Market trends

---

## Example Use Cases

This dataset can be used to analyze:

* Most in-demand programming languages
* Remote vs on-site jobs
* Hiring trends by company
* Salary distributions
* Skills demand trends

---

## Future Improvements

Possible improvements include:

* Real-time streaming pipelines
* Kafka integration
* Data warehouse integration (Snowflake / BigQuery)
* Automated data quality checks
* CI/CD pipeline

---

## Author

**Sree Charan Sai Manne**

Master's in Computer Science
Aspiring Data Engineer

GitHub:
https://github.com/charanmanne3

LinkedIn:
[www.linkedin.com/in/manne-sree-charan-sai-777196217](http://www.linkedin.com/in/manne-sree-charan-sai-777196217)
