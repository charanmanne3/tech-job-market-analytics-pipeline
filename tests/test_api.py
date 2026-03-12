"""Tests for the FastAPI dashboard API."""

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from dashboard.api.main import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def _mock_data(monkeypatch):
    """Provide a small synthetic DataFrame so tests don't need real data files."""
    sample = pd.DataFrame(
        {
            "job_id": ["a1", "a2", "a3"],
            "title": ["Data Engineer", "Backend Dev", "ML Engineer"],
            "company": ["Acme", "Globex", "Acme"],
            "location": ["Remote", "New York", "Remote"],
            "salary_min": [90000, 110000, None],
            "salary_max": [130000, 150000, None],
            "posted_date": ["2025-03-01", "2025-03-02", "2025-03-03"],
            "skills": ["python, sql, docker", "python, java", "python, pytorch, docker"],
            "skill_count": [3, 2, 3],
            "is_remote": [True, False, True],
            "source": ["remotive", "remotive", "remoteok"],
        }
    )
    import dashboard.api.main as api_mod

    monkeypatch.setattr(api_mod, "_cached_df", sample)


def test_filters_endpoint():
    resp = client.get("/api/filters")
    assert resp.status_code == 200
    body = resp.json()
    assert "locations" in body
    assert "Remote" in body["locations"]


def test_dashboard_returns_all_sections():
    resp = client.get("/api/dashboard")
    assert resp.status_code == 200
    body = resp.json()
    for key in ("metrics", "top_locations", "top_companies", "timeline", "skills", "salary", "work_mode"):
        assert key in body, f"Missing section: {key}"


def test_dashboard_metrics():
    body = client.get("/api/dashboard").json()
    m = body["metrics"]
    assert m["total_jobs"] == 3
    assert m["companies"] == 2
    assert m["locations"] == 2


def test_dashboard_skills_rankings():
    body = client.get("/api/dashboard").json()
    skills = body["skills"]
    assert skills["rankings"][0]["skill"] == "Python"
    assert skills["unique_skills"] > 0
    assert skills["total_mentions"] > 0


def test_dashboard_salary():
    body = client.get("/api/dashboard").json()
    sal = body["salary"]
    assert sal["metrics"]["count"] == 2
    assert sal["metrics"]["median_min"] > 0


def test_dashboard_work_mode():
    body = client.get("/api/dashboard").json()
    wm = body["work_mode"]
    modes = {s["mode"] for s in wm["split"]}
    assert "Remote" in modes


def test_dashboard_location_filter():
    resp = client.get("/api/dashboard?locations=Remote")
    body = resp.json()
    assert body["metrics"]["total_jobs"] == 2


def test_dashboard_date_filter():
    resp = client.get("/api/dashboard?date_from=2025-03-02&date_to=2025-03-03")
    body = resp.json()
    assert body["metrics"]["total_jobs"] == 2


def test_reload_endpoint():
    resp = client.post("/api/reload")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_airflow_overview_endpoint(monkeypatch):
    import dashboard.api.main as api_mod

    monkeypatch.setenv("AIRFLOW_API_BASE_URL", "http://airflow.test/api/v1")

    def fake_airflow_request(path, params=None):
        if path == "/dags/job_market_pipeline":
            return {
                "dag_id": "job_market_pipeline",
                "is_paused": False,
                "is_active": True,
                "tags": [{"name": "etl"}],
            }
        if path == "/dags/job_market_pipeline/dagRuns":
            return {
                "dag_runs": [
                    {
                        "dag_run_id": "manual__2025-03-01T00:00:00+00:00",
                        "state": "success",
                        "run_type": "manual",
                        "logical_date": "2025-03-01T00:00:00+00:00",
                        "start_date": "2025-03-01T00:00:00+00:00",
                        "end_date": "2025-03-01T00:01:00+00:00",
                    }
                ]
            }
        if path == "/dags/job_market_pipeline/tasks":
            return {"tasks": [{"task_id": "extract_jobs"}, {"task_id": "transform_jobs"}]}
        return {}

    monkeypatch.setattr(api_mod, "_airflow_request", fake_airflow_request)
    resp = client.get("/api/airflow/overview")
    assert resp.status_code == 200
    body = resp.json()
    assert body["reachable"] is True
    assert body["dag"]["dag_id"] == "job_market_pipeline"
    assert body["task_count"] == 2
    assert body["run_summary"]["success"] == 1


def test_airflow_health_unreachable(monkeypatch):
    import dashboard.api.main as api_mod

    monkeypatch.setenv("AIRFLOW_API_BASE_URL", "http://airflow.test/api/v1")
    monkeypatch.setattr(api_mod, "_airflow_request", lambda *args, **kwargs: {"_error": "connection refused"})
    resp = client.get("/api/airflow/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["reachable"] is False
    assert "error" in body
