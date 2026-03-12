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
