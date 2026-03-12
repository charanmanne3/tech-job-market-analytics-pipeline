"""Tests for config module."""

from config.config import (
    BASE_DIR,
    DATABASE_URL,
    PROCESSED_DIR,
    RAW_DIR,
    TECH_SKILLS,
)


def test_base_dir_is_project_root():
    assert BASE_DIR.exists()
    assert (BASE_DIR / "config").is_dir()


def test_data_dirs_exist():
    assert RAW_DIR.parent.exists()
    assert PROCESSED_DIR.parent.exists()


def test_database_url_format():
    assert DATABASE_URL.startswith("postgresql+psycopg2://")


def test_tech_skills_not_empty():
    assert len(TECH_SKILLS) > 50


def test_tech_skills_are_lowercase():
    for skill in TECH_SKILLS:
        assert skill == skill.lower(), f"{skill} should be lowercase"


def test_no_duplicate_skills():
    assert len(TECH_SKILLS) == len(set(TECH_SKILLS))
