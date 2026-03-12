"""Tests for skill extraction logic."""

import pandas as pd

from transformations.skill_extraction import extract_skills, extract_skills_from_text


class TestExtractSkillsFromText:
    def test_finds_python(self):
        assert "python" in extract_skills_from_text("We need a Python developer")

    def test_finds_multiple_skills(self):
        text = "Must know Python, SQL, and Docker"
        skills = extract_skills_from_text(text)
        assert "python" in skills
        assert "sql" in skills
        assert "docker" in skills

    def test_empty_string_returns_empty(self):
        assert extract_skills_from_text("") == []

    def test_none_returns_empty(self):
        assert extract_skills_from_text(None) == []

    def test_no_skills_in_text(self):
        assert extract_skills_from_text("Looking for a nice person") == []

    def test_case_insensitive(self):
        assert "aws" in extract_skills_from_text("Experience with AWS required")
        assert "aws" in extract_skills_from_text("experience with aws required")

    def test_word_boundary(self):
        skills = extract_skills_from_text("We use Spark for processing")
        assert "spark" in skills

    def test_results_are_sorted(self):
        skills = extract_skills_from_text("docker kubernetes python aws")
        assert skills == sorted(skills)

    def test_results_are_deduplicated(self):
        skills = extract_skills_from_text("python Python PYTHON python")
        assert skills.count("python") == 1


class TestExtractSkillsDataFrame:
    def test_adds_skills_column(self):
        df = pd.DataFrame(
            {
                "description": ["Need Python and SQL experience"],
                "tags": ["python, backend"],
            }
        )
        result = extract_skills(df)
        assert "skills" in result.columns
        assert "skill_count" in result.columns

    def test_skill_count_is_correct(self):
        df = pd.DataFrame(
            {
                "description": ["Python, SQL, Docker expert needed"],
                "tags": [""],
            }
        )
        result = extract_skills(df)
        assert result.iloc[0]["skill_count"] >= 3

    def test_handles_missing_description(self):
        df = pd.DataFrame(
            {
                "description": [None, ""],
                "tags": ["python", None],
            }
        )
        result = extract_skills(df)
        assert len(result) == 2

    def test_empty_dataframe(self):
        df = pd.DataFrame({"description": pd.Series(dtype=str), "tags": pd.Series(dtype=str)})
        result = extract_skills(df)
        assert len(result) == 0
        assert "skills" in result.columns
