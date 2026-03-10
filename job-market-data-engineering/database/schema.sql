-- ============================================================================
-- Tech Job Market Analytics — PostgreSQL Schema
-- ============================================================================
-- Idempotent: safe to run on every pipeline execution.
-- ============================================================================

CREATE TABLE IF NOT EXISTS jobs (
    job_id       VARCHAR(255) PRIMARY KEY,
    title        VARCHAR(500) NOT NULL,
    company      VARCHAR(500),
    location     VARCHAR(500),
    is_remote    BOOLEAN DEFAULT FALSE,
    salary_raw   TEXT,
    salary_min   NUMERIC(12, 2),
    salary_max   NUMERIC(12, 2),
    job_type     VARCHAR(100),
    category     VARCHAR(200),
    description  TEXT,
    posted_date  DATE,
    url          TEXT,
    source       VARCHAR(50),
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS skills (
    skill_id   SERIAL PRIMARY KEY,
    skill_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS job_skills (
    job_id   VARCHAR(255) REFERENCES jobs(job_id) ON DELETE CASCADE,
    skill_id INTEGER      REFERENCES skills(skill_id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, skill_id)
);

-- Analytical indexes
CREATE INDEX IF NOT EXISTS idx_jobs_location    ON jobs (location);
CREATE INDEX IF NOT EXISTS idx_jobs_company     ON jobs (company);
CREATE INDEX IF NOT EXISTS idx_jobs_posted_date ON jobs (posted_date);
CREATE INDEX IF NOT EXISTS idx_jobs_source      ON jobs (source);
CREATE INDEX IF NOT EXISTS idx_jobs_is_remote   ON jobs (is_remote);
CREATE INDEX IF NOT EXISTS idx_skills_name      ON skills (skill_name);

-- Auto-update trigger for updated_at
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_jobs_updated ON jobs;
CREATE TRIGGER trg_jobs_updated
    BEFORE UPDATE ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();
