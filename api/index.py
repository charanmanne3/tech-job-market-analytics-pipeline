"""Vercel serverless entrypoint — re-exports the FastAPI app."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dashboard.api.main import app  # noqa: F401
