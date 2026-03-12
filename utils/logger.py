"""
Centralized logging utility.

Every module in the pipeline should obtain its logger via:

    from utils.logger import get_logger
    logger = get_logger(__name__)

This guarantees a consistent format, a single log-level knob, and
file + console output across ingestion, transformation, and loading.
"""

import logging
import os
import sys
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = "%(asctime)s | %(name)-24s | %(levelname)-8s | %(message)s"
LOG_DATE_FMT = "%Y-%m-%d %H:%M:%S"

_CONFIGURED = False


def _in_airflow_task_context() -> bool:
    """Detect execution inside an Airflow task runtime."""
    return any(
        os.getenv(k)
        for k in (
            "AIRFLOW_CTX_DAG_ID",
            "AIRFLOW_CTX_TASK_ID",
            "AIRFLOW_CTX_RUN_ID",
        )
    )


def _configure_root() -> None:
    """One-time setup of the root logger (console + rotating file)."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    root = logging.getLogger()
    root.setLevel(LOG_LEVEL)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FMT)

    # Airflow task runtime wraps stdout/stderr and forwards logs internally.
    # Attaching another stdout handler here can create recursive log emission.
    if not _in_airflow_task_context():
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(formatter)
        root.addHandler(console)

    file_handler = logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a named logger with the project-wide configuration."""
    _configure_root()
    return logging.getLogger(name)
