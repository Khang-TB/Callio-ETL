"""Logging helpers for the Callio ETL pipeline."""
from __future__ import annotations

import logging


def configure_logging(level: str) -> logging.Logger:
    """Configure and return the root logger used across the application."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S%z",
        force=True,
    )
    return logging.getLogger("callio_etl")
