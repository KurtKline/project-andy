from __future__ import annotations  # otherwise type hint loguru.Logger would throw runtime error

from pathlib import Path
from sys import stdout

import loguru
from loguru import logger


def get_logger(log_path: Path) -> loguru.Logger:
    """
    Returns loguru logger with stdout output and log file output.

    stdout is to show logs in the terminal when running the script
    """
    logger.remove()
    logger.add(stdout)
    logger.add(f"{log_path}")

    return logger
