from __future__ import annotations  # otherwise type hint loguru.Logger would throw runtime error
import copy
from pathlib import Path
from sys import stdout

from loguru import logger
import loguru


def get_logger(log_path: Path) -> loguru.Logger:
    """
    Returns loguru logger with stdout output and log file output.

    copy.deepcopy is used because if you have multiple .py
    files using loguru to output to .log files, it prevents logs
    from one logger appearing in the other's .log file

    stdout is to show logs in the terminal when running the script
    """
    logger.remove()
    logger_ = copy.deepcopy(logger)
    logger_.add(stdout)
    logger_.add(log_path)

    return logger_
