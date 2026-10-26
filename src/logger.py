import logging
import sys
from logging.handlers import RotatingFileHandler
from src.config.config import LOGS_DIR, LOG_LEVEL_CONSOLE, LOG_LEVEL_FILE


def setup_logging(log_filename = "extract_load.log"):
    root = logging.getLogger()
    if root.handlers:
        return root

    root.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, LOG_LEVEL_CONSOLE, logging.INFO))
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-7s | %(module)-9s:%(lineno)-4d | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    root.addHandler(console_handler)

    file_handler = RotatingFileHandler(
        LOGS_DIR / log_filename, maxBytes=10_000_000, backupCount=5, encoding="utf-8"
    )
    file_handler.setLevel(getattr(logging, LOG_LEVEL_FILE, logging.DEBUG))
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s.%(msecs)03d | %(levelname)-7s | %(name)-14s | %(module)-9s:%(lineno)-4d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(file_handler)

    return root


def get_logger(name = __name__):
    setup_logging()
    return logging.getLogger(name)