import logging
import sys
from logging.handlers import RotatingFileHandler
from src.config import LOGS_DIR, LOG_LEVEL_CONSOLE, LOG_LEVEL_FILE


def setup_logging(log_filename: str = "etl_process.log"):
    """Cấu hình logging ra cả console và file xoay vòng (rotating file)."""
    root = logging.getLogger()
    if root.handlers:
        return root

    root.setLevel(logging.DEBUG)

    # 1. Ghi ra màn hình Console (ngắn gọn)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, LOG_LEVEL_CONSOLE, logging.INFO))
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S")
    )
    root.addHandler(console_handler)

    # 2. Ghi ra File (chi tiết từng file, dòng code)
    file_handler = RotatingFileHandler(
        LOGS_DIR / log_filename, maxBytes=10_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setLevel(getattr(logging, LOG_LEVEL_FILE, logging.DEBUG))
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s.%(msecs)03d | %(levelname)-8s | %(name)s | %(module)s:%(lineno)d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(file_handler)

    return root


def get_logger(name: str = __name__):
    """Lấy logger cho từng module, tự động cấu hình nếu chưa có."""
    setup_logging()
    return logging.getLogger(name)